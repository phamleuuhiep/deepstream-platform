/*
 * Tuần 1 - Đa luồng RTSP -> metadata/sự kiện sang Redis + cảnh báo người ở yên quá lâu.
 *
 * Pipeline:
 *   nvurisrcbin x N (RTSP, NVDEC, smart record tuỳ chọn)
 *     -> nvstreammux -> nvinfer -> nvtracker --[probe]--> fakesink
 *
 * Probe ở cuối chuỗi (sau nvtracker và các SGIE nếu có):
 *   - Gửi metadata từng frame lên Redis Stream  <prefix>:frames  (có thể giãn bằng -f)
 *   - Theo dõi người (class person) đứng/ngồi yên: tâm bbox không lệch quá -m * chiều cao bbox
 *     so với điểm neo. Yên đủ -t giây -> sự kiện dwell_alert (+ snapshot JPEG, + clip smart record),
 *     khi người rời đi / mất track -> dwell_end. Sự kiện vào <prefix>:events.
 *   - Redis chạy ở thread riêng, hàng đợi có giới hạn: Redis chậm/chết thì bỏ bớt metadata frame
 *     (đếm lại), không chặn pipeline; sự kiện không bao giờ bị bỏ.
 *   - Watchdog camera: cam không có frame quá RECONNECT_SEC -> camera_offline + reset nguồn trong
 *     thread pool (song song). Reconnect có sẵn của nvurisrcbin chạy tuần tự ~3s/cam nên 32 cam
 *     rớt cùng lúc mất ~100s mới lên lại hết.
 *     Vừa nối mà decoder đã có dữ liệu thì chưa reset: NVDEC tự bỏ frame trước IDR đầu tiên (không ra hình
 *     vỡ), cam GOP dài / fps thấp có thể mất hàng chục giây mới có hình đầu (chờ tối đa FIRST_FRAME_MAX_SEC).
 *   - Người bị che quá TRACK_LOST_SEC rồi hiện lại đúng chỗ cũ trong MERGE_SEC: gộp vào lượt ở yên cũ,
 *     không đếm lại từ đầu (sự kiện dwell_resume nếu lượt đó đã cảnh báo).
 *   - Tự xoá ảnh/clip cũ hơn -k ngày hoặc khi thư mục vượt -K GB (xoá file cũ nhất trước).
 *   - Mỗi prefix Redis chỉ 1 bản app (khoá /tmp/w1-app-<prefix>.lock): 2 bản sẽ gửi trùng mọi thứ.
 *   - Nhận diện khuôn mặt (-F, cần SGIE tìm mặt gie 2 + embedding gie 3): so vector mặt với gallery trên Redis
 *     (web đăng ký), khớp -> face_recognized; mặt lạ đủ rõ -> <prefix>:faces_unknown (đăng ký từ camera).
 *     Hạng khách (VIP/thường) theo người đã đăng ký, hoặc classifier gie 4 nếu có -> vip_detected.
 *     Xem docs/PIPELINE_KHUON_MAT_VIP.md.
 *   - Tuần 2: mọi JSON có "schema_version"; -M <URL gốc>: sự kiện mang "<field>_url" cho ảnh/clip (tools/uploader.py
 *     upload lên S3/MinIO, đánh dấu <file>.up) và chỉ dọn file local đã upload; vùng ghế (ROI) từng camera
 *     ("roi" trong JSON thông tin camera): chỉ tính ở yên cho người đứng chân trong vùng; -g GPU (nhiều instance,
 *     mỗi GPU 1 bộ camera); thống kê mỗi -s giây vào <prefix>:stats:g<GPU> cho exporter Prometheus.
 *
 * Dùng: w1-app [tuỳ chọn] <uri> [uri ...]   (xem usage())
 *       w1-app [tuỳ chọn] -i cameras.txt    mỗi dòng "[công ty] <toà nhà> <cam_id> <uri> [fps [JSON]]": mọi JSON
 *                                           gửi Redis có thêm "company", "building", log in theo công ty/toà;
 *                                           fps > 0: chỉ xử lý chừng ấy frame/giây của cam đó;
 *                                           JSON thông tin camera (tên công ty/toà, tầng, ...): ghi vào hash
 *                                           <prefix>:camera_info, sự kiện camera_info, và "camera" của mọi sự kiện
 */
#include <fcntl.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <glib-unix.h>
#include <gst/gst.h>
#include <hiredis/hiredis.h>
#include <locale.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <unistd.h>

#include "gstnvdsinfer.h"
#include "gstnvdsmeta.h"
#include "gst-nvdssr.h"
#include "nvbufsurface.h"
#include "nvds_obj_encode.h"

#define MUXER_WIDTH 1920
#define MUXER_HEIGHT 1080
/* = 1/FPS lớn nhất của nguồn (30fps); 40000 sẽ chặn ở 25 batch/s với nguồn live */
#define MUXER_BATCH_TIMEOUT_USEC 33000
#define TRACKER_LIB "/opt/nvidia/deepstream/deepstream/lib/libnvds_nvmultiobjecttracker.so"
#define TRACKER_CFG "/opt/nvidia/deepstream/deepstream/samples/configs/deepstream-app/config_tracker_NvDCF_perf.yml"
#define PGIE_CFG "/workspace/configs/task13_pgie.txt"
#define ENGINE_FMT \
  "/opt/nvidia/deepstream/deepstream-7.0/samples/models/Primary_Detector/resnet18_trafficcamnet.etlt_b%u_gpu%u_int8.engine"
#define MAX_SGIE 4
#define MAX_SOURCES 64
#define REDIS_BATCH 256
#define TRACK_LOST_SEC 5.0      /* mất track quá lâu -> coi như người đã rời đi */
#define SR_POST_SEC 10          /* clip ghi thêm sau thời điểm cảnh báo */
#define RECONNECT_SEC 10        /* không có frame quá lâu -> reset nguồn */
#define SHUTDOWN_TIMEOUT_SEC 15 /* Ctrl+C mà không có EOS sau chừng này giây thì thoát cứng */
#define MERGE_SEC 15.0          /* track mất hình < chừng này giây rồi hiện lại đúng chỗ -> gộp lượt ở yên */
#define MERGE_MIN_STILL_SEC 10.0 /* chỉ chờ gộp track đã yên ít nhất chừng này (người đi ngang thì không) */
#define FIRST_FRAME_MAX_SEC 70  /* đã nối, decoder có dữ liệu nhưng chưa ra hình (chờ IDR, GOP dài): chờ tối đa chừng này rồi mới reset */
#define RETENTION_EVERY_SEC 600 /* chu kỳ dọn ảnh/clip */
#define LOCK_WAIT_SEC 60
#define SCHEMA_VERSION 2        /* tăng khi đổi ý nghĩa field; handoff/deepstream_platform_kit/contract/ */
#define ROI_MAX_PTS 32        /* chờ bản app cũ đang thoát nhả khoá chạy 1 bản */
#define RETENTION_MIN_AGE_SEC 600 /* không xoá file mới ghi (clip có thể đang ghi dở) */
/* gie-unique-id các tầng nhận diện, phải khớp configs/sgie*.txt */
#define FACE_DET_GIE 2          /* SGIE1 tìm mặt trong bbox người (configs/sgie1_face_detect.txt) */
#define FACE_EMB_GIE 3          /* SGIE2 embedding ArcFace, output-tensor-meta (configs/sgie2_face_embed.txt) */
#define VIP_GIE 4               /* SGIE3 classifier VIP/thường trên người (configs/sgie3_vip_classifier.txt) */
#define FACE_DIM 512
#define FACE_VOTES 2            /* khớp cùng 1 người chừng này lần trên 1 track mới báo (tránh khớp nhầm 1 frame) */
#define FACE_COOLDOWN_SEC 300   /* cùng người, cùng cam: không báo lại trong chừng này giây (track bị cắt rồi nối) */
#define FACE_RELOAD_SEC 3       /* chu kỳ xem gallery trên Redis có đổi không */
#define FACE_UNKNOWN_MIN_PX 64  /* mặt lạ cao ít nhất chừng này mới gửi lên web để đăng ký từ camera */
#define FACE_UNKNOWN_HITS 3     /* ... và đã thấy chừng này lần mà không khớp ai */

/* ---------------- cấu hình ---------------- */
typedef struct {
  gchar *redis_host;
  gint redis_port;
  gchar *prefix;
  guint frames_maxlen;
  guint max_queue;
  guint publish_every;
  gdouble dwell_sec;
  gdouble move_ratio;
  gchar *snap_dir;
  guint sr_cache;          /* 0 = tắt smart record */
  gchar *clip_dir;
  guint stats_sec;
  gchar *log_path;         /* NULL = tự đặt theo giờ trong /outputs/logs */
  gboolean annotate;       /* vẽ bbox lên clip sau khi ghi xong */
  gchar *pgie_cfg;         /* NULL = model mẫu TrafficCamNet (configs/task13_pgie.txt) */
  gint person_class;       /* class ID dùng cho logic ở yên */
  gchar *sgie_cfg[MAX_SGIE];
  guint num_sgie;
  gchar *cam_list;         /* -i: file danh sách camera theo toà nhà */
  guint keep_days;         /* xoá ảnh/clip cũ hơn chừng này ngày (0 = không xoá theo tuổi) */
  gdouble keep_gb;         /* tổng ảnh+clip vượt chừng này GB thì xoá file cũ nhất (0 = không giới hạn) */
  gdouble face_thr;        /* cosine tối thiểu để coi là cùng người (0 = tắt nhận diện khuôn mặt) */
  gchar *media_base;       /* -M: URL gốc của ảnh/clip đã upload (vd. http://minio:9000/ds-media), NULL = không upload */
  guint gpu;               /* -g: GPU chạy toàn bộ pipeline */
} Config;

static Config cfg = {
  .redis_host = "127.0.0.1", .redis_port = 6390, .prefix = "ds",
  .frames_maxlen = 100000, .max_queue = 50000, .publish_every = 1,
  .dwell_sec = 120, .move_ratio = 0.3,
  .snap_dir = "/outputs/w1_snapshots", .sr_cache = 0, .clip_dir = "/outputs/w1_clips",
  .stats_sec = 5, .log_path = NULL, .annotate = TRUE,
  .pgie_cfg = NULL, .person_class = 2,
  .keep_days = 30, .keep_gb = 50, .face_thr = 0,
};

#define LOG_DIR "/outputs/logs"
static FILE *log_fp;          /* toàn bộ log console */
static FILE *alerts_fp;       /* chỉ các cảnh báo, 1 dòng / cảnh báo */
static gboolean use_color;
static gchar *annotate_bin;
#define RED_BG (use_color ? "\033[1;37;41m" : "")
#define RED (use_color ? "\033[1;31m" : "")
#define YELLOW (use_color ? "\033[1;33m" : "")
#define GREEN (use_color ? "\033[1;32m" : "")
#define RESET (use_color ? "\033[0m" : "")

/* Đường dẫn trên host cho dễ mở: /outputs/... -> outputs/... */
static const gchar *
host_path (const gchar *p)
{
  return p && g_str_has_prefix (p, "/outputs/") ? p + 1 : p;
}

/* Giờ Việt Nam (UTC+7) cho log; JSON gửi Redis vẫn dùng UTC */
static gchar *
vn_time (gint64 ts_ms, const gchar *fmt)
{
  GTimeZone *tz = g_time_zone_new_offset (7 * 3600);
  GDateTime *utc = g_date_time_new_from_unix_utc (ts_ms / 1000);
  GDateTime *dt = g_date_time_to_timezone (utc, tz);
  gchar *s = g_date_time_format (dt, fmt);
  g_date_time_unref (dt);
  g_date_time_unref (utc);
  g_time_zone_unref (tz);
  return s;
}

/* ---------------- Redis publisher (thread riêng) ---------------- */
typedef struct {
  gboolean is_event;
  const gchar *key;        /* stream khác frames/events (vd. <prefix>:faces_unknown), NULL = mặc định */
  gchar *cam;
  const gchar *company;    /* trỏ vào cam_companies[], không free */
  const gchar *building;   /* trỏ vào cam_buildings[], không free */
  gchar *type;
  gint64 ts_ms;
  gchar *data;
} RedisMsg;

static GAsyncQueue *rqueue;
static GThread *rthread;
static volatile gint rstop;
static volatile gint redis_up;
static gint64 n_published, n_dropped, n_events, n_events_created;
static GMutex stats_lock;
static gchar *stats_json;          /* thống kê mới nhất, thread Redis ghi vào <prefix>:stats:g<GPU> */
#define CNT_INC(x) __atomic_fetch_add (&(x), 1, __ATOMIC_RELAXED)
#define CNT_GET(x) __atomic_load_n (&(x), __ATOMIC_RELAXED)

static guint num_sources;
static gchar *cam_ids[MAX_SOURCES];
static gchar *cam_uris[MAX_SOURCES];
static gchar *cam_buildings[MAX_SOURCES];   /* NULL khi cam truyền thẳng uri, không qua -i */
static gchar *cam_companies[MAX_SOURCES];   /* NULL khi dòng -i không có công ty */
static gchar *cam_groups[MAX_SOURCES];      /* "công ty/toà" hoặc "toà", để in [stats] */
static gdouble cam_fps[MAX_SOURCES];        /* 0 = xử lý mọi frame */
static guint cam_roi_n[MAX_SOURCES];        /* số đỉnh vùng ghế (0 = cả khung hình) */
static gfloat cam_roi[MAX_SOURCES][ROI_MAX_PTS][2];   /* đỉnh đa giác, toạ độ 0..1 theo khung hình */
static gchar *cam_info[MAX_SOURCES];        /* JSON thông tin camera từ -i (đẩy nguyên cho platform), NULL nếu không có */
static gint64 cam_last_kept_us[MAX_SOURCES];

static RedisMsg *redis_stop_marker = (RedisMsg *) 1;

static gint
cam_index (const gchar *cam)
{
  for (guint i = 0; i < num_sources; i++)
    if (g_str_equal (cam_ids[i], cam))
      return i;
  return -1;
}

static void
msg_free (RedisMsg *m)
{
  g_free (m->cam);
  g_free (m->type);
  g_free (m->data);
  g_free (m);
}

static gchar *faces_unknown_key;   /* <prefix>:faces_unknown */

/* -M: mỗi field "x":"/outputs/<đường dẫn>" của sự kiện có thêm "x_url":"<URL gốc>/<đường dẫn>" (key trên S3 = đường
 * dẫn tương đối trong /outputs, tools/uploader.py upload đúng key đó). Ảnh *_frame.jpg có thêm "x_crop_url" (ảnh crop
 * người đi kèm). URL có ngay khi gửi sự kiện, file lên S3 sau vài giây (clip: sau clip_ready) */
static void
add_media_urls (GString *js)
{
  static GRegex *re;
  if (!re)
    re = g_regex_new ("\"([a-z_]+)\":\"/outputs/([^\"]+)\"", G_REGEX_OPTIMIZE, 0, NULL);
  GMatchInfo *mi;
  GString *add = g_string_new ("");
  g_regex_match (re, js->str, 0, &mi);
  while (g_match_info_matches (mi)) {
    gchar *k = g_match_info_fetch (mi, 1), *rel = g_match_info_fetch (mi, 2);
    g_string_append_printf (add, ",\"%s_url\":\"%s/%s\"", k, cfg.media_base, rel);
    if (g_str_has_suffix (rel, "_frame.jpg"))
      g_string_append_printf (add, ",\"%s_crop_url\":\"%s/%.*s_crop.jpg\"", k, cfg.media_base,
          (int) (strlen (rel) - strlen ("_frame.jpg")), rel);
    g_free (k);
    g_free (rel);
    g_match_info_next (mi, NULL);
  }
  g_match_info_free (mi);
  if (add->len && js->len && js->str[js->len - 1] == '}')
    g_string_insert (js, js->len - 1, add->str);
  g_string_free (add, TRUE);
}

static void
enqueue_to (const gchar *key, gboolean is_event, const gchar *cam, const gchar *type, gint64 ts_ms, gchar *data)
{
  /* Quá tải: chỉ bỏ metadata frame, sự kiện luôn giữ lại */
  /* length < 0 khi thread Redis đang chờ (hàng đợi rỗng) */
  if (!is_event && g_async_queue_length (rqueue) >= (gint) cfg.max_queue) {
    CNT_INC (n_dropped);
    g_free (data);
    return;
  }
  RedisMsg *m = g_new0 (RedisMsg, 1);
  m->is_event = is_event;
  m->key = key;
  m->cam = g_strdup (cam);
  gint idx = cam_index (cam);
  m->company = idx >= 0 ? cam_companies[idx] : NULL;
  m->building = idx >= 0 ? cam_buildings[idx] : NULL;
  m->type = g_strdup (type);
  m->ts_ms = ts_ms;
  /* Chèn "company"/"building" vào đầu mọi JSON (frame lẫn sự kiện) tại 1 chỗ; sự kiện có thêm
   * "camera": thông tin camera (frame thì không, để giữ nhẹ: ~1000 frame/s) */
  const gchar *info = is_event && idx >= 0 && !g_str_equal (type, "camera_info") ? cam_info[idx] : NULL;
  if (m->building && data[0] == '{') {
    m->data = g_strdup_printf ("{%s%s%s\"building\":\"%s\",%s%s%s%s", m->company ? "\"company\":\"" : "",
        m->company ? m->company : "", m->company ? "\"," : "", m->building,
        info ? "\"camera\":" : "", info ? info : "", info ? "," : "", data + 1);
    g_free (data);
  } else {
    m->data = data;
  }
  if (m->data[0] == '{') {
    GString *o = g_string_new (NULL);
    g_string_printf (o, "{\"schema_version\":%d,%s", SCHEMA_VERSION, m->data + 1);
    if (is_event && cfg.media_base)
      add_media_urls (o);
    g_free (m->data);
    m->data = g_string_free (o, FALSE);
  }
  if (is_event && !key)
    CNT_INC (n_events_created);
  g_async_queue_push (rqueue, m);
}

static void
enqueue (gboolean is_event, const gchar *cam, const gchar *type, gint64 ts_ms, gchar *data)
{
  enqueue_to (NULL, is_event, cam, type, ts_ms, data);
}

/* Nối + AUTH, không ghi gì (thread gallery khuôn mặt dùng riêng 1 kết nối) */
static redisContext *
redis_open (void)
{
  struct timeval tv = { 2, 0 };
  redisContext *c = redisConnectWithTimeout (cfg.redis_host, cfg.redis_port, tv);
  if (!c || c->err) {
    if (c)
      redisFree (c);
    return NULL;
  }
  redisSetTimeout (c, tv);
  /* Sai mật khẩu, hoặc Redis có mật khẩu mà không AUTH (TCP vẫn nối được nhưng mọi lệnh bị từ chối rồi bị
   * ngắt -> nối lại liên tục, spam log, không tắt được): kiểm tra quyền ngay ở đây bằng PING, báo 1 lần. */
  static gboolean warned = FALSE;
  const gchar *pass = g_getenv ("REDIS_PASSWORD");
  redisReply *r = pass && *pass ? redisCommand (c, "AUTH %s", pass) : redisCommand (c, "PING");
  gboolean authed = r && r->type != REDIS_REPLY_ERROR;
  if (!authed && !warned)
    g_printerr ("[redis] %s:%d từ chối (%s): sai hoặc thiếu REDIS_PASSWORD, thử lại sau mỗi 2s\n",
        cfg.redis_host, cfg.redis_port, r ? r->str : c->errstr);
  warned |= !authed;
  if (r)
    freeReplyObject (r);
  if (!authed) {
    redisFree (c);
    return NULL;
  }
  return c;
}

static redisContext *
redis_connect (void)
{
  redisContext *c = redis_open ();
  if (!c)
    return NULL;
  /* Danh sách camera để platform biết cam_id <-> uri (ghi lại mỗi lần nối) */
  gchar *key = g_strdup_printf ("%s:cameras", cfg.prefix);
  for (guint i = 0; i < num_sources; i++) {
    redisReply *r = redisCommand (c, "HSET %s %s %s", key, cam_ids[i], cam_uris[i]);
    if (r)
      freeReplyObject (r);
  }
  g_free (key);
  /* cam_id -> toà nhà, cam_id -> công ty */
  key = g_strdup_printf ("%s:buildings", cfg.prefix);
  gchar *ckey = g_strdup_printf ("%s:companies", cfg.prefix);
  for (guint i = 0; i < num_sources; i++) {
    redisReply *r = NULL;
    if (cam_buildings[i] && (r = redisCommand (c, "HSET %s %s %s", key, cam_ids[i], cam_buildings[i])))
      freeReplyObject (r);
    if (cam_companies[i] && (r = redisCommand (c, "HSET %s %s %s", ckey, cam_ids[i], cam_companies[i])))
      freeReplyObject (r);
  }
  g_free (key);
  g_free (ckey);
  /* cam_id -> JSON thông tin camera (tên công ty/toà/camera, tầng, khu vực, ...) */
  key = g_strdup_printf ("%s:camera_info", cfg.prefix);
  for (guint i = 0; i < num_sources; i++) {
    redisReply *r = NULL;
    if (cam_info[i] && (r = redisCommand (c, "HSET %s %s %s", key, cam_ids[i], cam_info[i])))
      freeReplyObject (r);
  }
  g_free (key);
  return c;
}

static gpointer
redis_thread_func (gpointer data)
{
  redisContext *c = NULL;
  gchar *frames_key = g_strdup_printf ("%s:frames", cfg.prefix);
  gchar *events_key = g_strdup_printf ("%s:events", cfg.prefix);
  gchar maxlen_frames[16], maxlen_events[16];
  g_snprintf (maxlen_frames, sizeof (maxlen_frames), "%u", cfg.frames_maxlen);
  g_snprintf (maxlen_events, sizeof (maxlen_events), "%u", 100000u);
  RedisMsg *batch[REDIS_BATCH];
  guint pending = 0;   /* batch giữ lại khi gửi lỗi, gửi lại sau khi nối lại */
  gboolean stopping = FALSE;

  while (TRUE) {
    if (!c) {
      c = redis_connect ();
      g_atomic_int_set (&redis_up, c != NULL);
      if (!c) {
        if (g_atomic_int_get (&rstop))
          break;          /* đang tắt mà Redis không nối được: bỏ phần còn lại */
        g_usleep (2 * G_USEC_PER_SEC);
        continue;
      }
      g_print ("[redis] đã kết nối %s:%d\n", cfg.redis_host, cfg.redis_port);
    }

    /* Thống kê mới (print_stats) -> <prefix>:stats:g<GPU>, hết hạn sau 60 s nếu app chết (exporter báo app down) */
    g_mutex_lock (&stats_lock);
    gchar *st = g_steal_pointer (&stats_json);
    g_mutex_unlock (&stats_lock);
    if (st) {
      redisReply *r = redisCommand (c, "SET %s:stats:g%u %s EX 60", cfg.prefix, cfg.gpu, st);
      if (r)
        freeReplyObject (r);
      g_free (st);
    }

    /* Gom batch: chờ tối đa 50 ms cho message đầu, sau đó lấy hết những gì đang có */
    while (pending < REDIS_BATCH && !stopping) {
      RedisMsg *m = pending == 0 ? g_async_queue_timeout_pop (rqueue, 50000)
          : g_async_queue_try_pop (rqueue);
      if (!m)
        break;
      if (m == redis_stop_marker) {
        stopping = TRUE;
        break;
      }
      batch[pending++] = m;
    }
    if (pending == 0) {
      if (stopping)
        break;
      continue;
    }

    /* Pipelining: append cả batch rồi mới đọc reply */
    for (guint i = 0; i < pending; i++) {
      RedisMsg *m = batch[i];
      gchar ts[24];
      g_snprintf (ts, sizeof (ts), "%" G_GINT64_FORMAT, m->ts_ms);
      const char *argv[] = { "XADD", m->key ? m->key : m->is_event ? events_key : frames_key, "MAXLEN", "~",
        m->key ? "1000" : m->is_event ? maxlen_events : maxlen_frames, "*",
        "cam", m->cam, "type", m->type, "ts", ts, "data", m->data,
        "building", m->building, "company", m->company };
      /* Field building/company chỉ có khi cam khai báo qua -i */
      redisAppendCommandArgv (c, G_N_ELEMENTS (argv) - (m->building ? 0 : 2) - (m->company ? 0 : 2),
          argv, NULL);
    }
    gboolean ok = TRUE;
    for (guint i = 0; i < pending; i++) {
      redisReply *r = NULL;
      if (redisGetReply (c, (void **) &r) != REDIS_OK) {
        ok = FALSE;
        break;
      }
      if (r->type == REDIS_REPLY_ERROR)
        g_printerr ("[redis] lỗi: %s\n", r->str);
      freeReplyObject (r);
    }
    if (!ok) {
      g_printerr ("[redis] mất kết nối (%s), thử lại sau 2s\n", c->errstr);
      redisFree (c);
      c = NULL;
      g_atomic_int_set (&redis_up, 0);
      /* Giữ sự kiện để gửi lại; metadata frame của batch lỗi thì bỏ (đã cũ) */
      guint keep = 0;
      for (guint i = 0; i < pending; i++) {
        if (batch[i]->is_event)
          batch[keep++] = batch[i];
        else {
          CNT_INC (n_dropped);
          msg_free (batch[i]);
        }
      }
      pending = keep;
      continue;
    }
    for (guint i = 0; i < pending; i++) {
      if (batch[i]->is_event && !batch[i]->key)
        CNT_INC (n_events);
      CNT_INC (n_published);
      msg_free (batch[i]);
    }
    pending = 0;
  }

  for (guint i = 0; i < pending; i++)
    msg_free (batch[i]);
  if (c)
    redisFree (c);
  g_free (frames_key);
  g_free (events_key);
  return NULL;
}

/* ---------------- dwell (người ở yên) ---------------- */
typedef struct {
  guint src;
  guint64 tid;                    /* ID của lượt ở yên = tracking ID đầu tiên, giữ nguyên khi gộp track */
  guint64 track_tid;              /* tracking ID hiện tại (khác tid sau khi gộp track bị che) */
  gdouble anchor_cx, anchor_cy;   /* điểm neo: vị trí khi bắt đầu đứng yên */
  gint64 start_ts_ms;             /* thời điểm bắt đầu đứng yên (wall clock của frame) */
  gint64 last_ts_ms;
  gint64 last_seen_mono_us;
  gboolean alerted;
  gfloat last_bbox[4];
  gchar *snapshot;
  gchar *clip;
} DwellTrack;

typedef struct {
  GstElement *src;
  guint idx;
  guint start_before;
  guint duration;
  gchar *tag;
  gint64 alert_ms;
} SrRequest;

typedef struct {
  guint idx;
  gchar *path;
  gchar *tag;
  gint64 clip_start_ms;
  gdouble alert_at;
} AnnotateJob;

static GThreadPool *annotate_pool;

static GHashTable *dwell;           /* key (src<<48 | tid) -> DwellTrack */
static GPtrArray *dwell_ghosts;     /* track đã mất hình, chờ gộp với track mới ở đúng chỗ cũ */
static GstElement *src_elems[MAX_SOURCES];
static volatile gint sr_busy[MAX_SOURCES];
static guint sr_session[MAX_SOURCES];
static gchar *sr_tag[MAX_SOURCES];               /* tag clip đang ghi của từng cam */
static gint64 sr_alert_ms[MAX_SOURCES];          /* thời điểm cảnh báo của clip đang ghi */
static gint64 src_last_frame_us[MAX_SOURCES];   /* monotonic, cập nhật trong probe */
static gint64 src_last_data_us[MAX_SOURCES];    /* monotonic, gói nén cuối cùng vào decoder */
static gint64 src_last_reset_us[MAX_SOURCES];
static volatile gint src_resetting[MAX_SOURCES];
static volatile gint src_eos[MAX_SOURCES];       /* server đóng phiên (EOS) -> reset ngay */
static guint64 src_frames_at_reset[MAX_SOURCES];
static gboolean src_online[MAX_SOURCES];
static gboolean src_is_rtsp[MAX_SOURCES];
static GThreadPool *reset_pool;
static GstElement *mux_elem;
static volatile gint shutting_down;
static NvDsObjEncCtxHandle enc_ctx;
static guint64 src_frames[MAX_SOURCES], src_frames_prev[MAX_SOURCES];
static gint64 last_sweep_us;

/* Chân người (giữa cạnh dưới bbox) có nằm trong vùng ghế của camera không (ray casting). Không có vùng = cả khung */
static gboolean
roi_contains (guint src, const NvOSD_RectParams *r)
{
  guint n = cam_roi_n[src];
  if (n < 3)
    return TRUE;
  gfloat x = (r->left + r->width / 2) / MUXER_WIDTH, y = (r->top + r->height) / MUXER_HEIGHT;
  gboolean in = FALSE;
  for (guint i = 0, j = n - 1; i < n; j = i++) {
    gfloat xi = cam_roi[src][i][0], yi = cam_roi[src][i][1], xj = cam_roi[src][j][0], yj = cam_roi[src][j][1];
    if ((yi > y) != (yj > y) && x < (xj - xi) * (y - yi) / (yj - yi) + xi)
      in = !in;
  }
  return in;
}

/* "roi":[[x,y],...] trong JSON thông tin camera (dashboard master vẽ, lưu ở sites.json), toạ độ 0..1 */
static void
roi_parse (guint idx, const gchar *info)
{
  cam_roi_n[idx] = 0;
  const gchar *p = info ? strstr (info, "\"roi\"") : NULL;
  if (!p || !(p = strchr (p, '[')))
    return;
  guint n = 0;
  gint depth = 0;
  gfloat v[2 * ROI_MAX_PTS];
  for (; *p && n < G_N_ELEMENTS (v); p++) {
    if (*p == '[')
      depth++;
    else if (*p == ']' && --depth == 0)
      break;
    else if (g_ascii_isdigit (*p) || *p == '.' || *p == '-') {
      gchar *end;
      v[n++] = g_ascii_strtod (p, &end);
      p = end - 1;
    }
  }
  if (n < 6)
    return;
  for (guint i = 0; i + 1 < n; i += 2) {
    cam_roi[idx][i / 2][0] = CLAMP (v[i], 0, 1);
    cam_roi[idx][i / 2][1] = CLAMP (v[i + 1], 0, 1);
  }
  cam_roi_n[idx] = n / 2;
}

static void
dwell_free (DwellTrack *t)
{
  g_free (t->snapshot);
  g_free (t->clip);
  g_free (t);
}

/* ISO 8601 UTC, vd. 2026-10-23T05:00:00.123Z (platform tự đổi sang giờ địa phương) */
static gchar *
iso_time (gint64 ts_ms)
{
  GDateTime *dt = g_date_time_new_from_unix_utc (ts_ms / 1000);
  gchar *base = g_date_time_format (dt, "%Y-%m-%dT%H:%M:%S");
  gchar *s = g_strdup_printf ("%s.%03dZ", base, (int) (ts_ms % 1000));
  g_free (base);
  g_date_time_unref (dt);
  return s;
}

static void
emit_dwell_event (const gchar *type, DwellTrack *t, gint64 now_ms, const gchar *reason)
{
  gchar *start = iso_time (t->start_ts_ms);
  gchar *now = iso_time (now_ms);
  /* Sau khi gộp track: tid vẫn là ID của lượt (để platform ghép alert/end), track_tid là ID hiện tại */
  gchar *track = t->track_tid != t->tid
      ? g_strdup_printf (",\"track_tid\":%" G_GUINT64_FORMAT, t->track_tid) : g_strdup ("");
  gchar *data = g_strdup_printf (
      "{\"type\":\"%s\",\"cam\":\"%s\",\"tid\":%" G_GUINT64_FORMAT
      "%s,\"start_ts\":\"%s\",\"%s\":\"%s\",\"start_ms\":%" G_GINT64_FORMAT
      ",\"dwell_s\":%.1f,\"bbox\":[%.0f,%.0f,%.0f,%.0f],\"snapshot\":%s%s%s,\"clip\":%s%s%s"
      "%s%s%s}",
      type, cam_ids[t->src], t->tid, track, start,
      g_str_equal (type, "dwell_alert") ? "alert_ts" : "end_ts", now, t->start_ts_ms,
      (now_ms - t->start_ts_ms) / 1000.0,
      t->last_bbox[0], t->last_bbox[1], t->last_bbox[2], t->last_bbox[3],
      t->snapshot ? "\"" : "", t->snapshot ? t->snapshot : "null", t->snapshot ? "\"" : "",
      t->clip ? "\"" : "", t->clip ? t->clip : "null", t->clip ? "\"" : "",
      reason ? ",\"reason\":\"" : "", reason ? reason : "", reason ? "\"" : "");
  gdouble dur = (now_ms - t->start_ts_ms) / 1000.0;
  gchar *vn_start = vn_time (t->start_ts_ms, "%H:%M:%S");
  gchar *vn_now = vn_time (now_ms, "%H:%M:%S %d/%m/%Y");
  const gchar *bld = cam_groups[t->src];
  if (g_str_equal (type, "dwell_alert")) {
    /* In cả khối bằng 1 lần g_print để không bị dòng log khác chen vào giữa */
    g_print ("\n%s  !!! CẢNH BÁO Ở YÊN QUÁ LÂU (ngồi/đứng một chỗ) !!!  %s\n"
        "%s  Toà    :%s %s\n"
        "%s  Camera :%s %s\n"
        "%s  Người  :%s #%" G_GUINT64_FORMAT " (tracking ID)\n"
        "%s  Ở yên  :%s %d phút %02d giây, từ %s đến %s (giờ VN)\n"
        "%s  Bbox   :%s [x=%.0f y=%.0f w=%.0f h=%.0f]\n"
        "%s  Ảnh    :%s %s\n"
        "%s  Clip   :%s %s\n\n",
        RED_BG, RESET, RED, RESET, bld ? bld : "-", RED, RESET, cam_ids[t->src], RED, RESET, t->tid,
        RED, RESET, (int) dur / 60, (int) dur % 60, vn_start, vn_now,
        RED, RESET, t->last_bbox[0], t->last_bbox[1], t->last_bbox[2], t->last_bbox[3],
        RED, RESET, t->snapshot ? host_path (t->snapshot) : "(không có)",
        RED, RESET, t->clip ? "đang ghi, xong sau ~10s (bản có bbox: *_bbox.mp4)"
            : "tắt (thêm -r 135 để ghi clip)");
    if (alerts_fp) {
      fprintf (alerts_fp, "%s\tCẢNH BÁO\ttoà %s\t%s\tngười #%" G_GUINT64_FORMAT "\tở yên %.0fs\ttừ %s\t%s\n",
          vn_now, bld ? bld : "-", cam_ids[t->src], t->tid, dur, vn_start,
          t->snapshot ? host_path (t->snapshot) : "");
      fflush (alerts_fp);
    }
  } else {
    const gchar *why = g_strcmp0 (reason, "moved") == 0 ? "đã đứng dậy/di chuyển"
        : g_strcmp0 (reason, "lost") == 0 ? "rời khỏi khung hình" : "dừng app";
    g_print ("%s[HẾT Ở YÊN]%s %s%s%s%s người #%" G_GUINT64_FORMAT ": ở yên %d phút %02d giây (%s -> %s), %s\n",
        YELLOW, RESET, bld ? "toà " : "", bld ? bld : "", bld ? " " : "", cam_ids[t->src], t->tid, (int) dur / 60, (int) dur % 60, vn_start,
        vn_now, why);
  }
  g_free (vn_start);
  g_free (vn_now);
  enqueue (TRUE, cam_ids[t->src], type, now_ms, data);
  g_free (track);
  g_free (start);
  g_free (now);
}

/* Chạy ở main loop: phát start-sr cho nvurisrcbin (không làm trong streaming thread) */
static gboolean
do_start_sr (gpointer data)
{
  SrRequest *req = (SrRequest *) data;
  NvDsSRSessionId sess = 0;
  if (g_atomic_int_get (&shutting_down)) {
    g_atomic_int_set (&sr_busy[req->idx], 0);
    g_free (req->tag);
    g_free (req);
    return G_SOURCE_REMOVE;
  }
  g_signal_emit_by_name (req->src, "start-sr", &sess, req->start_before, req->duration, req->tag);
  sr_session[req->idx] = sess;
  sr_alert_ms[req->idx] = req->alert_ms;
  g_print ("[SR] %s: bắt đầu ghi clip (lùi %us + ghi thêm %us)\n", cam_ids[req->idx],
      req->start_before, req->duration);
  g_free (req);   /* tag được trả lại trong sr-done và giải phóng ở đó */
  return G_SOURCE_REMOVE;
}

static void
on_sr_done (GstElement *src, NvDsSRRecordingInfo *info, gpointer user_tag, gpointer data)
{
  guint idx = GPOINTER_TO_UINT (data);
  sr_session[idx] = G_MAXUINT;
  g_atomic_int_set (&sr_busy[idx], 0);
  gchar *path = g_build_filename (info->dirpath, info->filename, NULL);
  gint64 now_ms = g_get_real_time () / 1000;
  gchar *data_json = g_strdup_printf ("{\"type\":\"clip_ready\",\"cam\":\"%s\",\"tag\":\"%s\","
      "\"file\":\"%s\",\"duration_ms\":%" G_GUINT64_FORMAT "}",
      cam_ids[idx], user_tag ? (gchar *) user_tag : "", path, info->duration);
  g_print ("%s[CLIP]%s %s: %s (%.0fs, video gốc của camera)\n", GREEN, RESET, cam_ids[idx],
      host_path (path), info->duration / 1000.0);
  enqueue (TRUE, cam_ids[idx], "clip_ready", now_ms, data_json);

  if (cfg.annotate && annotate_bin && info->duration > 0) {
    AnnotateJob *job = g_new0 (AnnotateJob, 1);
    job->idx = idx;
    job->path = g_strdup (path);
    job->tag = g_strdup (user_tag ? (gchar *) user_tag : "");
    /* clip = [alert - lùi, alert + SR_POST_SEC] -> suy ra giờ đầu clip và vị trí cảnh báo */
    job->clip_start_ms = sr_alert_ms[idx] + SR_POST_SEC * 1000 - (gint64) info->duration;
    job->alert_at = (sr_alert_ms[idx] - job->clip_start_ms) / 1000.0;
    g_thread_pool_push (annotate_pool, job, NULL);
  }
  g_free (path);
  g_free (user_tag);
}

/* Thread pool (tối đa 2, giới hạn phiên NVENC của GeForce): chạy w1-annotate cho từng clip */
static void
annotate_worker (gpointer data, gpointer user_data)
{
  AnnotateJob *job = (AnnotateJob *) data;
  gchar *base = g_strndup (job->path, strlen (job->path) - (g_str_has_suffix (job->path, ".mp4") ? 4 : 0));
  gchar *out = g_strdup_printf ("%s_bbox.mp4", base);
  gchar *t = g_strdup_printf ("%.0f", cfg.dwell_sec);
  gchar *a = g_strdup_printf ("%.1f", job->alert_at);
  gchar *m = g_strdup_printf ("%.2f", cfg.move_ratio);
  gchar *st = g_strdup_printf ("%" G_GINT64_FORMAT, job->clip_start_ms);
  gchar *p = g_strdup_printf ("%d", cfg.person_class);
  gchar *argv[] = { annotate_bin, "-i", job->path, "-o", out, "-t", t, "-a", a, "-m", m,
    "-c", cam_ids[job->idx], "-s", st, "-p", p,
    cfg.pgie_cfg ? "-C" : NULL, cfg.pgie_cfg, NULL };
  gint status = 0;
  GError *err = NULL;
  gboolean ok = g_spawn_sync (NULL, argv, NULL,
      G_SPAWN_STDOUT_TO_DEV_NULL | G_SPAWN_STDERR_TO_DEV_NULL, NULL, NULL, NULL, NULL,
      &status, &err) && g_spawn_check_wait_status (status, NULL);
  if (ok) {
    g_print ("%s[CLIP+BBOX]%s %s: %s (khung đỏ = người ở yên quá %.0fs)\n", GREEN, RESET,
        cam_ids[job->idx], host_path (out), cfg.dwell_sec);
    gchar *json = g_strdup_printf ("{\"type\":\"clip_annotated\",\"cam\":\"%s\",\"tag\":\"%s\","
        "\"file\":\"%s\",\"source\":\"%s\"}", cam_ids[job->idx], job->tag, out, job->path);
    enqueue (TRUE, cam_ids[job->idx], "clip_annotated", g_get_real_time () / 1000, json);
  } else {
    g_printerr ("Vẽ bbox lỗi cho %s: %s\n", job->path, err ? err->message : "exit != 0");
  }
  g_clear_error (&err);
  g_free (base); g_free (out); g_free (t); g_free (a); g_free (m); g_free (st); g_free (p);
  g_free (job->path);
  g_free (job->tag);
  g_free (job);
}

static void
on_dwell_alert (DwellTrack *t, NvBufSurface *surf, NvDsFrameMeta *fm, NvDsObjectMeta *obj,
    gint64 now_ms)
{
  gchar *tag = g_strdup_printf ("%s_tid%" G_GUINT64_FORMAT "_%" G_GINT64_FORMAT,
      cam_ids[t->src], t->tid, t->start_ts_ms);

  /* Snapshot: 1 ảnh crop người + 1 ảnh toàn khung, encode JPEG trên GPU */
  if (enc_ctx && surf) {
    NvDsObjEncUsrArgs args = { 0 };
    args.saveImg = TRUE;
    args.quality = 85;
    g_snprintf (args.fileNameImg, FILE_NAME_SIZE, "%s/%s_crop.jpg", cfg.snap_dir, tag);
    nvds_obj_enc_process (enc_ctx, &args, surf, obj, fm);
    NvDsObjEncUsrArgs fargs = { 0 };
    fargs.saveImg = TRUE;
    fargs.quality = 80;
    fargs.isFrame = TRUE;
    g_snprintf (fargs.fileNameImg, FILE_NAME_SIZE, "%s/%s_frame.jpg", cfg.snap_dir, tag);
    nvds_obj_enc_process (enc_ctx, &fargs, surf, NULL, fm);
    t->snapshot = g_strdup_printf ("%s/%s_frame.jpg", cfg.snap_dir, tag);
  }

  /* Clip: lùi về lúc bắt đầu đứng yên (tối đa bằng cache) + SR_POST_SEC sau cảnh báo */
  if (cfg.sr_cache && src_elems[t->src]) {
    if (g_atomic_int_compare_and_exchange (&sr_busy[t->src], 0, 1)) {
      SrRequest *req = g_new0 (SrRequest, 1);
      req->src = src_elems[t->src];
      req->idx = t->src;
      /* clip = [now - start_before, now + duration] */
      req->start_before = MIN ((guint) ceil (cfg.dwell_sec) + 2, cfg.sr_cache);
      req->duration = SR_POST_SEC;
      req->tag = g_strdup (tag);
      req->alert_ms = now_ms;
      g_free (sr_tag[t->src]);
      sr_tag[t->src] = g_strdup (tag);
      t->clip = g_strdup_printf ("pending:%s", tag);
      g_idle_add (do_start_sr, req);
    } else {
      /* cam đang ghi clip cho sự kiện khác cùng lúc, clip đó đã bao gồm người này */
      t->clip = g_strdup_printf ("pending:%s", sr_tag[t->src] ? sr_tag[t->src] : "");
    }
  }
  emit_dwell_event ("dwell_alert", t, now_ms, NULL);
  g_free (tag);
}

/* Track mới xuất hiện: có phải người vừa bị che ở đúng chỗ đó không (cùng cam, gần điểm neo,
 * cỡ bbox tương đương)? Có thì lấy ra khỏi danh sách chờ gộp, chọn ứng viên gần nhất. */
static DwellTrack *
dwell_take_ghost (guint src, gdouble cx, gdouble cy, gdouble h)
{
  gint best = -1;
  gdouble best_d = G_MAXDOUBLE;
  for (guint i = 0; i < dwell_ghosts->len; i++) {
    DwellTrack *g = g_ptr_array_index (dwell_ghosts, i);
    gdouble ratio = h / MAX (g->last_bbox[3], 1);
    gdouble d = hypot (cx - g->anchor_cx, cy - g->anchor_cy);
    if (g->src == src && ratio > 0.67 && ratio < 1.5 && d <= cfg.move_ratio * MAX (h, 1) && d < best_d) {
      best = i;
      best_d = d;
    }
  }
  return best < 0 ? NULL : g_ptr_array_remove_index_fast (dwell_ghosts, best);
}

static void
dwell_resume (DwellTrack *t, guint64 new_tid, gint64 ts_ms)
{
  gdouble gap = (ts_ms - t->last_ts_ms) / 1000.0;
  guint64 prev = t->track_tid;
  t->track_tid = new_tid;
  gchar *vn_start = vn_time (t->start_ts_ms, "%H:%M:%S");
  g_print ("[GỘP TRACK] %s người #%" G_GUINT64_FORMAT ": track #%" G_GUINT64_FORMAT " -> #%" G_GUINT64_FORMAT
      " sau %.1fs bị che, vẫn tính ở yên từ %s\n", cam_ids[t->src], t->tid, prev, new_tid, gap, vn_start);
  g_free (vn_start);
  /* Lượt đã cảnh báo: báo platform/web track_tid mới để tiếp tục tô đỏ đúng người */
  if (t->alerted && prev != new_tid)
    enqueue (TRUE, cam_ids[t->src], "dwell_resume", ts_ms, g_strdup_printf (
        "{\"type\":\"dwell_resume\",\"cam\":\"%s\",\"tid\":%" G_GUINT64_FORMAT ",\"start_ms\":%" G_GINT64_FORMAT
        ",\"track_tid\":%" G_GUINT64_FORMAT ",\"prev_track_tid\":%" G_GUINT64_FORMAT ",\"gap_s\":%.1f}",
        cam_ids[t->src], t->tid, t->start_ts_ms, new_tid, prev, gap));
}

static void
dwell_update (NvDsFrameMeta *fm, NvDsObjectMeta *obj, NvBufSurface *surf, gint64 ts_ms,
    gint64 mono_us)
{
  NvOSD_RectParams *r = &obj->rect_params;
  guint64 key = ((guint64) fm->source_id << 48) | (obj->object_id & 0xFFFFFFFFFFFFull);
  gdouble cx = r->left + r->width / 2, cy = r->top + r->height / 2;
  DwellTrack *t = g_hash_table_lookup (dwell, &key);

  if (!t) {
    t = dwell_take_ghost (fm->source_id, cx, cy, r->height);
    if (t) {
      dwell_resume (t, obj->object_id, ts_ms);
    } else {
      t = g_new0 (DwellTrack, 1);
      t->src = fm->source_id;
      t->tid = t->track_tid = obj->object_id;
      t->anchor_cx = cx;
      t->anchor_cy = cy;
      t->start_ts_ms = ts_ms;
    }
    g_hash_table_insert (dwell, g_memdup2 (&key, sizeof (key)), t);
  } else if (hypot (cx - t->anchor_cx, cy - t->anchor_cy) > cfg.move_ratio * MAX (r->height, 1)) {
    /* Di chuyển: kết thúc lượt đứng yên (nếu đã cảnh báo) và đặt lại điểm neo */
    if (t->alerted)
      emit_dwell_event ("dwell_end", t, t->last_ts_ms, "moved");
    t->anchor_cx = cx;
    t->anchor_cy = cy;
    t->start_ts_ms = ts_ms;
    t->alerted = FALSE;
    g_clear_pointer (&t->snapshot, g_free);
    g_clear_pointer (&t->clip, g_free);
  }
  t->last_ts_ms = ts_ms;
  t->last_seen_mono_us = mono_us;
  t->last_bbox[0] = r->left;
  t->last_bbox[1] = r->top;
  t->last_bbox[2] = r->width;
  t->last_bbox[3] = r->height;

  if (!t->alerted && (ts_ms - t->start_ts_ms) >= cfg.dwell_sec * 1000) {
    t->alerted = TRUE;
    on_dwell_alert (t, surf, fm, obj, ts_ms);
  }
}

static void
dwell_close (DwellTrack *t, const gchar *reason)
{
  if (t->alerted)
    emit_dwell_event ("dwell_end", t, t->last_ts_ms, reason);
  dwell_free (t);
}

/* Người ra khỏi vùng ghế: kết thúc lượt ở yên (dwell_end reason "left_roi" nếu đã cảnh báo) */
static void
dwell_leave (guint src, guint64 tid)
{
  guint64 key = ((guint64) src << 48) | (tid & 0xFFFFFFFFFFFFull);
  DwellTrack *t = g_hash_table_lookup (dwell, &key);
  if (!t)
    return;
  gpointer k;
  g_hash_table_steal_extended (dwell, &key, &k, NULL);
  g_free (k);
  dwell_close (t, "left_roi");
}

/* Track mất hình quá TRACK_LOST_SEC: nếu đã ở yên đủ lâu thì có thể chỉ bị che, chuyển sang danh sách
 * chờ gộp; hết MERGE_SEC mà không ai hiện lại đúng chỗ thì mới kết thúc lượt (dwell_end lost).
 * shutdown = TRUE: đóng hết (track đang thấy -> shutdown, track đang chờ gộp -> lost). */
static void
dwell_sweep (gint64 now, gboolean shutdown)
{
  GHashTableIter it;
  gpointer key, value;
  g_hash_table_iter_init (&it, dwell);
  while (g_hash_table_iter_next (&it, &key, &value)) {
    DwellTrack *t = (DwellTrack *) value;
    if (!shutdown && (now - t->last_seen_mono_us) / 1e6 < TRACK_LOST_SEC)
      continue;
    g_hash_table_iter_steal (&it);
    g_free (key);
    if (!shutdown && (t->last_ts_ms - t->start_ts_ms) / 1000.0 >= MERGE_MIN_STILL_SEC)
      g_ptr_array_add (dwell_ghosts, t);
    else
      dwell_close (t, shutdown ? "shutdown" : "lost");
  }
  for (guint i = 0; i < dwell_ghosts->len;) {
    DwellTrack *t = g_ptr_array_index (dwell_ghosts, i);
    if (shutdown || (now - t->last_seen_mono_us) / 1e6 >= MERGE_SEC)
      dwell_close (g_ptr_array_remove_index_fast (dwell_ghosts, i), "lost");
    else
      i++;
  }
}

/* ---------------- nhận diện khuôn mặt + hạng khách (VIP) ----------------
 * Gallery do web ghi lên Redis (web/faces.py), app đọc lại khi <prefix>:faces_ver đổi:
 *   <prefix>:face_tags  hash  <id> -> "<tier>\t<công ty>\t<tên>"  (công ty rỗng = mọi công ty)
 *   <prefix>:face_emb   hash  <id>/<k> -> 512 float32 đã chuẩn hoá L2 (mỗi người nhiều ảnh mẫu)
 * Probe: mặt (gie 2, parent = người có tracking ID) có tensor embedding (gie 3) -> cosine với gallery. */
typedef struct {
  gchar *id, *name, *name_js, *tier, *company;
  gfloat emb[FACE_DIM];
} FaceRef;

typedef struct {
  gchar *cand;                  /* người đang khớp liên tiếp, chưa đủ FACE_VOTES */
  guint votes;
  gchar *id, *name_js, *tier;   /* đã nhận diện (NULL = chưa) */
  gfloat score;
  const gchar *cls_tier;        /* nhãn classifier VIP ("vip" / "normal"), NULL nếu không có tầng đó */
  gfloat cls_prob;
  gboolean vip_sent, unknown_sent;
  guint unknown_hits;
  gint64 last_seen_mono_us;
} FaceTrack;

static GPtrArray *face_refs;      /* FaceRef*, thay cả mảng khi gallery đổi */
static guint face_people;
static GMutex face_lock;
static GHashTable *face_tracks;   /* key (src<<48 | tid người) -> FaceTrack */
static GHashTable *face_last_hit; /* "cam/id" -> ts_ms lần báo gần nhất (FACE_COOLDOWN_SEC) */
static GThread *face_thread;
static gint64 n_face_hits;

/* Chuỗi đưa vào JSON (tên người nhập trên web): escape " \ và ký tự điều khiển */
static gchar *
json_esc (const gchar *s)
{
  GString *o = g_string_sized_new (strlen (s) + 8);
  for (const guchar *p = (const guchar *) s; *p; p++) {
    if (*p == '"' || *p == '\\')
      g_string_append_c (o, '\\');
    if (*p < 0x20)
      g_string_append_printf (o, "\\u%04x", *p);
    else
      g_string_append_c (o, *p);
  }
  return g_string_free (o, FALSE);
}

static void
face_ref_free (FaceRef *f)
{
  g_free (f->id); g_free (f->name); g_free (f->name_js); g_free (f->tier); g_free (f->company);
  g_free (f);
}

static void
face_track_free (FaceTrack *t)
{
  g_free (t->cand); g_free (t->id); g_free (t->name_js); g_free (t->tier);
  g_free (t);
}

static GPtrArray *
face_gallery_load (redisContext *c, guint *people)
{
  redisReply *tags = redisCommand (c, "HGETALL %s:face_tags", cfg.prefix);
  redisReply *embs = tags ? redisCommand (c, "HGETALL %s:face_emb", cfg.prefix) : NULL;
  GPtrArray *out = NULL;
  if (embs && tags->type == REDIS_REPLY_ARRAY && embs->type == REDIS_REPLY_ARRAY) {
    out = g_ptr_array_new_with_free_func ((GDestroyNotify) face_ref_free);
    GHashTable *meta = g_hash_table_new (g_str_hash, g_str_equal);
    for (size_t i = 0; i + 1 < tags->elements; i += 2)
      g_hash_table_insert (meta, tags->element[i]->str, tags->element[i + 1]->str);
    *people = g_hash_table_size (meta);
    for (size_t i = 0; i + 1 < embs->elements; i += 2) {
      redisReply *k = embs->element[i], *v = embs->element[i + 1];
      const gchar *slash = strrchr (k->str, '/');
      if (!slash || v->len != FACE_DIM * sizeof (gfloat))
        continue;
      gchar *id = g_strndup (k->str, slash - k->str);
      const gchar *m = g_hash_table_lookup (meta, id);
      gchar **p = m ? g_strsplit (m, "\t", 3) : NULL;
      /* id/công ty đi vào tên file ảnh và JSON: chỉ nhận chữ, số, _ - (web đã kiểm tra, đây là chặn lần 2) */
      if (p && g_strv_length (p) == 3 && g_regex_match_simple ("^[A-Za-z0-9_-]+$", id, 0, 0)
          && g_regex_match_simple ("^[A-Za-z0-9_]*$", p[1], 0, 0)) {
        FaceRef *f = g_new (FaceRef, 1);
        f->id = id;
        f->tier = g_strdup (g_str_equal (p[0], "vip") ? "vip" : "normal");
        f->company = g_strdup (p[1]);
        f->name = g_strdup (p[2]);
        f->name_js = json_esc (p[2]);
        memcpy (f->emb, v->str, sizeof (f->emb));
        g_ptr_array_add (out, f);
      } else {
        g_free (id);
      }
      g_strfreev (p);
    }
    g_hash_table_destroy (meta);
  }
  if (tags)
    freeReplyObject (tags);
  if (embs)
    freeReplyObject (embs);
  return out;
}

/* Thread riêng, 1 kết nối Redis riêng: đọc lại gallery khi web đăng ký / xoá người (faces_ver đổi) */
static gpointer
face_thread_func (gpointer data)
{
  redisContext *c = NULL;
  gchar *ver = NULL;
  while (!g_atomic_int_get (&shutting_down)) {
    if (!c)
      c = redis_open ();
    redisReply *r = c ? redisCommand (c, "GET %s:faces_ver", cfg.prefix) : NULL;
    if (c && !r) {
      redisFree (c);
      c = NULL;
    }
    if (r) {
      const gchar *now = r->type == REDIS_REPLY_STRING ? r->str : "0";
      guint people = 0;
      GPtrArray *refs = g_strcmp0 (now, ver) ? face_gallery_load (c, &people) : NULL;
      if (refs) {
        g_mutex_lock (&face_lock);
        GPtrArray *old = face_refs;
        face_refs = refs;
        face_people = people;
        g_mutex_unlock (&face_lock);
        if (old)
          g_ptr_array_unref (old);
        g_print ("[face] gallery: %u người, %u ảnh mẫu\n", people, refs->len);
        g_free (ver);
        ver = g_strdup (now);
      }
      freeReplyObject (r);
    }
    for (guint i = 0; i < FACE_RELOAD_SEC * 10 && !g_atomic_int_get (&shutting_down); i++)
      g_usleep (G_USEC_PER_SEC / 10);
  }
  if (c)
    redisFree (c);
  g_free (ver);
  return NULL;
}

/* Ảnh mẫu khớp nhất (đều đã chuẩn hoá L2 nên cosine = tích vô hướng). Người đăng ký cho công ty khác
 * bị bỏ qua: VIP của công ty A không được báo ở camera công ty B. Gọi khi đang giữ face_lock. */
static const FaceRef *
face_match (const gfloat *v, const gchar *company, gfloat *score)
{
  const FaceRef *best = NULL;
  *score = -1;
  for (guint i = 0; face_refs && i < face_refs->len; i++) {
    const FaceRef *f = g_ptr_array_index (face_refs, i);
    if (*f->company && g_strcmp0 (f->company, company))
      continue;
    gfloat d = 0;
    for (guint k = 0; k < FACE_DIM; k++)
      d += v[k] * f->emb[k];
    if (d > *score) {
      *score = d;
      best = f;
    }
  }
  return best;
}

static FaceTrack *
face_track_get (guint src, guint64 tid, gint64 mono)
{
  guint64 key = ((guint64) src << 48) | (tid & 0xFFFFFFFFFFFFull);
  FaceTrack *t = g_hash_table_lookup (face_tracks, &key);
  if (!t) {
    t = g_new0 (FaceTrack, 1);
    g_hash_table_insert (face_tracks, g_memdup2 (&key, sizeof (key)), t);
  }
  t->last_seen_mono_us = mono;
  return t;
}

/* Ảnh toàn khung + crop 1 object (mặt hoặc người), JPEG trên GPU. Trả về đường dẫn ảnh toàn khung */
static gchar *
face_snapshot (NvBufSurface *surf, NvDsFrameMeta *fm, NvDsObjectMeta *obj, const gchar *tag, gchar **crop)
{
  *crop = NULL;
  if (!enc_ctx || !surf)
    return NULL;
  NvDsObjEncUsrArgs args = { 0 };
  args.saveImg = TRUE;
  args.quality = 90;
  g_snprintf (args.fileNameImg, FILE_NAME_SIZE, "%s/%s_crop.jpg", cfg.snap_dir, tag);
  nvds_obj_enc_process (enc_ctx, &args, surf, obj, fm);
  NvDsObjEncUsrArgs fargs = { 0 };
  fargs.saveImg = TRUE;
  fargs.quality = 80;
  fargs.isFrame = TRUE;
  g_snprintf (fargs.fileNameImg, FILE_NAME_SIZE, "%s/%s_frame.jpg", cfg.snap_dir, tag);
  nvds_obj_enc_process (enc_ctx, &fargs, surf, NULL, fm);
  *crop = g_strdup_printf ("%s/%s_crop.jpg", cfg.snap_dir, tag);
  return g_strdup_printf ("%s/%s_frame.jpg", cfg.snap_dir, tag);
}

#define RECT4(r) (r).left, (r).top, (r).width, (r).height
#define JS_PATH(p) (p) ? "\"" : "", (p) ? (p) : "null", (p) ? "\"" : ""

static void
face_recognized (NvDsFrameMeta *fm, NvDsObjectMeta *face, NvDsObjectMeta *person, NvBufSurface *surf,
    FaceTrack *t, const gchar *name, gint64 ts_ms)
{
  guint src = fm->source_id;
  gchar *hit_key = g_strdup_printf ("%s/%s", cam_ids[src], t->id);
  gint64 *last = g_hash_table_lookup (face_last_hit, hit_key);
  gboolean vip = g_str_equal (t->tier, "vip");
  if (last && ts_ms - *last < FACE_COOLDOWN_SEC * 1000) {
    g_free (hit_key);
    return;     /* vừa báo người này ở cam này (track bị cắt rồi nối lại) */
  }
  g_hash_table_replace (face_last_hit, hit_key, g_memdup2 (&ts_ms, sizeof (ts_ms)));
  CNT_INC (n_face_hits);
  gchar *tag = g_strdup_printf ("%s_face_%s_%" G_GINT64_FORMAT, cam_ids[src], t->id, ts_ms);
  gchar *crop, *snap = face_snapshot (surf, fm, face, tag, &crop);
  gchar *now = iso_time (ts_ms);
  enqueue (TRUE, cam_ids[src], "face_recognized", ts_ms, g_strdup_printf (
      "{\"type\":\"face_recognized\",\"cam\":\"%s\",\"ts\":\"%s\",\"tid\":%" G_GUINT64_FORMAT
      ",\"person_id\":\"%s\",\"name\":\"%s\",\"tier\":\"%s\",\"score\":%.3f"
      ",\"bbox\":[%.0f,%.0f,%.0f,%.0f],\"face_bbox\":[%.0f,%.0f,%.0f,%.0f],\"snapshot\":%s%s%s,\"face_crop\":%s%s%s}",
      cam_ids[src], now, person->object_id, t->id, t->name_js, t->tier, t->score,
      RECT4 (person->rect_params), RECT4 (face->rect_params), JS_PATH (snap), JS_PATH (crop)));
  const gchar *grp = cam_groups[src];
  g_print ("%s[KHUÔN MẶT]%s %s%s%s người #%" G_GUINT64_FORMAT " = %s%s (%s, cosine %.2f)\n",
      vip ? YELLOW : GREEN, RESET, grp ? grp : "", grp ? " " : "", cam_ids[src], person->object_id, name,
      vip ? " ★ VIP" : "", t->id, t->score);
  g_free (now); g_free (tag); g_free (snap); g_free (crop);
}

/* Mặt lạ đủ rõ: gửi vector + ảnh crop lên <prefix>:faces_unknown, web cho đăng ký ngay từ camera
 * (vector do chính pipeline tính nên khớp tốt hơn ảnh chụp điện thoại). 1 lần / track. */
static void
face_unknown (NvDsFrameMeta *fm, NvDsObjectMeta *face, NvDsObjectMeta *person, NvBufSurface *surf,
    const gfloat *v, gint64 ts_ms)
{
  guint src = fm->source_id;
  gchar *tag = g_strdup_printf ("%s_unk_%" G_GUINT64_FORMAT "_%" G_GINT64_FORMAT, cam_ids[src], person->object_id, ts_ms);
  gchar *crop, *snap = face_snapshot (surf, fm, face, tag, &crop);
  gchar *b64 = g_base64_encode ((const guchar *) v, FACE_DIM * sizeof (gfloat));
  enqueue_to (faces_unknown_key, TRUE, cam_ids[src], "face_unknown", ts_ms, g_strdup_printf (
      "{\"type\":\"face_unknown\",\"cam\":\"%s\",\"tid\":%" G_GUINT64_FORMAT ",\"ts_ms\":%" G_GINT64_FORMAT
      ",\"bbox\":[%.0f,%.0f,%.0f,%.0f],\"face_bbox\":[%.0f,%.0f,%.0f,%.0f],\"snapshot\":%s%s%s,\"face_crop\":%s%s%s"
      ",\"emb\":\"%s\"}", cam_ids[src], person->object_id, ts_ms,
      RECT4 (person->rect_params), RECT4 (face->rect_params), JS_PATH (snap), JS_PATH (crop), b64));
  g_free (b64); g_free (tag); g_free (snap); g_free (crop);
}

/* 1 object mặt (gie 2) có embedding (gie 3): khớp gallery, gom phiếu theo track người (parent) */
static void
face_update (NvDsFrameMeta *fm, NvDsObjectMeta *face, NvBufSurface *surf, gint64 ts_ms, gint64 mono)
{
  NvDsObjectMeta *person = face->parent;
  if (!person || person->object_id == UNTRACKED_OBJECT_ID || person->class_id != cfg.person_class)
    return;
  const gfloat *raw = NULL;
  for (NvDsMetaList *l = face->obj_user_meta_list; l && !raw; l = l->next) {
    NvDsUserMeta *um = (NvDsUserMeta *) l->data;
    if (um->base_meta.meta_type != NVDSINFER_TENSOR_OUTPUT_META)
      continue;
    NvDsInferTensorMeta *tm = (NvDsInferTensorMeta *) um->user_meta_data;
    if (tm->unique_id == FACE_EMB_GIE && tm->num_output_layers > 0
        && tm->output_layers_info[0].inferDims.numElements == FACE_DIM)
      raw = (const gfloat *) tm->out_buf_ptrs_host[0];
  }
  if (!raw)
    return;     /* mặt nhỏ hơn input-object-min-* của SGIE2: không có embedding */
  gfloat v[FACE_DIM], n = 0;
  for (guint k = 0; k < FACE_DIM; k++)
    n += raw[k] * raw[k];
  if (n < 1e-12f)
    return;
  n = 1.0f / sqrtf (n);
  for (guint k = 0; k < FACE_DIM; k++)
    v[k] = raw[k] * n;

  FaceTrack *t = face_track_get (fm->source_id, person->object_id, mono);
  gfloat score;
  gchar *id = NULL, *name = NULL, *name_js = NULL, *tier = NULL;
  g_mutex_lock (&face_lock);
  const FaceRef *f = face_match (v, cam_companies[fm->source_id], &score);
  if (f && score >= cfg.face_thr) {
    id = g_strdup (f->id);
    name = g_strdup (f->name);
    name_js = g_strdup (f->name_js);
    tier = g_strdup (f->tier);
  }
  g_mutex_unlock (&face_lock);

  if (!id) {
    t->votes = 0;
    if (!t->id && !t->unknown_sent && ++t->unknown_hits >= FACE_UNKNOWN_HITS
        && face->rect_params.height >= FACE_UNKNOWN_MIN_PX) {
      t->unknown_sent = TRUE;
      face_unknown (fm, face, person, surf, v, ts_ms);
    }
    return;
  }
  if (g_strcmp0 (t->cand, id) == 0) {
    t->votes++;
  } else {
    g_free (t->cand);
    t->cand = g_strdup (id);
    t->votes = 1;
  }
  if (g_strcmp0 (t->id, id) == 0) {
    t->score = MAX (t->score, score);
  } else if (t->votes >= FACE_VOTES) {
    g_free (t->id); g_free (t->name_js); g_free (t->tier);
    t->id = id; t->name_js = name_js; t->tier = tier;
    id = name_js = tier = NULL;
    t->score = score;
    face_recognized (fm, face, person, surf, t, name, ts_ms);
  }
  g_free (id); g_free (name); g_free (name_js); g_free (tier);
}

/* Người có kết quả classifier VIP (gie 4): ghi nhãn vào track; "vip" ở người chưa nhận ra mặt -> vip_detected */
static void
vip_update (NvDsFrameMeta *fm, NvDsObjectMeta *obj, NvBufSurface *surf, gint64 ts_ms, gint64 mono)
{
  for (NvDsMetaList *lc = obj->classifier_meta_list; lc; lc = lc->next) {
    NvDsClassifierMeta *cm = (NvDsClassifierMeta *) lc->data;
    if (cm->unique_component_id != VIP_GIE || !cm->label_info_list)
      continue;
    NvDsLabelInfo *li = (NvDsLabelInfo *) cm->label_info_list->data;
    const gchar *label = li->pResult_label ? li->pResult_label : li->result_label;
    FaceTrack *t = face_track_get (fm->source_id, obj->object_id, mono);
    t->cls_tier = g_ascii_strcasecmp (label, "vip") == 0 ? "vip" : "normal";
    t->cls_prob = li->result_prob;
    if (t->cls_tier[0] == 'v' && !t->id && !t->vip_sent) {
      t->vip_sent = TRUE;
      CNT_INC (n_face_hits);
      gchar *tag = g_strdup_printf ("%s_vip_%" G_GUINT64_FORMAT "_%" G_GINT64_FORMAT, cam_ids[fm->source_id],
          obj->object_id, ts_ms);
      gchar *crop, *snap = face_snapshot (surf, fm, obj, tag, &crop);
      gchar *now = iso_time (ts_ms);
      enqueue (TRUE, cam_ids[fm->source_id], "vip_detected", ts_ms, g_strdup_printf (
          "{\"type\":\"vip_detected\",\"cam\":\"%s\",\"ts\":\"%s\",\"tid\":%" G_GUINT64_FORMAT
          ",\"tier\":\"vip\",\"source\":\"classifier\",\"prob\":%.3f,\"bbox\":[%.0f,%.0f,%.0f,%.0f]"
          ",\"snapshot\":%s%s%s,\"crop\":%s%s%s}", cam_ids[fm->source_id], now, obj->object_id, t->cls_prob,
          RECT4 (obj->rect_params), JS_PATH (snap), JS_PATH (crop)));
      g_print ("%s[VIP]%s %s người #%" G_GUINT64_FORMAT " (classifier %.2f, chưa đăng ký khuôn mặt)\n",
          YELLOW, RESET, cam_ids[fm->source_id], obj->object_id, t->cls_prob);
      g_free (now); g_free (tag); g_free (snap); g_free (crop);
    }
  }
}

/* Thêm vào JSON của object người: "face":{...}, "tier", "tier_src" (xem configs/sgie3_vip_classifier.txt) */
static void
face_json (GString *js, guint src, guint64 tid)
{
  guint64 key = ((guint64) src << 48) | (tid & 0xFFFFFFFFFFFFull);
  FaceTrack *t = g_hash_table_lookup (face_tracks, &key);
  if (!t)
    return;
  if (t->id)
    g_string_append_printf (js, ",\"face\":{\"id\":\"%s\",\"name\":\"%s\",\"score\":%.3f},\"tier\":\"%s\","
        "\"tier_src\":\"face\"", t->id, t->name_js, t->score, t->tier);
  else if (t->cls_tier)
    g_string_append_printf (js, ",\"tier\":\"%s\",\"tier_src\":\"classifier\",\"tier_prob\":%.3f",
        t->cls_tier, t->cls_prob);
}

static void
face_sweep (gint64 now_mono)
{
  GHashTableIter it;
  gpointer key, value;
  g_hash_table_iter_init (&it, face_tracks);
  while (g_hash_table_iter_next (&it, &key, &value))
    if ((now_mono - ((FaceTrack *) value)->last_seen_mono_us) / 1e6 >= TRACK_LOST_SEC)
      g_hash_table_iter_remove (&it);
  gint64 now_ms = g_get_real_time () / 1000;
  g_hash_table_iter_init (&it, face_last_hit);
  while (g_hash_table_iter_next (&it, &key, &value))
    if (now_ms - *(gint64 *) value > FACE_COOLDOWN_SEC * 1000)
      g_hash_table_iter_remove (&it);
}

/* ---------------- probe sau nvtracker ---------------- */
static GstPadProbeReturn
tracker_src_probe (GstPad *pad, GstPadProbeInfo *info, gpointer user_data)
{
  GstBuffer *buf = GST_PAD_PROBE_INFO_BUFFER (info);
  NvDsBatchMeta *batch_meta = gst_buffer_get_nvds_batch_meta (buf);
  if (!batch_meta)
    return GST_PAD_PROBE_OK;

  GstMapInfo map;
  NvBufSurface *surf = NULL;
  if (gst_buffer_map (buf, &map, GST_MAP_READ))
    surf = (NvBufSurface *) map.data;
  gint64 mono = g_get_monotonic_time ();

  for (NvDsMetaList *l_frame = batch_meta->frame_meta_list; l_frame; l_frame = l_frame->next) {
    NvDsFrameMeta *fm = (NvDsFrameMeta *) l_frame->data;
    if (fm->source_id >= MAX_SOURCES)
      continue;
    /* ntp_timestamp: giờ hệ thống lúc frame vào streammux (ns) */
    gint64 ts_ms = fm->ntp_timestamp ? (gint64) (fm->ntp_timestamp / 1000000)
        : g_get_real_time () / 1000;
    src_last_frame_us[fm->source_id] = mono;
    gboolean publish = (src_frames[fm->source_id]++ % cfg.publish_every) == 0;
    GString *js = publish ? g_string_sized_new (256 + 128 * fm->num_obj_meta) : NULL;

    if (js)
      g_string_append_printf (js, "{\"cam\":\"%s\",\"frame\":%d,\"ts_ms\":%" G_GINT64_FORMAT
          ",\"w\":%u,\"h\":%u,\"num_obj\":%u,\"objs\":[", cam_ids[fm->source_id], fm->frame_num,
          ts_ms, MUXER_WIDTH, MUXER_HEIGHT, fm->num_obj_meta);

    /* Lượt 1: mặt (gie 2) và classifier VIP (gie 4) cập nhật trạng thái track người trước, để lượt 2
     * ghi được tên / hạng khách vào JSON của người bất kể mặt nằm trước hay sau người trong danh sách */
    for (NvDsMetaList *l_obj = fm->obj_meta_list; l_obj; l_obj = l_obj->next) {
      NvDsObjectMeta *obj = (NvDsObjectMeta *) l_obj->data;
      if (obj->unique_component_id == FACE_DET_GIE && cfg.face_thr > 0)
        face_update (fm, obj, surf, ts_ms, mono);
      else if (obj->classifier_meta_list && obj->object_id != UNTRACKED_OBJECT_ID)
        vip_update (fm, obj, surf, ts_ms, mono);
    }

    gboolean first = TRUE;
    for (NvDsMetaList *l_obj = fm->obj_meta_list; l_obj; l_obj = l_obj->next) {
      NvDsObjectMeta *obj = (NvDsObjectMeta *) l_obj->data;
      NvOSD_RectParams *r = &obj->rect_params;
      gboolean tracked = obj->object_id != UNTRACKED_OBJECT_ID;
      gboolean primary = obj->unique_component_id <= 1;
      if (js) {
        g_string_append_printf (js, "%s{\"cls\":%d,\"label\":\"%s\",\"tid\":%" G_GINT64_FORMAT
            ",\"conf\":%.3f,\"bbox\":[%.1f,%.1f,%.1f,%.1f]", first ? "" : ",", obj->class_id,
            obj->obj_label, tracked ? (gint64) obj->object_id : -1, obj->confidence,
            r->left, r->top, r->width, r->height);
        /* Object do SGIE tạo (mặt): "gie" = tầng tạo ra nó, "ptid" = tracking ID của người chứa nó */
        if (!primary)
          g_string_append_printf (js, ",\"gie\":%d,\"ptid\":%" G_GINT64_FORMAT, obj->unique_component_id,
              obj->parent && obj->parent->object_id != UNTRACKED_OBJECT_ID ? (gint64) obj->parent->object_id : -1);
        else if (tracked && obj->class_id == cfg.person_class) {
          face_json (js, fm->source_id, obj->object_id);
          if (cam_roi_n[fm->source_id] >= 3)
            g_string_append_printf (js, ",\"in_roi\":%s", roi_contains (fm->source_id, r) ? "true" : "false");
        }
        /* Kết quả classifier SGIE (-S): "attrs":[{"gie":2,"label":"sitting","prob":0.91}] */
        if (obj->classifier_meta_list) {
          gboolean fa = TRUE;
          g_string_append (js, ",\"attrs\":[");
          for (NvDsMetaList *lc = obj->classifier_meta_list; lc; lc = lc->next) {
            NvDsClassifierMeta *cm = (NvDsClassifierMeta *) lc->data;
            for (NvDsMetaList *ll = cm->label_info_list; ll; ll = ll->next) {
              NvDsLabelInfo *li = (NvDsLabelInfo *) ll->data;
              g_string_append_printf (js, "%s{\"gie\":%d,\"label\":\"%s\",\"prob\":%.3f}",
                  fa ? "" : ",", cm->unique_component_id,
                  li->pResult_label ? li->pResult_label : li->result_label, li->result_prob);
              fa = FALSE;
            }
          }
          g_string_append_c (js, ']');
        }
        g_string_append_c (js, '}');
        first = FALSE;
      }
      if (tracked && primary && obj->class_id == cfg.person_class) {
        if (roi_contains (fm->source_id, r))
          dwell_update (fm, obj, surf, ts_ms, mono);
        else
          dwell_leave (fm->source_id, obj->object_id);
      }
    }
    if (js) {
      g_string_append (js, "]}");
      enqueue (FALSE, cam_ids[fm->source_id], "frame", ts_ms, g_string_free (js, FALSE));
    }
  }

  if (enc_ctx)
    nvds_obj_enc_finish (enc_ctx);   /* đợi các ảnh JPEG của batch này ghi xong */
  if (surf)
    gst_buffer_unmap (buf, &map);

  if (mono - last_sweep_us > G_USEC_PER_SEC) {
    dwell_sweep (mono, FALSE);
    face_sweep (mono);
    last_sweep_us = mono;
  }
  return GST_PAD_PROBE_OK;
}

/* ---------------- pipeline ---------------- */
static GMainLoop *loop;
static GstElement *pipeline;

/* Lỗi từ 1 camera (element nằm trong bin src-N), hoặc từ element không còn thuộc pipeline: bin camera cũ đã bị
 * watchdog gỡ ra để thay bin mới (replace_source) nhưng rtspsrc bên trong vẫn báo lỗi muộn, vd. sau khi app khởi
 * động lâu vì build engine TensorRT -> không được coi là lỗi của cả pipeline */
static gboolean
is_from_source (GstObject *obj)
{
  GstObject *top = NULL;
  for (GstObject *o = obj; o; o = GST_OBJECT_PARENT (o)) {
    if (g_str_has_prefix (GST_OBJECT_NAME (o), "src-"))
      return TRUE;
    top = o;
  }
  return top != GST_OBJECT (pipeline);
}

static gboolean
bus_call (GstBus *bus, GstMessage *msg, gpointer data)
{
  switch (GST_MESSAGE_TYPE (msg)) {
    case GST_MESSAGE_EOS:
      g_print ("End of stream\n");
      g_main_loop_quit (loop);
      break;
    case GST_MESSAGE_WARNING: {
      GError *err;
      gst_message_parse_warning (msg, &err, NULL);
      g_printerr ("WARNING from %s: %s\n", GST_OBJECT_NAME (msg->src), err->message);
      g_error_free (err);
      break;
    }
    case GST_MESSAGE_ERROR: {
      GError *err;
      gchar *debug;
      gst_message_parse_error (msg, &err, &debug);
      /* Lỗi từ 1 camera (mất mạng, sai URL...) không được làm sập cả pipeline:
       * nvurisrcbin tự reconnect theo rtsp-reconnect-interval */
      if (is_from_source (msg->src)) {
        g_printerr ("ERROR (camera, bỏ qua, chờ reconnect) from %s: %s\n",
            GST_OBJECT_NAME (msg->src), err->message);
      } else {
        g_printerr ("ERROR from %s: %s\n%s\n", GST_OBJECT_NAME (msg->src), err->message,
            debug ? debug : "");
        g_main_loop_quit (loop);
      }
      g_free (debug);
      g_error_free (err);
      break;
    }
    default:
      break;
  }
  return TRUE;
}

static gboolean
send_eos_to_pad (GstElement *element, GstPad *pad, gpointer data)
{
  gst_pad_send_event (pad, gst_event_new_eos ());
  return TRUE;
}

static gboolean
on_shutdown_timeout (gpointer data)
{
  g_printerr ("Không nhận được EOS sau %ds, thoát cứng\n", SHUTDOWN_TIMEOUT_SEC);
  g_main_loop_quit (loop);
  return G_SOURCE_REMOVE;
}

/* Ctrl+C: dừng các clip smart record đang ghi (để file MP4 được đóng đúng, không mất moov),
 * rồi đẩy EOS vào từng sink pad của mux. Có hẹn giờ thoát cứng để không bao giờ treo. */
static gboolean
on_sigint (gpointer data)
{
  static gboolean sent = FALSE;
  if (sent) {
    g_main_loop_quit (loop);
    return G_SOURCE_REMOVE;
  }
  g_print ("\nSIGINT: dừng smart record, gửi EOS (Ctrl+C lần nữa để thoát ngay)\n");
  g_atomic_int_set (&shutting_down, 1);
  for (guint i = 0; i < num_sources; i++)
    if (g_atomic_int_get (&sr_busy[i]) && sr_session[i] != G_MAXUINT)
      g_signal_emit_by_name (src_elems[i], "stop-sr", sr_session[i]);
  gst_element_foreach_sink_pad (GST_ELEMENT (data), send_eos_to_pad, NULL);
  g_timeout_add_seconds (SHUTDOWN_TIMEOUT_SEC, on_shutdown_timeout, NULL);
  sent = TRUE;
  return G_SOURCE_CONTINUE;
}

/* ---------------- watchdog camera (reconnect song song) ---------------- */
static void
camera_status_event (guint idx, const gchar *type, gdouble silent_s)
{
  gint64 now_ms = g_get_real_time () / 1000;
  gchar *ts = iso_time (now_ms);
  gchar *data = g_strdup_printf ("{\"type\":\"%s\",\"cam\":\"%s\",\"uri\":\"%s\",\"ts\":\"%s\","
      "\"silent_s\":%.1f}", type, cam_ids[idx], cam_uris[idx], ts, silent_s);
  g_print ("[CAM] %s %s (%.0fs không có hình)\n", type, cam_ids[idx], silent_s);
  enqueue (TRUE, cam_ids[idx], type, now_ms, data);
  g_free (ts);
}

static GstElement *create_source (guint idx);

/* Bước 2 (main loop, nhanh): gỡ bin cũ khỏi pipeline, tạo nvurisrcbin mới và link lại vào
 * đúng sink pad cũ của mux (on_src_pad_added). Tạo mới thay vì NULL->PLAYING tại chỗ:
 * nvurisrcbin không nối lại ghost pad sau khi đã có dữ liệu, frame sẽ không tới mux nữa.
 * Không release request pad của mux: release lúc mux đang "chờ nguồn" bị deadlock. */
static gboolean
replace_source (gpointer data)
{
  guint idx = GPOINTER_TO_UINT (data) - 1;
  gst_bin_remove (GST_BIN (pipeline), src_elems[idx]);
  if (!g_atomic_int_get (&shutting_down)) {
    src_elems[idx] = create_source (idx);
    gst_bin_add (GST_BIN (pipeline), src_elems[idx]);
    gst_element_sync_state_with_parent (src_elems[idx]);
  }
  src_frames_at_reset[idx] = src_frames[idx];
  src_last_reset_us[idx] = g_get_monotonic_time ();
  g_atomic_int_set (&src_resetting[idx], 0);
  return G_SOURCE_REMOVE;
}

/* Bước 1 (thread pool, chậm ~vài giây/cam): đưa bin cũ về NULL song song cho mọi cam lỗi */
static void
reset_source_worker (gpointer data, gpointer user_data)
{
  guint idx = GPOINTER_TO_UINT (data) - 1;
  gst_element_set_state (src_elems[idx], GST_STATE_NULL);
  g_idle_add (replace_source, data);
}

static gboolean
watch_sources (gpointer data)
{
  if (g_atomic_int_get (&shutting_down))
    return G_SOURCE_REMOVE;
  gint64 now = g_get_monotonic_time ();
  for (guint i = 0; i < num_sources; i++) {
    gdouble silent = (now - src_last_frame_us[i]) / 1e6;
    gboolean eos = g_atomic_int_compare_and_exchange (&src_eos[i], 1, 0);
    /* Vừa nối, decoder đang nhận dữ liệu nhưng chưa ra frame nào (chờ IDR: GOP dài, cam fps thấp):
     * reset lúc này chỉ làm lại từ đầu, không bao giờ kịp tới IDR. Chờ tối đa FIRST_FRAME_MAX_SEC */
    gboolean waiting_idr = src_frames[i] == src_frames_at_reset[i]
        && src_last_data_us[i] > src_last_reset_us[i] && (now - src_last_data_us[i]) / 1e6 < 2
        && (now - src_last_reset_us[i]) / 1e6 < FIRST_FRAME_MAX_SEC;
    if (!eos && (silent < RECONNECT_SEC || waiting_idr)) {
      if (src_frames[i] == src_frames_at_reset[i])
        continue;   /* chưa có hình kể từ lần reset gần nhất: chưa báo online */
      if (!src_online[i]) {
        src_online[i] = TRUE;
        camera_status_event (i, "camera_online", 0);
      }
      continue;
    }
    if (src_online[i]) {
      src_online[i] = FALSE;
      camera_status_event (i, "camera_offline", silent);
    }
    if (src_is_rtsp[i] && (eos || (now - src_last_reset_us[i]) / 1e6 >= RECONNECT_SEC)
        && g_atomic_int_compare_and_exchange (&src_resetting[i], 0, 1)) {
      g_print ("[CAM] reset %s\n", cam_ids[i]);
      g_thread_pool_push (reset_pool, GUINT_TO_POINTER (i + 1), NULL);
    }
  }
  return G_SOURCE_CONTINUE;
}

static gboolean
print_stats (gpointer data)
{
  guint64 total = 0, active = 0;
  guint64 delta[MAX_SOURCES];
  GString *line = g_string_new ("");
  gchar face_line[96] = "";
  if (cfg.face_thr > 0)
    g_snprintf (face_line, sizeof (face_line), " | face: gallery=%u người, track=%u, nhận ra=%" G_GINT64_FORMAT,
        g_atomic_int_get ((gint *) &face_people), g_hash_table_size (face_tracks), CNT_GET (n_face_hits));
  for (guint i = 0; i < num_sources; i++) {
    guint64 d = delta[i] = src_frames[i] - src_frames_prev[i];
    src_frames_prev[i] = src_frames[i];
    total += d;
    active += d > 0;
    if (i < 8 && !cam_buildings[0])
      g_string_append_printf (line, " %s=%.1f", cam_ids[i], (gdouble) d / cfg.stats_sec);
  }
  /* Có toà nhà (-i): in theo công ty/toà, vd. " acme/A=4/4 acme/B=7/8 (mất acme-B-cam5)" */
  for (guint i = 0; i < num_sources && cam_buildings[0]; i++) {
    gboolean seen = FALSE;
    for (guint j = 0; j < i && !seen; j++)
      seen = g_strcmp0 (cam_groups[j], cam_groups[i]) == 0;
    if (seen)
      continue;
    guint n = 0, up = 0;
    GString *lost = g_string_new ("");
    for (guint j = i; j < num_sources; j++) {
      if (g_strcmp0 (cam_groups[j], cam_groups[i]) != 0)
        continue;
      n++;
      if (delta[j])
        up++;
      else
        g_string_append_printf (lost, "%s%s", lost->len ? "," : "", cam_ids[j]);
    }
    g_string_append_printf (line, " %s=%u/%u%s%s%s", cam_groups[i] ? cam_groups[i] : "-",
        up, n, lost->len ? " (mất " : "", lost->str, lost->len ? ")" : "");
    g_string_free (lost, TRUE);
  }
  /* Cho exporter Prometheus (tools/ds_exporter.py): JSON 1 dòng, thread Redis ghi vào <prefix>:stats:g<GPU> */
  GString *sj = g_string_new (NULL);
  glong rss_kb = 0;
  gchar *status = NULL;
  if (g_file_get_contents ("/proc/self/status", &status, NULL, NULL)) {
    const gchar *v = strstr (status, "VmRSS:");
    rss_kb = v ? atol (v + 6) : 0;
    g_free (status);
  }
  g_string_printf (sj, "{\"ts\":%" G_GINT64_FORMAT ",\"gpu\":%u,\"pid\":%d,\"fps_total\":%.1f,\"cams_online\":%"
      G_GUINT64_FORMAT ",\"cams_total\":%u,\"queue\":%d,\"sent\":%" G_GINT64_FORMAT ",\"drop\":%" G_GINT64_FORMAT
      ",\"events\":%" G_GINT64_FORMAT ",\"events_created\":%" G_GINT64_FORMAT ",\"dwell\":%u,\"redis_up\":%d"
      ",\"rss_mb\":%.1f,\"face_hits\":%" G_GINT64_FORMAT ",\"cams\":{", g_get_real_time () / 1000, cfg.gpu, getpid (),
      (gdouble) total / cfg.stats_sec, active, num_sources, MAX (0, g_async_queue_length (rqueue)),
      CNT_GET (n_published), CNT_GET (n_dropped), CNT_GET (n_events), CNT_GET (n_events_created),
      g_hash_table_size (dwell), g_atomic_int_get (&redis_up), rss_kb / 1024.0, CNT_GET (n_face_hits));
  for (guint i = 0; i < num_sources; i++)
    g_string_append_printf (sj, "%s\"%s\":%.1f", i ? "," : "", cam_ids[i], (gdouble) delta[i] / cfg.stats_sec);
  g_string_append (sj, "}}");
  g_mutex_lock (&stats_lock);
  g_free (stats_json);
  stats_json = g_string_free (sj, FALSE);
  g_mutex_unlock (&stats_lock);

  g_print ("[stats] %.1f FPS tổng, %" G_GUINT64_FORMAT "/%u cam có hình |%s%s | redis=%s "
      "queue=%d sent=%" G_GINT64_FORMAT " drop=%" G_GINT64_FORMAT " events=%" G_GINT64_FORMAT "/%" G_GINT64_FORMAT
      " | dwell=%u%s\n", (gdouble) total / cfg.stats_sec, active, num_sources, line->str,
      num_sources > 8 && !cam_buildings[0] ? " ..." : "", g_atomic_int_get (&redis_up) ? "UP" : "DOWN",
      MAX (0, g_async_queue_length (rqueue)), CNT_GET (n_published),
      CNT_GET (n_dropped), CNT_GET (n_events), CNT_GET (n_events_created),
      g_hash_table_size (dwell), face_line);
  g_string_free (line, TRUE);
  return G_SOURCE_CONTINUE;
}

/* EOS từ camera RTSP (NVR/server đóng phiên) không được tới mux, nếu không đủ N nguồn EOS là
 * cả pipeline dừng. Chặn lại và đánh dấu reset; EOS do chính app gửi khi tắt thì cho qua. */
static GstPadProbeReturn
mux_sink_event_probe (GstPad *pad, GstPadProbeInfo *info, gpointer data)
{
  guint idx = GPOINTER_TO_UINT (data);
  if (GST_EVENT_TYPE (GST_PAD_PROBE_INFO_EVENT (info)) != GST_EVENT_EOS
      || g_atomic_int_get (&shutting_down) || !src_is_rtsp[idx])
    return GST_PAD_PROBE_OK;
  g_print ("[CAM] %s gửi EOS (server đóng phiên) -> chặn, reset nguồn\n", cam_ids[idx]);
  g_atomic_int_set (&src_eos[idx], 1);
  return GST_PAD_PROBE_DROP;
}

/* fps của cam (-i, cột 5): bỏ bớt frame ngay trước streammux, nên nvinfer/tracker chỉ chạy chừng ấy
 * frame/giây cho cam đó. Smart record không ảnh hưởng (ghi từ luồng nén bên trong nvurisrcbin). */
static GstPadProbeReturn
fps_limit_probe (GstPad *pad, GstPadProbeInfo *info, gpointer data)
{
  guint idx = GPOINTER_TO_UINT (data);
  if (cam_fps[idx] <= 0)
    return GST_PAD_PROBE_OK;
  gint64 now = g_get_monotonic_time ();
  /* dung sai 10% để nguồn 30 fps giới hạn 15 vẫn ra đúng 15, không tụt xuống 10 vì lệch nhịp */
  if (now - cam_last_kept_us[idx] < 0.9 * G_USEC_PER_SEC / cam_fps[idx])
    return GST_PAD_PROBE_DROP;
  cam_last_kept_us[idx] = now;
  return GST_PAD_PROBE_OK;
}

static void
on_src_pad_added (GstElement *src, GstPad *pad, gpointer data)
{
  GstElement *mux = (GstElement *) data;
  guint idx = GPOINTER_TO_UINT (g_object_get_data (G_OBJECT (src), "src-index"));
  GstCaps *caps = gst_pad_get_current_caps (pad);
  if (!caps)
    caps = gst_pad_query_caps (pad, NULL);
  const gchar *name = gst_structure_get_name (gst_caps_get_structure (caps, 0));
  if (g_str_has_prefix (name, "video")) {
    gchar pad_name[16];
    g_snprintf (pad_name, sizeof (pad_name), "sink_%u", idx);
    GstPad *sinkpad = gst_element_get_static_pad (mux, pad_name);
    if (!sinkpad) {   /* lần đầu; khi reconnect pad đã có sẵn */
      sinkpad = gst_element_request_pad_simple (mux, pad_name);
      gst_pad_add_probe (sinkpad, GST_PAD_PROBE_TYPE_EVENT_DOWNSTREAM, mux_sink_event_probe,
          GUINT_TO_POINTER (idx), NULL);
      gst_pad_add_probe (sinkpad, GST_PAD_PROBE_TYPE_BUFFER, fps_limit_probe, GUINT_TO_POINTER (idx), NULL);
    }
    if (gst_pad_is_linked (sinkpad) || gst_pad_link (pad, sinkpad) == GST_PAD_LINK_OK)
      g_print ("%s đã vào streammux\n", cam_ids[idx]);
    else
      g_printerr ("Không link được %s vào streammux\n", cam_ids[idx]);
    gst_object_unref (sinkpad);
  }
  gst_caps_unref (caps);
}

/* In thẳng UTF-8: g_print tự đổi charset theo locale, mà locale có thể bị thư viện khác
 * (TensorRT) đổi giữa chừng -> tiếng Việt thành '?' */
static void
print_raw (const gchar *s)
{
  fputs (s, stdout);
  fflush (stdout);
  if (log_fp) {
    fputs (s, log_fp);
    fflush (log_fp);
  }
}

static void
printerr_raw (const gchar *s)
{
  fputs (s, stderr);
  if (log_fp) {
    fputs (s, log_fp);
    fflush (log_fp);
  }
}

static gchar *
cam_id_from_uri (const gchar *uri, guint idx)
{
  const gchar *slash = strrchr (uri, '/');
  if (slash && slash[1] && !strchr (slash + 1, '.'))
    return g_strdup (slash + 1);
  return g_strdup_printf ("src%u", idx);
}

/* Gói nén vào decoder = camera vẫn gửi dữ liệu, kể cả khi chưa ra hình (NVDEC chờ IDR đầu tiên) */
static GstPadProbeReturn
decoder_input_probe (GstPad *pad, GstPadProbeInfo *info, gpointer data)
{
  src_last_data_us[GPOINTER_TO_UINT (data)] = g_get_monotonic_time ();
  return GST_PAD_PROBE_OK;
}

static void
on_deep_element_added (GstBin *bin, GstBin *sub_bin, GstElement *elem, gpointer data)
{
  GstElementFactory *f = gst_element_get_factory (elem);
  if (!f || !g_str_has_prefix (GST_OBJECT_NAME (f), "nvv4l2decoder"))
    return;
  GstPad *sink = gst_element_get_static_pad (elem, "sink");
  if (!sink)
    return;
  gst_pad_add_probe (sink, GST_PAD_PROBE_TYPE_BUFFER, decoder_input_probe, data, NULL);
  gst_object_unref (sink);
}

static GstElement *
create_source (guint idx)
{
  gchar name[32];
  g_snprintf (name, sizeof (name), "src-%u", idx);
  GstElement *src = gst_element_factory_make ("nvurisrcbin", name);
  g_object_set (src, "uri", cam_uris[idx], "gpu-id", cfg.gpu, "cudadec-memtype", 0, NULL);
  if (src_is_rtsp[idx]) {
    /* TCP. Tắt reconnect có sẵn (tuần tự), dùng watch_sources() thay thế */
    g_object_set (src, "latency", 200, "rtsp-reconnect-interval", 0,
        "select-rtp-protocol", 4, NULL);
    g_signal_connect (src, "deep-element-added", G_CALLBACK (on_deep_element_added), GUINT_TO_POINTER (idx));
    if (cfg.sr_cache) {
      g_object_set (src, "smart-record", 2, "smart-rec-dir-path", cfg.clip_dir,
          "smart-rec-file-prefix", cam_ids[idx], "smart-rec-cache", cfg.sr_cache,
          "smart-rec-container", 0, "smart-rec-default-duration",
          cfg.sr_cache + SR_POST_SEC, NULL);
      g_signal_connect (src, "sr-done", G_CALLBACK (on_sr_done), GUINT_TO_POINTER (idx));
    }
  }
  sr_session[idx] = G_MAXUINT;
  g_atomic_int_set (&sr_busy[idx], 0);
  g_object_set_data (G_OBJECT (src), "src-index", GUINT_TO_POINTER (idx));
  g_signal_connect (src, "pad-added", G_CALLBACK (on_src_pad_added), mux_elem);
  return src;
}

static gboolean
add_camera (const gchar *company, const gchar *building, const gchar *id, const gchar *uri, gdouble fps,
    const gchar *info)
{
  if (num_sources >= MAX_SOURCES) {
    g_printerr ("Quá %d camera\n", MAX_SOURCES);
    return FALSE;
  }
  guint i = num_sources;
  cam_uris[i] = g_strdup (uri);
  cam_ids[i] = id ? g_strdup (id) : cam_id_from_uri (uri, i);
  cam_buildings[i] = g_strdup (building);
  cam_companies[i] = g_strdup (company);
  cam_fps[i] = fps;
  cam_info[i] = g_strdup (info);
  roi_parse (i, info);
  cam_groups[i] = company ? g_strdup_printf ("%s/%s", company, building) : g_strdup (building);
  for (guint j = 0; j < i; j++) {
    if (g_str_equal (cam_ids[j], cam_ids[i])) {
      g_printerr ("Trùng cam_id %s (cam_id phải khác nhau giữa các công ty/toà)\n", cam_ids[i]);
      return FALSE;
    }
  }
  num_sources++;
  return TRUE;
}

/* -i file: mỗi dòng "[công ty] <toà nhà> <cam_id> <uri> [fps [JSON]]", dòng bắt đầu bằng # là chú thích.
 * JSON (phần còn lại của dòng, có thể chứa dấu cách) chỉ được kiểm tra sơ bộ rồi đẩy nguyên lên Redis */
static gboolean
load_camera_list (const gchar *path)
{
  gchar *text = NULL;
  GError *err = NULL;
  if (!g_file_get_contents (path, &text, NULL, &err)) {
    g_printerr ("Không đọc được %s: %s\n", path, err->message);
    g_error_free (err);
    return FALSE;
  }
  gchar **lines = g_strsplit (text, "\n", -1);
  gboolean ok = TRUE;
  for (guint n = 0; lines[n] && ok; n++) {
    gchar *l = g_strstrip (lines[n]);
    if (!*l || *l == '#')
      continue;
    GRegex *ws = g_regex_new ("\\s+", 0, 0, NULL);
    gchar **tok = g_regex_split_full (ws, l, -1, 0, 0, 6, NULL);
    g_regex_unref (ws);
    guint nt = g_strv_length (tok);
    /* 6 cột: có JSON thông tin; 5 cột: có fps; 4 cột: có công ty; 3 cột: chỉ toà */
    gdouble fps = 0;
    const gchar *info = NULL;
    if (nt == 6) {
      info = tok[5];
      if (info[0] != '{' || !g_str_has_suffix (info, "}") || strchr (info, '\n')) {
        g_printerr ("%s dòng %u: cột 6 phải là JSON object {...}\n", path, n + 1);
        ok = FALSE;
      }
      nt = 5;
    }
    if (ok && nt == 5) {
      gchar *end;
      fps = g_ascii_strtod (tok[4], &end);
      if (*end || fps < 0 || fps > 120) {
        g_printerr ("%s dòng %u: fps '%s' không hợp lệ (0 = mọi frame)\n", path, n + 1, tok[4]);
        ok = FALSE;
      }
      nt = 4;
    } else if (ok && nt != 3 && nt != 4) {
      g_printerr ("%s dòng %u: cần \"[công ty] <toà nhà> <cam_id> <uri> [fps [JSON]]\"\n", path, n + 1);
      ok = FALSE;
    }
    if (ok) {
      /* Tên công ty/toà/cam đi vào tên file ảnh/clip và JSON: chỉ cho chữ, số, _ . - */
      for (guint k = 0; k + 1 < nt && ok; k++)
        ok = g_regex_match_simple ("^[A-Za-z0-9_.-]+$", tok[k], 0, 0);
      if (!ok)
        g_printerr ("%s dòng %u: tên công ty/toà/cam chỉ gồm chữ, số, _ . -\n", path, n + 1);
      else if (nt == 4)
        ok = add_camera (tok[0], tok[1], tok[2], tok[3], fps, info);
      else
        ok = add_camera (NULL, tok[0], tok[1], tok[2], fps, info);
    }
    g_strfreev (tok);
  }
  g_strfreev (lines);
  g_free (text);
  return ok;
}

/* ---------------- tự xoá ảnh/clip cũ (thread riêng) ---------------- */
typedef struct {
  gchar *path;
  gint64 mtime;
  gint64 size;
} OutFile;

static void
out_file_free (gpointer p)
{
  g_free (((OutFile *) p)->path);
  g_free (p);
}

static gint
out_file_cmp (gconstpointer a, gconstpointer b)
{
  gint64 x = (*(OutFile **) a)->mtime, y = (*(OutFile **) b)->mtime;
  return (x > y) - (x < y);
}

static void
retention_scan (const gchar *dir, GPtrArray *files)
{
  GDir *d = g_dir_open (dir, 0, NULL);
  if (!d)
    return;
  const gchar *name;
  while ((name = g_dir_read_name (d))) {
    if (!g_str_has_suffix (name, ".mp4") && !g_str_has_suffix (name, ".jpg"))
      continue;
    gchar *p = g_build_filename (dir, name, NULL);
    GStatBuf st;
    if (g_stat (p, &st) == 0 && S_ISREG (st.st_mode)) {
      OutFile *f = g_new (OutFile, 1);
      f->path = p;
      f->mtime = st.st_mtime;
      f->size = st.st_size;
      g_ptr_array_add (files, f);
    } else {
      g_free (p);
    }
  }
  g_dir_close (d);
}

/* Xoá file cũ nhất trước: quá -k ngày, hoặc cho tới khi tổng dung lượng về dưới -K GB.
 * Có upload (-M): lượt 1 chỉ xoá file đã upload (có <file>.up do tools/uploader.py ghi); vẫn vượt -K (S3 chết lâu)
 * thì lượt 2 xoá cả file chưa upload, kèm cảnh báo, để không đầy đĩa làm hỏng ghi clip/ảnh mới */
static void
retention_run (void)
{
  GPtrArray *files = g_ptr_array_new_with_free_func (out_file_free);
  retention_scan (cfg.snap_dir, files);
  retention_scan (cfg.clip_dir, files);
  g_ptr_array_sort (files, out_file_cmp);
  gint64 now = g_get_real_time () / G_USEC_PER_SEC, total = 0, freed = 0;
  gint64 cap = (gint64) (cfg.keep_gb * 1e9);
  guint n = 0, n_unuploaded = 0;
  for (guint i = 0; i < files->len; i++)
    total += ((OutFile *) g_ptr_array_index (files, i))->size;
  for (guint pass = 0; pass < (cfg.media_base ? 2 : 1); pass++) {
    for (guint i = 0; i < files->len; i++) {
      OutFile *f = g_ptr_array_index (files, i);
      if (f->size < 0)
        continue;   /* đã xoá ở lượt 1 */
      if (now - f->mtime < RETENTION_MIN_AGE_SEC)
        break;      /* đã sắp theo thời gian: các file sau còn mới hơn */
      gboolean old = cfg.keep_days && now - f->mtime > (gint64) cfg.keep_days * 86400;
      gboolean over = cap && total > cap;
      gchar *marker = g_strdup_printf ("%s.up", f->path);
      gboolean uploaded = !cfg.media_base || g_file_test (marker, G_FILE_TEST_EXISTS);
      gboolean del = pass == 0 ? uploaded && (old || over) : !uploaded && over;
      if (del && g_unlink (f->path) == 0) {
        g_unlink (marker);
        total -= f->size;
        freed += f->size;
        n++;
        n_unuploaded += !uploaded;
        f->size = -1;
      }
      g_free (marker);
    }
  }
  if (n)
    g_print ("[dọn file] xoá %u ảnh/clip cũ nhất (%.1f MB; giữ %u ngày, tối đa %.0f GB), còn %.2f GB\n",
        n, freed / 1e6, cfg.keep_days, cfg.keep_gb, total / 1e9);
  if (n_unuploaded)
    g_printerr ("[dọn file] CẢNH BÁO: vượt %.0f GB nên đã xoá %u file CHƯA upload (uploader/S3 có chạy không?)\n",
        cfg.keep_gb, n_unuploaded);
  g_ptr_array_free (files, TRUE);
}

static gpointer
retention_thread_func (gpointer data)
{
  while (!g_atomic_int_get (&shutting_down)) {
    retention_run ();
    for (guint s = 0; s < RETENTION_EVERY_SEC && !g_atomic_int_get (&shutting_down); s++)
      g_usleep (G_USEC_PER_SEC);
  }
  return NULL;
}

/* Mỗi prefix Redis chỉ 1 bản app: 2 bản cùng chạy gửi trùng mọi frame/sự kiện và tốn gấp đôi GPU.
 * Khoá tự nhả khi tiến trình thoát (kể cả bị kill), fd giữ mở suốt đời app. */
static gboolean
acquire_instance_lock (void)
{
  /* Mỗi GPU 1 bản (-g): 2 instance cùng prefix trên 2 GPU, mỗi bản 1 bộ camera riêng */
  gchar *path = cfg.gpu ? g_strdup_printf ("/tmp/w1-app-%s-gpu%u.lock", cfg.prefix, cfg.gpu)
      : g_strdup_printf ("/tmp/w1-app-%s.lock", cfg.prefix);
  int fd = open (path, O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    g_printerr ("Không mở được %s, bỏ qua kiểm tra chạy trùng\n", path);
    g_free (path);
    return TRUE;
  }
  /* Bản cũ vừa bị dừng/kill có thể còn vài chục giây nhả GPU rồi mới nhả khoá: chờ tối đa LOCK_WAIT_SEC */
  gboolean locked = flock (fd, LOCK_EX | LOCK_NB) == 0;
  for (guint i = 0; !locked && i < LOCK_WAIT_SEC; i++) {
    if (i == 0)
      g_printerr ("w1-app khác (prefix '%s') đang giữ khoá, chờ tối đa %ds cho nó thoát hẳn...\n", cfg.prefix,
          LOCK_WAIT_SEC);
    g_usleep (G_USEC_PER_SEC);
    locked = flock (fd, LOCK_EX | LOCK_NB) == 0;
  }
  if (!locked) {
    gchar pid[32] = "";
    ssize_t n = read (fd, pid, sizeof (pid) - 1);
    g_strstrip (pid);
    g_printerr ("Đã có w1-app khác đang chạy với prefix Redis '%s' (pid %s). Hai bản cùng chạy sẽ gửi trùng "
        "mọi frame/sự kiện.\nDừng bản cũ trước: docker exec deepstream_dev pkill -INT -f "
        "'^/workspace/apps/w1_multistream/w1-app' (hoặc dùng -P <prefix> khác để test)\n",
        cfg.prefix, n > 0 ? pid : "?");
    close (fd);
    g_free (path);
    return FALSE;
  }
  gchar *pid = g_strdup_printf ("%d\n", getpid ());
  if (ftruncate (fd, 0) != 0 || write (fd, pid, strlen (pid)) < 0)
    g_printerr ("Không ghi được pid vào %s\n", path);
  g_free (pid);
  g_free (path);
  return TRUE;
}

/* Model riêng (-c): batch = số camera nên engine phải theo số camera, vd. model-engine-file=.../model_b4_gpu0_fp16.engine
 * -> model_b19_gpu0_fp16.engine. Engine tạo qua engine-create-func-name (YOLO của DeepStream-Yolo) được nvinfer lưu
 * vào THƯ MỤC HIỆN TẠI với tên model_b<batch>_gpu0_<fp16|int8|fp32>.engine: chuyển vào thư mục của model-engine-file
 * để engine build lần đầu nằm đúng chỗ, lần sau nạp lại được thay vì build lại (YOLO ~5 phút). */
static void
pgie_engine_for_batch (GstElement *pgie, const gchar *path)
{
  GKeyFile *kf = g_key_file_new ();
  gchar *engine = g_key_file_load_from_file (kf, path, G_KEY_FILE_NONE, NULL)
      ? g_key_file_get_string (kf, "property", "model-engine-file", NULL) : NULL;
  g_key_file_free (kf);
  if (!engine)
    return;
  g_strstrip (engine);
  gchar *repl = g_strdup_printf ("_b%u_gpu%u_", num_sources, cfg.gpu);
  GRegex *re = g_regex_new ("_b[0-9]+_gpu[0-9]+_", 0, 0, NULL);
  gchar *fixed = g_regex_replace_literal (re, engine, -1, 0, repl, 0, NULL);
  g_regex_unref (re);
  g_object_set (pgie, "model-engine-file", fixed, NULL);
  gchar *dir = g_path_get_dirname (fixed);
  if (g_mkdir_with_parents (dir, 0755) == 0 && chdir (dir) == 0)
    g_print ("PGIE engine: %s%s\n", fixed, g_file_test (fixed, G_FILE_TEST_EXISTS) ? "" : " (chưa có, build lần đầu vài phút)");
  g_free (dir);
  g_free (fixed);
  g_free (repl);
  g_free (engine);
}

static void
usage (const char *prog)
{
  g_printerr (
      "Dùng: %s [tuỳ chọn] <uri> [uri ...]\n"
      "      %s [tuỳ chọn] -i cameras.txt\n"
      "  -i file       danh sách camera, mỗi dòng: [công ty] <toà nhà> <cam_id> <uri> [fps, 0 = mọi frame]\n"
      "  -R host:port  Redis (mặc định 127.0.0.1:6390, mật khẩu qua env REDIS_PASSWORD)\n"
      "  -P prefix     tiền tố key Redis (mặc định ds -> ds:frames, ds:events, ds:cameras)\n"
      "  -L maxlen     độ dài tối đa stream frames (mặc định 100000)\n"
      "  -f N          gửi metadata 1/N frame mỗi cam (mặc định 1 = mọi frame)\n"
      "  -t giây       ngưỡng ở yên để cảnh báo (mặc định 120)\n"
      "  -m tỉ lệ      dịch chuyển tối đa còn coi là yên, theo chiều cao bbox (mặc định 0.3)\n"
      "  -r giây       bật smart record, cache giây (cần >= -t để clip phủ từ đầu; 0 = tắt)\n"
      "  -s giây       chu kỳ in thống kê (mặc định 5)\n"
      "  -l file       ghi log (mặc định /outputs/logs/w1_<giờ>.log; cảnh báo riêng ở alerts.log)\n"
      "  -A            không vẽ bbox lên clip (mặc định có, ra file *_bbox.mp4)\n"
      "  -c file       config nvinfer của model detect (mặc định model mẫu TrafficCamNet)\n"
      "  -p id         class ID của 'người' trong model để tính ở yên (mặc định 2)\n"
      "  -S file       thêm SGIE sau tracker (lặp lại được, tối đa 4)\n"
      "  -k ngày       xoá ảnh/clip cũ hơn chừng này ngày (mặc định 30, 0 = không xoá theo tuổi)\n"
      "  -K GB         tổng ảnh+clip vượt chừng này thì xoá file cũ nhất (mặc định 50, 0 = không giới hạn)\n"
      "  -F cosine     bật nhận diện khuôn mặt, ngưỡng cosine để coi là cùng người (vd. 0.42). Cần 2 SGIE\n"
      "                tìm mặt (gie-unique-id 2) + embedding (3), xem scripts/run_face.sh\n"
      "  -M url        URL gốc ảnh/clip đã upload (tools/uploader.py), vd. http://192.168.1.30:9000/ds-media:\n"
      "                sự kiện có thêm <field>_url, chỉ dọn file local đã upload (<file>.up)\n"
      "  -g gpu        GPU chạy pipeline (mặc định 0). Mỗi GPU 1 bản app, mỗi bản 1 bộ camera riêng\n"
      "  -o thư mục    ảnh/clip vào <thư mục>/w1_snapshots, w1_clips thay cho /outputs/... (mặc định); -k/-K chỉ dọn\n"
      "                trong đó. App test / soak PHẢI dùng -o: -k/-K quét chung thư mục sẽ xoá ảnh/clip của app thật\n",
      prog, prog);
}

int
main (int argc, char *argv[])
{
  setlocale (LC_ALL, "");
  g_set_print_handler (print_raw);
  g_set_printerr_handler (printerr_raw);
  int opt;
  while ((opt = getopt (argc, argv, "R:P:L:f:t:m:r:s:l:Ac:p:S:i:k:K:F:M:g:o:h")) != -1) {
    switch (opt) {
      case 'R': {
        gchar **hp = g_strsplit (optarg, ":", 2);
        cfg.redis_host = g_strdup (hp[0]);
        if (hp[1])
          cfg.redis_port = atoi (hp[1]);
        g_strfreev (hp);
        break;
      }
      case 'P': cfg.prefix = optarg; break;
      case 'L': cfg.frames_maxlen = (guint) atoi (optarg); break;
      case 'f': cfg.publish_every = MAX (1, atoi (optarg)); break;
      case 't': cfg.dwell_sec = atof (optarg); break;
      case 'm': cfg.move_ratio = atof (optarg); break;
      case 'r': cfg.sr_cache = (guint) atoi (optarg); break;
      case 's': cfg.stats_sec = MAX (1, atoi (optarg)); break;
      case 'l': cfg.log_path = optarg; break;
      case 'A': cfg.annotate = FALSE; break;
      case 'c': cfg.pgie_cfg = optarg; break;
      case 'p': cfg.person_class = atoi (optarg); break;
      case 'S':
        if (cfg.num_sgie < MAX_SGIE)
          cfg.sgie_cfg[cfg.num_sgie++] = optarg;
        break;
      case 'i': cfg.cam_list = optarg; break;
      case 'k': cfg.keep_days = (guint) atoi (optarg); break;
      case 'K': cfg.keep_gb = MAX (0, atof (optarg)); break;
      case 'F': cfg.face_thr = CLAMP (atof (optarg), 0, 1); break;
      case 'M': cfg.media_base = g_strdup (optarg); break;
      case 'g': cfg.gpu = (guint) atoi (optarg); break;
      case 'o':
        /* Ảnh/clip vào thư mục riêng; dọn file (-k/-K) cũng chỉ trong thư mục này */
        cfg.snap_dir = g_build_filename (optarg, "w1_snapshots", NULL);
        cfg.clip_dir = g_build_filename (optarg, "w1_clips", NULL);
        break;
      default: usage (argv[0]); return -1;
    }
  }
  if (cfg.cam_list && !load_camera_list (cfg.cam_list))
    return -1;
  for (int i = optind; i < argc; i++)
    if (!add_camera (NULL, NULL, NULL, argv[i], 0, NULL))
      return -1;
  if (num_sources == 0) {
    usage (argv[0]);
    return -1;
  }
  if (!acquire_instance_lock ())
    return 1;

  gst_init (&argc, &argv);
  loop = g_main_loop_new (NULL, FALSE);
  dwell = g_hash_table_new_full (g_int64_hash, g_int64_equal, g_free, (GDestroyNotify) dwell_free);
  dwell_ghosts = g_ptr_array_new ();
  face_tracks = g_hash_table_new_full (g_int64_hash, g_int64_equal, g_free, (GDestroyNotify) face_track_free);
  face_last_hit = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, g_free);
  faces_unknown_key = g_strdup_printf ("%s:faces_unknown", cfg.prefix);
  g_mkdir_with_parents (cfg.snap_dir, 0755);
  g_mkdir_with_parents (LOG_DIR, 0755);
  use_color = isatty (1);
  if (!cfg.log_path) {
    gchar *ts = vn_time (g_get_real_time () / 1000, "%Y%m%d_%H%M%S");
    cfg.log_path = g_strdup_printf ("%s/w1_%s.log", LOG_DIR, ts);
    g_free (ts);
  }
  log_fp = fopen (cfg.log_path, "a");
  alerts_fp = fopen (LOG_DIR "/alerts.log", "a");
  gchar *dir = g_path_get_dirname (argv[0]);
  annotate_bin = g_build_filename (dir, "w1-annotate", NULL);
  g_free (dir);
  if (!g_file_test (annotate_bin, G_FILE_TEST_IS_EXECUTABLE))
    g_clear_pointer (&annotate_bin, g_free);
  annotate_pool = g_thread_pool_new (annotate_worker, NULL, 2, FALSE, NULL);
  g_print ("Log: %s | cảnh báo: %s/alerts.log\n", host_path (cfg.log_path), host_path (LOG_DIR));
  if (cfg.sr_cache)
    g_mkdir_with_parents (cfg.clip_dir, 0755);

  pipeline = gst_pipeline_new ("w1-pipeline");
  GstElement *mux = gst_element_factory_make ("nvstreammux", "mux");
  GstElement *pgie = gst_element_factory_make ("nvinfer", "pgie");
  GstElement *tracker = gst_element_factory_make ("nvtracker", "tracker");
  GstElement *sink = gst_element_factory_make ("fakesink", "sink");
  if (!pipeline || !mux || !pgie || !tracker || !sink) {
    g_printerr ("Không tạo được element\n");
    return -1;
  }
  gst_bin_add_many (GST_BIN (pipeline), mux, pgie, tracker, sink, NULL);

  gboolean any_live = FALSE;
  mux_elem = mux;
  for (guint i = 0; i < num_sources; i++) {
    src_is_rtsp[i] = g_str_has_prefix (cam_uris[i], "rtsp://");
    any_live |= src_is_rtsp[i];
    src_elems[i] = create_source (i);
    gst_bin_add (GST_BIN (pipeline), src_elems[i]);
  }
  g_print ("%u nguồn: %s ... %s, GPU %u\n", num_sources, cam_ids[0], cam_ids[num_sources - 1], cfg.gpu);
  for (guint i = 0; i < num_sources; i++)
    if (cam_roi_n[i] >= 3)
      g_print ("Vùng ghế (ROI) %s: %u đỉnh, chỉ tính ở yên cho người đứng chân trong vùng\n", cam_ids[i], cam_roi_n[i]);
  if (cfg.media_base)
    g_print ("Upload ảnh/clip: sự kiện mang URL %s/..., chỉ dọn file local đã upload\n", cfg.media_base);

  g_object_set (mux, "batch-size", num_sources, "width", MUXER_WIDTH, "height", MUXER_HEIGHT,
      "batched-push-timeout", MUXER_BATCH_TIMEOUT_USEC, "live-source", any_live,
      "attach-sys-ts", TRUE, "gpu-id", cfg.gpu, NULL);
  g_object_set (pgie, "config-file-path", cfg.pgie_cfg ? cfg.pgie_cfg : PGIE_CFG,
      "batch-size", num_sources, "gpu-id", cfg.gpu, NULL);
  if (!cfg.pgie_cfg) {
    /* Model mẫu: trỏ engine đúng batch */
    gchar *engine = g_strdup_printf (ENGINE_FMT, num_sources, cfg.gpu);
    g_object_set (pgie, "model-engine-file", engine, NULL);
    g_free (engine);
  } else {
    pgie_engine_for_batch (pgie, cfg.pgie_cfg);
  }
  g_object_set (tracker, "ll-lib-file", TRACKER_LIB, "ll-config-file", TRACKER_CFG,
      "tracker-width", 640, "tracker-height", 384, "gpu-id", cfg.gpu, NULL);
  g_object_set (sink, "sync", FALSE, "async", FALSE, NULL);
  if (!gst_element_link_many (mux, pgie, tracker, NULL)) {
    g_printerr ("Không link được mux -> pgie -> tracker\n");
    return -1;
  }
  /* SGIE (-S): chạy sau tracker trên từng object, vd. classifier ngồi/đứng, embedding khuôn mặt */
  GstElement *last = tracker;
  for (guint i = 0; i < cfg.num_sgie; i++) {
    gchar name[16];
    g_snprintf (name, sizeof (name), "sgie%u", i + 1);
    GstElement *sgie = gst_element_factory_make ("nvinfer", name);
    g_object_set (sgie, "config-file-path", cfg.sgie_cfg[i], "gpu-id", cfg.gpu, NULL);
    gst_bin_add (GST_BIN (pipeline), sgie);
    if (!gst_element_link (last, sgie)) {
      g_printerr ("Không link được %s\n", name);
      return -1;
    }
    g_print ("SGIE %u: %s\n", i + 1, cfg.sgie_cfg[i]);
    last = sgie;
  }
  if (!gst_element_link (last, sink)) {
    g_printerr ("Không link được -> sink\n");
    return -1;
  }

  enc_ctx = nvds_obj_enc_create_context (cfg.gpu);
  if (!enc_ctx)
    g_printerr ("Không tạo được JPEG encoder, sẽ không lưu snapshot\n");

  /* Probe ở cuối chuỗi (sink pad của fakesink): thấy cả kết quả tracker lẫn SGIE */
  GstPad *probe_pad = gst_element_get_static_pad (sink, "sink");
  gst_pad_add_probe (probe_pad, GST_PAD_PROBE_TYPE_BUFFER, tracker_src_probe, NULL, NULL);
  gst_object_unref (probe_pad);

  rqueue = g_async_queue_new ();
  rthread = g_thread_new ("redis", redis_thread_func, NULL);
  if (cfg.face_thr > 0) {
    if (cfg.num_sgie < 2)
      g_printerr ("Cảnh báo: -F cần SGIE tìm mặt + embedding (-S sgie1_face_detect.txt -S sgie2_face_embed.txt)\n");
    g_print ("Nhận diện khuôn mặt: ngưỡng cosine %.2f, gallery %s:face_emb (web đăng ký), mặt lạ -> %s\n",
        cfg.face_thr, cfg.prefix, faces_unknown_key);
    face_thread = g_thread_new ("faces", face_thread_func, NULL);
  }

  GstBus *bus = gst_pipeline_get_bus (GST_PIPELINE (pipeline));
  guint bus_watch_id = gst_bus_add_watch (bus, bus_call, NULL);
  gst_object_unref (bus);
  g_unix_signal_add (SIGINT, on_sigint, mux);
  g_timeout_add_seconds (cfg.stats_sec, print_stats, NULL);
  reset_pool = g_thread_pool_new (reset_source_worker, NULL, num_sources, FALSE, NULL);
  gint64 t0 = g_get_monotonic_time ();
  for (guint i = 0; i < num_sources; i++)
    src_last_frame_us[i] = src_last_reset_us[i] = t0;   /* cho mỗi cam RECONNECT_SEC để lên lần đầu */
  g_timeout_add_seconds (1, watch_sources, NULL);
  GThread *retention_thread = cfg.keep_days || cfg.keep_gb > 0
      ? g_thread_new ("retention", retention_thread_func, NULL) : NULL;

  g_print ("Running... Redis %s:%d prefix=%s, dwell=%.0fs, smart-record=%s, giữ ảnh/clip %u ngày / %.0f GB\n",
      cfg.redis_host, cfg.redis_port, cfg.prefix, cfg.dwell_sec, cfg.sr_cache ? "on" : "off",
      cfg.keep_days, cfg.keep_gb);
  /* Mỗi lần app khởi động (vd. sau khi đổi cấu hình ở master): báo thông tin từng camera lên ds:events,
   * consumer của platform cập nhật danh mục camera mà không phải đọc lại hash */
  for (guint i = 0; i < num_sources; i++) {
    if (cam_info[i])
      enqueue (TRUE, cam_ids[i], "camera_info", g_get_real_time () / 1000,
          g_strdup_printf ("{\"type\":\"camera_info\",\"cam\":\"%s\",%s", cam_ids[i], cam_info[i] + 1));
  }
  gst_element_set_state (pipeline, GST_STATE_PLAYING);
  g_main_loop_run (loop);
  g_atomic_int_set (&shutting_down, 1);
  g_thread_pool_free (reset_pool, FALSE, TRUE);
  guint pending_annot = g_thread_pool_unprocessed (annotate_pool);
  if (pending_annot)
    g_print ("Bỏ qua %u clip chưa kịp vẽ bbox (chạy tay: w1-annotate -i <clip> -o <out>)\n",
        pending_annot);
  g_thread_pool_free (annotate_pool, TRUE, TRUE);   /* đợi clip đang vẽ, bỏ phần còn xếp hàng */
  gst_element_set_state (pipeline, GST_STATE_NULL);

  if (retention_thread)
    g_thread_join (retention_thread);
  if (face_thread)
    g_thread_join (face_thread);

  /* Đóng các lượt đứng yên còn mở, rồi xả hàng đợi Redis */
  dwell_sweep (g_get_monotonic_time (), TRUE);
  g_atomic_int_set (&rstop, 1);
  g_async_queue_push (rqueue, redis_stop_marker);
  g_thread_join (rthread);
  print_stats (NULL);

  if (enc_ctx)
    nvds_obj_enc_destroy_context (enc_ctx);
  gst_object_unref (pipeline);
  g_source_remove (bus_watch_id);
  g_hash_table_destroy (dwell);
  g_ptr_array_free (dwell_ghosts, TRUE);
  g_hash_table_destroy (face_tracks);
  g_hash_table_destroy (face_last_hit);
  if (face_refs)
    g_ptr_array_unref (face_refs);
  g_main_loop_unref (loop);
  return 0;
}
