/*
 * Task 1.3 - Trích xuất metadata cơ bản bằng pad probe sau nvtracker.
 *
 * Pipeline:
 *   nvurisrcbin (x N, file/RTSP, bên trong dùng nvv4l2decoder - NVDEC)
 *     -> nvstreammux -> nvinfer -> nvtracker --[probe]--> fakesink
 *
 * Probe gắn vào src pad của nvtracker, duyệt
 *   NvDsBatchMeta -> NvDsFrameMeta -> NvDsObjectMeta
 * và lấy bbox, class ID, tracking ID. Kết quả in ra console (mỗi N frame)
 * và ghi đầy đủ ra file JSONL (1 dòng / frame).
 *
 * Dùng: task13-meta-app [-o out.jsonl] [-p print_every] [-c pgie_config] <uri> [uri ...]
 */
#include <glib.h>
#include <locale.h>
#include <glib-unix.h>
#include <gst/gst.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "gstnvdsmeta.h"
#include "nvds_tracker_meta.h"

#define MUXER_WIDTH 1920
#define MUXER_HEIGHT 1080
/* = 1/FPS lớn nhất của nguồn (30fps); 40000 sẽ chặn ở 25 batch/s với nguồn live */
#define MUXER_BATCH_TIMEOUT_USEC 33000
#define TRACKER_LIB "/opt/nvidia/deepstream/deepstream/lib/libnvds_nvmultiobjecttracker.so"
#define TRACKER_CFG "/opt/nvidia/deepstream/deepstream/samples/configs/deepstream-app/config_tracker_NvDCF_perf.yml"
#define DEFAULT_PGIE_CFG "/workspace/configs/task13_pgie.txt"
#define DEFAULT_ENGINE_FMT \
  "/opt/nvidia/deepstream/deepstream-7.0/samples/models/Primary_Detector/resnet18_trafficcamnet.etlt_b%u_gpu0_int8.engine"
#define DEFAULT_OUT "/outputs/w0_t3_metadata.jsonl"
#define MAX_CLASSES 16
#define MAX_SOURCES 64
#define MAX_PRINT_OBJS 5

typedef struct {
  FILE *out;
  guint print_every;
  guint64 frames;
  guint64 src_frames[MAX_SOURCES];
  guint64 objects;
  guint64 untracked;
  guint64 class_count[MAX_CLASSES];
  GHashTable *track_ids;      /* object_id -> class_id, đếm số ID duy nhất */
  gint64 t_first_us;
  gint64 t_last_us;
} ProbeCtx;

static GMainLoop *loop;
static GstElement *pipeline;

static GstPadProbeReturn
tracker_src_probe (GstPad *pad, GstPadProbeInfo *info, gpointer user_data)
{
  ProbeCtx *ctx = (ProbeCtx *) user_data;
  GstBuffer *buf = GST_PAD_PROBE_INFO_BUFFER (info);
  NvDsBatchMeta *batch_meta = gst_buffer_get_nvds_batch_meta (buf);
  if (!batch_meta)
    return GST_PAD_PROBE_OK;

  gint64 now = g_get_monotonic_time ();
  if (!ctx->t_first_us)
    ctx->t_first_us = now;
  ctx->t_last_us = now;

  for (NvDsMetaList *l_frame = batch_meta->frame_meta_list; l_frame; l_frame = l_frame->next) {
    NvDsFrameMeta *frame_meta = (NvDsFrameMeta *) l_frame->data;
    gboolean do_print = ctx->print_every && (frame_meta->frame_num % ctx->print_every == 0);
    guint printed = 0;

    ctx->frames++;
    if (frame_meta->source_id < MAX_SOURCES)
      ctx->src_frames[frame_meta->source_id]++;
    if (do_print)
      g_print ("[src %u] frame %d  pts=%.3fs  objs=%u  (%ux%u)\n",
          frame_meta->source_id, frame_meta->frame_num,
          (double) frame_meta->buf_pts / GST_SECOND, frame_meta->num_obj_meta,
          frame_meta->source_frame_width, frame_meta->source_frame_height);

    if (ctx->out)
      fprintf (ctx->out,
          "{\"src\":%u,\"frame\":%d,\"pts_ms\":%.1f,\"ntp_ts\":%" G_GUINT64_FORMAT
          ",\"batch_id\":%u,\"num_obj\":%u,\"objs\":[",
          frame_meta->source_id, frame_meta->frame_num,
          (double) frame_meta->buf_pts / GST_MSECOND, frame_meta->ntp_timestamp,
          frame_meta->batch_id, frame_meta->num_obj_meta);

    gboolean first = TRUE;
    for (NvDsMetaList *l_obj = frame_meta->obj_meta_list; l_obj; l_obj = l_obj->next) {
      NvDsObjectMeta *obj = (NvDsObjectMeta *) l_obj->data;
      /* rect_params: bbox sau tracker (đã làm mượt), toạ độ theo độ phân giải streammux */
      NvOSD_RectParams *r = &obj->rect_params;
      gboolean tracked = obj->object_id != UNTRACKED_OBJECT_ID;

      ctx->objects++;
      if (obj->class_id >= 0 && obj->class_id < MAX_CLASSES)
        ctx->class_count[obj->class_id]++;
      if (tracked)
        g_hash_table_insert (ctx->track_ids, g_memdup2 (&obj->object_id, sizeof (guint64)),
            GINT_TO_POINTER (obj->class_id));
      else
        ctx->untracked++;

      if (do_print && printed < MAX_PRINT_OBJS) {
        g_print ("    cls=%d %-9s tid=%-6" G_GINT64_FORMAT " conf=%.2f trk_conf=%.2f "
            "bbox=[l=%.0f t=%.0f w=%.0f h=%.0f]\n",
            obj->class_id, obj->obj_label, tracked ? (gint64) obj->object_id : -1,
            obj->confidence, obj->tracker_confidence,
            r->left, r->top, r->width, r->height);
        printed++;
      }

      if (ctx->out) {
        fprintf (ctx->out,
            "%s{\"cls\":%d,\"label\":\"%s\",\"tid\":%" G_GINT64_FORMAT
            ",\"conf\":%.3f,\"trk_conf\":%.3f,\"bbox\":[%.1f,%.1f,%.1f,%.1f]}",
            first ? "" : ",", obj->class_id, obj->obj_label,
            tracked ? (gint64) obj->object_id : -1,
            obj->confidence, obj->tracker_confidence,
            r->left, r->top, r->width, r->height);
        first = FALSE;
      }
    }
    if (do_print && frame_meta->num_obj_meta > MAX_PRINT_OBJS)
      g_print ("    ... (+%u obj)\n", frame_meta->num_obj_meta - MAX_PRINT_OBJS);
    if (ctx->out)
      fputs ("]}\n", ctx->out);
  }
  return GST_PAD_PROBE_OK;
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
      gchar *debug;
      GError *err;
      gst_message_parse_warning (msg, &err, &debug);
      g_printerr ("WARNING from %s: %s\n", GST_OBJECT_NAME (msg->src), err->message);
      g_free (debug);
      g_error_free (err);
      break;
    }
    case GST_MESSAGE_ERROR: {
      gchar *debug;
      GError *err;
      gst_message_parse_error (msg, &err, &debug);
      g_printerr ("ERROR from %s: %s\n", GST_OBJECT_NAME (msg->src), err->message);
      if (debug)
        g_printerr ("Debug: %s\n", debug);
      g_free (debug);
      g_error_free (err);
      g_main_loop_quit (loop);
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

/* Ctrl+C -> gửi EOS để pipeline dừng sạch và JSONL được flush đủ.
 * nvurisrcbin RTSP không chuyển tiếp EOS gửi từ pipeline, nên đẩy EOS thẳng
 * vào từng sink pad của nvstreammux; mux ra EOS khi mọi nguồn đã EOS. */
static gboolean
on_sigint (gpointer data)
{
  static gboolean sent = FALSE;
  if (sent) {
    g_main_loop_quit (loop);
    return G_SOURCE_REMOVE;
  }
  g_print ("\nSIGINT: gửi EOS (Ctrl+C lần nữa để thoát ngay)\n");
  gst_element_foreach_sink_pad (GST_ELEMENT (data), send_eos_to_pad, NULL);
  sent = TRUE;
  return G_SOURCE_CONTINUE;
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
    GstPad *sinkpad = gst_element_request_pad_simple (mux, pad_name);
    if (gst_pad_link (pad, sinkpad) != GST_PAD_LINK_OK)
      g_printerr ("Không link được source %u vào streammux\n", idx);
    else
      g_print ("Source %u đã link vào streammux (%s)\n", idx, name);
    gst_object_unref (sinkpad);
  }
  gst_caps_unref (caps);
}

int
main (int argc, char *argv[])
{
  const gchar *out_path = DEFAULT_OUT;
  const gchar *pgie_cfg = DEFAULT_PGIE_CFG;
  gboolean custom_pgie = FALSE;
  ProbeCtx ctx = { 0 };
  ctx.print_every = 30;
  int opt;

  while ((opt = getopt (argc, argv, "o:p:c:")) != -1) {
    switch (opt) {
      case 'o': out_path = optarg; break;
      case 'p': ctx.print_every = (guint) atoi (optarg); break;
      case 'c': pgie_cfg = optarg; custom_pgie = TRUE; break;
      default:
        g_printerr ("Dùng: %s [-o out.jsonl] [-p print_every] [-c pgie_config] <uri> [uri ...]\n", argv[0]);
        return -1;
    }
  }
  guint num_sources = argc - optind;
  if (num_sources == 0) {
    g_printerr ("Dùng: %s [-o out.jsonl] [-p print_every] [-c pgie_config] <uri> [uri ...]\n", argv[0]);
    return -1;
  }

  setlocale (LC_ALL, "");
  gst_init (&argc, &argv);
  loop = g_main_loop_new (NULL, FALSE);
  ctx.track_ids = g_hash_table_new_full (g_int64_hash, g_int64_equal, g_free, NULL);

  if (strcmp (out_path, "-") != 0 && strlen (out_path) > 0) {
    ctx.out = fopen (out_path, "w");
    if (!ctx.out) {
      g_printerr ("Không mở được %s\n", out_path);
      return -1;
    }
  }

  pipeline = gst_pipeline_new ("task13-pipeline");
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
  for (guint i = 0; i < num_sources; i++) {
    const gchar *uri = argv[optind + i];
    gboolean is_rtsp = g_str_has_prefix (uri, "rtsp://");
    gchar name[32];
    g_snprintf (name, sizeof (name), "src-%u", i);
    GstElement *src = gst_element_factory_make ("nvurisrcbin", name);
    if (!src) {
      g_printerr ("Không tạo được nvurisrcbin\n");
      return -1;
    }
    g_object_set (src, "uri", uri, "gpu-id", 0, "cudadec-memtype", 0, NULL);
    if (is_rtsp) {
      any_live = TRUE;
      /* select-rtp-protocol=4: TCP (ổn định hơn UDP qua MediaMTX/camera thật) */
      g_object_set (src, "latency", 200, "rtsp-reconnect-interval", 10,
          "select-rtp-protocol", 4, NULL);
    }
    g_object_set_data (G_OBJECT (src), "src-index", GUINT_TO_POINTER (i));
    g_signal_connect (src, "pad-added", G_CALLBACK (on_src_pad_added), mux);
    gst_bin_add (GST_BIN (pipeline), src);
    g_print ("Source %u: %s\n", i, uri);
  }

  g_object_set (mux, "batch-size", num_sources, "width", MUXER_WIDTH, "height", MUXER_HEIGHT,
      "batched-push-timeout", MUXER_BATCH_TIMEOUT_USEC, "live-source", any_live, NULL);
  g_object_set (pgie, "config-file-path", pgie_cfg, "batch-size", num_sources, NULL);
  if (!custom_pgie) {
    /* Trỏ tới engine đúng batch; nếu chưa có nvinfer tự build và lưu đúng tên này */
    gchar *engine = g_strdup_printf (DEFAULT_ENGINE_FMT, num_sources);
    g_object_set (pgie, "model-engine-file", engine, NULL);
    g_free (engine);
  }
  g_object_set (tracker, "ll-lib-file", TRACKER_LIB, "ll-config-file", TRACKER_CFG,
      "tracker-width", 640, "tracker-height", 384, "gpu-id", 0, NULL);
  g_object_set (sink, "sync", any_live, "async", FALSE, NULL);

  if (!gst_element_link_many (mux, pgie, tracker, sink, NULL)) {
    g_printerr ("Không link được mux -> pgie -> tracker -> sink\n");
    return -1;
  }

  GstPad *tracker_src = gst_element_get_static_pad (tracker, "src");
  gst_pad_add_probe (tracker_src, GST_PAD_PROBE_TYPE_BUFFER, tracker_src_probe, &ctx, NULL);
  gst_object_unref (tracker_src);

  GstBus *bus = gst_pipeline_get_bus (GST_PIPELINE (pipeline));
  guint bus_watch_id = gst_bus_add_watch (bus, bus_call, NULL);
  gst_object_unref (bus);
  g_unix_signal_add (SIGINT, on_sigint, mux);

  g_print ("Running... (probe trên nvtracker:src, JSONL -> %s)\n", ctx.out ? out_path : "(tắt)");
  gst_element_set_state (pipeline, GST_STATE_PLAYING);
  g_main_loop_run (loop);
  gst_element_set_state (pipeline, GST_STATE_NULL);

  /* Tổng kết */
  gdouble secs = (ctx.t_last_us - ctx.t_first_us) / 1e6;
  guint track_by_class[MAX_CLASSES] = { 0 };
  GHashTableIter it;
  gpointer k, v;
  g_hash_table_iter_init (&it, ctx.track_ids);
  while (g_hash_table_iter_next (&it, &k, &v))
    if (GPOINTER_TO_INT (v) >= 0 && GPOINTER_TO_INT (v) < MAX_CLASSES)
      track_by_class[GPOINTER_TO_INT (v)]++;

  g_print ("\n===== TỔNG KẾT TASK 1.3 =====\n");
  g_print ("Sources            : %u\n", num_sources);
  g_print ("Frames xử lý       : %" G_GUINT64_FORMAT "\n", ctx.frames);
  for (guint s = 0; s < num_sources && s < MAX_SOURCES; s++)
    g_print ("  src %u: %" G_GUINT64_FORMAT " frames  (%s)\n", s, ctx.src_frames[s], argv[optind + s]);
  g_print ("Thời gian          : %.2f s  (~%.1f FPS tổng, ~%.1f FPS/nguồn)\n", secs,
      secs > 0 ? ctx.frames / secs : 0, secs > 0 ? ctx.frames / secs / num_sources : 0);
  g_print ("Object meta        : %" G_GUINT64_FORMAT " (untracked: %" G_GUINT64_FORMAT ")\n",
      ctx.objects, ctx.untracked);
  g_print ("Tracking ID duy nhất: %u\n", g_hash_table_size (ctx.track_ids));
  for (guint c = 0; c < MAX_CLASSES; c++)
    if (ctx.class_count[c])
      g_print ("  class %u: %" G_GUINT64_FORMAT " detections, %u track ID\n", c,
          ctx.class_count[c], track_by_class[c]);
  if (ctx.out) {
    fclose (ctx.out);
    g_print ("JSONL: %s\n", out_path);
  }

  gst_object_unref (pipeline);
  g_source_remove (bus_watch_id);
  g_hash_table_destroy (ctx.track_ids);
  g_main_loop_unref (loop);
  return 0;
}
