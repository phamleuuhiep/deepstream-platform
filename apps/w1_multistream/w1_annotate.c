/*
 * Vẽ bounding box lên clip smart record (clip gốc của camera không có bbox).
 *
 * Pipeline (1 file, 1 phiên NVENC):
 *   nvurisrcbin(file) -> nvstreammux -> nvinfer -> nvtracker --[probe]--> nvdsosd -> nvv4l2h264enc -> mp4
 *
 * Probe tính lại thời gian ở yên của từng người trong clip (cùng luật với w1-app):
 *   xanh lá = người bình thường, vàng = đang ở yên (kèm số giây), đỏ = quá ngưỡng (CẢNH BÁO).
 *   Model hiện tại (TrafficCamNet) không phân biệt ngồi/đứng nên nhãn ghi "ở yên".
 * Clip bắt đầu ~ lúc người bắt đầu ngồi, nên ngưỡng trong clip = min(-t, -a).
 *
 * Dùng: w1-annotate -i in.mp4 -o out.mp4 [-t ngưỡng_s] [-a giây_cảnh_báo_trong_clip]
 *                   [-m tỉ lệ] [-c cam] [-s epoch_ms_đầu_clip] [-p class_người] [-C config_nvinfer]
 */
#include <glib.h>
#include <gst/gst.h>
#include <locale.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "gstnvdsmeta.h"

#define MUXER_WIDTH 1920
#define MUXER_HEIGHT 1080
#define TRACKER_LIB "/opt/nvidia/deepstream/deepstream/lib/libnvds_nvmultiobjecttracker.so"
#define TRACKER_CFG "/opt/nvidia/deepstream/deepstream/samples/configs/deepstream-app/config_tracker_NvDCF_perf.yml"
#define PGIE_CFG "/workspace/configs/task13_pgie.txt"
#define ENGINE_B1 \
  "/opt/nvidia/deepstream/deepstream-7.0/samples/models/Primary_Detector/resnet18_trafficcamnet.etlt_b1_gpu0_int8.engine"
#define STILL_MIN_SEC 5.0     /* yên dưới mức này vẫn vẽ màu xanh */

typedef struct {
  gdouble anchor_cx, anchor_cy;
  gdouble start_s;
} Still;

static gdouble dwell_sec = 120, alert_at = -1, move_ratio = 0.3;
static gint person_class = 2;
static const gchar *pgie_cfg = NULL;   /* NULL = model mẫu TrafficCamNet */
static const gchar *cam = "";
static gint64 start_epoch_ms = 0;
static GHashTable *tracks;
static GMainLoop *loop;
static guint alerted_frames;

static void
set_color (NvOSD_ColorParams *c, gdouble r, gdouble g, gdouble b, gdouble a)
{
  c->red = r;
  c->green = g;
  c->blue = b;
  c->alpha = a;
}

static GstPadProbeReturn
osd_probe (GstPad *pad, GstPadProbeInfo *info, gpointer data)
{
  NvDsBatchMeta *bm = gst_buffer_get_nvds_batch_meta (GST_PAD_PROBE_INFO_BUFFER (info));
  if (!bm)
    return GST_PAD_PROBE_OK;
  gdouble threshold = alert_at > 0 ? MIN (dwell_sec, alert_at) : dwell_sec;

  for (NvDsMetaList *lf = bm->frame_meta_list; lf; lf = lf->next) {
    NvDsFrameMeta *fm = lf->data;
    gdouble t = (gdouble) fm->buf_pts / GST_SECOND;
    gboolean any_alert = FALSE;

    for (NvDsMetaList *lo = fm->obj_meta_list; lo; lo = lo->next) {
      NvDsObjectMeta *obj = lo->data;
      NvOSD_RectParams *r = &obj->rect_params;
      NvOSD_TextParams *tp = &obj->text_params;
      r->border_width = 2;
      set_color (&r->border_color, 0.2, 0.8, 0.2, 1);   /* xanh lá */
      if (obj->class_id != person_class || obj->object_id == UNTRACKED_OBJECT_ID) {
        set_color (&r->border_color, 0.3, 0.6, 1.0, 0.8);
        tp->display_text = g_strdup ("");   /* nvdsosd giải phóng text cũ */
        continue;
      }

      gdouble cx = r->left + r->width / 2, cy = r->top + r->height / 2;
      Still *s = g_hash_table_lookup (tracks, &obj->object_id);
      if (!s) {
        s = g_new0 (Still, 1);
        s->anchor_cx = cx;
        s->anchor_cy = cy;
        s->start_s = t;
        g_hash_table_insert (tracks, g_memdup2 (&obj->object_id, sizeof (guint64)), s);
      } else if (hypot (cx - s->anchor_cx, cy - s->anchor_cy) > move_ratio * MAX (r->height, 1)) {
        s->anchor_cx = cx;
        s->anchor_cy = cy;
        s->start_s = t;
      }
      gdouble still = t - s->start_s;

      g_free (tp->display_text);
      if (still >= threshold) {
        any_alert = TRUE;
        r->border_width = 6;
        set_color (&r->border_color, 1, 0, 0, 1);
        tp->display_text = g_strdup_printf ("CẢNH BÁO #%" G_GUINT64_FORMAT " ở yên %d:%02d",
            obj->object_id, (int) still / 60, (int) still % 60);
        set_color (&tp->text_bg_clr, 0.8, 0, 0, 0.9);
      } else if (still >= STILL_MIN_SEC) {
        r->border_width = 3;
        set_color (&r->border_color, 1, 0.85, 0, 1);
        tp->display_text = g_strdup_printf ("#%" G_GUINT64_FORMAT " ở yên %ds", obj->object_id,
            (int) still);
        set_color (&tp->text_bg_clr, 0.5, 0.4, 0, 0.8);
      } else {
        tp->display_text = g_strdup_printf ("#%" G_GUINT64_FORMAT, obj->object_id);
        set_color (&tp->text_bg_clr, 0.1, 0.4, 0.1, 0.7);
      }
      tp->set_bg_clr = 1;
      tp->font_params.font_name = "Serif";
      tp->font_params.font_size = 14;
      set_color (&tp->font_params.font_color, 1, 1, 1, 1);
      tp->x_offset = (guint) MAX (0, r->left);
      tp->y_offset = (guint) MAX (0, r->top - 26);
    }
    alerted_frames += any_alert;

    /* Dòng tiêu đề: cam + giờ thực (giờ VN) + trạng thái */
    NvDsDisplayMeta *dm = nvds_acquire_display_meta_from_pool (bm);
    NvOSD_TextParams *hp = &dm->text_params[0];
    dm->num_labels = 1;
    gchar clock[32] = "";
    if (start_epoch_ms) {
      GTimeZone *tz = g_time_zone_new_offset (7 * 3600);
      GDateTime *dt = g_date_time_new_from_unix_utc ((start_epoch_ms + (gint64) (t * 1000)) / 1000);
      GDateTime *local = g_date_time_to_timezone (dt, tz);
      gchar *f = g_date_time_format (local, "%H:%M:%S");
      g_snprintf (clock, sizeof (clock), "%s", f);
      g_free (f);
      g_date_time_unref (local);
      g_date_time_unref (dt);
      g_time_zone_unref (tz);
    }
    hp->display_text = g_strdup_printf ("%s  %s  t=%d:%02d  ngưỡng %ds%s", cam, clock,
        (int) t / 60, (int) t % 60, (int) threshold, any_alert ? "   >>> CẢNH BÁO Ở YÊN QUÁ LÂU <<<" : "");
    hp->x_offset = 20;
    hp->y_offset = 20;
    hp->font_params.font_name = "Serif";
    hp->font_params.font_size = 20;
    set_color (&hp->font_params.font_color, 1, 1, 1, 1);
    hp->set_bg_clr = 1;
    if (any_alert)
      set_color (&hp->text_bg_clr, 0.8, 0, 0, 0.9);
    else
      set_color (&hp->text_bg_clr, 0, 0, 0, 0.6);
    nvds_add_display_meta_to_frame (fm, dm);
  }
  return GST_PAD_PROBE_OK;
}

static gboolean
bus_call (GstBus *bus, GstMessage *msg, gpointer data)
{
  if (GST_MESSAGE_TYPE (msg) == GST_MESSAGE_EOS) {
    g_main_loop_quit (loop);
  } else if (GST_MESSAGE_TYPE (msg) == GST_MESSAGE_ERROR) {
    GError *err;
    gst_message_parse_error (msg, &err, NULL);
    g_printerr ("ERROR from %s: %s\n", GST_OBJECT_NAME (msg->src), err->message);
    g_error_free (err);
    *(gboolean *) data = FALSE;
    g_main_loop_quit (loop);
  }
  return TRUE;
}

static void
on_pad_added (GstElement *src, GstPad *pad, gpointer data)
{
  GstCaps *caps = gst_pad_query_caps (pad, NULL);
  if (g_str_has_prefix (gst_structure_get_name (gst_caps_get_structure (caps, 0)), "video")) {
    GstPad *sinkpad = gst_element_request_pad_simple (GST_ELEMENT (data), "sink_0");
    gst_pad_link (pad, sinkpad);
    gst_object_unref (sinkpad);
  }
  gst_caps_unref (caps);
}

int
main (int argc, char *argv[])
{
  const gchar *in = NULL, *out = NULL;
  int opt;
  setlocale (LC_ALL, "");
  while ((opt = getopt (argc, argv, "i:o:t:a:m:c:s:p:C:")) != -1) {
    switch (opt) {
      case 'i': in = optarg; break;
      case 'o': out = optarg; break;
      case 't': dwell_sec = atof (optarg); break;
      case 'a': alert_at = atof (optarg); break;
      case 'm': move_ratio = atof (optarg); break;
      case 'c': cam = optarg; break;
      case 's': start_epoch_ms = g_ascii_strtoll (optarg, NULL, 10); break;
      case 'p': person_class = atoi (optarg); break;
      case 'C': pgie_cfg = optarg; break;
      default: break;
    }
  }
  if (!in || !out) {
    g_printerr ("Dùng: %s -i in.mp4 -o out.mp4 [-t ngưỡng] [-a giây_cảnh_báo] [-m tỉ_lệ] [-c cam] "
        "[-s epoch_ms] [-p class_người] [-C config_nvinfer]\n", argv[0]);
    return 2;
  }
  gst_init (&argc, &argv);
  loop = g_main_loop_new (NULL, FALSE);
  tracks = g_hash_table_new_full (g_int64_hash, g_int64_equal, g_free, g_free);

  GstElement *pipeline = gst_pipeline_new ("annotate");
  GstElement *src = gst_element_factory_make ("nvurisrcbin", "src");
  GstElement *mux = gst_element_factory_make ("nvstreammux", "mux");
  GstElement *pgie = gst_element_factory_make ("nvinfer", "pgie");
  GstElement *tracker = gst_element_factory_make ("nvtracker", "tracker");
  GstElement *conv1 = gst_element_factory_make ("nvvideoconvert", "conv1");
  GstElement *osd = gst_element_factory_make ("nvdsosd", "osd");
  GstElement *conv2 = gst_element_factory_make ("nvvideoconvert", "conv2");
  GstElement *caps = gst_element_factory_make ("capsfilter", "caps");
  GstElement *enc = gst_element_factory_make ("nvv4l2h264enc", "enc");
  GstElement *parse = gst_element_factory_make ("h264parse", "parse");
  GstElement *qtmux = gst_element_factory_make ("qtmux", "qtmux");
  GstElement *sink = gst_element_factory_make ("filesink", "sink");
  if (!pipeline || !src || !mux || !pgie || !tracker || !conv1 || !osd || !conv2 || !caps || !enc
      || !parse || !qtmux || !sink) {
    g_printerr ("Không tạo được element\n");
    return 1;
  }

  gchar *uri = g_str_has_prefix (in, "/") ? g_strdup_printf ("file://%s", in) : g_strdup (in);
  g_object_set (src, "uri", uri, NULL);
  g_free (uri);
  g_object_set (mux, "batch-size", 1, "width", MUXER_WIDTH, "height", MUXER_HEIGHT,
      "batched-push-timeout", 40000, "live-source", FALSE, NULL);
  g_object_set (pgie, "config-file-path", pgie_cfg ? pgie_cfg : PGIE_CFG, "batch-size", 1, NULL);
  if (!pgie_cfg)
    g_object_set (pgie, "model-engine-file", ENGINE_B1, NULL);
  g_object_set (tracker, "ll-lib-file", TRACKER_LIB, "ll-config-file", TRACKER_CFG,
      "tracker-width", 640, "tracker-height", 384, NULL);
  GstCaps *c = gst_caps_from_string ("video/x-raw(memory:NVMM), format=I420");
  g_object_set (caps, "caps", c, NULL);
  gst_caps_unref (c);
  g_object_set (enc, "bitrate", 6000000, NULL);
  g_object_set (sink, "location", out, "sync", FALSE, NULL);

  gst_bin_add_many (GST_BIN (pipeline), src, mux, pgie, tracker, conv1, osd, conv2, caps, enc,
      parse, qtmux, sink, NULL);
  g_signal_connect (src, "pad-added", G_CALLBACK (on_pad_added), mux);
  if (!gst_element_link_many (mux, pgie, tracker, conv1, osd, conv2, caps, enc, parse, qtmux, sink,
          NULL)) {
    g_printerr ("Không link được pipeline\n");
    return 1;
  }
  GstPad *p = gst_element_get_static_pad (osd, "sink");
  gst_pad_add_probe (p, GST_PAD_PROBE_TYPE_BUFFER, osd_probe, NULL, NULL);
  gst_object_unref (p);

  gboolean ok = TRUE;
  GstBus *bus = gst_pipeline_get_bus (GST_PIPELINE (pipeline));
  gst_bus_add_watch (bus, bus_call, &ok);
  gst_object_unref (bus);

  gst_element_set_state (pipeline, GST_STATE_PLAYING);
  g_main_loop_run (loop);
  gst_element_set_state (pipeline, GST_STATE_NULL);
  gst_object_unref (pipeline);
  printf ("annotate %s: %s (%u frame có cảnh báo)\n", ok ? "OK" : "LỖI", out, alerted_frames);
  return ok ? 0 : 1;
}
