# DeepStream workspace

**Mới vào dự án:** `docs/DEEPSTREAM_LA_GI.md` (DeepStream là gì, dự án dùng nó thế nào).

**Model từ team AI:** `docs/NHAN_MODEL_TU_TEAM_AI.md` (file cần nhận, cách đưa vào pipeline). Chạy lại app: `docs/CHAY_LAI_APP.md`.

**Nhận diện khuôn mặt + khách VIP:** `docs/PIPELINE_KHUON_MAT_VIP.md` (YOLO COCO → mặt trong người → ArcFace → VIP;
đăng ký khuôn mặt + thông báo trên dashboard).

Mount vào container `deepstream_dev`:

| Host                               | Container    |
|------------------------------------|--------------|
| `/home/hiepplh/deepstream`         | `/workspace` |
| `/home/hiepplh/deepstream/outputs` | `/outputs`   |

## Cấu trúc

```
apps/      app C tự viết (task13_metadata: Task 0.3; w1_multistream: tuần 1). Build bằng make trong container
configs/   config deepstream-app / nvinfer, danh sách cam, sites.json + app.json (dashboard master ghi)
docs/      giới thiệu DeepStream, báo cáo tuần, chạy lại app, nhận model từ team AI (doc cũ ở archive/docs_cu/)
handoff/   bộ bàn giao cho team platform (deepstream_platform_kit)
master/    agent thực hiện các nút bấm của dashboard master (container ds_agent)
scripts/   script chạy pipeline, giả lập RTSP, Redis dev, web, agent
tools/     công cụ lẻ (redis_consumer.py: consumer mẫu đọc Redis Streams)
web/       platform demo: web xem camera, cảnh báo, ảnh/clip từ Redis
models/    model của team AI (= /workspace/models)          ┐
videos/    video test cho RTSP giả lập (xem videos/README.md) ├ không commit (.gitignore)
outputs/   kết quả, log, clip (= /outputs)                   ┘
uploader/  upload ảnh/clip lên S3/MinIO (container ds_uploader)
monitor/   exporter Prometheus + cấu hình Prometheus/Grafana riêng
```

Không commit (xem `.gitignore`): file bí mật (`.redis.env`, `web/.auth`, `web/companies.auth`), dữ liệu lớn
(`models/`, `videos/`, `outputs/`), file build của app C, `configs/.master/`, `archive/`, file `.zip` trong `handoff/`.

### Cài đặt từ bản clone mới

```bash
git clone <repo> /home/hiepplh/deepstream && cd /home/hiepplh/deepstream
# 1. Container DeepStream: mount repo vào /workspace, outputs/ vào /outputs
docker run -d --name deepstream_dev --gpus all --network host \
  -v "$PWD:/workspace" -v "$PWD/outputs:/outputs" nvcr.io/nvidia/deepstream:7.0-samples-multiarch sleep infinity
./scripts/setup_build_deps.sh                 # gstreamer-dev, hiredis trong container
# 2. Video test (videos/README.md), model (models/README.md)
# 3. Redis dev (tự sinh mật khẩu vào .redis.env), web, agent
REDIS_BIND=192.168.1.30 ./scripts/redis_dev.sh start
./scripts/web.sh start && ./scripts/master.sh start
```

## Giả lập camera RTSP (Task 0.1)

```bash
./scripts/rtsp_sim.sh start     # MediaMTX :8554 + 4 FFmpeg -> rtsp://127.0.0.1:8554/cam1..cam4
./scripts/rtsp_sim.sh start 32  # 1 video -> cam1..cam32 (tuần 1)
./scripts/rtsp_sim.sh sites     # cam theo công ty/toà (configs/sites.json, sửa ở dashboard master)
./scripts/rtsp_sim.sh status
./scripts/rtsp_sim.sh stop
```

cam1 H.264 1080p30, cam2 H.265 1080p30, cam3/cam4 H.264 1472x1384 2fps. Chỉ dùng RTSP qua TCP.
MediaMTX còn phát HLS ở `127.0.0.1:8888/<cam>/index.m3u8` (chỉ localhost) cho web demo xem live.

## Pipeline deepstream-app (Task 0.2)

```bash
./scripts/run_pipeline.sh                      # configs/my_pipeline.txt
./scripts/run_pipeline.sh configs/khac.txt     # config khác
```

Kết quả: `outputs/w0_t2_pipeline_1080p.mp4`.

Chuyển sang RTSP: trong `configs/my_pipeline.txt` đặt `[source0] type=4`, `uri=rtsp://127.0.0.1:8554/cam1`,
`[streammux] live-source=1`, `batched-push-timeout=33000`. Cần chạy `rtsp_sim.sh start` trước.

## Đọc metadata sau nvtracker (Task 0.3)

```bash
./scripts/run_task13.sh                                    # 4 luồng RTSP giả lập, Ctrl+C để dừng
./scripts/run_task13.sh -p 30 file:///opt/nvidia/deepstream/deepstream/samples/streams/sample_1080p_h264.mp4
./scripts/run_task13.sh -o /outputs/x.jsonl rtsp://<ip>:554/<path>   # camera thật
```

Pipeline: `nvurisrcbin (NVDEC) -> nvstreammux -> nvinfer -> nvtracker --[probe]--> fakesink`.
Probe trên `nvtracker:src` duyệt `NvDsBatchMeta -> NvDsFrameMeta -> NvDsObjectMeta`, in ra console
mỗi `-p` frame và ghi mỗi frame một dòng JSON vào `outputs/w0_t3_metadata.jsonl` (mặc định):

```json
{"src":0,"frame":2,"pts_ms":133.3,"ntp_ts":...,"batch_id":0,"num_obj":12,
 "objs":[{"cls":2,"label":"person","tid":0,"conf":0.498,"trk_conf":0.943,"bbox":[116.0,488.0,88.3,288.0]}]}
```

`bbox` = `[left, top, width, height]` (pixel, theo độ phân giải streammux 1920x1080), `tid=-1` nếu chưa được track.
Lần đầu chạy với N nguồn mà chưa có engine `_bN_`, nvinfer tự build (~1 phút).

Build thủ công: `docker exec deepstream_dev make -C /workspace/apps/task13_metadata`.
Container được tạo lại thì chạy `./scripts/setup_build_deps.sh` (image samples không có header GStreamer;
`run_task13.sh` tự gọi khi thiếu).

## Tuần 1: đa luồng RTSP -> Redis, cảnh báo ở yên

```bash
./scripts/redis_dev.sh start                  # Redis dev :6390, mật khẩu trong .redis.env (xem: redis_dev.sh info)
./scripts/rtsp_sim.sh start 32
./scripts/run_w1.sh 32 -t 120 -r 135          # Ctrl+C để dừng
./scripts/w1_edge_test.sh 32                  # test edge case
```

**Chia camera theo công ty / toà nhà:** sửa ở dashboard master, hoặc sửa tay `configs/sites.json`
(công ty: mã + tên; toà: mã + tên + địa chỉ; camera: camera_id, tên, tầng, khu vực, video, fps xử lý, giây bắt đầu), rồi

```bash
./scripts/rtsp_sim.sh sites                                  # phát luồng rtsp://127.0.0.1:8554/<camera_id>
./scripts/run_w1.sh -i configs/cameras_sim.txt -t 120 -r 135 # file danh sách cam do lệnh trên sinh ra
./scripts/web.sh publish                                     # mỗi công ty 1 link public trycloudflare + tài khoản riêng
./scripts/web.sh links                                       # in lại link + tài khoản; unpublish [công ty] để tắt
```

Cam thật: tự viết file cùng định dạng `configs/cameras_sim.txt` (mỗi dòng `<công ty> <toà> <cam_id> <uri> [fps]`).
`fps` > 0: app chỉ phân tích chừng ấy frame/giây của cam đó (bỏ bớt frame trước nvinfer, đỡ tải GPU; clip vẫn đủ fps).
Khi chạy với `-i`, mọi JSON gửi Redis có thêm `"company"`, `"building"`, entry Redis có thêm 2 field cùng tên,
hash `ds:companies` / `ds:buildings` (cam_id -> công ty / toà), log `[stats]` và cảnh báo in theo công ty/toà.
Bản video lệch giờ để các cam không trùng hình được tạo một lần ở `videos/.sim/`.

Chi tiết (bản cũ): `archive/docs_cu/HUONG_DAN_TUAN1.md`. Ảnh: `outputs/w1_snapshots/`, clip: `outputs/w1_clips/`
(`*_bbox.mp4` là bản có vẽ khung), log: `outputs/logs/` (cảnh báo riêng ở `alerts.log`).
Dọn kết quả test: `./scripts/clean_tests.sh`.

## Nhận diện khuôn mặt + khách VIP

```bash
./scripts/prepare_models.sh                          # 1 lần: YOLO11s ONNX + lib parse, FaceNet, InsightFace
./scripts/run_face.sh -i configs/cameras_sim.txt -t 120 -r 135   # hoặc tick "Nhận diện khuôn mặt + VIP" ở master
```

Pipeline: `nvinfer YOLO11 COCO (gie 1) -> nvtracker -> nvinfer FaceNet trên person (gie 2) -> nvinfer ArcFace trên face (gie 3)
[-> nvinfer VIP classifier trên person (gie 4)]`. Tham số `-F <cosine>` của app bật phần so khớp. Đăng ký khuôn mặt ở tab
**Khuôn mặt** của dashboard (ảnh, webcam, hoặc mặt lạ camera vừa gặp); nhận ra thì `face_recognized` vào `ds:events` và
dashboard hiện thông báo. Chi tiết, thông số từng tầng, schema Redis: `docs/PIPELINE_KHUON_MAT_VIP.md`.

## Tuần 2: platform, vùng ghế, tự hồi phục, metric

Checklist + kết quả test: `docs/BAO_CAO_TUAN2.md` (`./scripts/w2_bc_test.sh <bài>`, mỗi lần 1 bài).

```bash
./scripts/upload.sh minio && ./scripts/upload.sh start   # MinIO dev (:9000, bucket ds-media) + uploader; app tự thêm -M khi có .s3.env
./scripts/monitor.sh start                               # exporter :9466 + Prometheus :9091 + Grafana :3001 (dashboard "DeepStream pipeline")
```

- Mọi JSON gửi Redis có `schema_version: 2`; sự kiện có `<field>_url` (ảnh/clip trên S3) khi app chạy `-M <URL gốc>`.
- **Vùng ghế (ROI):** dashboard master → nút **Vùng ghế** của camera → bấm các đỉnh trên video live → **Lưu & áp dụng**. App chỉ tính
  ở yên cho người đứng chân trong vùng (`roi` trong `configs/sites.json`).
- **Tự hồi phục:** agent kiểm mỗi 5 s, bật lại thành phần chết theo thứ tự, ghi sự kiện `system_heal` (ô **Tự hồi phục** ở master).
- **Nhiều GPU:** `-g <GPU>`: mỗi GPU 1 bản app, mỗi bản 1 bộ camera riêng (cùng prefix được).
- **App test chạy song song app thật phải có `-o <thư mục>`** (ảnh/clip và dọn file `-k/-K` chỉ trong thư mục đó): thiếu `-o`, app test
  dọn chung `outputs/w1_snapshots`, `w1_clips` và xoá ảnh/clip của app thật (đã xảy ra 05/11).

## Platform demo: xem kết quả trên web (tuần 2)

```bash
./scripts/web.sh start      # http://127.0.0.1:8095 (VS Code Remote: tab PORTS -> Forward 8095)
./scripts/web.sh logs | stop | restart
./scripts/web.sh tunnel     # link public tạm https://*.trycloudflare.com, bắt đăng nhập (tài khoản trong web/.auth)
./scripts/web.sh tunnel-stop
```

**Dashboard master** (tab *Cấu hình (master)* ở trang admin): thêm/xoá công ty, toà; mỗi toà là bảng camera
(camera_id, tên, tầng, khu vực, video, fps xử lý, giây bắt đầu, kèm fps thực + giờ frame mới nhất, link riêng), ngưỡng ở yên, rồi *Lưu & áp dụng*; xem link + tài khoản từng công ty, đổi mật khẩu, tạo link mới,
chạy lại/dừng app. Nút bấm do agent thực hiện (`./scripts/master.sh start`, container `ds_agent`, `master/agent.py`):
web không có quyền docker, chỉ ghi `configs/sites.json`, `configs/app.json` và yêu cầu vào `configs/.master/`.
App do master chạy nền trong `deepstream_dev` (không cần terminal); đừng chạy song song `run_w1.sh`.

**Mỗi công ty 1 dashboard riêng.** `./scripts/web.sh publish` chạy cho mỗi công ty 1 web riêng (`ds_web_<công ty>`,
chỉ nghe 127.0.0.1:8120+) và 1 tunnel riêng (`ds_web_tunnel_<công ty>`), nên mỗi công ty có 1 link
`https://*.trycloudflare.com` riêng, đăng nhập bằng tài khoản riêng (`web/companies.auth`). Link đổi khi tunnel
khởi động lại (cả khi reboot): xem lại bằng `./scripts/web.sh links`. Trong LAN vẫn xem được ở `/c/<công ty>/` của web chung. Server chỉ trả cam, cảnh báo, sự kiện, ảnh/clip, video live của công ty đó;
không hiện địa chỉ Redis hay URI camera. **Mỗi camera 1 link riêng**: `<link công ty>/cam/<camera_id>` (video live
có khung + thông tin camera + cảnh báo của riêng camera đó; bấm vào ô camera ở dashboard là tới).
Khung bbox vẽ trên video nhận qua **1 WebSocket mỗi trang** (`api/ws`, server đẩy 4 lần/giây, ô nhỏ giãn còn 10 fps,
~10 KB/s mỗi cam) thay vì hỏi lại liên tục. Trang `/` là admin (tài khoản `web/.auth`), thấy tất cả và có link sang từng công ty.

Web đọc Redis (`ds:frames`, `ds:events`, `ds:cameras`, `ds:camera_info`, `ds:companies`, `ds:buildings`, `ds:faces_unknown`) và `outputs/`;
chỉ ghi gallery khuôn mặt (`ds:faces`, `ds:face_tags`, `ds:face_emb`, `ds:faces_ver`). Gồm 3 tab chính + tab **Khuôn mặt**:
**Camera** (trạng thái, fps; bấm vào cam để xem **video live HLS từ MediaMTX, có vẽ bbox đồng bộ theo timestamp**,
người ở yên quá ngưỡng hiện khung đỏ), **Cảnh báo** (ảnh toàn khung + crop, giờ bắt đầu/cảnh báo/kết thúc,
xem clip gốc / clip có khung), **Sự kiện** (toàn bộ `ds:events`, lọc theo loại). Mã nguồn: `web/app.py`, `web/static/index.html`.
Chỉ để thử; app platform thật do team khác làm theo hợp đồng dữ liệu trong `archive/docs_cu/HUONG_DAN_TUAN1.md`.

## Không chạy được trong Docker?

| Hiện tượng | Nguyên nhân | Cách xử lý |
|-----------|------------|-----------|
| `docker: command not found` / "chạy script này trên HOST" | Đang ở trong container (`docker exec -it deepstream_dev bash`). `rtsp_sim.sh`, `redis_dev.sh`, `w1_edge_test.sh`, `setup_build_deps.sh`, `run_pipeline.sh` gọi docker nên chỉ chạy trên host | Thoát container, `cd /home/hiepplh/deepstream` rồi chạy. `run_w1.sh` / `run_task13.sh` chạy được ở cả hai nơi |
| `0/N cam có hình`, `Could not open resource`, `Not found` | RTSP giả lập chưa bật | Trên host: `./scripts/rtsp_sim.sh start 32`. App tự nối lại sau ~10 s, không cần chạy lại |
| `redis=DOWN` / `[redis] AUTH thất bại` | Redis dev chưa bật, sai `-R`, hoặc mật khẩu không khớp `.redis.env` | Trên host: `./scripts/redis_dev.sh start`. App vẫn chạy, giữ sự kiện trong hàng đợi |
| Tắt script trên host nhưng app vẫn chạy trong container | Chạy không có terminal: `docker exec` không chuyển Ctrl+C vào container | `run_w1.sh`/`run_task13.sh` đã tự chuyển tín hiệu. Nếu vẫn còn: `docker exec deepstream_dev pkill -INT -f w1-app` |
| Lỗi thiếu header khi build (`gst/gst.h`, `hiredis.h`) | Container `deepstream_dev` bị tạo lại | Trên host: `./scripts/setup_build_deps.sh` |
| Lần đầu chạy N cam đứng ~1 phút | nvinfer đang build engine batch N | Chờ, chỉ bị lần đầu |
