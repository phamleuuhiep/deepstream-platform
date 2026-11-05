# DeepStream → Platform: bộ bàn giao (26/10/2026)

Gói này dành cho team platform để phát triển phần nhận, lưu và hiển thị kết quả từ pipeline DeepStream
(phát hiện người, tracking, **cảnh báo người ở yên quá 2 phút**, kèm ảnh và clip bằng chứng).

Toàn bộ dữ liệu trong gói được ghi lại từ pipeline thật, chạy ngày 26/10 trên 4 camera giả lập.
Có thể **chạy và phát triển không cần GPU, camera hay DeepStream**.

| Thư mục | Nội dung |
|---|---|
| `docker-compose.yml` | Redis + replay dữ liệu mẫu + web demo, chạy 1 lệnh |
| `contract/` | JSON Schema cho `ds:events` và `ds:frames`. **Đây là hợp đồng dữ liệu** |
| `sample_data/` | 16 sự kiện, 60 s metadata frame (4 cam × 30 fps), danh sách cam, 8 ảnh và 8 clip |
| `replay/replay.py` | Phát lại `sample_data/` vào Redis theo đúng nhịp thời gian thật, như app DeepStream đang chạy |
| `demo_web/` | Web demo bên DeepStream làm để tự kiểm tra kết quả. **Chỉ tham khảo, không phải sản phẩm** |
| `examples/redis_consumer.py` | Consumer mẫu dùng consumer group |

---

## 1. Chạy thử trong 2 phút

Cần Docker và Docker Compose v2.

```bash
docker compose up --build
# mở http://127.0.0.1:8095
```

- Khoảng 2 phút sau sẽ có 4 cảnh báo ở yên. Sau khoảng 10 s có clip gốc, sau khoảng 30–50 s có clip vẽ khung.
- Ctrl+C: replay gửi `dwell_end` với `reason=shutdown`, giống app thật khi tắt.
- Tua nhanh: `REPLAY_SPEED=8 docker compose up`, cảnh báo xuất hiện sau khoảng 15 s.
- Nếu cổng đã bị chiếm: `REDIS_PORT=6391 WEB_PORT=8096 docker compose up`.
- Code platform nối vào Redis ở **`127.0.0.1:6390`** (không có mật khẩu), giống môi trường dev bên DeepStream.

Chạy replay không cần Docker (Python ≥ 3.9):

```bash
pip install redis
python replay/replay.py --redis 127.0.0.1:6379 --data sample_data --speed 4
python examples/redis_consumer.py --port 6379
```

---

## 2. Kiến trúc

```
Camera RTSP ×N ─► DeepStream (GPU): giải mã → nvinfer (detect) → nvtracker ─► Redis Streams ─► PLATFORM
                                         │                                      ds:frames
                                         ├─ ảnh JPEG lúc cảnh báo                ds:events
                                         └─ clip MP4 (smart record)              ds:cameras
```

- DeepStream **chỉ ghi** vào Redis. Platform **đọc** bằng consumer group, nên chạy được nhiều instance và message chưa ACK sẽ được giao lại.
- Gửi Redis chạy trên thread riêng, có hàng đợi. Redis chết thì pipeline video vẫn chạy. Khi quá tải, app chỉ bỏ metadata frame, **không bao giờ bỏ sự kiện**.
- Quy mô đã đo: 32 cam 1080p30 trên 1 GPU, khoảng 960 frame/s vào `ds:frames`, không mất message.

---

## 3. Hợp đồng dữ liệu (Redis)

Prefix mặc định là `ds`, có thể đổi phía DeepStream.

| Key | Kiểu | Nội dung | Giới hạn |
|---|---|---|---|
| `ds:events` | Stream | Sự kiện nghiệp vụ (bảng dưới) | `MAXLEN ~100000` |
| `ds:frames` | Stream | Metadata từng frame: bbox, class, tracking ID | `MAXLEN ~100000` (32 cam × 30 fps thì chỉ giữ khoảng 100 s) |
| `ds:cameras` | Hash | `cam_id → rtsp uri` | Ghi lại mỗi lần app nối Redis |

Mỗi entry trong stream có đúng 4 field:

| Field | Ví dụ | Ghi chú |
|---|---|---|
| `cam` | `cam3` | ID camera, lấy theo đoạn cuối của URL RTSP |
| `type` | `dwell_alert` | Trùng với `data.type` |
| `ts` | `1792985495060` | Epoch **ms**, dạng chuỗi |
| `data` | `{...}` | Chuỗi JSON, schema trong `contract/` |

### Các loại sự kiện (`ds:events`)

| `type` | Khi nào | Field chính |
|---|---|---|
| `camera_online` | Cam bắt đầu có hình, hoặc có hình lại | `uri`, `ts`, `silent_s` |
| `camera_offline` | Không có frame quá 10 s (app tự nối lại) | `uri`, `ts`, `silent_s` |
| `dwell_alert` | Một người ở yên đủ ngưỡng (mặc định 120 s). **Mỗi lượt chỉ 1 lần** | `tid`, `start_ts`, `alert_ts`, `start_ms`, `dwell_s`, `bbox`, `snapshot`, `clip` |
| `dwell_end` | Người đó di chuyển (`moved`), mất track quá 5 s (`lost`), hoặc app tắt (`shutdown`) | như trên, thay `alert_ts` bằng `end_ts`, thêm `reason` |
| `clip_ready` | Đã ghi xong clip gốc, khoảng 10 s sau cảnh báo | `tag`, `file`, `duration_ms` |
| `clip_annotated` | Đã dựng xong clip có vẽ khung, khoảng 15–40 s sau `clip_ready` | `tag`, `file`, `source` |

Ví dụ thật lấy từ `sample_data/`:

```jsonc
// dwell_alert
{"type":"dwell_alert","cam":"cam4","tid":2,"start_ts":"2026-10-26T03:29:35.043Z","alert_ts":"2026-10-26T03:31:35.060Z",
 "start_ms":1792985375043,"dwell_s":120.0,"bbox":[81,464,125,314],
 "snapshot":"/outputs/w1_snapshots/cam4_tid2_1792985375043_frame.jpg","clip":"pending:cam4_tid2_1792985375043"}
// clip_ready
{"type":"clip_ready","cam":"cam4","tag":"cam4_tid2_1792985375043","file":"/outputs/w1_clips/cam4_-1_00000_20261026-033135_866.mp4","duration_ms":106400}
// dwell_end  (sample_data/example_dwell_end.json)
{"type":"dwell_end","cam":"cam4","tid":69,"start_ts":"…","end_ts":"…","start_ms":1792984925047,"dwell_s":444.9,…,"reason":"shutdown"}
```

### Ghép sự kiện thành 1 "cảnh báo" bên platform

```
dwell_alert ──(clip = "pending:<tag>")──► clip_ready.tag ──► clip_annotated.tag
     └──(cam, tid, start_ms)──► dwell_end
```

- **Khoá của một lượt ở yên:** `(cam, tid, start_ms)`. Không dùng `tid` một mình, vì tracking ID chỉ duy nhất trong 1 cam và 1 lần chạy app.
- **Ghép clip:** bỏ tiền tố `pending:` của `dwell_alert.clip` để lấy `tag`, rồi khớp với `tag` của `clip_ready`/`clip_annotated`. Coi `tag` là chuỗi không có nghĩa (opaque), không tự suy ra từ các field khác.
- `clip` có thể là `null` nếu bên DeepStream tắt smart record. `snapshot` có thể là `null` nếu encode ảnh lỗi.
- Có thể có `dwell_alert` mà không bao giờ có `dwell_end`, ví dụ khi app bị kill cứng. Platform nên tự đóng lượt sau một khoảng timeout.
- Ảnh crop người: thay `_frame.jpg` bằng `_crop.jpg` trong `snapshot`.

### Quy ước chung

- Thời gian: ISO 8601 **UTC** (đuôi `Z`), mili giây. Field `*_ms` là epoch ms. Platform tự đổi sang giờ VN khi hiển thị.
- Timestamp lấy theo giờ của server DeepStream lúc frame vào pipeline, nên server cần đồng bộ NTP.
- `bbox = [left, top, width, height]`, đơn vị pixel trên khung **1920×1080** (đã scale từ độ phân giải gốc của camera).
- Class của model hiện tại (TrafficCamNet): `0 car, 1 bicycle, 2 person, 3 roadsign`. **Model sẽ đổi** (PeopleNet hoặc model của team AI), nên đọc theo `label`, đừng hard-code `cls`.
- `tid = -1`: object chưa được track.
- "Ở yên" nghĩa là đứng **hoặc** ngồi một chỗ. Phiên bản hiện tại chưa phân biệt được ngồi và đứng (xem mục 6).

### Đọc đúng cách

- Dùng **consumer group** (`XREADGROUP` + `XACK` sau khi lưu xong). Mẫu: `examples/redis_consumer.py`.
- Xử lý **idempotent** theo stream ID (`1792985495083-0`), vì message có thể được giao lại.
- Thứ tự chỉ đảm bảo **trong cùng 1 stream**. `dwell_alert` luôn đến trước `clip_ready` của nó.
- `ds:frames` có lưu lượng lớn (960 msg/s với 32 cam, mỗi message khoảng 2,2 KB). Nếu chỉ cần lưu lịch sử, nên lấy mẫu thưa (ví dụ 1 bản ghi/giây/track) vào DB time-series. Phía DeepStream cũng có thể gửi thưa sẵn bằng tham số `-f N` (ví dụ 5 fps/cam).

---

## 4. File ảnh và clip

| Loại | Kích thước (thực tế) | Có sẵn lúc nào |
|---|---|---|
| `*_frame.jpg`: toàn khung 1920×1080 | ~200 KB | Ngay lúc `dwell_alert` |
| `*_crop.jpg`: crop người | ~10 KB | Ngay lúc `dwell_alert` |
| Clip gốc MP4 (H.264, không có khung) | Cam thật 4 Mbps: ~60 MB / 2 phút | Lúc `clip_ready` |
| `*_bbox.mp4`: clip vẽ khung (xanh = bình thường, vàng = đang ở yên, đỏ = quá ngưỡng) | Tương tự | Lúc `clip_annotated` |

Clip phủ từ khoảng **2 s trước lúc bắt đầu ở yên** đến **10 s sau cảnh báo**. Nếu cảnh báo xảy ra ngay sau khi app khởi động thì bộ đệm chưa đầy, nên clip sẽ ngắn hơn (clip mẫu cam4 chỉ dài 106 s).

> **Lưu ý:** đường dẫn trong sự kiện (`/outputs/...`) hiện là đường dẫn **local trên server DeepStream**.
> Trong gói này, file nằm ở `sample_data/outputs/` (map vào `/outputs`). Clip mẫu đã được nén xuống
> 960×540 cho nhẹ, bản thật là 1080p. Cách chuyển file sang platform cần thống nhất (mục 5).

---

## 5. Cần hai bên thống nhất

1. **Redis của platform:** host, port, mật khẩu (phía DeepStream đọc qua biến môi trường `REDIS_PASSWORD`). **Bắt buộc bật AOF** (`appendonly yes`): đã test với Redis không persistence, restart là mất sự kiện.
2. **Chuyển ảnh và clip:** chọn một trong ba cách:
   - (a) thư mục dùng chung;
   - (b) DeepStream upload lên S3/MinIO, rồi sự kiện mang URL thay cho đường dẫn;
   - (c) platform tự kéo qua HTTP.
   Đề xuất **(b)**. **Đã làm (05/11):** uploader đẩy lên S3/MinIO, sự kiện có thêm `<field>_url` (vd. `snapshot_url`,
   `snapshot_crop_url`, `file_url`). Cần platform gửi endpoint/bucket/khoá S3 thật để thay MinIO dev.
3. **`ds:frames`:** platform có cần đủ 30 fps/cam không, hay 5 fps/cam (`-f 6`) là đủ. Ngoài ra là `MAXLEN` mong muốn.
4. **ID camera:** hiện lấy theo đoạn cuối URL (`.../cam3` → `cam3`). Cần quy ước ID cố định, ví dụ theo mã camera trong hệ thống platform.
5. **Version của schema:** **đã có** `schema_version: 2` ở mọi sự kiện và frame (05/11), xem `contract/`. Platform nên bỏ qua các field lạ để tương thích về sau.
6. **Thời gian giữ ảnh/clip**, và ai chịu trách nhiệm xoá.

---

## 6. Giới hạn hiện tại và việc sắp tới (phía DeepStream)

- Mới phát hiện "ở yên", **chưa phân biệt ngồi và đứng**. Dự kiến thêm vùng ghế (ROI) và pose/classifier. Có thể sẽ thêm field mới vào `dwell_alert` (ví dụ `posture`, `zone`).
- Người bị che quá 5 s rồi xuất hiện lại sẽ thành track mới, và bộ đếm thời gian bắt đầu lại.
- Có thể thêm sự kiện mới: `face_match` (PoC khuôn mặt), và metric vận hành (fps, cam offline).
- **Dữ liệu replay:** các file và `tag` giữ tên gốc, còn timestamp được dời về thời điểm chạy. Frame mẫu 60 s phát lặp vòng, nên `tid` sẽ lặp lại theo chu kỳ. Với `--speed > 1`, `dwell_s` vẫn là 120 dù thời gian thực ngắn hơn.

## 7. Web demo (`demo_web/`)

Bên DeepStream làm web này để tự kiểm tra kết quả: FastAPI đọc Redis, cộng 1 trang HTML thuần. Gồm 3 tab:
**Camera** (trạng thái, fps, bbox live vẽ từ metadata), **Cảnh báo** (ảnh, giờ, clip), **Sự kiện**.
Có thể tham khảo cách ghép sự kiện trong hàm `alerts()` của `demo_web/app.py`.
Web **không có đăng nhập và không lưu DB**. Đây không phải thiết kế đề xuất cho sản phẩm.

API của demo, chỉ để tham khảo:
- `GET /api/state`: trạng thái từng cam.
- `GET /api/alerts`: danh sách cảnh báo đã ghép.
- `GET /api/events`: sự kiện thô.
- `/media/...`: ảnh và clip.

---

Liên hệ phía DeepStream: tindd@hbcnexus.ai
