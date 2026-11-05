# Checklist Tuần 2 – Tích hợp platform, "ngồi" thật, chạy ổn định

Tuần 02/11 – 06/11/2026, cập nhật 05/11. Kế hoạch chi tiết: `archive/docs_cu/NGHIEM_THU.md` (Tuần 2).
Mỗi việc có 1 lệnh test, chạy trên host từ thư mục `/home/hiepplh/deepstream`. **Mỗi lần chỉ chạy 1 lệnh** (các bài
dùng chung MediaMTX và luồng `w2-*`). Chạy xong hết thì dọn bằng `./scripts/w2_bc_test.sh don-test`. Kết quả từng bài
in ra terminal và lưu vào `outputs/bao_cao_tuan2/<số>_<bài>.txt` (+ ảnh).

Bài test không đụng app đang chạy: app test dùng prefix Redis riêng (`w2*`), luồng RTSP riêng (`w2-*`), chạy trên
GPU 1. Riêng bài `tu-hoi-phuc` cố ý làm chết hệ thống thật (các camera mất hình ~1 phút).

**Cách chụp minh chứng:** chụp cửa sổ terminal lúc bài test vừa chạy xong (thấy được lệnh, bảng kết quả và dòng
`ĐẠT`), cộng thêm ảnh/màn hình ghi ở từng mục. File `.txt` cùng tên trong `outputs/bao_cao_tuan2/` là bản đầy đủ để
đính kèm.

## Tuần 2

- [x] **App gửi sự kiện vào Redis của platform, platform đọc được bằng consumer group** (Task 2.1)
  - Test: `./scripts/w2_bc_test.sh platform` (~2 phút)
  - Đạt khi: consumer group của platform nhận đủ `dwell_alert` → `dwell_end` → `clip_ready` của 1 lượt test, mọi sự kiện có `schema_version`
  - Kết quả 05/11: group `platform` nhận 16/16 sự kiện, 0 sự kiện thiếu `schema_version: 2`, 0 pending; 4 lượt ở yên đủ 3 sự kiện.
    (Lượt cảnh báo trong 10 s cuối không có clip vì app test tắt trước khi ghi xong 10 s sau cảnh báo.)
  - Đã làm: mọi JSON (sự kiện + frame) có `schema_version: 2`; hợp đồng dữ liệu `handoff/deepstream_platform_kit/contract/` cập nhật theo.
    Redis dev đang dùng thay Redis platform (chưa nhận thông tin Redis thật); đổi địa chỉ bằng `-R host:port` + `REDIS_PASSWORD`.
  - 📷 Minh chứng: terminal (bảng "Từng lượt ở yên" + JSON 1 sự kiện có `schema_version`) · *(kéo thả hình vào đây)*

- [x] **Ảnh và clip upload lên S3/MinIO, sự kiện mang URL** (Task 2.1)
  - Test: `./scripts/w2_bc_test.sh upload` (~3 phút)
  - Đạt khi: mở được ảnh và clip qua URL trong sự kiện; file local chỉ bị dọn sau khi đã upload
  - Kết quả 05/11: 14/14 URL trả HTTP 200 (ảnh toàn khung, ảnh crop, clip MP4); file giả 40 ngày tuổi đã upload bị xoá, file chưa upload được giữ.
  - Đã làm: MinIO dev `ds_minio` (:9000, bucket `ds-media`) + uploader `ds_uploader` (`uploader/uploader.py`, đọc `ds:events`
    bằng consumer group, thử lại khi S3 lỗi, ghi `<file>.up`); app `-M <URL gốc>` thêm `snapshot_url`, `snapshot_crop_url`,
    `file_url`, ... vào sự kiện và chỉ dọn file đã có `.up`. Lệnh: `./scripts/upload.sh minio|start|status|logs`.
    S3 thật của platform: viết `.s3.env` rồi `./scripts/upload.sh restart` (xem đầu `scripts/upload.sh`).
  - 📷 Minh chứng: terminal (danh sách URL `200`) + mở 1 URL ảnh trong trình duyệt (vd. `http://192.168.1.30:9000/ds-media/w1_snapshots/...`);
    file đã tải qua URL: `outputs/bao_cao_tuan2/2_upload_tai_ve_qua_url.jpg`, `.mp4` · *(kéo thả hình vào đây)*

- [x] **Ngắt Redis/S3 5 phút không mất sự kiện** (Task 2.1)
  - Test: `./scripts/w2_bc_test.sh ngat-platform` (~8 phút; `OUTAGE=60 ./scripts/w2_bc_test.sh ngat-platform` để thử nhanh)
  - Đạt khi: số sự kiện platform nhận bằng số app tạo ra, file upload bù sau khi nối lại, `drop=0` với sự kiện
  - Kết quả 05/11: trong 5 phút ngắt, hàng đợi app lên 17.379 message; nối lại 30 s đã gửi hết. App tạo 57 sự kiện = Redis có 57 =
    consumer group nhận 57; uploader upload bù, 91/91 URL mở được. `drop=2` là 2 frame metadata của lô đang gửi lúc mất kết nối
    (thiết kế: frame có thể bỏ, sự kiện không bao giờ bỏ).
  - Ghi chú: bài dùng Redis test riêng (:6391, có AOF) và MinIO test riêng (:9010), tắt bằng `docker stop`, không đụng Redis/MinIO thật.
  - 📷 Minh chứng: terminal (các dòng "đang ngắt: redis=DOWN queue=..." rồi "sau 30s: redis=UP queue=0 ... events=44/44" và dòng tổng kết) · *(kéo thả hình vào đây)*

- [x] **Model phát hiện người ngồi tốt hơn TrafficCamNet** (Task 2.2)
  - Test: `./scripts/w2_bc_test.sh model-nguoi` (~3 phút, lần đầu ~8 phút do build engine)
  - Đạt khi: bảng so sánh PeopleNet và TrafficCamNet trên `dwell_sitting_180s.mp4` và `sample_office.mp4`; người ngồi được phát hiện ngay từ đầu video
  - Kết quả 05/11 (thêm YOLO11s đang chạy thật):

    | Model | % frame thấy người ngồi | thấy lần đầu | `sample_office`: người/frame |
    |---|---|---|---|
    | TrafficCamNet | 30,2 % | 1,6 s | 0,00 (không thấy ai) |
    | PeopleNet v2.6.3 (FP16) | 88,4 % | 0,7 s | 0,99 |
    | **YOLO11s COCO** (đang dùng) | **99,6 %** | **0,2 s** | 1,96 |
  - Đã làm: `configs/pgie_peoplenet.txt` (PeopleNet tải bằng `./scripts/prepare_models.sh`). App thật đã chạy YOLO11s từ 03/11.
  - 📷 Minh chứng: terminal (bảng) + ảnh `outputs/bao_cao_tuan2/4_model_nguoi_giay10.jpg` (cùng 1 frame, 3 model; khung đỏ = người ngồi:
    TrafficCamNet không có khung đỏ) · *(kéo thả hình vào đây)*

- [x] **Chỉ cảnh báo người trong vùng ghế/khu chờ (ROI)** (Task 2.2)
  - Test: (tuỳ chọn) dashboard master → camera dùng video `dwell_sitting_180s.mp4` → nút **Vùng ghế** → bấm 4 điểm quanh chỗ người
    ngồi trên gờ tường → **Dùng vùng này** → **Lưu & áp dụng**; rồi `./scripts/w2_bc_test.sh roi` (~2 phút; chưa vẽ thì bài dùng vùng mặc định)
  - Đạt khi: người ngồi trong vùng bị cảnh báo; người đứng chờ ngoài vùng không bị cảnh báo
  - Kết quả 05/11: cùng 1 video, camera có vùng ghế: 1 cảnh báo (người ngồi); camera không có vùng: 2 cảnh báo (người ngồi + người đứng chờ ngay cạnh).
  - Đã làm: trình vẽ vùng trên video live ở dashboard master, lưu `roi` trong `configs/sites.json` → cột JSON của `cameras_sim.txt`;
    app chỉ tính ở yên khi chân người (giữa cạnh dưới bbox) trong vùng, ra khỏi vùng → `dwell_end` lý do `left_roi`; video live vẽ vùng màu vàng.
    Làm trong app (point-in-polygon) thay cho `nvdsanalytics`: đủ cho 1 vùng/cam, không thêm plugin vào pipeline.
  - 📷 Minh chứng: (1) màn hình trình vẽ vùng ghế trên dashboard master; (2) terminal; (3) ảnh `outputs/bao_cao_tuan2/5_roi_canh_bao.jpg`
    (vàng = vùng ghế, đỏ = người bị cảnh báo) · *(kéo thả hình vào đây)*

- [x] **Độ chính xác cảnh báo ngồi trên ≥ 10 lượt có gán nhãn** (Task 2.2)
  - Test: `./scripts/w2_bc_test.sh do-lech` (~4 phút; lần đầu thêm ~3 phút tạo video)
  - Đạt khi: độ lệch thời gian cảnh báo ≤ 5 s ở mọi lượt, 0 cảnh báo nhầm
  - Kết quả 05/11 (ngưỡng 30 s, có vùng ghế): 10/10 lượt ngồi đủ lâu cảnh báo đúng, lệch lớn nhất 0,2 s; 9/9 lượt ngồi ngắn (18 s)
    không cảnh báo; 0 cảnh báo nhầm (người đứng chờ ngoài vùng đứng yên 45 s ở các lượt dương cũng không bị báo).
  - Nhãn: `outputs/bao_cao_tuan2/6_do_lech_nhan.csv`. Video lượt test do script tạo (`videos/.test/lech_*.mp4`): màn đen rồi cắt sang
    cảnh có người ngồi, nên lúc bắt đầu ngồi biết chính xác. Là video tổng hợp từ 1 cảnh; cần gán nhãn lại trên camera thật.
  - 📷 Minh chứng: terminal (bảng 19 lượt + dòng tổng kết) · *(kéo thả hình vào đây)*

- [x] **Tự bật lại thành phần bị chết** (Task 2.4)
  - Test: `./scripts/w2_bc_test.sh tu-hoi-phuc` (xoá `ds_mediamtx` và kill app thật, ~2 phút)
  - Đạt khi: trong 1 phút agent tự bật lại theo thứ tự, dashboard ghi lại sự kiện, `[stats]` về `19/19 cam có hình`
  - Kết quả 05/11: agent phát hiện sau ~7 s, bật lại sim → app trong 15 s; app mới chạy và gửi thống kê sau 32 s; 14/19 cam có hình
    sau 41 s, 19/19 sau 78 s (5 cam giả lập phát video 2 fps GOP dài chờ khung hình đầu thêm ~40 s; camera thật GOP 1–2 s thì lên cùng lúc).
  - Đã làm: agent kiểm mỗi 5 s, thành phần chết thì bật lại đúng thứ tự (DeepStream → Redis → camera giả lập → upload → app → web),
    ghi log lên khung log dashboard master + sự kiện `system_heal` vào `ds:events` (tab Sự kiện). Không bật lại app mà người dùng
    đã bấm **Dừng app**; bật lỗi 3 lần liên tiếp thì nghỉ 5 phút. Tắt/bật: ô **Tự hồi phục** trên dashboard master.
    Server khởi động lại: `ds_agent` tự lên (`--restart unless-stopped`) rồi bật nốt `deepstream_dev` và app.
  - 📷 Minh chứng: terminal (dòng thời gian +Xs) + dashboard master khung **Hệ thống** và log "TỰ HỒI PHỤC" + tab **Sự kiện** lọc `system_heal` · *(kéo thả hình vào đây)*

- [x] **Dashboard Grafana cho pipeline** (Task 2.4)
  - Test: `./scripts/w2_bc_test.sh grafana`, rồi mở `http://192.168.1.30:3001` → dashboard **DeepStream pipeline** (Prefix Redis = `ds`)
  - Đạt khi: có fps/cam, số cam có hình, queue/drop Redis, số cảnh báo, RAM/VRAM app
  - Kết quả 05/11: đủ 12 panel: app chạy, cam có hình, FPS tổng + từng cam, queue/drop Redis, sự kiện/phút theo loại, cảnh báo ở yên 24 h,
    RAM (RSS) và VRAM app, upload S3.
  - Đã làm: exporter `ds_exporter` (:9466/metrics, `monitor/ds_exporter.py`) + Prometheus `ds_prometheus` (:9091) + Grafana `ds_grafana`
    (:3001, xem không cần đăng nhập), bật bằng `./scripts/monitor.sh start`. Grafana/Prometheus **chung** của server (:3000/:9090) thuộc
    tài khoản `nhatdm`, không sửa được từ đây: nhờ admin thêm job scrape trong `monitor/prometheus.yml` (target `192.168.1.30:9466`) và
    import `monitor/grafana/dashboards/deepstream.json`.
  - 📷 Minh chứng: màn hình Grafana dashboard **DeepStream pipeline** · *(kéo thả hình vào đây)*

- [ ] **Soak test 24 giờ trên 32 cam** (Task 2.4)
  - Test: `./scripts/w2_bc_test.sh soak start` → sau 24 giờ `./scripts/w2_bc_test.sh soak report` → `./scripts/w2_bc_test.sh soak stop`
  - Đạt khi: 24 h không crash, RAM/VRAM không tăng dần, mọi lần rớt cam đều tự hồi phục, `drop=0`
  - Trạng thái 05/11: **chưa chạy đủ 24 giờ.** Công cụ đã sẵn sàng và đã chạy thử: 32 cam trên GPU 1, đúng pipeline đang chạy thật
    (YOLO11s + tìm mặt + embedding, ngưỡng ở yên 120 s, ghi clip), prefix `w2soak`; ghi mỗi phút RAM/VRAM/cam/FPS/drop vào
    `outputs/bao_cao_tuan2/9_soak.csv`, app chết thì ghi lại và bật lại; `report` tính xu hướng RAM/VRAM (MB/giờ), số lần rớt cam/hồi phục.
    Chạy thử 05/11 (~7 phút): 32/32 cam, ~947 FPS tổng (~30 fps/cam) trên GPU 1, RAM app ~6,6 GB, VRAM 3,9 GB, `drop=0`, 0 lần chết.
    Ảnh/clip + dọn file của soak nằm riêng trong `outputs/w2soak/` (xem mục Sự cố).
    Đêm 04/11 chưa có công cụ này. Soak chiếm GPU 1 (dùng chung với dịch vụ khác) 24 giờ: chọn khung giờ rồi chạy `soak start`.
  - 📷 Minh chứng: terminal `soak report` + Grafana (Prefix Redis = `w2soak`, khoảng 24 h, panel RAM/VRAM phẳng) · *(kéo thả hình vào đây)*

- [x] **Hơn 40 cam bằng cách chia sang GPU 1** (Task 2.4)
  - Test: `./scripts/w2_bc_test.sh gpu1` (~4 phút)
  - Đạt khi: 2 instance (GPU 0 và GPU 1) cùng chạy, tổng > 40 cam, mỗi cam ~30 fps, không gửi trùng dữ liệu
  - Kết quả 05/11: 44 cam (22 + 22), 44/44 có hình, fps mỗi cam 28,2–29,9 (trung bình 29,5), 0 cam trùng giữa 2 instance,
    metadata ~1.293 bản ghi/s = tổng FPS (không trùng). Chạy cùng lúc app thật (19 cam, YOLO + khuôn mặt) trên GPU 0.
  - Đã làm: app `-g <GPU>` (mọi element chạy trên GPU đó, khoá chạy 1 bản theo prefix + GPU). Bài test dùng TrafficCamNet; với YOLO11s +
    khuôn mặt, 1 GPU hiện chạy 19 cam ở ~420 FPS (đủ 30 fps/cam), số cam tối đa mỗi GPU cần đo thêm (soak 32 cam).
  - 📷 Minh chứng: terminal (2 dòng `[stats]` GPU 0/GPU 1 + dòng tổng kết) + `nvidia-smi` lúc đang chạy (2 GPU đều có tiến trình w1-app) · *(kéo thả hình vào đây)*

- [ ] **Camera thật Hikvision/Dahua** (Task 2.4, nếu có camera)
  - Test: `./scripts/w2_bc_test.sh cam-that rtsp://<user>:<mk>@<ip>:554/Streaming/Channels/101 rtsp://<user>:<mk>@<ip>:554/Streaming/Channels/102`
    (Dahua: `/cam/realmonitor?channel=1&subtype=0` và `subtype=1`); trong lúc chạy, rút dây mạng camera ~30 s rồi cắm lại
  - Đạt khi: cả main stream và sub stream (H.265) lên hình, rút mạng camera rồi cắm lại thì tự hồi phục
  - Trạng thái 05/11: **chưa có camera thật.** Bài test đã sẵn (probe codec/độ phân giải từng luồng, app 180 s, in `camera_offline`/`camera_online`).
  - 📷 Minh chứng: terminal (probe 2 luồng + các dòng `[CAM]`) · *(kéo thả hình vào đây)*

## Task 2.3 – Khuôn mặt (làm 03/11 – 05/11)

- [x] **Nhận diện khuôn mặt người đã đăng ký, thông báo trên dashboard** (Task 2.3)
  - Test: `./scripts/w2_bc_test.sh khuon-mat` (~3 phút)
  - Đạt khi: đăng ký 1 người từ camera rồi chạy lại thì nhận ra (sự kiện `face_recognized`); video người khác không nhận nhầm
  - Kết quả 05/11: nhận ra người đạp xe ở cosine 0,80 (ngưỡng 0,42); 172 khuôn mặt người khác (`sample_walk.mov`) đem so khớp, 0 nhận nhầm.
    Đo ngưỡng (19 cam, 200 s): người khác trên 16 cam video mẫu không lượt nào đạt 0,30; chính người đăng ký (video tự quay
    bằng điện thoại) đạt 0,31–0,49. Ngưỡng 0,42–0,45 là an toàn.
  - Đã làm (khác kế hoạch): YOLO11s COCO → FaceNet tìm mặt **trong bbox người** → ArcFace (InsightFace) → so cosine với gallery trên
    Redis (`ds:face_emb`), không dùng Qdrant: vài trăm người so trực tiếp trong app < 1 ms/frame, khỏi thêm 1 dịch vụ. Hạng khách
    VIP/thường theo hồ sơ (classifier VIP chờ model team AI). Tab **Khuôn mặt** trên dashboard: đăng ký bằng ảnh / webcam / mặt lạ
    camera vừa gặp, thông báo góc màn hình. Chi tiết: `docs/PIPELINE_KHUON_MAT_VIP.md`.
  - Chưa làm / lưu ý: model InsightFace chỉ cho nghiên cứu, YOLO11 AGPL-3.0; cần sự đồng ý của người được đăng ký (NĐ 13/2023).
  - 📷 Minh chứng: terminal + dashboard tab **Khuôn mặt** (danh sách người đăng ký, "Nhận diện gần đây") + thông báo góc phải
    khi camera nhận ra · *(kéo thả hình vào đây)*

## Việc làm sớm ngày 31/10

- [x] **Bật từng thành phần hệ thống trên dashboard, theo đúng thứ tự**
  - Test: trang admin, khung **Hệ thống**: bấm **Dừng app**, đợi đèn **App w1** chuyển đỏ, bấm **Bật** ở dòng đó
  - Đạt khi: đèn chuyển xanh, log ghi `app đang chạy`, `[stats]` về `19/19 cam có hình`; nếu app không lên thì log hiện dòng lỗi (đủ trong `outputs/logs/w1_stderr.log`)
  - Ghi chú 05/11: khung **Hệ thống** có thêm dòng **Upload ảnh/clip (S3)**; app đã bấm **Dừng** thì tự hồi phục không bật lại.
  - 📷 Minh chứng: *(kéo thả hình vào đây)*

- [x] **Nút xoá cảnh báo cũ (giữ 10 phút gần nhất)**
  - Test: trang admin, bấm **Xoá cảnh báo cũ** rồi xác nhận
  - Đạt khi: log ghi số sự kiện, số file ảnh/clip và dung lượng đã xoá; tab Cảnh báo chỉ còn cảnh báo trong 10 phút gần nhất
  - 📷 Minh chứng: *(kéo thả hình vào đây)*

- [x] **App chạy liên tục 22 giờ với 19 cam (soak sơ bộ)**
  - Test: `L=outputs/logs/w1_20261030_120826.log; grep stats $L | sed -n '1p;$p'; grep -o 'drop=[0-9]*' $L | sort | uniq -c`
  - Đạt khi: chạy từ 30/10 12:08 đến 31/10 10:17 (22 giờ 09 phút), cả 15.950 dòng `[stats]` đều `drop=0`, 32,8 triệu message vào Redis
  - Ghi chú: chưa đo RAM/VRAM theo thời gian, làm đủ ở soak test 24 giờ
  - 📷 Minh chứng: *(kéo thả hình vào đây)*

## Sự cố 05/11 10:12: mất ảnh/clip cũ của hệ thống thật

- Lần chạy thử soak đầu tiên chạy app test với `-k 1 -K 5` (giữ 1 ngày / 5 GB, để soak không đầy đĩa). App lúc đó luôn ghi và dọn
  ở `outputs/w1_snapshots`, `outputs/w1_clips` **dùng chung với app thật**, nên đã xoá **6.134 ảnh/clip cũ hơn 1 ngày (1,7 GB)**
  của hệ thống thật. Dừng soak sau ~1,5 phút, trước lượt dọn thứ 2.
- Mất: ảnh của các cảnh báo trước 04/11 10:12 (251 cảnh báo cũ trong `ds:events` giờ không có ảnh trên tab Cảnh báo) và clip trước
  05/11 09:15. Không khôi phục được (chưa có bản sao: uploader chỉ upload sự kiện mới từ 05/11 09:08). Không mất: sự kiện / metadata
  trong Redis, minh chứng tuần 1 (`outputs/bao_cao_tuan1/`), app thật vẫn chạy bình thường.
- Đã sửa: app có `-o <thư mục>`: ảnh/clip **và dọn file** chỉ trong thư mục đó; mọi app test/soak trong `scripts/w2_bc_test.sh` dùng
  `-o` (`outputs/w2test/`, `outputs/w2soak/`). Từ nay ảnh/clip mới được upload lên MinIO, file local chỉ bị dọn khi đã upload.

## Thay đổi trong app / hệ thống (để đối chiếu)

| Phần | Thay đổi tuần 2 | Ở đâu |
|---|---|---|
| `w1-app` | `schema_version: 2` mọi JSON; `-M <URL>` (URL ảnh/clip, chỉ dọn file đã upload); ROI từng cam; `-g <GPU>`; `-o <thư mục>` (ảnh/clip + dọn file riêng); thống kê `<prefix>:stats:g<GPU>` mỗi 5 s (RSS, fps/cam, queue, drop, sự kiện đã tạo/đã gửi); `[stats]` in `events=đã gửi/đã tạo` | `apps/w1_multistream/w1_app.c` |
| Upload | MinIO dev `ds_minio` + `ds_uploader` | `scripts/upload.sh`, `uploader/` |
| Agent master | tự hồi phục mỗi 5 s, thành phần "upload", `-M` khi có `.s3.env`, chỉ dừng/đếm app chính (không đụng app test) | `master/agent.py` |
| Dashboard | vẽ vùng ghế, ô Tự hồi phục, sự kiện `system_heal`, tab Khuôn mặt | `web/` |
| Metric | `ds_exporter` + Prometheus + Grafana riêng | `scripts/monitor.sh`, `monitor/` |
| Model | PeopleNet để so sánh; app thật chạy YOLO11s + khuôn mặt | `configs/pgie_peoplenet.txt`, `scripts/prepare_models.sh` |
| Hợp đồng dữ liệu | `schema_version`, `*_url`, `face_recognized`, `vip_detected`, `system_heal`, `left_roi` | `handoff/deepstream_platform_kit/contract/` |
| Test | 12 bài | `scripts/w2_bc_test.sh` |

## Còn mở

- Thông tin Redis + S3 thật của platform (đang dùng Redis dev + MinIO dev; đổi bằng `-R` và `.s3.env`).
- Soak test 24 giờ trên 32 cam: cần chọn khung giờ dùng GPU 1.
- Camera thật Hikvision/Dahua.
- Model classifier VIP của team AI; định nghĩa "VIP nhìn thấy được" trước khi gán nhãn.
- Gán nhãn độ lệch trên video ngồi thật (hiện là video tổng hợp từ `dwell_sitting_180s.mp4`).
