# Checklist Tuần 1 – Đa luồng RTSP & edge case

Ngày 30/10/2026. Mỗi việc có 1 lệnh test, chạy trên host từ thư mục `/home/hiepplh/deepstream`. Mỗi lần chỉ chạy 1 lệnh. Chạy xong hết thì dọn bằng `./scripts/w1_bc_test.sh don-test`. Ảnh và file kết quả nằm trong `outputs/bao_cao_tuan1/`.

## Tuần 1

- [x] **19 luồng RTSP giả lập chạy trên MediaMTX**
  - Test: `./scripts/w1_bc_test.sh rtsp`
  - Đạt khi: cả 19 luồng in ra codec, độ phân giải và fps, không có dòng LỖI
  - 📷 Minh chứng: *(kéo thả hình vào đây)*

- [x] **App gửi metadata và sự kiện sang Redis**
  - Test: `./scripts/w1_bc_test.sh redis`
  - Đạt khi: có dữ liệu trong `ds:frames` và `ds:events`, 19 camera, dòng `[stats]` báo `19/19 cam có hình`, `redis=UP`, `drop=0`
  - 📷 Minh chứng: *(kéo thả hình vào đây)*

- [x] **Cảnh báo người ở yên quá 2 phút, kèm ảnh và clip**
  - Test: `./scripts/w1_bc_test.sh canh-bao`, chụp thêm tab Cảnh báo trên web
  - Đạt khi: cảnh báo ghi `ở yên 120s`, có giờ bắt đầu, có ảnh người bị cảnh báo; clip có khung đỏ
  - 📷 Minh chứng: *(kéo thả hình vào đây)*

## Việc bổ sung ngày 30/10

- [x] **Người bị che vài giây không bị đếm lại thời gian ở yên từ đầu**
  - Test: `./scripts/w1_bc_test.sh gop-track` (khoảng 2 phút)
  - Đạt khi: log có `[GỘP TRACK] ... sau 8.1s bị che`, người đó chỉ bị cảnh báo 1 lần, tổng thời gian ở yên khoảng 89 s liền mạch
  - 📷 Minh chứng: *(kéo thả hình vào đây)*

- [x] **Tự xoá ảnh/clip cũ (quá 30 ngày hoặc vượt 50 GB)**
  - Test: `./scripts/w1_bc_test.sh don-file`
  - Đạt khi: log có `[dọn file] xoá 2 ảnh/clip cũ nhất`, chỉ còn lại file giả 1 ngày tuổi, file thật không bị xoá
  - 📷 Minh chứng: *(kéo thả hình vào đây)*

- [x] **Kiểm chứng không bị vỡ hình khi nối camera giữa chừng**
  - Test: `./scripts/w1_bc_test.sh nvdec`
  - Đạt khi: ảnh `3_nvdec_frame1.jpg` là hình sạch; ảnh đối chứng `3_ffmpeg_showall_frame1.jpg` xám/vỡ (nên đặt 2 ảnh cạnh nhau)
  - 📷 Minh chứng: *(kéo thả hình vào đây)*

- [x] **Sửa lỗi camera fps thấp bị reset liên tục, không bao giờ có hình**
  - Test: `./scripts/w1_bc_test.sh gop-dai` (khoảng 2 phút)
  - Đạt khi: lúc đầu `0/1 cam có hình`, khoảng 40 s sau có hình 2 fps, `Số lần reset: 0`
  - 📷 Minh chứng: *(kéo thả hình vào đây)*

- [x] **Chặn chạy 2 bản app cùng lúc**
  - Test: `./scripts/w1_bc_test.sh chay-trung`
  - Đạt khi: bản thứ hai báo `Đã có w1-app khác đang chạy`, mã thoát 1, chỉ còn 1 bản app
  - 📷 Minh chứng: *(kéo thả hình vào đây)*

- [x] **Sửa lỗi thiếu/sai mật khẩu Redis làm spam log và không tắt được app**
  - Test: `./scripts/w1_bc_test.sh sai-mk` (khoảng 1 phút)
  - Đạt khi: cả 2 trường hợp chỉ có 1 dòng `[redis]`, `redis=DOWN`, tắt sạch (mã 124)
  - 📷 Minh chứng: *(kéo thả hình vào đây)*

## Chưa làm

- [ ] **Nhận diện khuôn mặt + vector DB (Task 1.2)**
  - Lý do: chưa có model; cần team AI bàn giao model vào thư mục `models/`
  - Đã có: bản thiết kế trong `archive/docs_cu/HUONG_DAN_TUAN1.md`
