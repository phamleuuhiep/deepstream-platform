# Chạy lại app w1

Mọi lệnh chạy trên host, từ thư mục `/home/hiepplh/deepstream`.

## 1. Kiểm tra các thành phần đang chạy

Cách nhanh nhất là mở trang admin `http://192.168.1.30:8095/` và xem khung **Hệ thống** ở đầu trang. Mỗi thành phần có đèn xanh hoặc đỏ, cái nào tắt thì có nút **Bật**. Nút **Bật tất cả theo thứ tự** chỉ bật những thành phần đang tắt, theo thứ tự:

1. DeepStream (`deepstream_dev`)
2. Redis (`ds_redis`)
3. Camera giả lập (`ds_mediamtx` + `ds_sim_*`)
4. App w1
5. Web và link của từng công ty (`ds_web_<cty>`, `ds_web_tunnel_<cty>`)

Web admin (`ds_web`) và agent (`ds_agent`) phải bật trên host trước (`./scripts/web.sh start`, `./scripts/master.sh start`), vì trang admin và các nút bấm cần hai thành phần này. Lưu ý: bật lại **Camera giả lập** sẽ tạo lại mọi cam, nên các cam đang chạy cũng bị ngắt vài giây.

Kiểm tra bằng lệnh:

```bash
docker ps --format '{{.Names}}' | grep -E '^(deepstream_dev|ds_mediamtx|ds_redis|ds_agent|ds_web)$'
```

Phải có đủ 5 tên. Nếu thiếu, bật lại theo thứ tự sau (thường chỉ cần khi server vừa khởi động lại):

```bash
docker start deepstream_dev
./scripts/redis_dev.sh start
./scripts/rtsp_sim.sh sites          # MediaMTX + 19 cam giả lập, ghi configs/cameras_sim.txt
./scripts/web.sh start
./scripts/master.sh start            # agent thực hiện nút bấm trên dashboard
```

## 2. Chạy lại app (khuyên dùng)

Cách 1: mở trang admin (`http://192.168.1.30:8095/`, tài khoản trong `web/.auth`). Trang này mở thẳng phần cấu hình master, không có thanh tab. Kéo xuống cuối trang, ở dòng **Agent** bấm **Chạy lại app**. Dashboard của từng công ty (`/c/<công ty>/` hoặc link public của công ty) không có nút này; ở `/c/<công ty>/` bấm **← Về master** để quay lại.

Cách 2: dùng lệnh, kết quả giống cách 1:

```bash
echo '{"action":"app_restart"}' > configs/.master/request.json
sleep 15; cat configs/.master/status.json      # "ok": true là xong
```

Agent sẽ dừng app cũ (dừng sạch, đóng clip đang ghi), build lại rồi chạy nền với cấu hình trong `configs/app.json` (ngưỡng ở yên, smart record) và danh sách cam `configs/cameras_sim.txt`.

Muốn dừng hẳn app thì thay `app_restart` bằng `app_stop`.

## 3. Kiểm tra app đã chạy đúng

```bash
LOG=$(ls -t outputs/logs/w1_*.log | head -1); tail -f "$LOG" | grep --line-buffered stats
```

App chạy đúng khi:
- Dòng `[stats]` báo `19/19 cam có hình`, `redis=UP`, `drop=0`.
- Lần đầu chạy với số cam mới, app mất 1–2 phút để build engine.
- Cam 2 fps cần tới khoảng 60 s mới có hình.

## 4. Chạy tay để xem log trực tiếp (khi debug)

Phải dừng bản đang chạy nền trước, vì app không cho chạy 2 bản cùng lúc:

```bash
echo '{"action":"app_stop"}' > configs/.master/request.json; sleep 10
./scripts/run_w1.sh -i configs/cameras_sim.txt -t 120 -r 135     # Ctrl+C để dừng
```

Debug xong, bấm **Chạy lại app** để app chạy nền trở lại.

## 5. Lỗi thường gặp

- **`Đã có w1-app khác đang chạy ...`**: đang có bản app khác chạy. Dừng nó bằng `app_stop` rồi chạy lại.
- **`[redis] ... từ chối`, hoặc `redis=DOWN`**: Redis chưa chạy hoặc mật khẩu sai. Chạy `./scripts/redis_dev.sh start`. Mật khẩu nằm trong `.redis.env`.
- **`0/19 cam có hình` kéo dài**: luồng RTSP chưa phát. Kiểm tra bằng `./scripts/rtsp_sim.sh status`, cần thì chạy lại `./scripts/rtsp_sim.sh sites`.
- **`status.json` báo `"ok": false`**: xem lỗi trong mục `log` của file đó, hoặc chạy `./scripts/master.sh logs`.
- **`app không khởi động được`**: dashboard hiện các dòng lỗi cuối. Lỗi đầy đủ nằm trong `outputs/logs/w1_stderr.log`.
