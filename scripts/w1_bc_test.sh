#!/usr/bin/env bash
# Chạy lại từng bài test trong docs/BAO_CAO_TUAN1.md để lấy minh chứng (ảnh chụp màn hình / file đính kèm Notion).
# Không đụng app đang chạy: app test dùng prefix Redis riêng (w1test), luồng RTSP test riêng (test-*),
# log riêng outputs/logs/bc_<bài>.log, và chỉ dừng đúng tiến trình test (timeout trong container, không pkill).
# Kết quả mỗi bài in ra màn hình và lưu vào outputs/bao_cao_tuan1/<bài>.txt (+ ảnh nếu có).
# Dùng: ./scripts/w1_bc_test.sh <bài>
#   rtsp        E1  các luồng RTSP giả lập + probe từng luồng                     (~40 s)
#   redis       E2  dữ liệu app đang gửi lên Redis                                (~2 s)
#   canh-bao    E3  cảnh báo ở yên 2 phút gần nhất: alerts.log, ảnh, clip          (~2 s)
#   gop-track   #1  người bị che 8 s -> gộp vào lượt ở yên cũ                    (~2 phút)
#   don-file    #2  tự xoá ảnh/clip cũ (file giả 40 ngày tuổi)                     (~40 s)
#   nvdec       #3  5 frame đầu sau khi nối giữa GOP: NVDEC so với ffmpeg          (~20 s)
#   gop-dai     #4  cam 1 keyframe / 60 s vẫn lên hình, không reset                (~100 s)
#   chay-trung  #5  bản app thứ 2 cùng prefix bị từ chối                           (~2 s)
#   sai-mk      #6  thiếu / sai mật khẩu Redis: 1 dòng cảnh báo, tắt sạch          (~1 phút)
#   don-test        xoá container, key Redis, ảnh/clip do các bài test tạo (giữ outputs/bao_cao_tuan1/)
set -euo pipefail
if ! command -v docker >/dev/null; then
  echo "Lỗi: chạy script này trên HOST (cd /home/hiepplh/deepstream), không chạy trong container." >&2
  exit 1
fi
cd "$(dirname "$0")/.."
. scripts/redis_env.sh

APP=/workspace/apps/w1_multistream/w1-app
PREFIX=w1test
RTSP=rtsp://127.0.0.1:8554
FFMPEG_IMAGE=linuxserver/ffmpeg:latest
OUT=outputs/bao_cao_tuan1
mkdir -p "$OUT"

# run_app <giây> <tên log> <tham số w1-app...>: app test chạy <giây> giây rồi nhận SIGINT (chỉ tiến trình này),
# không tắt được trong 30 s nữa thì bị kill. In mã thoát của timeout (124 = tắt bằng SIGINT, 137 = phải kill).
run_app() {
  local sec=$1 log=$2; shift 2
  docker exec deepstream_dev rm -f "/outputs/logs/$log"   # log do app (root) tạo
  echo "... app test chạy ${sec}s (log: outputs/logs/$log)"
  local rc=0
  docker exec -e LANG=C.UTF-8 -e REDIS_PASSWORD="${PASS_OVERRIDE-${REDIS_PASSWORD:-}}" deepstream_dev \
    timeout -s INT -k 30 "$sec" "$APP" -P "$PREFIX" -l "/outputs/logs/$log" "$@" >/dev/null 2>&1 || rc=$?
  APP_RC=$rc
}

# Tiêu đề chung cho mỗi file minh chứng
header() {
  echo "=== $1 ==="
  echo "Thời điểm: $(date '+%H:%M:%S %d/%m/%Y') · máy: $(hostname)"
  echo
}

test_rtsp() {
  header "E1 - Luồng RTSP giả lập (MediaMTX :8554)"
  grep -v '^#' configs/cameras_sim.txt | awk '{print $3, $4}' | while read -r cam uri; do
    r=$(docker run --rm --network host --entrypoint ffprobe "$FFMPEG_IMAGE" -v fatal -rtsp_transport tcp \
      -timeout 5000000 -select_streams v:0 -show_entries stream=codec_name,width,height,avg_frame_rate \
      -of csv=p=0 "$uri" || true)
    printf "%-16s %-45s %s\n" "$cam" "${uri/127.0.0.1/192.168.1.30}" "${r:-LỖI: không có luồng}"
  done
}

test_redis() {
  header "E2 - Dữ liệu app gửi lên Redis (prefix ds)"
  local cli=(./scripts/redis_dev.sh cli)
  echo "XLEN ds:frames  = $("${cli[@]}" XLEN ds:frames)"
  echo "XLEN ds:events  = $("${cli[@]}" XLEN ds:events)"
  echo "HLEN ds:cameras = $("${cli[@]}" HLEN ds:cameras)"
  echo
  echo "--- 1 bản ghi ds:frames mới nhất (cắt 400 ký tự):"
  "${cli[@]}" XREVRANGE ds:frames + - COUNT 1 | grep '^{' | cut -c1-400 || true
  echo
  echo "--- 5 sự kiện mới nhất (loại + camera):"
  "${cli[@]}" XREVRANGE ds:events + - COUNT 5 | grep -oE '"type":"[a-z_]+","cam":"[^"]+"' || true
  echo
  echo "--- Log app đang chạy:"
  grep '\[stats\]' "$(ls -t outputs/logs/w1_*.log | head -1)" | tail -1 | cut -c1-260 || true
}

test_canh_bao() {
  header "E3 - Cảnh báo ở yên quá 2 phút (Task 1.3)"
  echo "--- 5 cảnh báo gần nhất (outputs/logs/alerts.log):"
  tail -5 outputs/logs/alerts.log
  local snap clip
  snap=$(ls -t outputs/w1_snapshots/*_frame.jpg 2>/dev/null | head -1 || true)
  clip=$(ls -t outputs/w1_clips/*_bbox.mp4 2>/dev/null | head -1 || true)
  echo
  echo "Ảnh toàn khung mới nhất: ${snap:-(chưa có)}"
  echo "Clip có khung mới nhất : ${clip:-(chưa có)}"
  [[ -n "$snap" ]] && cp "$snap" "$OUT/E3_canh_bao_frame.jpg" && cp "${snap%_frame.jpg}_crop.jpg" "$OUT/E3_canh_bao_crop.jpg"
  [[ -n "$clip" ]] && cp "$clip" "$OUT/E3_canh_bao_clip_bbox.mp4"
  echo "Đã chép ảnh/clip vào $OUT/E3_* để đính kèm"
}

test_gop_track() {
  header "#1 - Gộp track khi người bị che 8 s"
  # Video test: dwell_sitting_180s, ô đen che người đứng yên (góc trái) từ giây 40 đến 48
  local v=videos/.test/occl_8s.mp4
  if [[ ! -f $v ]]; then
    echo "... tạo video che 8 s (1 lần, ~1 phút)"
    mkdir -p videos/.test
    docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/videos:/videos" --entrypoint ffmpeg "$FFMPEG_IMAGE" \
      -hide_banner -loglevel error -i /videos/dwell_sitting_180s.mp4 -t 120 -an \
      -vf "drawbox=x=50:y=430:w=200:h=380:color=black:t=fill:enable='between(t,40,48)'" \
      -c:v libx264 -preset veryfast -g 60 -bf 0 -pix_fmt yuv420p "/$v"
  fi
  # Phát lại từ đầu ngay trước khi app nối: người được bắt từ ~giây 8, cảnh báo (-t 25) trước lúc bị che
  docker rm -f ds_test_occl >/dev/null 2>&1 || true
  docker run -d --name ds_test_occl --network host -v "$PWD/videos/.test:/videos:ro" "$FFMPEG_IMAGE" \
    -hide_banner -loglevel warning -re -stream_loop -1 -i /videos/occl_8s.mp4 -an -c:v copy -f rtsp \
    -rtsp_transport tcp "$RTSP/test-occl" >/dev/null
  sleep 2
  ./scripts/redis_dev.sh cli DEL "$PREFIX:events" >/dev/null
  run_app 95 bc_gop_track.log -t 25 -r 40 -A -s 10 "$RTSP/test-occl"
  docker rm -f ds_test_occl >/dev/null
  echo
  echo "--- Log app (outputs/logs/bc_gop_track.log):"
  grep -E 'CẢNH BÁO|Người  |Ở yên  |GỘP TRACK|HẾT Ở YÊN' outputs/logs/bc_gop_track.log | sed 's/\x1b\[[0-9;]*m//g' || true
  echo
  echo "--- Sự kiện trên Redis ($PREFIX:events):"
  ./scripts/redis_dev.sh cli XRANGE "$PREFIX:events" - + | grep -oE '\{"type":"dwell_(alert|resume|end)".*' \
    | sed -E 's/,"snapshot":"[^"]*"//; s/,"clip":"[^"]*"//' | cut -c1-260 || true
  local snap
  snap=$(ls -t outputs/w1_snapshots/test-occl_*_frame.jpg 2>/dev/null | head -1 || true)
  [[ -n "$snap" ]] && cp "$snap" "$OUT/1_gop_track_canh_bao.jpg" && echo && echo "Ảnh lúc cảnh báo: $OUT/1_gop_track_canh_bao.jpg"
  echo
  echo "Đạt khi: người bị che (thường là #1, đứng góc trái) có [GỘP TRACK] ~8 s, chỉ 1 CẢNH BÁO; trên Redis có"
  echo "dwell_resume cùng tid + start_ms với dwell_alert của người đó, và dwell_end của người đó tính liền mạch từ trước"
  echo "lúc bị che (dwell_s > 80). Người khác (vd. người ngồi, model bắt chập chờn) có cảnh báo riêng là bình thường."
}

test_don_file() {
  header "#2 - Tự xoá ảnh/clip cũ (-k 30 ngày / -K 50 GB)"
  docker exec deepstream_dev bash -c 'cd /outputs/w1_snapshots && head -c 200000 /dev/urandom > zz_bc_old1.jpg \
    && cp zz_bc_old1.jpg zz_bc_old2.jpg && cp zz_bc_old1.jpg zz_bc_new.jpg \
    && touch -d "40 days ago" zz_bc_old*.jpg && touch -d "1 day ago" zz_bc_new.jpg'
  echo "--- Trước khi chạy (2 file giả 40 ngày, 1 file giả 1 ngày):"
  ls -l --time-style=+%F outputs/w1_snapshots/zz_bc_* | awk '{print $6, $7}'
  echo "Tổng ảnh+clip thật: $(du -sh outputs/w1_snapshots outputs/w1_clips | awk '{print $1}' | paste -sd+) "
  run_app 30 bc_don_file.log -s 5 "$RTSP/acme-A-cam2"
  echo
  echo "--- Log app:"
  grep 'dọn file' outputs/logs/bc_don_file.log || echo "(không có dòng [dọn file])"
  echo
  echo "--- Sau khi chạy:"
  ls -l --time-style=+%F outputs/w1_snapshots/zz_bc_* | awk '{print $6, $7}'
  docker exec deepstream_dev rm -f /outputs/w1_snapshots/zz_bc_new.jpg
  echo
  echo "Đạt khi: [dọn file] xoá 2 file (0.4 MB), sau khi chạy chỉ còn zz_bc_new.jpg (đã tự dọn sau bước này)."
}

test_nvdec() {
  header "#3 - Nối RTSP giữa GOP: frame đầu tiên (acme-A-cam2, GOP 8,3 s)"
  # Đúng đường của app: nvurisrcbin (RTSP TCP -> NVDEC), ghi từng frame ra JPEG trong 15 s (> 1 GOP).
  # sync=false: video có B-frame (timestamp giảm), sync=true sẽ coi mọi frame là trễ và bỏ hết.
  docker exec deepstream_dev bash -c "rm -rf /tmp/bc_nvdec && mkdir /tmp/bc_nvdec && t0=\$(date +%s.%N); \
    timeout -s INT -k 3 15 gst-launch-1.0 -q nvurisrcbin uri=$RTSP/acme-A-cam2 select-rtp-protocol=4 ! nvvideoconvert \
    ! 'video/x-raw,format=I420' ! jpegenc ! multifilesink sync=false location=/tmp/bc_nvdec/f_%04d.jpg >/dev/null 2>&1; \
    f=\$(ls /tmp/bc_nvdec | sort | head -1); \
    [ -n \"\$f\" ] || { echo 'NVDEC: không ra frame nào trong 15 s'; exit 0; }; \
    i=1; for g in \$(ls /tmp/bc_nvdec | sort | head -5); do cp /tmp/bc_nvdec/\$g /outputs/bao_cao_tuan1/3_nvdec_frame\$i.jpg; i=\$((i+1)); done; \
    echo \"NVDEC: \$(ls /tmp/bc_nvdec | wc -l) frame trong 15 s; frame đầu tiên ra sau \
\$(awk -v a=\$(stat -c %.3Y /tmp/bc_nvdec/\$f) -v b=\$t0 'BEGIN{printf \"%.1f\", a-b}') s kể từ lúc nối (chờ IDR)\"; \
    rm -rf /tmp/bc_nvdec"
  echo "Ảnh 5 frame đầu của NVDEC: $OUT/3_nvdec_frame1..5.jpg"
  # Đối chứng: ffmpeg -flags2 +showall xuất cả frame trước keyframe -> hình xám/vỡ nếu decoder không chờ IDR
  docker run --rm --user "$(id -u):$(id -g)" --network host -v "$PWD/$OUT:/out" --entrypoint ffmpeg "$FFMPEG_IMAGE" \
    -hide_banner -loglevel error -rtsp_transport tcp -flags2 +showall -i "$RTSP/acme-A-cam2" -frames:v 3 \
    -y /out/3_ffmpeg_showall_frame%d.jpg || true
  echo "Ảnh đối chứng ffmpeg (không chờ keyframe): $OUT/3_ffmpeg_showall_frame1..3.jpg"
  echo
  echo "Đạt khi: frame đầu của NVDEC ra sau vài giây (chờ IDR) và 3_nvdec_frame1.jpg là hình sạch;"
  echo "3_ffmpeg_showall_frame1.jpg (xám, chỉ có mảng chuyển động) là hình vỡ mà NVDEC đã tránh được."
}

test_gop_dai() {
  header "#4 - Cam GOP dài (sample_cam5: 2 fps, 1 keyframe / 60 s): lên hình, không reset"
  # Luồng riêng, app nối vào 20 s sau keyframe duy nhất -> chắc chắn phải chờ ~40 s tới keyframe vòng sau
  # (dùng luồng acme-A-cam3 có sẵn thì may rủi: nối trúng đầu vòng lặp là có hình ngay, không chứng minh gì)
  docker rm -f ds_test_gop60 >/dev/null 2>&1 || true
  docker run -d --name ds_test_gop60 --network host -v "$PWD/videos:/videos:ro" "$FFMPEG_IMAGE" \
    -hide_banner -loglevel warning -re -stream_loop -1 -i /videos/sample_cam5.mp4 -an -c:v copy -f rtsp \
    -rtsp_transport tcp "$RTSP/test-gop60" >/dev/null
  echo "... phát test-gop60, chờ 20 s cho qua keyframe đầu vòng lặp"
  sleep 20
  run_app 100 bc_gop_dai.log -s 10 "$RTSP/test-gop60"
  docker rm -f ds_test_gop60 >/dev/null
  local L=outputs/logs/bc_gop_dai.log
  echo
  echo "--- Log app (mỗi dòng [stats] cách nhau 10 s):"
  grep -E '\[stats\]|camera_online|\[CAM\] reset' "$L" | cut -c1-120 || true
  echo
  echo "Số lần reset: $(grep -c '\[CAM\] reset' "$L" || true)"
  echo "Giây đến khi có hình (≈ số dòng [stats] 0/1 trước camera_online × 10): \
$(awk '/camera_online/{print n*10; exit} /\[stats\]/{n++}' "$L")"
  echo
  echo "--- App thật đang chạy (19 cam, log $(ls -t outputs/logs/w1_*.log | head -1)):"
  echo "Số lần reset: $(grep -c '\[CAM\] reset' "$(ls -t outputs/logs/w1_*.log | head -1)" || true)"
  echo
  echo "Đạt khi: các dòng [stats] đầu là 0/1 cam có hình (đang chờ keyframe), sau ~40 s có camera_online và 2.0 fps,"
  echo "0 lần reset. (Bản cũ: cứ 10 s không có hình là reset, nối lại từ đầu -> không bao giờ có hình.)"
}

test_chay_trung() {
  header "#5 - Chặn chạy 2 bản app cùng prefix Redis"
  echo "--- App đang chạy:"
  docker exec deepstream_dev pgrep -af w1-app
  echo
  echo "--- Thử chạy bản thứ 2 (prefix ds):"
  local rc=0
  docker exec -e LANG=C.UTF-8 deepstream_dev "$APP" -i /workspace/configs/cameras_sim.txt 2>&1 || rc=$?
  echo "Mã thoát: $rc"
  echo
  echo "--- App đang chạy sau đó (vẫn đúng 1 bản):"
  docker exec deepstream_dev pgrep -af w1-app
  echo
  echo "Đạt khi: bản thứ 2 báo pid bản đang chạy, mã thoát 1, vẫn chỉ 1 tiến trình w1-app (prefix ds)."
}

test_sai_mk() {
  header "#6 - Thiếu / sai mật khẩu Redis"
  local name pw
  for name in thieu sai; do
    [[ $name == thieu ]] && pw="" || pw="mat-khau-sai"
    PASS_OVERRIDE=$pw run_app 25 "bc_sai_mk_$name.log" -s 5 "$RTSP/acme-A-cam2"
    local L=outputs/logs/bc_sai_mk_$name.log
    echo "--- $([[ $name == thieu ]] && echo 'Thiếu mật khẩu' || echo 'Sai mật khẩu'):"
    echo "Số dòng [redis] trong 25 s : $(grep -c '\[redis\]' "$L" || true)"
    grep '\[redis\]' "$L" | head -2 || true
    grep -E '\[stats\]' "$L" | tail -1 | grep -oE 'redis=[A-Z]+' || true
    echo "Tắt bằng SIGINT            : $([[ $APP_RC == 124 ]] && echo "sạch (mã $APP_RC)" || echo "KHÔNG (mã $APP_RC, 137 = phải kill)")"
    grep -q 'End of stream' "$L" && echo "Log có 'End of stream'    : có"
    echo
  done
  echo "Đạt khi: mỗi trường hợp chỉ 1 dòng [redis], redis=DOWN, tắt sạch (mã 124)."
}

clean_test() {
  docker rm -f ds_test_occl ds_test_gop60 >/dev/null 2>&1 || true
  ./scripts/redis_dev.sh cli DEL "$PREFIX:frames" "$PREFIX:events" "$PREFIX:cameras" >/dev/null
  docker exec deepstream_dev bash -c 'rm -f /outputs/w1_clips/test-occl_* /outputs/w1_snapshots/test-occl_* \
    /outputs/w1_snapshots/zz_bc_* /tmp/w1-app-w1test.lock'
  echo "Đã xoá container, key Redis $PREFIX:*, ảnh/clip test. Giữ: $OUT/, outputs/logs/bc_*.log, videos/.test/"
}

case "${1:-}" in
  rtsp)       test_rtsp       | tee "$OUT/E1_rtsp.txt" ;;
  redis)      test_redis      | tee "$OUT/E2_redis.txt" ;;
  canh-bao)   test_canh_bao   | tee "$OUT/E3_canh_bao.txt" ;;
  gop-track)  test_gop_track  | tee "$OUT/1_gop_track.txt" ;;
  don-file)   test_don_file   | tee "$OUT/2_don_file.txt" ;;
  nvdec)      test_nvdec      | tee "$OUT/3_nvdec.txt" ;;
  gop-dai)    test_gop_dai    | tee "$OUT/4_gop_dai.txt" ;;
  chay-trung) test_chay_trung | tee "$OUT/5_chay_trung.txt" ;;
  sai-mk)     test_sai_mk     | tee "$OUT/6_sai_mk.txt" ;;
  don-test)   clean_test ;;
  *) sed -n '2,/^set -euo/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
