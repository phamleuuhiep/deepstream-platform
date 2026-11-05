#!/usr/bin/env bash
# Giả lập camera IP: MediaMTX (RTSP server) + FFmpeg phát lặp file video thành luồng RTSP.
# Dùng:
#   ./scripts/rtsp_sim.sh start                 4 cam khác nhau (cam1 H.264, cam2 H.265, cam3/4 2fps)
#   ./scripts/rtsp_sim.sh start 32 [video.mp4]  1 video -> cam1..cam32 (1 FFmpeg đọc file, tee ra 32 luồng, không re-encode)
#   ./scripts/rtsp_sim.sh sites [configs/sites.json]
#                                               cam theo công ty/toà (sửa ở dashboard master: video, giây bắt đầu, fps,
#                                               tên công ty/toà/camera, tầng, khu vực)
#                                               và ghi danh sách cam cho app vào configs/cameras_sim.txt
#   ./scripts/rtsp_sim.sh restart-feed          ngắt/nối lại toàn bộ luồng (test reconnect)
#   ./scripts/rtsp_sim.sh stop|status
# Luồng: rtsp://127.0.0.1:8554/<cam> (TCP). Video lấy trong videos/.
set -euo pipefail
# Script gọi docker nên phải chạy trên host, không chạy trong container deepstream_dev
if ! command -v docker >/dev/null; then
  echo "Lỗi: chạy script này trên HOST (cd /home/hiepplh/deepstream), không chạy trong container." >&2
  exit 1
fi
cd "$(dirname "$0")/.."

MTX_IMAGE=bluenviron/mediamtx:1.15.3
FFMPEG_IMAGE=linuxserver/ffmpeg:latest
MTX_NAME=ds_mediamtx
VIDEO_DIR="$PWD/videos"
SAMPLES=/opt/nvidia/deepstream/deepstream/samples/streams
RTSP=rtsp://127.0.0.1:8554
declare -A CAMS=(
  [cam1]=sample_1080p_h264.mp4
  [cam2]=sample_1080p_h265.mp4
  [cam3]=sample_cam5.mp4
  [cam4]=sample_cam6.mp4
)

start_mediamtx() {
  # RTSP qua TCP cho DeepStream + HLS chỉ nghe 127.0.0.1:8888 để web demo (scripts/web.sh) xem live
  # qua proxy có đăng nhập. Tắt RTMP/WebRTC/SRT để không chiếm thêm port trên server dùng chung.
  # --user riêng: root trên host đã hết inotify instance (max_user_instances=128) -> MediaMTX
  # báo "too many open files" khi watch config nếu chạy bằng root.
  docker run -d --name "$MTX_NAME" --restart unless-stopped -p 8554:8554 -p 127.0.0.1:8888:8888 \
    --user 54321:54321 -e MTX_RTSPTRANSPORTS=tcp -e MTX_RTMP=no -e MTX_WEBRTC=no -e MTX_SRT=no \
    -e MTX_HLS=yes -e MTX_HLSALWAYSREMUX=yes "$MTX_IMAGE" >/dev/null
  sleep 2
}

ffmpeg_run() {  # ffmpeg_run <container> <ffmpeg args...>
  local name=$1; shift
  docker run -d --name "$name" --restart unless-stopped --network host \
    -v "$VIDEO_DIR:/videos:ro" "$FFMPEG_IMAGE" -hide_banner -loglevel warning "$@" >/dev/null
}

start_mixed() {
  for f in "${CAMS[@]}"; do
    [[ -f "$VIDEO_DIR/$f" ]] || docker cp -q "deepstream_dev:$SAMPLES/$f" "$VIDEO_DIR/"
  done
  start_mediamtx
  for cam in $(printf '%s\n' "${!CAMS[@]}" | sort); do
    ffmpeg_run "ds_${cam}" -re -stream_loop -1 -i "/videos/${CAMS[$cam]}" \
      -an -c:v copy -f rtsp -rtsp_transport tcp "$RTSP/${cam}"
    echo "$RTSP/${cam}  <- ${CAMS[$cam]}"
  done
}

start_fanout() {  # start_fanout <N> <video>
  local n=$1 video=$2 outs=""
  [[ -f "$VIDEO_DIR/$video" ]] || docker cp -q "deepstream_dev:$SAMPLES/$video" "$VIDEO_DIR/"
  start_mediamtx
  for i in $(seq 1 "$n"); do
    # onfail=ignore: 1 đầu ra lỗi không kéo sập các luồng còn lại
    outs+="${outs:+|}[f=rtsp:rtsp_transport=tcp:onfail=ignore]$RTSP/cam$i"
  done
  ffmpeg_run ds_fanout -re -stream_loop -1 -i "/videos/$video" -map 0:v -c:v copy -f tee "$outs"
  echo "$RTSP/cam1 .. cam$n  <- $video"
}

video_duration() {  # giây (số nguyên) của videos/<file>
  docker run --rm --entrypoint ffprobe -v "$VIDEO_DIR:/videos:ro" "$FFMPEG_IMAGE" -v error \
    -show_entries format=duration -of csv=p=0 "/videos/$1" | cut -d. -f1
}

# Bản video bắt đầu từ giây <off>: ghép [off -> hết] + [0 -> off], không encode lại. Phát lặp file này
# thay cho "-ss off -stream_loop -1" (cách đó làm timestamp nhảy lùi mỗi vòng lặp). Lưu ở videos/.sim/
rotated_video() {  # rotated_video <video> <off> -> in tên file (tương đối trong videos/)
  local v=$1 off=$2 out=".sim/${1%.*}_off$2.mp4"
  if [[ $off -gt 0 && ! -f "$VIDEO_DIR/$out" ]]; then
    mkdir -p "$VIDEO_DIR/.sim"
    printf "file '/videos/%s'\ninpoint %s\nfile '/videos/%s'\noutpoint %s\n" "$v" "$off" "$v" "$off" \
      > "$VIDEO_DIR/.sim/list.txt"
    docker run --rm --user "$(id -u):$(id -g)" --entrypoint ffmpeg -v "$VIDEO_DIR:/videos" "$FFMPEG_IMAGE" -hide_banner \
      -loglevel error -f concat -safe 0 -i /videos/.sim/list.txt -c copy "/videos/$out" >/dev/null
    rm -f "$VIDEO_DIR/.sim/list.txt"
  fi
  [[ $off -gt 0 ]] && echo "$out" || echo "$v"
}

start_sites() {  # start_sites <configs/sites.json>
  local conf=$1 out=configs/cameras_sim.txt total=0
  declare -A dur
  local lines=()
  # sites.py kiểm tra cấu hình (mã công ty/toà, camera_id trùng, ...) và in mỗi cam 1 dòng
  local sim; sim=$(python3 scripts/sites.py sim "$conf") || exit 1
  mapfile -t lines <<<"$sim"
  for line in "${lines[@]}"; do
    read -r co b cam v fps off <<<"$line"
    [[ -f "$VIDEO_DIR/$v" ]] || docker cp -q "deepstream_dev:$SAMPLES/$v" "$VIDEO_DIR/" 2>/dev/null \
      || { echo "Lỗi: không có videos/$v" >&2; exit 1; }
    [[ -n "${dur[$v]:-}" ]] || dur[$v]=$(video_duration "$v")
    total=$(( total + 1 ))
  done
  [[ $total -le 64 ]] || { echo "Lỗi: $total cam, app chỉ nhận tối đa 64" >&2; exit 1; }

  # Cấu hình hợp lệ mới dừng luồng cũ
  docker ps -a -q --filter "name=^/(${MTX_NAME}|ds_cam[0-9]+|ds_fanout|ds_sim_.+)$" | grep -q . && stop >/dev/null
  start_mediamtx
  # Danh sách cam cho app, kèm JSON thông tin camera (tên công ty, toà, tầng, ...) để app đẩy lên Redis
  python3 scripts/sites.py app "$conf" "$RTSP" "$out"
  local f; declare -A keep
  for line in "${lines[@]}"; do
    read -r co b cam v fps off <<<"$line"
    # Giây bắt đầu vượt độ dài video thì quay vòng
    [[ ${dur[$v]} -gt 0 ]] && off=$(( off % dur[$v] ))
    f=$(rotated_video "$v" "$off"); keep[$f]=1
    # 1 FFmpeg / cam (container ds_sim_<cam>): 1 luồng lỗi không kéo các cam khác
    ffmpeg_run "ds_sim_$cam" -re -stream_loop -1 -i "/videos/$f" \
      -an -c:v copy -f rtsp -rtsp_transport tcp "$RTSP/$cam"
    printf '%-34s <- %s (từ giây %s)%s\n' "$RTSP/$cam" "$v" "$off" "$([[ $fps -gt 0 ]] && echo ", xử lý $fps fps")"
  done
  # Xoá bản video lệch giờ không còn cam nào dùng (mỗi giây bắt đầu khác nhau là 1 file cỡ video gốc)
  for f in "$VIDEO_DIR"/.sim/*.mp4; do
    [[ -e "$f" && -z "${keep[.sim/$(basename "$f")]:-}" ]] && rm -f "$f"
  done
  echo "$total cam, $(printf '%s\n' "${lines[@]}" | awk '{print $1}' | sort -u | wc -l) công ty. Danh sách cho app: $out"
}

stop() {
  docker rm -f "$MTX_NAME" ds_fanout $(printf 'ds_%s ' "${!CAMS[@]}") \
    $(docker ps -a -q --filter 'name=^/ds_sim_') >/dev/null 2>&1 || true
  echo "Đã dừng RTSP giả lập"
}

status() {
  docker ps -a --filter "name=^/(${MTX_NAME}|ds_cam[0-9]+|ds_fanout|ds_sim_.+)$" --format '{{.Names}}\t{{.Status}}'
}

case "${1:-}" in
  start)
    # Chạy lại khi đang chạy: dừng bản cũ trước (có thể đổi số cam / video)
    docker ps -a -q --filter "name=^/(${MTX_NAME}|ds_cam[0-9]+|ds_fanout|ds_sim_.+)$" | grep -q . && stop >/dev/null
    if [[ -n "${2:-}" ]]; then start_fanout "$2" "${3:-sample_1080p_h264.mp4}"; else start_mixed; fi ;;
  sites)
    CONF=${2:-configs/sites.json}
    [[ -f "$CONF" ]] || { echo "Lỗi: không có $CONF" >&2; exit 1; }
    start_sites "$CONF" ;;
  restart-feed)
    docker restart $(docker ps -q --filter "name=^/(ds_cam[0-9]+|ds_fanout|ds_sim_.+)$") >/dev/null && echo "Đã khởi động lại FFmpeg" ;;
  stop) stop ;;
  status) status ;;
  *) echo "Dùng: $0 start [N [video]] | sites [file] | restart-feed | stop | status"; exit 1 ;;
esac
