#!/usr/bin/env bash
# Tuần 1: chạy app đa luồng RTSP -> Redis + cảnh báo người ở yên.
# Dùng:
#   ./scripts/run_w1.sh 32                       32 cam giả lập rtsp://127.0.0.1:8554/cam1..32
#   ./scripts/run_w1.sh 32 -t 120 -r 135         + ngưỡng 2 phút, bật smart record (cache 135s)
#   ./scripts/run_w1.sh -R 10.0.0.5:6379 rtsp://cam-a/... rtsp://cam-b/...   Redis platform + cam thật
#   ./scripts/run_w1.sh -i configs/cameras_sim.txt -t 120 -r 135              cam theo toà nhà
#                                                 (file do rtsp_sim.sh sites sinh, hoặc tự viết cho cam thật)
# Tuỳ chọn app: chạy ./scripts/run_w1.sh -h. Chạy được cả trên host lẫn trong container
# (rtsp_sim.sh / redis_dev.sh thì phải chạy trên host).
set -euo pipefail
APP_DIR=/workspace/apps/w1_multistream
# Redis dev (không có -R): lấy mật khẩu trong .redis.env
[[ " $* " == *" -R "* ]] || . "$(dirname "$0")/redis_env.sh"

# Trong container (không có lệnh docker) thì build và chạy trực tiếp, trên host thì qua docker exec
IN_CONTAINER=0
command -v docker >/dev/null || IN_CONTAINER=1
if [[ $IN_CONTAINER == 1 ]]; then
  make -s -C "$APP_DIR"
else
  docker exec deepstream_dev bash -c "pkg-config --exists gstreamer-1.0 hiredis" \
    || "$(dirname "$0")/setup_build_deps.sh"
  docker exec deepstream_dev make -s -C "$APP_DIR"
fi

ARGS=()
if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
  N=$1; shift
  ARGS=("$@")
  for i in $(seq 1 "$N"); do ARGS+=("rtsp://127.0.0.1:8554/cam$i"); done
else
  ARGS=("$@")
fi
# -i configs/x.txt (tương đối từ thư mục dự án) -> /workspace/configs/x.txt trong container
for k in "${!ARGS[@]}"; do
  next=${ARGS[$((k + 1))]:-}
  if [[ "${ARGS[$k]}" == -i && -n "$next" && "$next" != /* ]]; then
    ARGS[$((k + 1))]="/workspace/$next"
  fi
done
if [[ $IN_CONTAINER == 1 ]]; then
  LANG=C.UTF-8 exec "$APP_DIR/w1-app" "${ARGS[@]}"
fi
if [[ -t 0 && -t 1 ]]; then
  # Có terminal: -it chuyển Ctrl+C vào app
  exec docker exec -it -e LANG=C.UTF-8 -e REDIS_PASSWORD="${REDIS_PASSWORD:-}" deepstream_dev \
  "$APP_DIR/w1-app" "${ARGS[@]}"
fi
# Không có terminal (chạy nền, timeout, CI): docker exec không chuyển tín hiệu vào container,
# nên tự gửi SIGINT cho app, tránh app chạy mồ côi trong container
trap 'docker exec deepstream_dev pkill -INT -f "^$APP_DIR/w1-app" || true' INT TERM
docker exec -e LANG=C.UTF-8 -e REDIS_PASSWORD="${REDIS_PASSWORD:-}" deepstream_dev \
  "$APP_DIR/w1-app" "${ARGS[@]}" &
PID=$!
while kill -0 "$PID" 2>/dev/null; do wait "$PID" || true; done
