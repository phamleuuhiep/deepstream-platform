#!/usr/bin/env bash
# Task 0.3: build (nếu cần) và chạy app đọc metadata sau nvtracker trong container deepstream_dev.
# Dùng: ./scripts/run_task13.sh [-o /outputs/x.jsonl] [-p print_every] <uri> [uri ...]
#   Mặc định (không truyền uri): 4 luồng RTSP giả lập rtsp://127.0.0.1:8554/cam1..cam4
#   Chạy được cả trên host lẫn trong container.
set -euo pipefail
APP_DIR=/workspace/apps/task13_metadata

# Trong container (không có lệnh docker) thì build và chạy trực tiếp, trên host thì qua docker exec
IN_CONTAINER=0
command -v docker >/dev/null || IN_CONTAINER=1
if [[ $IN_CONTAINER == 1 ]]; then
  make -s -C "$APP_DIR"
else
  # Lần đầu sau khi tạo lại container cần cài header GStreamer
  docker exec deepstream_dev bash -c "pkg-config --exists gstreamer-1.0" \
    || "$(dirname "$0")/setup_build_deps.sh"
  docker exec deepstream_dev make -s -C "$APP_DIR"
fi

ARGS=("$@")
if [[ ! " ${ARGS[*]} " =~ ://  ]]; then
  ARGS+=(rtsp://127.0.0.1:8554/cam{1..4})
fi
if [[ $IN_CONTAINER == 1 ]]; then
  LANG=C.UTF-8 exec "$APP_DIR/task13-meta-app" "${ARGS[@]}"
fi
if [[ -t 0 && -t 1 ]]; then
  # Có terminal: -it chuyển Ctrl+C vào app
  exec docker exec -it -e LANG=C.UTF-8 deepstream_dev "$APP_DIR/task13-meta-app" "${ARGS[@]}"
fi
# Không có terminal (chạy nền, timeout, CI): docker exec không chuyển tín hiệu vào container,
# nên tự gửi SIGINT cho app, tránh app chạy mồ côi trong container
trap 'docker exec deepstream_dev pkill -INT -f "^$APP_DIR/task13-meta-app" || true' INT TERM
docker exec -e LANG=C.UTF-8 deepstream_dev "$APP_DIR/task13-meta-app" "${ARGS[@]}" &
PID=$!
while kill -0 "$PID" 2>/dev/null; do wait "$PID" || true; done
