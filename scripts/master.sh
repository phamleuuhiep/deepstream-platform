#!/usr/bin/env bash
# Dashboard master = trang admin của web (./scripts/web.sh start).
# Script này bật agent thực hiện các nút bấm trên dashboard (container ds_agent, master/agent.py):
# bật camera giả lập theo cấu hình, chạy lại app, tạo/tắt link public, đổi mật khẩu công ty.
# Agent có quyền docker nhưng không mở cổng mạng; web chỉ ghi yêu cầu vào configs/.master/request.json.
# Dùng: ./scripts/master.sh start|stop|logs
set -euo pipefail
if ! command -v docker >/dev/null; then
  echo "Lỗi: chạy script này trên HOST (cd /home/hiepplh/deepstream), không chạy trong container." >&2
  exit 1
fi
ROOT=$(cd "$(dirname "$0")/.." && pwd)
NAME=ds_agent
IMAGE=ds-agent:1

case "${1:-}" in
  start|restart)
    docker image inspect "$IMAGE" >/dev/null 2>&1 || docker build -q -t "$IMAGE" "$ROOT/master" >/dev/null
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    mkdir -p "$ROOT/configs/.master"
    # Mount dự án ở đúng đường dẫn như trên host: script gọi docker run -v "$ROOT/..." thì daemon hiểu đúng.
    # Chạy bằng user hiện tại (+ nhóm của docker.sock) để file sinh ra vẫn thuộc user, không thuộc root
    docker run -d --name "$NAME" --restart unless-stopped --network host \
      --user "$(id -u):$(id -g)" --group-add "$(stat -c %g /var/run/docker.sock)" -e HOME=/tmp -e TZ=ICT-7 \
      -v /var/run/docker.sock:/var/run/docker.sock -v "$ROOT:$ROOT" -w "$ROOT" -e ROOT="$ROOT" \
      "$IMAGE" python master/agent.py >/dev/null
    # Web admin cần mount configs/ (ghi) + videos/ để có dashboard master
    if ! docker inspect ds_web --format '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null | grep -q '^MASTER_CONFIGS='; then
      "$ROOT/scripts/web.sh" restart
    fi
    echo "Agent đang chạy. Mở trang admin (./scripts/web.sh start): phần Hệ thống có nút bật từng thành phần" ;;
  stop) docker rm -f "$NAME" >/dev/null 2>&1 || true; echo "Đã dừng agent (dashboard master chỉ xem, không áp dụng được)" ;;
  logs) docker logs --tail 50 -f "$NAME" ;;
  *) echo "Dùng: $0 start|stop|logs"; exit 1 ;;
esac
