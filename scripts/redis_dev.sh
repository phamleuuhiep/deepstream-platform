#!/usr/bin/env bash
# Redis dev để test luồng metadata/sự kiện trước khi trỏ sang Redis của team platform.
# Dùng: ./scripts/redis_dev.sh start|stop|info|cli [lệnh redis...]
#   Luôn có mật khẩu, lưu ở .redis.env (tự tạo lần đầu, quyền 600); web.sh, run_w1.sh, agent tự đọc file này.
#   Luôn nghe 127.0.0.1:6390; nghe thêm IP LAN nếu đặt REDIS_BIND (lưu vào .redis.env):
#     REDIS_BIND=192.168.1.30 ./scripts/redis_dev.sh start
#   Giới hạn RAM 2GB, xoá key cũ nhất khi đầy, không lưu ra đĩa (stop = mất dữ liệu).
#   Đổi IP/mật khẩu: sửa .redis.env (hoặc xoá để sinh mật khẩu mới) rồi stop + start.
set -euo pipefail
# Script gọi docker nên phải chạy trên host, không chạy trong container deepstream_dev
if ! command -v docker >/dev/null; then
  echo "Lỗi: chạy script này trên HOST (cd /home/hiepplh/deepstream), không chạy trong container." >&2
  exit 1
fi
NAME=ds_redis
BIND_ARG="${REDIS_BIND:-}"
unset REDIS_PASSWORD REDIS
. "$(dirname "$0")/redis_env.sh"
REDIS_BIND="${BIND_ARG:-${REDIS_BIND:-127.0.0.1}}"
if [[ ! -f "$REDIS_ENV_FILE" || ( -n "$BIND_ARG" && "${1:-}" == start ) ]]; then
  REDIS_PASSWORD="${REDIS_PASSWORD:-$(head -c 18 /dev/urandom | base64 | tr -d '/+=')}"
  ( umask 077; printf 'REDIS_PASSWORD=%s\nREDIS_BIND=%s\n' "$REDIS_PASSWORD" "$REDIS_BIND" > "$REDIS_ENV_FILE" )
fi
export REDIS_PASSWORD
addrs() { echo "127.0.0.1:6390$([[ "$REDIS_BIND" != 127.0.0.1 ]] && echo " $REDIS_BIND:6390")"; }
case "${1:-}" in
  start)
    if docker ps -a -q --filter "name=^/$NAME$" | grep -q .; then
      docker start "$NAME" >/dev/null; echo "Redis dev: đã có sẵn ($(docker port "$NAME" 6379/tcp | tr '\n' ' '))"; exit 0
    fi
    ports=(-p 127.0.0.1:6390:6379)
    [[ "$REDIS_BIND" != 127.0.0.1 ]] && ports+=(-p "$REDIS_BIND:6390:6379")
    # Mật khẩu qua env + file config trong container, không nằm trên dòng lệnh (docker inspect/ps)
    docker run -d --name "$NAME" --restart unless-stopped "${ports[@]}" -e REDIS_PASSWORD redis:8.8-alpine \
      sh -c 'printf "requirepass %s\n" "$REDIS_PASSWORD" > /tmp/auth.conf && exec redis-server /tmp/auth.conf \
        --save "" --appendonly no --maxmemory 2gb --maxmemory-policy allkeys-lru' >/dev/null
    echo "Redis dev: $(addrs) (mật khẩu trong .redis.env)" ;;
  stop) docker rm -f "$NAME" >/dev/null 2>&1 || true; echo "Đã dừng Redis dev" ;;
  info)
    echo "Địa chỉ : $(addrs)"
    echo "Mật khẩu: $REDIS_PASSWORD"
    echo "Thử    : redis-cli -h $REDIS_BIND -p 6390 -a '$REDIS_PASSWORD' PING" ;;
  cli) shift; docker exec -i -e REDISCLI_AUTH="$REDIS_PASSWORD" "$NAME" redis-cli "$@" ;;
  *) echo "Dùng: $0 start|stop|info|cli [args]"; exit 1 ;;
esac
