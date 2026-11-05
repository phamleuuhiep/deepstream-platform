#!/usr/bin/env bash
# Upload ảnh/clip lên S3/MinIO (Task 2.1).
#   ./scripts/upload.sh minio          MinIO dev (ds_minio, :9000 API, :9001 console 127.0.0.1), bucket ds-media
#                                      cho đọc công khai (dev). Tự sinh tài khoản vào .s3.env lần đầu
#   ./scripts/upload.sh start          uploader (ds_uploader) đọc ds:events, upload theo .s3.env
#   ./scripts/upload.sh stop|restart|logs|status|info
# S3 thật của platform: tự viết .s3.env (S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, MEDIA_BASE = URL gốc
# đọc được file), rồi ./scripts/upload.sh restart và chạy lại app (agent thêm -M MEDIA_BASE khi có .s3.env).
# Biến môi trường cho bài test (không đụng bản chạy thật): S3_ENV, MINIO_NAME, MINIO_PORT, UPLOADER_NAME, PREFIX, REDIS
set -euo pipefail
command -v docker >/dev/null || { echo "Lỗi: chạy trên HOST" >&2; exit 1; }
ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/scripts/redis_env.sh"
S3_ENV=${S3_ENV:-$ROOT/.s3.env}
MINIO_NAME=${MINIO_NAME:-ds_minio}
MINIO_PORT=${MINIO_PORT:-9000}
UPLOADER_NAME=${UPLOADER_NAME:-ds_uploader}
PREFIX=${PREFIX:-ds}
BIND=${REDIS_BIND:-127.0.0.1}
REDIS=${REDIS:-$BIND:6390}
MINIO_IMAGE=minio/minio:latest        # bản RELEASE.2025-09-07T16-13-09Z có sẵn trên server (MinIO không còn phát image mới)
IMAGE=ds-uploader:1

minio() {
  if [[ ! -f "$S3_ENV" ]]; then
    ( umask 077; cat > "$S3_ENV" <<EOF
S3_ENDPOINT=http://$BIND:$MINIO_PORT
S3_BUCKET=ds-media
S3_ACCESS_KEY=ds-$(head -c 6 /dev/urandom | od -An -tx1 | tr -d ' \n')
S3_SECRET_KEY=$(head -c 24 /dev/urandom | base64 | tr -d '/+=')
MEDIA_BASE=http://$BIND:$MINIO_PORT/ds-media
EOF
    )
  fi
  set -a; . "$S3_ENV"; set +a
  docker rm -f "$MINIO_NAME" >/dev/null 2>&1 || true
  # Dữ liệu ở volume riêng (giữ qua lần tạo lại container)
  docker run -d --name "$MINIO_NAME" --restart unless-stopped -p "$BIND:$MINIO_PORT:9000" \
    -p "127.0.0.1:$((MINIO_PORT + 1)):9001" -v "${MINIO_NAME}_data:/data" \
    -e MINIO_ROOT_USER="$S3_ACCESS_KEY" -e MINIO_ROOT_PASSWORD="$S3_SECRET_KEY" \
    "$MINIO_IMAGE" server /data --console-address :9001 >/dev/null
  for _ in $(seq 20); do
    docker exec "$MINIO_NAME" mc alias set local http://127.0.0.1:9000 "$S3_ACCESS_KEY" "$S3_SECRET_KEY" >/dev/null 2>&1 && break
    sleep 1
  done
  docker exec "$MINIO_NAME" mc mb -p "local/$S3_BUCKET" >/dev/null
  # Dev: ai có URL cũng tải được (như link ảnh trong sự kiện). S3 thật thì platform tự quyết (presigned URL, ...)
  docker exec "$MINIO_NAME" mc anonymous set download "local/$S3_BUCKET" >/dev/null
  echo "MinIO $MINIO_NAME: $S3_ENDPOINT, bucket $S3_BUCKET (đọc công khai), console http://127.0.0.1:$((MINIO_PORT + 1))"
}

start() {
  [[ -f "$S3_ENV" ]] || { echo "Chưa có $S3_ENV: chạy '$0 minio' (dev) hoặc tự viết cho S3 thật" >&2; exit 1; }
  docker image inspect "$IMAGE" >/dev/null 2>&1 || docker build -q -t "$IMAGE" "$ROOT/uploader" >/dev/null
  docker rm -f "$UPLOADER_NAME" >/dev/null 2>&1 || true
  # outputs ghi được: uploader ghi <file>.up cạnh file đã upload
  docker run -d --name "$UPLOADER_NAME" --restart unless-stopped --network host --env-file "$S3_ENV" \
    -e REDIS="$REDIS" -e REDIS_PASSWORD="${REDIS_PASSWORD:-}" -e PREFIX="$PREFIX" -e TZ=ICT-7 \
    -v "$ROOT/uploader:/app:ro" -v "$ROOT/outputs:/outputs" "$IMAGE" >/dev/null
  echo "Uploader $UPLOADER_NAME: $PREFIX:events -> $(grep ^S3_ENDPOINT "$S3_ENV" | cut -d= -f2)/$(grep ^S3_BUCKET "$S3_ENV" | cut -d= -f2)"
}

case "${1:-}" in
  minio) minio ;;
  start) start ;;
  restart) start ;;
  stop) docker rm -f "$UPLOADER_NAME" >/dev/null 2>&1 && echo "Đã dừng $UPLOADER_NAME" || true ;;
  logs) docker logs --tail "${2:-50}" "$UPLOADER_NAME" ;;
  status) docker ps -a --filter "name=^/($MINIO_NAME|$UPLOADER_NAME)$" --format '{{.Names}}\t{{.Status}}' ;;
  info) grep -v SECRET "$S3_ENV" ;;
  *) echo "Dùng: $0 minio|start|stop|restart|logs|status|info"; exit 1 ;;
esac
