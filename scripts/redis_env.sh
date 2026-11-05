# Nạp cấu hình Redis dev từ .redis.env (do redis_dev.sh tạo): REDIS_PASSWORD, REDIS_BIND.
# Dùng: source "$(dirname "$0")/redis_env.sh". Đã đặt REDIS_PASSWORD hoặc REDIS (Redis khác) thì không nạp.
REDIS_ENV_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.redis.env"
if [[ -z "${REDIS_PASSWORD:-}" && -z "${REDIS:-}" && -r "$REDIS_ENV_FILE" ]]; then
  set -a; . "$REDIS_ENV_FILE"; set +a
fi
