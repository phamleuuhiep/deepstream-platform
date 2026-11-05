#!/usr/bin/env bash
# Platform demo: web xem kết quả DeepStream (camera live, cảnh báo + ảnh + clip, sự kiện).
# Dùng: ./scripts/web.sh start|stop|restart|logs|tunnel|tunnel-stop
#   Mặc định http://127.0.0.1:8095, đọc Redis dev <REDIS_BIND>:6390 (mật khẩu trong .redis.env) và outputs/ (chỉ đọc).
#   WEB_HOST=192.168.1.30 ./scripts/web.sh start  mở cho máy khác trong LAN
#   WEB_PORT=8096 ./scripts/web.sh start          đổi cổng
#   REDIS=10.0.0.5:6379 ./scripts/web.sh start    Redis khác (mật khẩu qua env REDIS_PASSWORD)
#   ./scripts/web.sh tunnel                       link public tạm https://*.trycloudflare.com (Cloudflare quick tunnel)
#   ./scripts/web.sh publish [configs/cameras_sim.txt]
#                                                 mỗi công ty 1 link public riêng: tạo tài khoản công ty mới (web/companies.auth),
#                                                 1 web chỉ của công ty đó (ds_web_<cty>, 127.0.0.1:8120+) + 1 tunnel riêng
#                                                 công ty đang có link thì giữ nguyên link; công ty đã bỏ thì tắt link
#   ./scripts/web.sh republish [công ty]          tạo lại web + tunnel (link mới) cho 1 công ty (không ghi = tất cả)
#   ./scripts/web.sh rebuild                      build lại image web (đổi requirements), tạo lại mọi web, giữ link
#   ./scripts/web.sh links                        in lại link public + tài khoản của từng công ty
#   ./scripts/web.sh unpublish [công ty]          tắt link public của 1 công ty (không ghi = tất cả)
# Đăng nhập: nếu có file web/.auth (WEB_USER=..., WEB_PASS=...) thì web bắt nhập user/mật khẩu.
#   Lệnh tunnel tự tạo web/.auth với mật khẩu ngẫu nhiên nếu chưa có: không public web khi chưa có mật khẩu.
set -euo pipefail
if ! command -v docker >/dev/null; then
  echo "Lỗi: chạy script này trên HOST (cd /home/hiepplh/deepstream), không chạy trong container." >&2
  exit 1
fi
ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/scripts/redis_env.sh"
REDIS=${REDIS:-${REDIS_BIND:-127.0.0.1}:6390}
NAME=ds_web
TUNNEL=ds_web_tunnel
IMAGE=ds-web:1
TUNNEL_IMAGE=cloudflare/cloudflared:2025.9.1
AUTH="$ROOT/web/.auth"
COMPANIES_AUTH="$ROOT/web/companies.auth"   # mỗi dòng: <công ty> <user> <mật khẩu>
PORT=${WEB_PORT:-8095}
HOST=${WEB_HOST:-$(docker inspect "$NAME" --format '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null \
  | sed -n 's/^HOST=//p')}
HOST=${HOST:-127.0.0.1}

randpass() { head -c 12 /dev/urandom | base64 | tr -d '/+='; }

tunnel_url() {  # link trycloudflare hiện tại của 1 container tunnel (đổi mỗi lần tunnel khởi động lại)
  docker logs "$1" 2>&1 | grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' | tail -1 || true
}

companies() {  # danh sách "<công ty> <user> <mật khẩu> <cổng web riêng>"
  awk '!/^#/ && NF == 3 {print $0, 8120 + n++}' "$COMPANIES_AUTH" 2>/dev/null || true
}

running() { [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" == true ]]; }

publish() {  # publish <file danh sách cam của w1-app> [all|<công ty>: tạo lại, link mới | web: chỉ tạo lại web, giữ link]
  local list=$1 force=${2:-} active
  [[ -f "$list" ]] || { echo "Lỗi: không có $list" >&2; exit 1; }
  ( umask 077; touch "$COMPANIES_AUTH" )
  # Công ty = cột 1 của dòng "<công ty> <toà> <cam_id> <uri> [fps]". Công ty mới thì tạo tài khoản
  active=$(sed 's/#.*//' "$list" | awk 'NF >= 4 {print $1}' | sort -u)
  for co in $active; do
    awk -v c="$co" '$1 == c {f = 1} END {exit !f}' "$COMPANIES_AUTH" \
      || printf '%s %s %s\n' "$co" "$co" "$(randpass)" >> "$COMPANIES_AUTH"
  done
  docker image inspect "$IMAGE" >/dev/null 2>&1 || docker build -q -t "$IMAGE" "$ROOT/web" >/dev/null
  while read -r co user pass port; do
    if ! grep -qx "$co" <<<"$active"; then
      # Công ty không còn trong danh sách cam: tắt link (giữ tài khoản, thêm lại thì dùng lại)
      running "ds_web_tunnel_$co" && echo "Tắt link $co (không còn trong $list)"
      docker rm -f "ds_web_$co" "ds_web_tunnel_$co" >/dev/null 2>&1 || true
      continue
    fi
    # Đang chạy thì giữ nguyên, để link của công ty không bị đổi
    if [[ "$force" != all && "$force" != "$co" && "$force" != web ]] && running "ds_web_$co" && running "ds_web_tunnel_$co"; then
      continue
    fi
    # web: tạo lại container web (image/code mới), tunnel đang chạy thì giữ -> link không đổi
    local keep_tunnel=0
    [[ "$force" == web ]] && running "ds_web_tunnel_$co" && keep_tunnel=1
    docker rm -f "ds_web_$co" >/dev/null 2>&1 || true
    [[ $keep_tunnel == 1 ]] || docker rm -f "ds_web_tunnel_$co" >/dev/null 2>&1 || true
    if ss -ltn | awk '{print $4}' | grep -q ":$port\$"; then
      echo "Lỗi: cổng $port (web riêng của $co) đang bị chương trình khác dùng" >&2; continue
    fi
    # Web chỉ của 1 công ty (COMPANY): dashboard ở /, chỉ nghe 127.0.0.1, ra Internet qua tunnel riêng.
    # Không truyền tài khoản admin vào: link public của công ty không mở được bằng tài khoản admin
    docker run -d --name "ds_web_$co" --restart unless-stopped --network host \
      -e COMPANY="$co" -e HOST=127.0.0.1 -e PORT="$port" -e REDIS="$REDIS" \
      -e REDIS_PASSWORD="${REDIS_PASSWORD:-}" \
      -v "$ROOT/web:/app:ro" -v "$ROOT/outputs:/outputs:ro" -v "$ROOT/models/buffalo_s:/models:ro" "$IMAGE" >/dev/null
    if [[ $keep_tunnel == 1 ]]; then echo "Tạo lại web của $co (giữ link)"; continue; fi
    docker run -d --name "ds_web_tunnel_$co" --restart unless-stopped --network host "$TUNNEL_IMAGE" \
      tunnel --no-autoupdate --url "http://127.0.0.1:$port" >/dev/null
    echo "Tạo link mới cho $co"
  done < <(companies)
  # Chờ các tunnel nhận link
  for _ in $(seq 1 40); do
    local missing=0
    for co in $active; do [[ -n "$(tunnel_url "ds_web_tunnel_$co")" ]] || missing=1; done
    [[ $missing == 0 ]] && break
    sleep 1
  done
  links
}

links() {
  local url
  printf '%-10s %-66s %s\n' "Công ty" "Link public" "Tài khoản"
  while read -r co user pass port; do
    url=""
    if docker ps -q --filter "name=^/ds_web_tunnel_$co$" | grep -q .; then url=$(tunnel_url "ds_web_tunnel_$co"); fi
    printf '%-10s %-66s %s / %s\n' "$co" "${url:-(chưa public: ./scripts/web.sh publish)}" "$user" "$pass"
  done < <(companies)
  echo "Link trycloudflare đổi mỗi khi tunnel khởi động lại (kể cả khi server reboot): chạy lại lệnh này để xem link mới."
}

unpublish() {  # unpublish [công ty]
  local names
  if [[ -n "${1:-}" ]]; then names="ds_web_$1 ds_web_tunnel_$1"
  else names=$(docker ps -a --format '{{.Names}}' | grep -E '^ds_web_(tunnel_)?[A-Za-z0-9_]+$' | grep -vx "$TUNNEL" || true)
  fi
  # shellcheck disable=SC2086
  [[ -n "$names" ]] && docker rm -f $names >/dev/null 2>&1 || true
  echo "Đã tắt link public${1:+ của $1}"
}

ensure_auth() {
  [[ -f "$AUTH" ]] && return
  ( umask 077; printf 'WEB_USER=platform\nWEB_PASS=%s\n' "$(randpass)" > "$AUTH" )
  echo "Đã tạo tài khoản đăng nhập trong web/.auth"
}

start() {
  docker image inspect "$IMAGE" >/dev/null 2>&1 || docker build -q -t "$IMAGE" "$ROOT/web" >/dev/null
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  local auth_args=()
  [[ -f "$AUTH" ]] && auth_args=(--env-file "$AUTH")
  # --network host để tới Redis dev (cổng 6390 trên host); mặc định chỉ nghe 127.0.0.1 vì web chứa ảnh/clip người
  # --user: file cấu hình do dashboard master ghi vẫn thuộc user trên host
  docker run -d --name "$NAME" --restart unless-stopped --network host --user "$(id -u):$(id -g)" "${auth_args[@]}" \
    -e HOST="$HOST" -e PORT="$PORT" -e REDIS="$REDIS" -e REDIS_PASSWORD="${REDIS_PASSWORD:-}" \
    -v "$ROOT/web:/app:ro" -v "$ROOT/outputs:/outputs:ro" -v "$ROOT/models/buffalo_s:/models:ro" \
    -e MASTER_CONFIGS=/configs -v "$ROOT/configs:/configs" -v "$ROOT/videos:/videos:ro" "$IMAGE" >/dev/null
  echo "Platform demo: http://$HOST:$PORT$([[ -f "$AUTH" ]] && echo " (có đăng nhập, xem web/.auth)")"
}

tunnel() {
  if [[ ! -f "$AUTH" ]] || ! docker inspect "$NAME" --format '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null | grep -q '^WEB_PASS='; then
    ensure_auth
    start
  fi
  docker rm -f "$TUNNEL" >/dev/null 2>&1 || true
  docker run -d --name "$TUNNEL" --restart unless-stopped --network host "$TUNNEL_IMAGE" \
    tunnel --no-autoupdate --url "http://$HOST:$PORT" >/dev/null
  local url=""
  for _ in $(seq 1 30); do
    url=$(docker logs "$TUNNEL" 2>&1 | grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' | head -1 || true)
    [[ -n "$url" ]] && break
    sleep 1
  done
  [[ -z "$url" ]] && { echo "Chưa lấy được link, xem: docker logs $TUNNEL" >&2; exit 1; }
  echo "Link public: $url"
  sed 's/^/  /' "$AUTH"
  echo "Link đổi mỗi lần chạy lại tunnel. Tắt: ./scripts/web.sh tunnel-stop"
}

case "${1:-}" in
  start|restart) start ;;
  stop) docker rm -f "$NAME" "$TUNNEL" >/dev/null 2>&1 || true; echo "Đã dừng platform demo" ;;
  logs) docker logs --tail 50 -f "$NAME" ;;
  tunnel) tunnel ;;
  tunnel-stop) docker rm -f "$TUNNEL" >/dev/null 2>&1 || true; echo "Đã tắt link public" ;;
  publish) publish "${2:-$ROOT/configs/cameras_sim.txt}" ;;
  republish) publish "$ROOT/configs/cameras_sim.txt" "${2:-all}" ;;
  rebuild)
    # Đổi web/requirements.txt hoặc Dockerfile: build lại image, tạo lại web admin + web từng công ty (giữ link)
    docker build -q -t "$IMAGE" "$ROOT/web" >/dev/null && start && publish "$ROOT/configs/cameras_sim.txt" web ;;
  links) links ;;
  unpublish) unpublish "${2:-}" ;;
  *) echo "Dùng: $0 start|stop|restart|logs|tunnel|tunnel-stop|publish [file cam]|republish [công ty]|rebuild|links|unpublish [công ty]"; exit 1 ;;
esac
