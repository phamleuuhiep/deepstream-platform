#!/usr/bin/env bash
# Metric + Grafana cho pipeline (Task 2.4).
#   ./scripts/monitor.sh start    ds_exporter (:9466/metrics) + Prometheus riêng (ds_prometheus :9091)
#                                 + Grafana riêng (ds_grafana :3001, dashboard "DeepStream pipeline" nạp sẵn)
#   ./scripts/monitor.sh stop|status|info
# Grafana/Prometheus chung của server (:3000/:9090, thư mục /home/nhatdm/monitoring) không sửa được từ đây: gửi
# admin job scrape trong monitor/prometheus.yml (target <host>:9466) + file monitor/grafana/dashboards/deepstream.json để import.
set -euo pipefail
command -v docker >/dev/null || { echo "Lỗi: chạy trên HOST" >&2; exit 1; }
ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/scripts/redis_env.sh"
BIND=${REDIS_BIND:-127.0.0.1}
REDIS=${REDIS:-$BIND:6390}
RUN="$ROOT/monitor/.run"                 # cấu hình đã điền địa chỉ + mật khẩu Grafana (không commit)
EXPORTER_IMAGE=ds-exporter:1
PROM_IMAGE=prom/prometheus:v3.11.3
GRAFANA_IMAGE=grafana/grafana:13.0.1-security-01

start() {
  mkdir -p "$RUN/datasources" "$RUN/dashboards"
  [[ -f "$RUN/grafana.env" ]] || ( umask 077; echo "GF_SECURITY_ADMIN_PASSWORD=$(head -c 12 /dev/urandom | base64 | tr -d '/+=')" > "$RUN/grafana.env" )
  sed "s/EXPORTER_ADDR/$BIND:9466/" "$ROOT/monitor/prometheus.yml" > "$RUN/prometheus.yml"
  sed "s#PROM_ADDR#$BIND:9091#" "$ROOT/monitor/grafana/provisioning/datasources/ds.yml" > "$RUN/datasources/ds.yml"
  cp "$ROOT/monitor/grafana/provisioning/dashboards/ds.yml" "$RUN/dashboards/ds.yml"
  docker image inspect "$EXPORTER_IMAGE" >/dev/null 2>&1 || docker build -q -t "$EXPORTER_IMAGE" "$ROOT/monitor" >/dev/null
  docker rm -f ds_exporter ds_prometheus ds_grafana >/dev/null 2>&1 || true
  # --gpus + --pid host: nvidia-smi thấy tiến trình w1-app (VRAM từng GPU)
  docker run -d --name ds_exporter --restart unless-stopped --network host --gpus all --pid host \
    -e NVIDIA_DRIVER_CAPABILITIES=utility -e REDIS="$REDIS" -e REDIS_PASSWORD="${REDIS_PASSWORD:-}" \
    -e LISTEN="$BIND:9466" -v "$ROOT/monitor:/app:ro" "$EXPORTER_IMAGE" >/dev/null
  docker run -d --name ds_prometheus --restart unless-stopped --network host \
    -v "$RUN/prometheus.yml:/etc/prometheus/prometheus.yml:ro" -v ds_prometheus_data:/prometheus "$PROM_IMAGE" \
    --config.file=/etc/prometheus/prometheus.yml --storage.tsdb.retention.time=15d \
    --web.listen-address="$BIND:9091" >/dev/null
  docker run -d --name ds_grafana --restart unless-stopped --network host --env-file "$RUN/grafana.env" \
    -e GF_SERVER_HTTP_ADDR="$BIND" -e GF_SERVER_HTTP_PORT=3001 -e GF_AUTH_ANONYMOUS_ENABLED=true \
    -e GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer -e GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/var/lib/grafana/dashboards/deepstream.json \
    -v "$RUN/datasources:/etc/grafana/provisioning/datasources:ro" -v "$RUN/dashboards:/etc/grafana/provisioning/dashboards:ro" \
    -v "$ROOT/monitor/grafana/dashboards:/var/lib/grafana/dashboards:ro" -v ds_grafana_data:/var/lib/grafana "$GRAFANA_IMAGE" >/dev/null
  info
}

info() {
  echo "Metric:     http://$BIND:9466/metrics"
  echo "Prometheus: http://$BIND:9091"
  echo "Grafana:    http://$BIND:3001 (xem không cần đăng nhập; admin / $(cut -d= -f2 "$RUN/grafana.env"))"
}

case "${1:-}" in
  start) start ;;
  stop) docker rm -f ds_exporter ds_prometheus ds_grafana >/dev/null 2>&1; echo "Đã dừng" ;;
  status) docker ps -a --filter "name=^/(ds_exporter|ds_prometheus|ds_grafana)$" --format '{{.Names}}\t{{.Status}}' ;;
  info) info ;;
  *) echo "Dùng: $0 start|stop|status|info"; exit 1 ;;
esac
