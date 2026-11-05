#!/usr/bin/env bash
# Test edge case tuần 1 trên N luồng RTSP giả lập (mặc định 32):
#   1. App khởi động TRƯỚC khi có camera            -> đo thời gian đến khi N/N cam có hình
#   2. Toàn bộ feed sập 12s rồi lên lại (server EOS) -> pipeline không được dừng, đo thời gian hồi phục
#   3. Redis chết 20s                                -> pipeline chạy tiếp, sự kiện không mất
#   4. Ctrl+C                                        -> dừng sạch (End of stream)
# Dùng: ./scripts/w1_edge_test.sh [N]     Log: outputs/w1_edge_test.log
set -uo pipefail
# Script gọi docker nên phải chạy trên host, không chạy trong container deepstream_dev
if ! command -v docker >/dev/null; then
  echo "Lỗi: chạy script này trên HOST (cd /home/hiepplh/deepstream), không chạy trong container." >&2
  exit 1
fi
cd "$(dirname "$0")/.."
N=${1:-32}
LOG=outputs/w1_edge_test.log
URIS=$(for i in $(seq 1 "$N"); do printf 'rtsp://127.0.0.1:8554/cam%d ' "$i"; done)

up_count() { grep -c " $N/$N cam có hình" "$LOG"; }
wait_full() {  # chờ thêm 1 dòng stats N/N mới, in số giây
  local before t0=$(date +%s); before=$(up_count)
  until [ "$(up_count)" -gt "$before" ]; do sleep 1; [ $(( $(date +%s) - t0 )) -gt 180 ] && { echo "TIMEOUT"; return; }; done
  echo "$(( $(date +%s) - t0 ))s"
}

./scripts/rtsp_sim.sh stop >/dev/null
docker ps -q --filter name=^/ds_redis$ | grep -q . || ./scripts/redis_dev.sh start >/dev/null
./scripts/redis_dev.sh cli FLUSHALL >/dev/null
docker exec deepstream_dev make -s -C /workspace/apps/w1_multistream

docker exec -e LANG=C.UTF-8 deepstream_dev bash -c \
  "/workspace/apps/w1_multistream/w1-app -s 1 -t 120 $URIS > /workspace/$LOG 2>&1" &
sleep 15
echo "[1] Bật camera sau khi app đã chạy 15s"
./scripts/rtsp_sim.sh start "$N" >/dev/null
echo "    -> $N/$N cam có hình sau $(wait_full)"
sleep 15

echo "[2] Feed sập 12s"
docker stop ds_fanout >/dev/null; sleep 12; docker start ds_fanout >/dev/null
echo "    -> hồi phục $N/$N cam sau $(wait_full) kể từ khi feed lên lại"
sleep 10

echo "[3] Redis chết 20s"
docker stop ds_redis >/dev/null; sleep 20; docker start ds_redis >/dev/null; sleep 10
grep -E "^\[stats\]" "$LOG" | tail -1 | sed -E 's/.*\| (redis=.*)/    -> \1/'

echo "[4] Ctrl+C"
docker exec deepstream_dev pkill -INT -f "^/workspace/apps/w1_multistream/w1-app"
wait
grep -qE "End of stream" "$LOG" && echo "    -> dừng sạch (End of stream)" || echo "    -> KHÔNG thấy End of stream"
echo "Sự kiện: $(grep -c camera_offline "$LOG") camera_offline, $(grep -c camera_online "$LOG") camera_online, \
$(grep -c 'gửi EOS (server' "$LOG") EOS bị chặn. Redis ds:events = $(./scripts/redis_dev.sh cli XLEN ds:events)"
