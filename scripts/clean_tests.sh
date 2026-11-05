#!/usr/bin/env bash
# Dọn kết quả test của tuần 1 (clip, ảnh, log, dữ liệu Redis dev). Giữ nguyên kết quả tuần 0 (w0_*)
# và video đầu vào trong videos/.
# Dùng: ./scripts/clean_tests.sh            xem sẽ xoá gì rồi hỏi xác nhận
#       ./scripts/clean_tests.sh -y         xoá luôn không hỏi
set -euo pipefail
if ! command -v docker >/dev/null; then
  echo "Lỗi: chạy script này trên HOST (cd /home/hiepplh/deepstream), không chạy trong container." >&2
  exit 1
fi
cd "$(dirname "$0")/.."

# File do container tạo thuộc root -> xoá qua docker exec. Chỉ xoá trong các thư mục/mẫu tên cố định.
TARGETS='/outputs/w1_clips /outputs/w1_snapshots /outputs/logs'
LOGS='/outputs/w1_*.log /outputs/.gdb.log'

echo "Sẽ xoá:"
docker exec deepstream_dev bash -c "
  for d in $TARGETS; do [ -d \$d ] && echo \"  \$d/  (\$(find \$d -type f | wc -l) file, \$(du -sh \$d | cut -f1))\"; done
  for f in $LOGS; do [ -f \$f ] && echo \"  \$f\"; done; true"
docker ps -q --filter name=^/ds_redis$ | grep -q . && echo "  Redis dev: ds:frames, ds:events, ds:cameras"

if [[ "${1:-}" != "-y" ]]; then
  read -r -p "Xoá? [y/N] " ans
  [[ "$ans" == [yY] ]] || { echo "Huỷ."; exit 0; }
fi

docker exec deepstream_dev bash -c "
  for d in $TARGETS; do [ -d \$d ] && find \$d -mindepth 1 -type f -delete; done
  rm -f $LOGS"
if docker ps -q --filter name=^/ds_redis$ | grep -q .; then
  "$(dirname "$0")/redis_dev.sh" cli DEL ds:frames ds:events ds:cameras >/dev/null
fi
echo "Đã dọn xong."
