#!/usr/bin/env bash
# Chạy pipeline DeepStream trong container deepstream_dev.
# Dùng: ./scripts/run_pipeline.sh [configs/<file>.txt]   (mặc định: configs/my_pipeline.txt)
set -euo pipefail
# Script gọi docker nên phải chạy trên host, không chạy trong container deepstream_dev
if ! command -v docker >/dev/null; then
  echo "Lỗi: chạy script này trên HOST (cd /home/hiepplh/deepstream), không chạy trong container." >&2
  exit 1
fi
CONFIG="${1:-configs/my_pipeline.txt}"
docker exec deepstream_dev deepstream-app -c "/workspace/${CONFIG#/workspace/}"
