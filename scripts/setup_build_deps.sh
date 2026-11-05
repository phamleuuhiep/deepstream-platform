#!/usr/bin/env bash
# Image deepstream:7.0-samples không có header GStreamer/hiredis -> cài để build app C (Task 0.3, tuần 1).
# Chỉ cần chạy lại khi container deepstream_dev bị tạo lại.
set -euo pipefail
# Script gọi docker nên phải chạy trên host, không chạy trong container deepstream_dev
if ! command -v docker >/dev/null; then
  echo "Lỗi: chạy script này trên HOST (cd /home/hiepplh/deepstream), không chạy trong container." >&2
  exit 1
fi
docker exec deepstream_dev bash -c '
  apt-get update -qq &&
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
    libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev libhiredis-dev >/dev/null &&
  echo "gstreamer-1.0 $(pkg-config --modversion gstreamer-1.0)"'
