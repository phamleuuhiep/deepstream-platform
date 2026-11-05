#!/usr/bin/env bash
# Chạy w1-app với pipeline nhiều tầng: YOLO11 COCO 80 class -> tracker -> tìm mặt trong bbox người -> embedding
# ArcFace -> (VIP classifier nếu có model). Tham số còn lại giống run_w1.sh. Xem docs/PIPELINE_KHUON_MAT_VIP.md.
#   ./scripts/run_face.sh -i configs/cameras_sim.txt -t 120 -r 135
#   FACE_THR=0.45 ./scripts/run_face.sh 4
# Cần model: ./scripts/prepare_models.sh (1 lần). Dừng app nền của master trước (chỉ 1 bản app mỗi prefix Redis).
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
for m in yolo11s.onnx libnvdsinfer_custom_impl_Yolo.so facenet/model.etlt buffalo_s/w600k_mbf.onnx; do
  [[ -f "$ROOT/models/$m" ]] || { echo "Thiếu models/$m: chạy ./scripts/prepare_models.sh" >&2; exit 1; }
done
C=/workspace/configs
ARGS=(-c $C/pgie_yolo11_coco.txt -p 0 -S $C/sgie1_face_detect.txt -S $C/sgie2_face_embed.txt -F "${FACE_THR:-0.42}")
[[ -f "$ROOT/models/vip_classifier.onnx" ]] && ARGS+=(-S $C/sgie3_vip_classifier.txt)
# Số cam (vd. 4) phải đứng đầu như run_w1.sh
if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
  N=$1; shift
  exec "$ROOT/scripts/run_w1.sh" "$N" "${ARGS[@]}" "$@"
fi
exec "$ROOT/scripts/run_w1.sh" "${ARGS[@]}" "$@"
