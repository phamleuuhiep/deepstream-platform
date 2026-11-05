#!/usr/bin/env bash
# Tải + chuyển đổi model cho pipeline nhiều tầng (YOLO COCO -> khuôn mặt -> embedding -> VIP).
# Chạy 1 lần trên HOST (cần Internet + docker), kết quả nằm ở models/ (= /workspace/models trong container):
#   models/yolo11s.onnx, models/yolo11s_labels.txt          PGIE: YOLO11s COCO 80 class (DeepStream-Yolo export)
#   models/libnvdsinfer_custom_impl_Yolo.so                 hàm parse output YOLO cho nvinfer
#   models/facenet/model.etlt, int8_calibration.txt         SGIE1: NVIDIA TAO FaceNet (DetectNet_v2, 1 class face)
#   models/buffalo_s/w600k_mbf.onnx                         SGIE2: InsightFace ArcFace MobileFaceNet, embedding 512 chiều
#   models/buffalo_s/det_500m.onnx                          SCRFD: web dùng để tìm mặt trong ảnh đăng ký
#   models/peoplenet/resnet34_peoplenet.onnx                NVIDIA PeopleNet (so sánh model người, Task 2.2)
# Chạy lại an toàn: bước nào đã có file kết quả thì bỏ qua (xoá file đó để làm lại).
# Giấy phép: YOLO11 = AGPL-3.0 (Ultralytics); buffalo_s/w600k_mbf = chỉ nghiên cứu, không thương mại (InsightFace);
# FaceNet = NVIDIA TAO EULA. Dùng thương mại phải thay model hoặc mua license.
set -euo pipefail
command -v docker >/dev/null || { echo "Lỗi: chạy trên HOST" >&2; exit 1; }
ROOT=$(cd "$(dirname "$0")/.." && pwd)
M="$ROOT/models"
YOLO=${YOLO:-yolo11s}
TORCH_IMAGE=pytorch/pytorch:2.6.0-cuda12.4-cudnn9-devel
# Image DeepStream bản "triton" có nvcc + header TensorRT 8.6 (bản samples đang chạy app thì không)
BUILD_IMAGE=nvcr.io/nvidia/deepstream:7.0-triton-multiarch
NGC=https://api.ngc.nvidia.com/v2/models/org/nvidia/team/tao/facenet/pruned_quantized_v2.0.1/files
cd "$M"

[[ -d DeepStream-Yolo ]] || git clone -q --depth 1 https://github.com/marcoslucianops/DeepStream-Yolo
[[ -f $YOLO.pt ]] || curl -fsSL -o "$YOLO.pt" "https://github.com/ultralytics/assets/releases/download/v8.3.0/$YOLO.pt"
if [[ ! -f facenet/model.etlt ]]; then
  mkdir -p facenet
  for f in model.etlt int8_calibration.txt; do
    url=$(curl -fsS "$NGC" | python3 -c "import sys,json; print([u for u in json.load(sys.stdin)['urls'] if '/files/$f?' in u][0])")
    curl -fsSL -o "facenet/$f" "$url"
  done
fi
# PeopleNet (so sánh model detect người, Task 2.2): configs/pgie_peoplenet.txt
PN=https://api.ngc.nvidia.com/v2/models/org/nvidia/team/tao/peoplenet/deployable_quantized_onnx_v2.6.3/files
if [[ ! -f peoplenet/resnet34_peoplenet.onnx ]]; then
  mkdir -p peoplenet
  for f in resnet34_peoplenet.onnx labels.txt; do
    url=$(curl -fsS "$PN" | python3 -c "import sys,json; print([u for u in json.load(sys.stdin)['urls'] if '/files/$f?' in u][0])")
    curl -fsSL -o "peoplenet/$f" "$url"
  done
fi
if [[ ! -f buffalo_s/w600k_mbf.onnx ]]; then
  curl -fsSL -o buffalo_s.zip https://github.com/deepinsight/insightface/releases/download/v0.7/buffalo_s.zip
  mkdir -p buffalo_s && python3 -c "import zipfile; zipfile.ZipFile('buffalo_s.zip').extractall('buffalo_s')"
fi

# 1. YOLO -> ONNX (batch động, opset 17, đã simplify) + labels (80 class COCO, đúng thứ tự class ID)
if [[ ! -f $YOLO.onnx ]]; then
  # Image PyTorch không có libGL: dùng opencv-python-headless thay bản opencv mà ultralytics kéo theo.
  # PyTorch 2.6 đổi torch.load mặc định weights_only=True, file .pt của Ultralytics cần tắt (file tải từ release chính thức)
  docker run --rm -v "$M:/m" -w /m -e YOLO="$YOLO" -e PIP_ROOT_USER_ACTION=ignore -e TORCH_FORCE_NO_WEIGHTS_ONLY_LOAD=1 \
    -e YOLO_CONFIG_DIR=/tmp/Ultralytics \
    "$TORCH_IMAGE" bash -c '
    set -e
    # onnxslim (--simplify) cần onnxruntime
    pip install -q --disable-pip-version-check "ultralytics>=8.3.100,<8.4" onnx==1.17.0 onnxslim==0.1.42 onnxruntime==1.20.1
    pip uninstall -q -y opencv-python 2>/dev/null || true
    pip install -q --disable-pip-version-check opencv-python-headless
    python3 -c "import ultralytics, cv2; print(\"ultralytics\", ultralytics.__version__, \"opencv\", cv2.__version__)"
    mkdir -p /tmp/x && cp DeepStream-Yolo/utils/export_yolo11.py $YOLO.pt /tmp/x/ && cd /tmp/x
    python3 export_yolo11.py -w $YOLO.pt --dynamic --simplify
    cp $YOLO.onnx /m/ && cp labels.txt /m/${YOLO}_labels.txt
    chown '"$(id -u):$(id -g)"' /m/$YOLO.onnx /m/${YOLO}_labels.txt'
fi

# 2. Lib parse YOLO cho nvinfer (CUDA 12.2 = CUDA của DeepStream 7.0)
if [[ ! -f libnvdsinfer_custom_impl_Yolo.so ]]; then
  docker run --rm -v "$M:/m" --entrypoint bash "$BUILD_IMAGE" -c '
    set -e
    cp -r /m/DeepStream-Yolo/nvdsinfer_custom_impl_Yolo /tmp/y
    CUDA_VER=12.2 make -s -C /tmp/y -j8 >/dev/null
    cp /tmp/y/libnvdsinfer_custom_impl_Yolo.so /m/
    chown '"$(id -u):$(id -g)"' /m/libnvdsinfer_custom_impl_Yolo.so'
fi
ls -la "$M"/{$YOLO.onnx,${YOLO}_labels.txt,libnvdsinfer_custom_impl_Yolo.so,buffalo_s/w600k_mbf.onnx,facenet/model.etlt}
echo "Xong. Lần đầu chạy app, nvinfer build engine TensorRT cho từng model (vài phút), lưu cạnh file model."
