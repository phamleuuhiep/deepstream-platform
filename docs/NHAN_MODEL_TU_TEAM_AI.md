# Nhận model từ team AI

Danh sách file và thông số cần nhận từ team AI, nơi đặt chúng, và cách kiểm tra trước khi đưa model vào app.
Mọi lệnh chạy trên host, từ thư mục `/home/hiepplh/deepstream`. Đường dẫn trong config là đường dẫn **trong container**
(`/workspace` = thư mục repo).

Môi trường đích: DeepStream 7.0, **TensorRT 8.6.1**, CUDA 12.4, RTX 4070 Ti SUPER (16 GB).

## 1. File cần nhận

### Model detect người (PGIE), bắt buộc

| File | Đặt ở | Yêu cầu |
|------|-------|---------|
| `<model>.onnx` | `models/` | opset ≤ 17, đã chạy `onnxsim`, **batch động**, input NCHW kích thước cố định |
| `<model>_labels.txt` | `models/` | mỗi dòng 1 tên class, đúng thứ tự class ID |
| Lib parse output (`libnvdsinfer_custom_impl_*.so`) | `models/` | chỉ với YOLO. Nếu team AI export bằng repo **DeepStream-Yolo** (marcoslucianops), chỉ cần biết phiên bản YOLO, lib thì tự build (mục 4, bước 2) |
| 3–5 ảnh mẫu + kết quả (bbox, score) model của họ ra | `videos/model_check/` | dùng để đối chiếu, xem mục 4, bước 3 |
| Video/ảnh test có gán nhãn, có cảnh **người ngồi** | `videos/` | để đo mAP; nhãn dạng COCO hoặc YOLO |

### Classifier ngồi/đứng (SGIE), khi team AI có

| File | Đặt ở | Yêu cầu |
|------|-------|---------|
| `<model>.onnx` | `models/` | như PGIE |
| `<model>_labels.txt` | `models/` | **1 dòng**, các nhãn cách nhau bằng `;`, vd. `standing;sitting` |
| Vài ảnh crop người + nhãn đúng | `videos/model_check/` | để đối chiếu |

### Khuôn mặt + embedding, classifier VIP

App đã có sẵn pipeline YOLO COCO → mặt → embedding → VIP (`docs/PIPELINE_KHUON_MAT_VIP.md`), dùng model công khai.
Model của team AI thay vào bằng config, không sửa code, nếu giữ đúng giao ước:

| Tầng | File | Giao ước |
|------|------|----------|
| Embedding mặt (thay ArcFace) | `<model>.onnx` | ra **đúng 1 output 512 số**, input là crop mặt không căn chỉnh; web `faces.py` phải dùng cùng model + tiền xử lý |
| Classifier VIP | `models/vip_classifier.onnx` + `models/vip_classifier_labels.txt` | crop người, labels **1 dòng** `normal;vip` (app tìm nhãn `vip`) |

Chỉ dùng khi đã duyệt phạm vi xử lý dữ liệu khuôn mặt (NĐ 13/2023).

## 2. Thông số cần hỏi kèm

Thông số nào sai thì app vẫn chạy được, nhưng sẽ bị bbox lệch hoặc không ra object nào.

| Hỏi team AI | Điền vào |
|-------------|----------|
| Kích thước input (vd. 640×640) | lệnh `trtexec`; `infer-dims` (SGIE) |
| Chuẩn hoá: chia 255, hay mean/std (giá trị bao nhiêu) | `net-scale-factor`, `offsets` |
| Ảnh vào là RGB hay BGR | `model-color-format` (0 = RGB, 1 = BGR) |
| Có letterbox (giữ tỉ lệ) không; pad đều 2 bên hay 1 góc | `maintain-aspect-ratio`, `symmetric-padding` |
| NMS đã nằm trong model chưa | `cluster-mode` (2 = chưa, 4 = rồi) |
| Ngưỡng confidence, IoU | `pre-cluster-threshold`, `nms-iou-threshold` |
| **Class ID của người** | tham số `-p` khi chạy app |
| Chỉ SGIE: kích thước người nhỏ nhất còn phân loại đúng (pixel) | `input-object-min-width`, `input-object-min-height` |

Cách quy đổi chuẩn hoá: DeepStream tính `input = net-scale-factor × (pixel − offsets)`.
- Chia 255: `net-scale-factor=0.0039215697906911373`, `offsets=0;0;0`.
- Mean/std (vd. ImageNet): `offsets` = mean × 255 (`123.675;116.28;103.53`), `net-scale-factor` = 1 / (std × 255).
  DeepStream chỉ có 1 scale chung cho 3 kênh, nên dùng std trung bình (≈ `0.0175`).

## 3. Tin nhắn gửi team AI

> Gửi giúp model detect người (và classifier ngồi/đứng nếu có):
> 1. File `.onnx`: opset ≤ 17, batch động, đã `onnxsim`. Chạy được trên TensorRT 8.6.1. Nếu là YOLO thì export bằng DeepStream-Yolo và cho biết phiên bản YOLO.
> 2. File labels, đúng thứ tự class ID.
> 3. Thông số: input size, RGB hay BGR, cách chuẩn hoá, có letterbox không, NMS đã nằm trong model chưa, ngưỡng conf/IoU, class ID của người.
> 4. 3–5 ảnh mẫu kèm kết quả model ra, để bên mình đối chiếu.
> 5. Tập test có gán nhãn, có cảnh người ngồi, kèm mAP các bạn đo được.

## 4. Khi nhận được: kiểm tra và đưa vào

### Bước 1. Kiểm tra ONNX build được engine

```bash
docker exec deepstream_dev /usr/src/tensorrt/bin/trtexec --onnx=/workspace/models/<model>.onnx --fp16 \
  --minShapes=<input>:1x3x640x640 --optShapes=<input>:4x3x640x640 --maxShapes=<input>:32x3x640x640
```

Đạt khi in ra throughput. Gặp lỗi `Unsupported opset` / `Failed to parse onnx` thì gửi lại cho team AI export lại.

### Bước 2. Viết config

```bash
cp configs/pgie_template_onnx.txt configs/pgie_<model>.txt
```

Sửa các dòng `<...>` và các thông số ở mục 2. `num-detected-classes` = số dòng của file labels.

Với YOLO, build lib parse của DeepStream-Yolo trong container, rồi bỏ comment 2 dòng `parse-bbox-func-name`
và `custom-lib-path` trong config:

```bash
docker exec -it deepstream_dev bash
cd /workspace/models && git clone https://github.com/marcoslucianops/DeepStream-Yolo
cd DeepStream-Yolo && CUDA_VER=12.4 make -C nvdsinfer_custom_impl_Yolo
cp nvdsinfer_custom_impl_Yolo/libnvdsinfer_custom_impl_Yolo.so /workspace/models/
```

SGIE: copy `configs/sgie_example_vehicletype.txt` thành `configs/sgie_<model>.txt`, bỏ các dòng `tlt-*`,
`int8-calib-file`, `uff-input-blob-name`, `output-blob-names`, `force-implicit-batch-dim`, rồi sửa:

```ini
onnx-file=/workspace/models/<model>.onnx
labelfile-path=/workspace/models/<model>_labels.txt
network-mode=2
operate-on-class-ids=<class người của PGIE>
infer-dims=3;224;224
```

### Bước 3. Đối chiếu trên 1 video

```bash
# Video có vẽ khung, xem bằng mắt (so với ảnh mẫu team AI gửi)
docker exec deepstream_dev /workspace/apps/w1_multistream/w1-annotate \
  -C /workspace/configs/pgie_<model>.txt -p <class_người> \
  -i /workspace/videos/<video>.mp4 -o /outputs/check_<model>.mp4 -t 120

# Metadata JSONL, dùng để tính mAP
./scripts/run_task13.sh -c /workspace/configs/pgie_<model>.txt -p 30 \
  -o /outputs/eval_<model>.jsonl file:///workspace/videos/<video>.mp4
```

Lần đầu nvinfer build engine (vài phút) và lưu cạnh file onnx. **Nhận onnx mới thì xoá file `.engine` cũ**,
nếu không app vẫn chạy engine cũ.

JSONL mỗi dòng 1 frame, `bbox = [left, top, width, height]` theo khung **1920×1080** của streammux (không phải
độ phân giải gốc của video):
```json
{"src":0,"frame":2,"pts_ms":133.3,"num_obj":12,"objs":[{"cls":2,"label":"person","tid":0,"conf":0.498,"trk_conf":0.943,"bbox":[116.0,488.0,88.3,288.0]}]}
```

### Bước 4. Chạy đa luồng

Nút **Chạy lại app** trên dashboard và `app_restart` chỉ chạy model mẫu TrafficCamNet, hoặc pipeline khuôn mặt
(YOLO11 + FaceNet + ArcFace, khi tick "Nhận diện khuôn mặt + VIP"); **chưa đọc config model khác**.
Muốn chạy model mới thì dừng app nền rồi chạy tay (xem thêm `docs/CHAY_LAI_APP.md`, mục 4):

```bash
echo '{"action":"app_stop"}' > configs/.master/request.json; sleep 10
./scripts/run_w1.sh -i configs/cameras_sim.txt -t 120 -r 135 \
  -c /workspace/configs/pgie_<model>.txt -p <class_người> \
  [-S /workspace/configs/sgie_<model>.txt]
```

Kết quả SGIE nằm trong `ds:frames`, ở `objs[].attrs`: `[{"gie":2,"label":"sitting","prob":0.93}]`.

## 5. Tiêu chí nhận model

| Tiêu chí | Mức tối thiểu | Đo bằng |
|----------|---------------|---------|
| Tốc độ | 32 cam × 30 fps trên 1 GPU (`[stats]` ≥ 900 FPS tổng, `32/32 cam`) | `run_w1.sh 32` sau `rtsp_sim.sh start 32` |
| Người ngồi | Bắt được trong ≥ 90% frame của `dwell_sitting_180s.mp4` | `check_<model>.mp4` + JSONL |
| Độ chính xác | mAP trên tập test ≥ model đang dùng | JSONL |
| VRAM | App ≤ 6 GB với 32 cam | `nvidia-smi` |

Không đạt tốc độ thì thử lần lượt: FP16 → INT8 (cần 200–500 ảnh camera thật để calibrate), giảm input
(640 → 512), `interval=1` trong config (detect 1 frame mỗi 2 frame, tracker nội suy), hoặc chia cam sang GPU 1.

## 6. Lỗi hay gặp

| Hiện tượng | Nguyên nhân |
|-----------|-------------|
| Không ra object nào / bbox lệch hẳn | Sai thông số mục 2 (chuẩn hoá, RGB/BGR, letterbox) hoặc sai lib parse |
| Lần nào chạy cũng build engine lại | `model-engine-file` không khớp tên engine thật (`<model>.onnx_b<batch>_gpu0_fp16.engine`, batch = số cam) |
| Đổi model nhưng kết quả như cũ | Còn file `.engine` cũ |
| FPS mỗi cam < 30 khi nhiều cam | Model quá nặng (mục 5), hoặc NVDEC quá tải (`nvidia-smi dmon -s u`, cột `dec` ~100%) |
