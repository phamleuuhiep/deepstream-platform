# DeepStream là gì, và dự án Vision dùng nó thế nào

Tài liệu cho người mới: không cần biết trước về DeepStream hay GStreamer.

## 1. DeepStream là gì

**DeepStream là bộ công cụ của NVIDIA để chạy AI trên nhiều luồng video cùng lúc, trên GPU.**

Hãy hình dung một **dây chuyền nhà máy**. Mỗi frame video là một sản phẩm chạy trên băng chuyền và đi qua từng trạm:
trạm giải nén, trạm gom hàng, trạm AI soi xem có người không, trạm đánh số từng người... Mỗi trạm làm đúng một việc
rồi chuyển frame cho trạm tiếp theo. DeepStream cung cấp sẵn các trạm này (gọi là **plugin**). Mình chỉ cần chọn
trạm, nối chúng thành dây chuyền (gọi là **pipeline**), và viết phần nghiệp vụ ở cuối dây chuyền.

### Sao không tự viết bằng OpenCV + PyTorch?

Tự viết thì làm được với 1–2 camera. Với vài chục camera thì không theo kịp, vì:

| Tự viết (OpenCV + PyTorch) | DeepStream |
|----------------------------|------------|
| Giải nén video bằng CPU, CPU nhanh chóng quá tải | Giải nén bằng chip riêng trên GPU (**NVDEC**), CPU gần như rảnh |
| Frame bị chép qua lại giữa RAM và GPU ở mỗi bước | Frame nằm trên GPU từ lúc giải nén tới lúc ra kết quả |
| Mỗi camera chạy model một lần | Gom frame của nhiều camera thành **1 batch**, chạy model 1 lần cho tất cả |
| Model PyTorch chạy chậm | Model được chuyển sang **TensorRT**, nhanh hơn nhiều lần |
| Tự viết code bám theo từng người qua các frame | Có sẵn **tracker** |
| Tự xử lý camera rớt mạng, tự nối lại | `nvurisrcbin` tự nối lại |

Kết quả ở dự án này: **32 camera × 30 fps trên 1 card RTX 4070 Ti SUPER**, card thứ hai vẫn còn trống.

## 2. DeepStream hoạt động thế nào

### Pipeline = các plugin nối nhau

DeepStream xây trên **GStreamer**, một framework xử lý video mã nguồn mở. Mỗi plugin là một khối có đầu vào và
đầu ra; dữ liệu chảy từ trái sang phải.

```
 nguồn video ──► giải nén ──► gom batch ──► AI detect ──► tracker ──► (AI phụ) ──► xuất kết quả
```

### Frame mang theo "phiếu ghi chú" (metadata)

Đây là ý quan trọng nhất. Mỗi trạm **không vẽ gì lên ảnh**; nó ghi kết quả vào một phiếu đi kèm frame,
gọi là **metadata**:

- `nvinfer` ghi: "có 1 object, class *person*, độ tin 0.87, khung ở (116, 488, 88, 288)".
- `nvtracker` ghi thêm: "đây là người số **17**, đã thấy từ 40 frame trước".
- Classifier phụ ghi thêm: "người 17 đang **ngồi**, xác suất 0.93".

Cuối dây chuyền, code của mình gắn một **probe** (giống một trạm kiểm tra) để đọc phiếu này rồi làm nghiệp vụ:
gửi dữ liệu, tính thời gian ở yên, phát cảnh báo.

### Các plugin chính

| Plugin | Vai trò | Ví von |
|--------|---------|--------|
| `nvurisrcbin` | Nhận video từ camera RTSP hoặc file, giải nén bằng NVDEC, tự nối lại khi rớt | Cổng nhập hàng |
| `nvstreammux` | Gom frame của N camera thành 1 batch, đưa về cùng kích thước (1920×1080) | Xếp hàng vào thùng |
| `nvinfer` (PGIE) | Chạy model **detect** trên cả frame: tìm người, xe... | Thanh tra chính |
| `nvtracker` | Gán **ID** cho từng object, bám theo nó qua các frame | Người đánh số thứ tự |
| `nvinfer` (SGIE) | Chạy model **phụ** trên từng object đã detect: ngồi/đứng, khuôn mặt... | Thanh tra chuyên môn |
| `nvdsosd` | Vẽ khung, chữ lên frame (chỉ khi cần xuất video để xem) | Dán nhãn |
| `nvv4l2h264enc` | Nén video bằng chip **NVENC** của GPU | Đóng gói |

**PGIE / SGIE**: *Primary / Secondary GPU Inference Engine*. PGIE chạy trên cả khung hình, SGIE chạy trên từng
object mà PGIE tìm ra.

### Model chạy thế nào

Team AI train model bằng PyTorch, rồi xuất ra file `.onnx`. Lần chạy đầu, `nvinfer` dùng **TensorRT** để biên dịch
file này thành file `.engine` tối ưu riêng cho card đang dùng (mất vài phút), lưu lại cho các lần sau. Cách nhận
và đưa model vào: `docs/NHAN_MODEL_TU_TEAM_AI.md`.

## 3. DeepStream trong dự án Vision

### Bức tranh tổng thể

```
  Camera (RTSP)                    Server GPU                                   Người dùng
 ┌─────────────┐   ┌──────────────────────────────────────────┐   ┌─────────┐   ┌──────────────────┐
 │ Camera thật │   │ w1-app (DeepStream, container            │   │         │   │ Web dashboard    │
 │ hoặc        ├──►│ deepstream_dev)                          ├──►│  Redis  ├──►│ - admin (master) │
 │ MediaMTX    │   │ detect người → bám ID → đo thời gian     │   │         │   │ - từng công ty   │
 │ (giả lập)   │   │ ở yên → cảnh báo + ảnh + clip            │   └─────────┘   │ - từng camera    │
 └─────────────┘   └───────────────┬──────────────────────────┘                 └──────────────────┘
                                   └──► outputs/: ảnh, clip, log
```

- **Camera**: hiện đang giả lập bằng MediaMTX, phát lại video test thành 19 camera RTSP, chia theo công ty và toà
  nhà (`configs/cameras_sim.txt`). Thay bằng camera Hikvision/Dahua thật chỉ cần đổi URL.
- **w1-app** (`apps/w1_multistream/w1_app.c`): ứng dụng DeepStream viết bằng C. Đây là phần duy nhất dùng
  DeepStream.
- **Redis**: nơi app gửi kết quả. `ds:frames` là kết quả từng frame, `ds:events` là các sự kiện, trong đó có
  cảnh báo.
- **Web** (`web/app.py`): chỉ đọc Redis, ảnh và clip để hiển thị. **Agent** (`master/agent.py`) thực hiện các
  nút bấm trên dashboard, như bật/tắt app.

### Pipeline bên trong w1-app

```
 cam 1 ─ nvurisrcbin ─┐
 cam 2 ─ nvurisrcbin ─┤                                              ┌─ gửi Redis (ds:frames)
  ...                 ├─► nvstreammux ─► nvinfer ─► nvtracker ─► probe ┼─ tính thời gian ở yên → ds:events
 cam N ─ nvurisrcbin ─┘   (batch N,     (detect    (NvDCF,          ├─ chụp ảnh khi cảnh báo
         (+ bỏ bớt frame   1920×1080)    người)     gán ID)          └─ lưu clip (smart record)
          nếu đặt fps)
```

Ứng với dự án:

| Bước | Trong dự án |
|------|-------------|
| `nvurisrcbin` | Mỗi camera một khối. Camera rớt mạng thì tự nối lại, các camera khác vẫn chạy bình thường |
| Bỏ bớt frame | Cột `fps` trong danh sách camera: camera chỉ cần 5 fps thì AI chỉ xử lý 5 frame/giây, đỡ tải GPU |
| `nvstreammux` | Gom 19 camera thành 1 batch |
| `nvinfer` | Hiện đang dùng **model mẫu TrafficCamNet** của NVIDIA (4 class: xe, xe đạp, người, biển báo). Model này nhận **người ngồi** kém, đang chờ team AI gửi model mới |
| `nvtracker` | Gán ID cho từng người. Nhờ có ID mới biết "người này đã ở yên bao lâu" |
| SGIE (tuỳ chọn, `-S`) | Chỗ để gắn classifier ngồi/đứng của team AI sau này |
| probe | Code nghiệp vụ của dự án, xem mục dưới |

### Một người ngồi quá lâu: từ frame tới cảnh báo

1. Camera sảnh toà A phát video, `nvurisrcbin` giải nén từng frame.
2. `nvinfer` tìm thấy một *person* ở góc trái khung hình.
3. `nvtracker` gán cho người đó **ID 17**. Ở các frame tiếp theo, đó vẫn là ID 17.
4. Ở mỗi frame, probe gửi lên Redis vị trí người 17, kèm camera, công ty, toà nhà và thời điểm.
5. Probe theo dõi vị trí người 17. Nếu người này dịch chuyển ít hơn 30% chiều cao khung người, probe coi là
   **ở yên** và bắt đầu đếm giờ.
6. Đủ **120 giây** (chỉnh được trên dashboard), app phát **cảnh báo**:
   - ghi sự kiện vào `ds:events` và `outputs/logs/alerts.log`;
   - chụp ảnh cả khung hình và ảnh cắt riêng người đó;
   - **smart record**: DeepStream luôn giữ sẵn đoạn video gần nhất trong bộ nhớ (ngưỡng + 15 giây, tức 135 giây), nên clip lưu ra có cả đoạn
     **trước** lúc cảnh báo, từ lúc người đó bắt đầu ngồi;
   - vẽ khung lên clip, tạo file `*_bbox.mp4` (xanh = bình thường, vàng = đang ở yên, đỏ = quá ngưỡng).
7. Web đọc sự kiện từ Redis và hiện cảnh báo trên dashboard của công ty đó, kèm ảnh và clip.

### Phần nào DeepStream lo, phần nào dự án tự viết

| DeepStream có sẵn | Dự án tự viết |
|-------------------|---------------|
| Nhận RTSP, giải nén, tự nối lại camera | Đọc danh sách camera theo công ty/toà, giới hạn fps từng camera |
| Gom batch, chạy model trên GPU | Config model (`configs/`), chọn class "người" |
| Tracker gán ID | Logic "ở yên quá lâu" (`dwell_update` trong `w1_app.c`) |
| Smart record (bộ nhớ đệm video) | Khi nào lưu clip, chụp ảnh, tự xoá file cũ |
| Vẽ khung, nén video bằng GPU | Màu khung theo trạng thái (`w1_annotate.c`) |
| | Gửi Redis, tự xử lý khi Redis chết, web, dashboard, agent |

## 4. Vì sao kiến trúc này hợp với dự án

- **Mở rộng theo số camera**: thêm camera chỉ cần thêm dòng vào danh sách. Hết sức thì chia sang GPU thứ hai.
- **Đổi model không cần sửa code**: model mới chỉ cần file `.onnx` và một file config.
- **Thêm khả năng mới bằng cách gắn thêm trạm**: classifier ngồi/đứng, nhận diện khuôn mặt hay vùng ROI đều là
  plugin gắn thêm vào pipeline, phần còn lại giữ nguyên.
- **Tách rời app AI và web**: app chỉ ghi vào Redis, web chỉ đọc. Một bên lỗi không kéo bên kia chết theo, và team
  platform có thể làm web thật mà không phải động vào DeepStream.

## 5. Thuật ngữ

| Từ | Nghĩa |
|----|-------|
| Pipeline | Chuỗi plugin nối nhau, video chảy qua từ đầu tới cuối |
| Plugin / element | Một khối xử lý trong pipeline |
| Batch | Nhiều frame (từ nhiều camera) gom lại để chạy model 1 lần |
| Metadata | Kết quả AI đi kèm mỗi frame: class, khung, ID, độ tin cậy |
| Probe | Điểm móc code của mình vào pipeline để đọc metadata |
| PGIE / SGIE | Model chính (chạy trên cả frame) / model phụ (chạy trên từng object) |
| Tracker | Bám object qua các frame, gán ID cố định |
| TensorRT, `.engine` | Bộ tối ưu model của NVIDIA; `.engine` là model đã biên dịch cho đúng card |
| NVDEC / NVENC | Chip giải nén / nén video riêng trên GPU, không tốn nhân CUDA |
| RTSP | Giao thức camera IP dùng để phát video |
| Smart record | Giữ sẵn video gần nhất trong bộ nhớ, khi có sự kiện thì lưu cả đoạn trước đó |
| Redis Stream | Hàng đợi tin nhắn; app ghi vào, web và platform đọc ra |
