# Pipeline nhiều tầng: YOLO COCO → khuôn mặt → nhận diện → khách VIP

App `w1-app` chạy thêm 3 tầng nvinfer phía sau YOLO: tìm mặt **chỉ trong bbox người**, tính embedding khuôn mặt,
so với người đã đăng ký trên dashboard, rồi xếp hạng khách (VIP / thường). Khi nhận ra ai, dashboard hiện thông báo.

Mọi lệnh chạy trên host, từ `/home/hiepplh/deepstream`. Đường dẫn trong config là đường dẫn trong container
(`/workspace` = thư mục repo).

## 1. Sơ đồ

```
nvurisrcbin x N ─► nvstreammux 1920x1080, batch N
                     │
                     ▼
  [PGIE  gie 1] nvinfer  YOLO11s COCO         cả khung     ─► 80 class: person(0), car, bag, ...
                     │
                     ▼
  [tracker]     nvtracker NvDCF                            ─► tracking ID cho mọi object
                     │
                     ▼
  [SGIE1 gie 2] nvinfer  FaceNet              trên person  ─► object "face", parent = person
                     │
                     ▼
  [SGIE2 gie 3] nvinfer  ArcFace MobileFaceNet trên face   ─► tensor 512 chiều (NvDsInferTensorMeta)
                     │
                     ▼
  [SGIE3 gie 4] nvinfer  VIP classifier       trên person  ─► nhãn vip / normal (khi team AI có model)
                     │
                     ▼
  probe (fakesink.sink) ─► so khớp gallery, hạng khách, sự kiện ─► Redis ─► web
```

Tracker đứng ngay sau PGIE (không phải cuối) vì 2 lý do: SGIE1 dùng tracking ID để không tìm mặt lại mỗi frame
(`secondary-reinfer-interval`), và app gom kết quả nhận diện theo track người (`parent->object_id`).

## 2. Các tầng nvinfer

| | PGIE | SGIE1 | SGIE2 | SGIE3 |
|---|---|---|---|---|
| Config | `configs/pgie_yolo11_coco.txt` | `configs/sgie1_face_detect.txt` | `configs/sgie2_face_embed.txt` | `configs/sgie3_vip_classifier.txt` |
| Model | YOLO11s (Ultralytics), ONNX | NVIDIA TAO FaceNet (DetectNet_v2), `.etlt` | InsightFace `w600k_mbf.onnx` | chưa có (team AI) |
| `gie-unique-id` | 1 | 2 | 3 | 4 |
| `process-mode` | 1 (cả frame) | 2 (trên object) | 2 | 2 |
| `operate-on-gie-id` / `class-ids` | – | 1 / 0 (person) | 2 / 0 (face) | 1 / 0 (person) |
| `network-type` | 0 detector | 0 detector | 100 tensor thô | 1 classifier |
| Input | 640x640, letterbox giữa | 736x416, giữ tỉ lệ | 112x112, kéo thẳng | 224x224 (tuỳ model) |
| Chuẩn hoá | RGB / 255 | RGB / 255 | RGB, (x − 127.5) / 127.5 | ImageNet (tuỳ model) |
| Chính xác | FP16 | INT8 (calib của NVIDIA) | FP16 | FP16 |
| Batch | = số camera (app ghi đè) | 16 crop người | 32 crop mặt | 16 crop người |
| Chạy khi nào | mọi frame (`interval=0`) | mỗi track người 1 lần / 5 frame, người ≥ 40x100 px | mọi mặt ≥ 36x36 px | 1 lần / track, lại khi bbox to ra hoặc sau 30 frame |
| Hậu xử lý | `NvDsInferParseYolo` (lib DeepStream-Yolo) + NMS | parser DetectNet_v2 có sẵn + NMS | không (app đọc tensor) | softmax/argmax có sẵn, ngưỡng 0.6 |
| Ra metadata | `NvDsObjectMeta` | `NvDsObjectMeta`, `parent` = người | `NvDsInferTensorMeta` trong `obj_user_meta_list` của mặt | `NvDsClassifierMeta` của người |

Ghi chú từng tầng (lý do chi tiết nằm ngay trong file config):

- **PGIE.** COCO bản dùng cho YOLO có **80 class**: 91 ID gốc của bài báo COCO, bỏ 11 ID không có ảnh. Thứ tự class
  ở `models/yolo11s_labels.txt`, `person` = class 0 nên app chạy với `-p 0`. Người có ngưỡng riêng 0.3
  (`[class-attrs-0]`) để không sót người ngồi / bị che.
- **SGIE1.** Chạy trên crop người nên mặt nhỏ ở xa vẫn đủ pixel cho model (crop được phóng lên 736x416). Giữ tỉ lệ
  (`maintain-aspect-ratio=1`) vì crop người cao còn input thì ngang: kéo thẳng sẽ làm méo mặt. nvinfer tự đổi toạ độ
  mặt về khung 1920x1080 (cộng offset của người, trừ phần letterbox). `topk=2`: 1 người tối đa 2 mặt.
- **SGIE2.** `network-type=100` + `output-tensor-meta=1`: nvinfer không parse gì, chỉ gắn 512 số float (đã chép về
  host) vào object mặt. `scaling-filter=1` (bilinear) để khớp cách web resize ảnh đăng ký.
- **SGIE3.** File labels 1 dòng `normal;vip`. Chỉ được thêm vào pipeline khi có `models/vip_classifier.onnx`.

## 3. App xử lý metadata (probe ở `fakesink.sink`)

Mỗi frame, probe duyệt object 2 lượt (`apps/w1_multistream/w1_app.c`):

1. **Lượt 1** cập nhật trạng thái theo track người (`FaceTrack`, khoá = camera + tracking ID người):
   - Object mặt (`unique_component_id == 2`) có tensor của gie 3: chuẩn hoá L2, tính cosine với từng ảnh mẫu trong
     gallery (chỉ mẫu của công ty sở hữu camera, hoặc mẫu "mọi công ty"), lấy mẫu cao nhất.
     - Cosine ≥ ngưỡng `-F` và **cùng 1 người khớp 2 lần liên tiếp** trên track đó → nhận ra → sự kiện
       `face_recognized` + ảnh toàn khung + crop mặt. Cùng người ở cùng camera không báo lại trong 5 phút.
     - Không khớp ai 3 lần, mặt cao ≥ 64 px → gửi vector + crop vào `ds:faces_unknown` (1 lần / track), để
       dashboard cho đăng ký thẳng từ camera.
   - Người có `NvDsClassifierMeta` của gie 4: ghi nhãn; `vip` ở người chưa nhận ra mặt → `vip_detected` (1 lần / track).
2. **Lượt 2** dựng JSON frame (`ds:frames`) và chạy logic ở yên như trước (chỉ trên người của PGIE, không tính mặt).

**Hạng khách** gửi kèm người (`objs[].tier`, `tier_src`):

| Thứ tự ưu tiên | Điều kiện | `tier` | `tier_src` |
|---|---|---|---|
| 1 | Mặt khớp người đã đăng ký | hạng trong hồ sơ (`vip` / `normal`) | `face` |
| 2 | Không khớp, có SGIE3 | nhãn classifier | `classifier` (+ `tier_prob`) |
| 3 | Không có cả hai | không gửi (chưa xác định) | – |

Hằng số ở đầu `w1_app.c`: `FACE_VOTES` (2), `FACE_COOLDOWN_SEC` (300), `FACE_UNKNOWN_MIN_PX` (64), `FACE_UNKNOWN_HITS` (3),
`FACE_RELOAD_SEC` (3). Gie ID các tầng cũng là hằng số (`FACE_DET_GIE`, `FACE_EMB_GIE`, `VIP_GIE`): đổi
`gie-unique-id` trong config thì sửa cả ở đây.

## 4. Dữ liệu ra Redis

Frame (`ds:frames`), thêm so với trước:

```json
{"cls":0,"label":"person","tid":7,"conf":0.91,"bbox":[...],
 "face":{"id":"3d02539515cc","name":"Nguyễn Văn A","score":0.69},"tier":"vip","tier_src":"face"}
{"cls":0,"label":"face","tid":-1,"conf":0.62,"bbox":[...],"gie":2,"ptid":7}
```

Object do SGIE tạo có `gie` (tầng tạo ra) và `ptid` (tracking ID người chứa nó); object của PGIE không có `gie`.

Sự kiện (`ds:events`):

```json
{"type":"face_recognized","cam":"acme-A-cam1","ts":"2026-11-03T06:05:18.541Z","tid":7,"person_id":"3d02539515cc",
 "name":"Nguyễn Văn A","tier":"vip","score":0.69,"bbox":[...],"face_bbox":[...],
 "snapshot":"/outputs/w1_snapshots/<cam>_face_<id>_<ms>_frame.jpg","face_crop":"..._crop.jpg"}
{"type":"vip_detected","cam":"...","tid":9,"tier":"vip","source":"classifier","prob":0.83,"bbox":[...],
 "snapshot":"...","crop":"..."}
```

Mặt lạ (`ds:faces_unknown`, giữ ~1000 bản ghi): `cam`, `tid`, `face_bbox`, `face_crop`, `emb` (512 float32 base64).

Gallery (web ghi, app đọc lại trong ~3 s khi `ds:faces_ver` đổi, không cần chạy lại app):

| Key | Kiểu | Nội dung |
|---|---|---|
| `ds:faces` | hash | `<id>` → JSON hồ sơ: `name`, `tier`, `company` (rỗng = mọi công ty), `n` mẫu, `thumb`, `created_ms` |
| `ds:face_tags` | hash | `<id>` → `"<tier>\t<công ty>\t<tên>"` (app đọc, khỏi parse JSON trong C) |
| `ds:face_emb` | hash | `<id>/<k>` → 512 float32 little-endian, đã chuẩn hoá L2 (tối đa 60 mẫu / người) |
| `ds:faces_ver` | string | tăng mỗi lần đổi gallery |

## 5. Dashboard

Tab **Khuôn mặt** (dashboard admin và từng công ty):

- **Đăng ký bằng ảnh / webcam**: 1–6 ảnh. Web (`web/faces.py`) tìm mặt bằng SCRFD (`det_500m.onnx`), tính vector
  bằng **đúng model và tiền xử lý của SGIE2**, mỗi ảnh 3 vector. Webcam cần mở dashboard qua https (link public) hoặc localhost.
- **Đăng ký từ camera**: chọn 1 hoặc vài ảnh trong "Mặt lạ camera vừa gặp" → đăng ký người mới, hoặc thêm làm mẫu
  cho người đã có. Vector do chính pipeline tính, khớp tốt nhất; không cần model ở web.
- **Người đã đăng ký**: lên / bỏ VIP, đổi tên, xoá.
- **Nhận diện gần đây** (24 giờ) và **thông báo góc phải** trên mọi trang (VIP màu vàng, giữ 15 s). Video live vẽ
  tên thay cho tracking ID, VIP có ★ và khung vàng; mặt có khung xanh lơ.

Phân quyền: tài khoản công ty chỉ thấy / sửa người của công ty mình, chỉ đăng ký từ mặt lạ ở camera của mình,
chỉ nhận thông báo của camera mình. Admin chọn công ty khi đăng ký ("Mọi công ty" = nhận ở mọi camera).

## 6. Cài đặt và chạy

```bash
# 1. Model (1 lần, cần Internet): YOLO11s -> ONNX + lib parse, FaceNet, InsightFace buffalo_s
./scripts/prepare_models.sh

# 2. Web: image mới có onnxruntime (đăng ký bằng ảnh), mount models/buffalo_s. Giữ nguyên link public
./scripts/web.sh rebuild
./scripts/master.sh restart            # agent mới biết bật pipeline khuôn mặt

# 3a. Từ dashboard master: tick "Nhận diện khuôn mặt + VIP", đặt ngưỡng cosine, Lưu & áp dụng
# 3b. Hoặc chạy tay (dừng app nền của master trước)
echo '{"action":"app_stop"}' > configs/.master/request.json; sleep 10
./scripts/run_face.sh -i configs/cameras_sim.txt -t 120 -r 135
```

Lần đầu nvinfer build engine cho từng tầng (YOLO theo số camera ~2–4 phút, FaceNet ~40 s, ArcFace ~1 phút) và lưu cạnh
file model. Log có dòng `[face] gallery: N người` và `[KHUÔN MẶT] <cam> người #<tid> = <tên>`; `[stats]` có thêm
`face: gallery=…, track=…, nhận ra=…`.

### Lưu ý vận hành (tránh "0/19 cam có hình")

- **Đổi số camera = build lại engine YOLO ~4–5 phút**, toàn bộ camera không có hình trong lúc đó. Engine lưu ở
  `models/yolo11s_engine/model_b<số cam>_gpu0_fp16.engine`; số camera đã từng chạy thì lần sau chỉ mất vài giây.
  Muốn đưa video test vào thì **đổi video của 1 camera có sẵn** thay vì thêm camera.
- **Trong lúc build, đừng bấm "Chạy lại app" / "Lưu & áp dụng"**: build bị huỷ, lần sau phải build lại từ đầu.
  Theo dõi ở log agent (tab Cấu hình) hoặc `ls models/yolo11s_engine/` (file mới xuất hiện là xong).
- Chạy lại app: agent dừng app bằng SIGINT (chờ 45 s), quá thì kill, rồi **chờ khoá `/tmp/w1-app-ds.lock` được nhả**
  mới chạy bản mới; app mới cũng tự chờ khoá tối đa 60 s. Nếu vẫn thấy "Đã có w1-app khác đang chạy": đợi 1–2 phút
  rồi bấm "Chạy lại app" một lần.
- Camera chạy `sample_cam5.mp4` (GOP dài) lên hình chậm hơn ~1 phút sau khi app chạy: bình thường.

## 7. Tinh chỉnh

| Muốn | Sửa |
|---|---|
| Ít nhận nhầm hơn | Tăng ngưỡng `-F` / ô "ngưỡng cosine" (0.42 → 0.5). Nhận ít người hơn |
| Nhận được người ở xa hơn | Giảm `input-object-min-*` của SGIE1/SGIE2 (mặt < 36 px thì vector kém tin cậy) |
| Đỡ GPU | Tăng `secondary-reinfer-interval` của SGIE1 (4 → 9), `interval=1` ở PGIE, hoặc fps xử lý của cam |
| Nhận tốt hơn cho 1 người | Thêm mẫu: nhiều ảnh nhiều góc, hoặc thêm ảnh từ camera thật ("Thêm làm mẫu cho người này") |

Ngưỡng mặc định **0.42**. Đo ngày 05/11 (gallery = 1 người đăng ký bằng ảnh, 19 camera, 200 s): **người khác** trên
16 camera video mẫu (có mặt rất gần) không lượt nào đạt 0.30; **chính người đó** trong video tự quay bằng điện thoại
(quay từ dưới lên, rất gần) đạt 0.31–0.33 ở lần khớp đầu và 0.46–0.49 khi chạy suốt đêm; ảnh đăng ký từ camera đạt
0.74–0.80. Khoảng 0.40–0.45 là an toàn; muốn nhận nhanh hơn thì thêm mẫu từ camera ("Thêm làm mẫu cho người này").
Nên đo lại trên camera thật trước khi dùng.

## 8. Đã kiểm thử (03/11/2026)

PGIE mẫu TrafficCamNet (person = class 2) + SGIE1 + SGIE2, video mẫu DeepStream, prefix Redis riêng `dsface`:

| Kiểm tra | Kết quả |
|---|---|
| Mặt chỉ được tìm trong bbox người, toạ độ đúng khung 1920x1080 (xem ảnh crop) | đạt |
| Mặt lạ ≥ 64 px → `faces_unknown`; đăng ký từ camera → chạy lại nhận ra (cosine 0.80) | đạt |
| Đăng ký bằng ảnh → camera nhận ra ở frame khác | đạt, cosine 0.69 (ngưỡng 0.40) |
| Cùng 1 khung hình: vector web vs vector pipeline | 0.82 (khung SCRFD thô: 0.55; đúng khung FaceNet: 0.91) |
| Tên có dấu `"` / tiếng Việt trong JSON sự kiện | đạt |
| Phân quyền công ty (xem, xoá, đăng ký mặt camera công ty khác, thông báo) | đạt |
| Thông báo trên dashboard, tên + ★VIP trên video | đạt |

**Chưa kiểm thử với YOLO11** (export cần chạy `prepare_models.sh`), và chưa đo tải GPU với 19–32 camera thật.

## 9. Giới hạn và việc còn lại

- **Model VIP chưa có.** Hiện VIP = hạng trong hồ sơ người đã đăng ký. Spec gửi team AI: classifier ảnh crop người,
  2 lớp theo thứ tự `normal;vip`, ONNX opset ≤ 17, batch động, kèm thông số tiền xử lý (xem
  `docs/NHAN_MODEL_TU_TEAM_AI.md`). Cần định nghĩa rõ "VIP nhìn thấy được" (đồng phục, thẻ, ...) trước khi gán nhãn.
- **Mặt không được căn chỉnh (alignment).** ArcFace chuẩn cần căn mặt theo 5 điểm mốc; DeepStream không làm sẵn, nên
  pipeline đưa thẳng crop vào model. Mặt nghiêng nhiều sẽ khớp kém. Muốn tốt hơn: thêm tầng landmark +
  `nvdspreprocess` tự viết, hoặc đổi sang model face recognition train trên mặt không căn.
- **Giấy phép.** InsightFace buffalo_s chỉ cho nghiên cứu, YOLO11 AGPL-3.0, FaceNet theo NVIDIA TAO EULA. Dùng thương
  mại phải thay model hoặc mua license.
- **Dữ liệu khuôn mặt là dữ liệu cá nhân nhạy cảm (NĐ 13/2023).** Cần sự đồng ý của người được đăng ký, thông báo
  có camera nhận diện, và quy định thời gian giữ ảnh. Ảnh mặt (`*_face_*`, `*_unk_*`) bị xoá cùng ảnh cảnh báo
  theo `-k` / `-K`; vector trong Redis giữ tới khi xoá người trên dashboard.
