"""Đăng ký khuôn mặt cho pipeline nhận diện (w1-app -F): ảnh -> vector ArcFace 512 chiều -> gallery trên Redis.

Gallery (w1-app đọc lại mỗi khi <prefix>:faces_ver đổi):
    <prefix>:faces      hash  <id> -> JSON {id, name, tier, company, created_ms, n, thumb, source}  (web hiển thị)
    <prefix>:face_tags  hash  <id> -> "<tier>\\t<công ty>\\t<tên>"                                  (w1-app đọc)
    <prefix>:face_emb   hash  <id>/<k> -> 512 float32 little-endian, đã chuẩn hoá L2                (w1-app đọc)
    <prefix>:faces_ver  số tăng mỗi lần đổi gallery

Vector phải cùng "không gian" với vector pipeline tính ở SGIE2 (configs/sgie2_face_embed.txt): cùng model
w600k_mbf.onnx, crop bbox mặt kéo thẳng về 112x112, RGB, (x - 127.5) / 127.5. Bbox mặt ở pipeline do FaceNet
tìm, ở đây do SCRFD (det_500m.onnx, cùng bộ buffalo_s) tìm. SCRFD cắt cao hơn (thêm trán), mà ArcFace không căn
mặt rất nhạy với khung: cùng 1 khung hình, vector trên khung SCRFD chỉ khớp vector pipeline cosine 0.55, trên khung
đã chỉnh giống FaceNet (đỉnh dời xuống 10%, cao x0.9) là 0.82 (đúng khung FaceNet: 0.91). Nên mỗi ảnh lấy 3 vector
ở 3 khung quanh mức chỉnh đó (FACENET_LIKE), pipeline khớp với vector gần nhất.
Đăng ký từ camera (mặt lạ trong <prefix>:faces_unknown) dùng thẳng vector pipeline đã tính, không cần model.
"""
import base64
import io
import json
import os
import re
import secrets
import threading
import time

import numpy as np

MODELS = os.getenv("FACE_MODELS", "/models")
DET_MODEL = os.path.join(MODELS, "det_500m.onnx")
EMB_MODEL = os.path.join(MODELS, "w600k_mbf.onnx")
DIM = 512
FACENET_LIKE = ((0.10, 0.90), (0.06, 0.94), (0.14, 0.86))   # (dời đỉnh xuống, tỉ lệ cao) theo chiều cao SCRFD
DET_SIZE = 640
DET_THRESH = 0.5
MAX_SAMPLES = 60             # tối đa vector / người
TIERS = ("vip", "normal")
ID_RE = re.compile(r"^[a-f0-9]{12}$")

_lock = threading.Lock()
_sess = {}


class FaceError(Exception):
    pass


def models_ready():
    return os.path.isfile(DET_MODEL) and os.path.isfile(EMB_MODEL)


def _session(path):
    with _lock:
        if path not in _sess:
            import onnxruntime as ort    # chỉ cần khi đăng ký bằng ảnh
            opt = ort.SessionOptions()
            opt.intra_op_num_threads = 2
            _sess[path] = ort.InferenceSession(path, opt, providers=["CPUExecutionProvider"])
        return _sess[path]


def _nms(boxes, scores, thr=0.4):
    order, keep = scores.argsort()[::-1], []
    while order.size:
        i = order[0]
        keep.append(i)
        xx1 = np.maximum(boxes[i, 0], boxes[order[1:], 0])
        yy1 = np.maximum(boxes[i, 1], boxes[order[1:], 1])
        xx2 = np.minimum(boxes[i, 2], boxes[order[1:], 2])
        yy2 = np.minimum(boxes[i, 3], boxes[order[1:], 3])
        inter = np.maximum(0, xx2 - xx1) * np.maximum(0, yy2 - yy1)
        area = lambda b: (b[..., 2] - b[..., 0]) * (b[..., 3] - b[..., 1])
        iou = inter / (area(boxes[i]) + area(boxes[order[1:]]) - inter + 1e-9)
        order = order[1:][iou < thr]
    return keep


def detect(img):
    """SCRFD: ảnh PIL RGB -> [(x1, y1, x2, y2, score)] theo pixel ảnh gốc, điểm cao trước."""
    w, h = img.size
    s = DET_SIZE / max(w, h)
    canvas = np.zeros((DET_SIZE, DET_SIZE, 3), np.float32)     # letterbox góc trên trái như InsightFace
    canvas[:round(h * s), :round(w * s)] = np.asarray(img.resize((round(w * s), round(h * s))), np.float32)
    blob = ((canvas - 127.5) / 128.0).transpose(2, 0, 1)[None]
    sess = _session(DET_MODEL)
    outs = [o[0] if o.ndim == 3 else o for o in sess.run(None, {sess.get_inputs()[0].name: blob})]
    # 3 stride x (score [N,1], bbox [N,4], kps [N,10]); stride nhỏ có nhiều anchor nhất
    by_kind = {k: sorted([o for o in outs if o.shape[1] == k], key=len, reverse=True) for k in (1, 4)}
    boxes, scores = [], []
    for stride, sc, bb in zip((8, 16, 32), by_kind[1], by_kind[4]):
        n = DET_SIZE // stride
        cy, cx = np.mgrid[:n, :n]
        centers = np.repeat(np.stack([cx.ravel(), cy.ravel()], 1) * stride, len(sc) // (n * n), axis=0)
        ok = sc[:, 0] >= DET_THRESH
        d = bb[ok] * stride
        c = centers[ok]
        boxes.append(np.stack([c[:, 0] - d[:, 0], c[:, 1] - d[:, 1], c[:, 0] + d[:, 2], c[:, 1] + d[:, 3]], 1))
        scores.append(sc[ok, 0])
    boxes, scores = np.concatenate(boxes) / s, np.concatenate(scores)
    return [(*boxes[i], float(scores[i])) for i in _nms(boxes, scores)]


def facenet_like(box, dy, sh):
    """Khung SCRFD -> khung gần với khung FaceNet của pipeline (xem đầu file)."""
    x1, y1, x2, y2 = box[:4]
    return x1, y1 + dy * (y2 - y1), x2, y1 + (dy + sh) * (y2 - y1)


def embed(img, box):
    """Crop bbox mặt, kéo thẳng về 112x112 như nvinfer (maintain-aspect-ratio=0, bilinear) -> vector L2."""
    crop = img.crop(tuple(round(v) for v in box[:4]))
    x = (np.asarray(crop.resize((112, 112), resample=2), np.float32) - 127.5) / 127.5   # 2 = bilinear
    sess = _session(EMB_MODEL)
    v = sess.run(None, {sess.get_inputs()[0].name: x.transpose(2, 0, 1)[None]})[0][0]
    return (v / np.linalg.norm(v)).astype("<f4")


def decode_image(data_url):
    from PIL import Image, ImageOps
    try:
        raw = base64.b64decode(data_url.split(",", 1)[-1], validate=False)
        img = ImageOps.exif_transpose(Image.open(io.BytesIO(raw))).convert("RGB")
    except Exception:
        raise FaceError("Không đọc được ảnh (JPEG/PNG)")
    if max(img.size) > 1920:
        img.thumbnail((1920, 1920))
    return img


def thumb_of(img, box=None):
    """Ảnh nhỏ 96 px (JPEG data URL) lưu kèm hồ sơ để danh sách hiển thị không cần file."""
    if box is not None:
        x1, y1, x2, y2 = box[:4]
        m = (x2 - x1) * 0.25
        img = img.crop((round(x1 - m), round(y1 - m), round(x2 + m), round(y2 + m)))
    img = img.copy()
    img.thumbnail((96, 96))
    buf = io.BytesIO()
    img.save(buf, "JPEG", quality=80)
    return "data:image/jpeg;base64," + base64.b64encode(buf.getvalue()).decode()


def vectors_from_photos(images):
    """Mỗi ảnh: mặt to nhất -> 3 vector (3 khung FACENET_LIKE). Trả về (vectors, thumb)."""
    if not models_ready():
        raise FaceError("Web chưa có model khuôn mặt (models/buffalo_s). Chạy ./scripts/prepare_models.sh "
                        "rồi ./scripts/web.sh rebuild, hoặc đăng ký từ camera")
    vecs, thumb = [], None
    for i, data in enumerate(images, 1):
        img = decode_image(data)
        faces = detect(img)
        if not faces:
            raise FaceError(f"Ảnh {i}: không thấy khuôn mặt nào")
        box = max(faces, key=lambda f: (f[2] - f[0]) * (f[3] - f[1]))
        if box[3] - box[1] < 40:
            raise FaceError(f"Ảnh {i}: mặt quá nhỏ ({int(box[3] - box[1])} px), chụp gần hơn")
        vecs += [embed(img, facenet_like(box, dy, sh)) for dy, sh in FACENET_LIKE]
        thumb = thumb or thumb_of(img, box)
    return vecs, thumb


class Gallery:
    def __init__(self, r, prefix):
        self.r, self.p = r, prefix

    def key(self, name):
        return f"{self.p}:{name}"

    def list(self, company=None):
        """company None = tất cả (admin); khác None: người của công ty đó."""
        out = []
        for raw in self.r.hvals(self.key("faces")):
            try:
                f = json.loads(raw)
            except ValueError:
                continue
            if company is None or f.get("company") == company:
                out.append(f)
        return sorted(out, key=lambda f: -f.get("created_ms", 0))

    def get(self, pid):
        raw = self.r.hget(self.key("faces"), pid) if ID_RE.match(pid or "") else None
        return json.loads(raw) if raw else None

    def _sample_keys(self, pid):
        return [k for k in self.r.hkeys(self.key("face_emb")) if k.startswith(pid + "/")]

    def save(self, name, tier, company, vecs, thumb, source, pid=None):
        """Tạo người mới (pid None) hoặc thêm vector cho người đã có."""
        if pid:
            f = self.get(pid)
            if not f:
                raise FaceError("Không có người này")
            used = {int(k.split("/")[1]) for k in self._sample_keys(pid)}
        else:
            pid, used = secrets.token_hex(6), set()
            f = {"id": pid, "name": name, "tier": tier, "company": company or "", "created_ms": int(time.time() * 1000),
                 "thumb": thumb, "source": source}
        if len(used) + len(vecs) > MAX_SAMPLES:
            raise FaceError(f"Mỗi người tối đa {MAX_SAMPLES} mẫu")
        free = (k for k in range(MAX_SAMPLES) if k not in used)
        p = self.r.pipeline()
        for v in vecs:
            p.hset(self.key("face_emb"), f"{pid}/{next(free)}", np.asarray(v, "<f4").tobytes())
        f["n"] = len(used) + len(vecs)
        f["thumb"] = f.get("thumb") or thumb
        p.hset(self.key("faces"), pid, json.dumps(f, ensure_ascii=False))
        p.hset(self.key("face_tags"), pid, f"{f['tier']}\t{f['company']}\t{f['name']}")
        p.incr(self.key("faces_ver"))
        p.execute()
        return f

    def update(self, pid, name, tier):
        f = self.get(pid)
        if not f:
            raise FaceError("Không có người này")
        f.update(name=name, tier=tier)
        p = self.r.pipeline()
        p.hset(self.key("faces"), pid, json.dumps(f, ensure_ascii=False))
        p.hset(self.key("face_tags"), pid, f"{f['tier']}\t{f['company']}\t{f['name']}")
        p.incr(self.key("faces_ver"))
        p.execute()
        return f

    def delete(self, pid):
        keys = self._sample_keys(pid)
        p = self.r.pipeline()
        if keys:
            p.hdel(self.key("face_emb"), *keys)
        p.hdel(self.key("faces"), pid)
        p.hdel(self.key("face_tags"), pid)
        p.incr(self.key("faces_ver"))
        p.execute()

    def unknown(self, msg_id):
        """1 mặt lạ do w1-app gửi (<prefix>:faces_unknown): dict có cam, face_crop, emb (base64 float32)."""
        got = self.r.xrange(self.key("faces_unknown"), msg_id, msg_id) if re.match(r"^\d+-\d+$", msg_id or "") else []
        if not got:
            raise FaceError("Mặt này đã quá cũ hoặc không còn")
        f = got[0][1]
        d = json.loads(f.get("data", "{}"))
        v = np.frombuffer(base64.b64decode(d.get("emb", "")), "<f4")
        if v.size != DIM:
            raise FaceError("Dữ liệu vector hỏng")
        d["cam"] = f.get("cam")
        d["vec"] = v / np.linalg.norm(v)
        return d
