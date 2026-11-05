"""Platform demo: xem kết quả DeepStream trên web thay vì đọc log.

Đọc Redis Streams của w1-app và thư mục outputs/ (chỉ ghi gallery khuôn mặt, xem faces.py):
    ds:frames   -> trạng thái live từng cam (fps, số object, bbox hiện tại)
    ds:events   -> danh sách cảnh báo (ghép dwell_alert + dwell_end + clip_ready + clip_annotated)
    ds:cameras  -> danh sách cam
    ds:companies, ds:buildings -> cam thuộc công ty / toà nào (w1-app chạy với -i)
    /outputs    -> ảnh snapshot, clip MP4 (phục vụ qua /media, có hỗ trợ tua video)
    MediaMTX    -> video live HLS của từng cam (proxy qua /hls, dùng chung đăng nhập)
    ds:faces*   -> gallery khuôn mặt (đăng ký / sửa / xoá ở tab "Khuôn mặt"), ds:faces_unknown = mặt lạ từ camera

Mỗi công ty 1 dashboard riêng ở /c/<công ty>/: chỉ thấy cam, cảnh báo, ảnh/clip, video live của mình,
đăng nhập bằng tài khoản riêng trong web/companies.auth (dòng "<công ty> <user> <mật khẩu>").
/ là trang admin xem tất cả (tài khoản WEB_USER/WEB_PASS), admin cũng vào được mọi /c/<công ty>/.
Chạy với COMPANY=<công ty>: web chỉ của 1 công ty, dashboard nằm ngay ở / (mỗi công ty 1 container + 1 link
public riêng, xem ./scripts/web.sh publish), không có trang admin và /c/...

Dashboard master (tab "Cấu hình" ở trang admin, cần MASTER_CONFIGS = thư mục configs/ mount ghi được):
sửa công ty / toà / camera (tên, địa chỉ, tầng, khu vực, video, fps xử lý, giây bắt đầu) trong configs/sites.json, rồi gửi yêu cầu cho agent (master/agent.py,
container ds_agent có quyền docker) qua configs/.master/request.json. Web không chạy lệnh nào.

Chạy: ./scripts/web.sh start  ->  http://127.0.0.1:8095   (link từng công ty: ./scripts/web.sh links)
"""
import asyncio
import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import threading
import time
from collections import defaultdict, deque

import httpx
import redis
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask

import faces

REDIS_HOST, REDIS_PORT = os.getenv("REDIS", "127.0.0.1:6390").rsplit(":", 1)
PREFIX = os.getenv("PREFIX", "ds")
OUTPUTS = os.getenv("OUTPUTS", "/outputs")
STATIC = os.path.join(os.path.dirname(__file__), "static")
LIVE_TIMEOUT_S = 5      # không có frame quá 5 s thì coi cam là mất hình
EVENT_SCAN = 5000       # số sự kiện gần nhất đọc để dựng danh sách cảnh báo
FRAME_BUF_MS = 20000    # giữ 20 s metadata mỗi cam để khớp bbox với video HLS (trễ vài giây)
HLS_UPSTREAM = os.getenv("HLS_UPSTREAM", "http://127.0.0.1:8888")
# Cam có URI dạng rtsp://127.0.0.1:8554/<path> (MediaMTX) thì xem live được qua HLS cùng <path>
HLS_RTSP_PREFIXES = tuple(os.getenv("HLS_RTSP_PREFIXES",
                                    "rtsp://127.0.0.1:8554/,rtsp://localhost:8554/").split(","))

r = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), password=os.getenv("REDIS_PASSWORD"),
                decode_responses=True, socket_timeout=5)
app = FastAPI(title="DeepStream platform demo")


def basic(user, pw):
    return "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode()


# Đăng nhập Basic Auth (bắt buộc khi public ra Internet):
#   admin   = WEB_USER + WEB_PASS: vào được mọi trang
#   công ty = web/companies.auth: chỉ vào được /c/<công ty>/
WEB_USER, WEB_PASS = os.getenv("WEB_USER", ""), os.getenv("WEB_PASS", "")
ADMIN_AUTH = basic(WEB_USER, WEB_PASS) if WEB_USER and WEB_PASS else None
COMPANIES_AUTH = os.getenv("COMPANIES_AUTH", os.path.join(os.path.dirname(__file__), "companies.auth"))
COMPANY_RE = re.compile(r"^/c/([A-Za-z0-9_]+)(/|$)")
COMPANY = os.getenv("COMPANY") or None   # web riêng của 1 công ty
_company_auth = {"mtime": None, "creds": {}}


def company_creds():
    """công ty -> header Basic; đọc lại khi file đổi (web.sh links thêm công ty mới không cần restart)."""
    try:
        mtime = os.path.getmtime(COMPANIES_AUTH)
    except OSError:
        return {}
    if mtime != _company_auth["mtime"]:
        creds = {}
        with open(COMPANIES_AUTH, encoding="utf-8") as f:
            for line in f:
                parts = line.split()
                if len(parts) == 3 and not parts[0].startswith("#"):
                    creds[parts[0]] = basic(parts[1], parts[2])
        _company_auth.update(mtime=mtime, creds=creds)
    return _company_auth["creds"]


def need_login(realm):
    return Response("Cần đăng nhập".encode(), status_code=401,
                    headers={"WWW-Authenticate": f'Basic realm="{realm}", charset="UTF-8"'})


@app.middleware("http")
async def basic_auth(request: Request, call_next):
    path = request.url.path
    creds = company_creds()
    # /static chỉ có hls.min.js, không có dữ liệu. Chưa đặt tài khoản nào = chạy local, không bắt đăng nhập
    # (web riêng của 1 công ty luôn bắt đăng nhập vì dùng để public)
    if path.startswith("/static/") or (not COMPANY and not ADMIN_AUTH and not creds):
        return await call_next(request)
    got = request.headers.get("authorization", "").encode()
    if ADMIN_AUTH and secrets.compare_digest(got, ADMIN_AUTH.encode()):
        return await call_next(request)
    m = COMPANY_RE.match(path)
    co = COMPANY or (m.group(1) if m else None)
    if not co:
        return need_login("DeepStream admin")
    want = creds.get(co)
    if not want or not secrets.compare_digest(got, want.encode()):
        # realm riêng mỗi công ty: trình duyệt hỏi lại khi sang dashboard công ty khác
        return need_login(f"Dashboard {co}")
    return await call_next(request)


def slim_obj(o):
    """Bản rút gọn 1 object để vẽ lên video: thêm tên / hạng khách (người) và gie (mặt) khi có."""
    s = {"label": o.get("label"), "tid": o.get("tid"), "bbox": [round(v) for v in o.get("bbox", [])]}
    if o.get("gie"):
        s["gie"] = o["gie"]
    if o.get("tier"):
        s["tier"] = o["tier"]
    if o.get("face"):
        s["name"] = o["face"].get("name")
    return s


class Live:
    """Thread đọc đuôi ds:frames: frame mới nhất + fps của từng cam, và bbox FRAME_BUF_MS gần nhất.

    Mỗi frame chỉ parse JSON 1 lần ở đây và rút gọn (label, tid, bbox số nguyên), để mọi người xem
    (WebSocket api/ws) dùng chung, không ai phải đọc Redis hay parse lại."""

    def __init__(self):
        self.lock = threading.Lock()
        self.last = {}                            # cam -> data JSON frame mới nhất
        self.seen_at = {}                         # cam -> time.time() lúc nhận frame cuối
        self.arrivals = defaultdict(deque)        # cam -> thời điểm nhận các frame trong 5 s
        self.buf = defaultdict(deque)             # cam -> {"ts", "objs"} rút gọn trong FRAME_BUF_MS gần nhất
        self.redis_ok = False

    def run(self):
        last_id = "$"
        while True:
            try:
                resp = r.xread({f"{PREFIX}:frames": last_id}, count=2000, block=1000)
                self.redis_ok = True
            except redis.RedisError:
                self.redis_ok = False
                time.sleep(2)
                continue
            now = time.time()
            newest = {}
            for _, msgs in resp or []:
                for msg_id, f in msgs:
                    last_id = msg_id
                    cam = f.get("cam", "?")
                    try:
                        d = json.loads(f["data"])
                    except (KeyError, ValueError):
                        continue
                    newest[cam] = d
                    self.arrivals[cam].append(now)
                    ts = int(f.get("ts", 0))
                    slim = {"ts": ts, "objs": [slim_obj(o) for o in d.get("objs", [])]}
                    with self.lock:
                        q = self.buf[cam]
                        q.append(slim)
                        while q and q[0]["ts"] < ts - FRAME_BUF_MS:
                            q.popleft()
            with self.lock:
                for cam, d in newest.items():
                    self.last[cam] = d
                    self.seen_at[cam] = now
                for q in self.arrivals.values():
                    while q and q[0] < now - LIVE_TIMEOUT_S:
                        q.popleft()

    def frames_since(self, cam, since_ms):
        with self.lock:
            return [x for x in self.buf.get(cam, ()) if x["ts"] > since_ms]

    def snapshot(self):
        now = time.time()
        with self.lock:
            out = {}
            for cam, data in self.last.items():
                q = self.arrivals[cam]
                while q and q[0] < now - LIVE_TIMEOUT_S:
                    q.popleft()
                age = now - self.seen_at[cam]
                out[cam] = {"fps": round(len(q) / LIVE_TIMEOUT_S, 1), "age_s": round(age, 1),
                            "online": age < LIVE_TIMEOUT_S, "data": data}
            return out


live = Live()
threading.Thread(target=live.run, daemon=True).start()


_cam_meta = {"at": 0.0, "companies": {}, "buildings": {}, "cameras": {}, "info": {}}


def cam_meta():
    """ds:companies, ds:buildings, ds:cameras, ds:camera_info (cache 3 s)."""
    if time.time() - _cam_meta["at"] > 3:
        try:
            info = {}
            for cam, raw in r.hgetall(f"{PREFIX}:camera_info").items():
                try:
                    info[cam] = json.loads(raw)
                except ValueError:
                    pass
            _cam_meta.update(companies=r.hgetall(f"{PREFIX}:companies"),
                             buildings=r.hgetall(f"{PREFIX}:buildings"),
                             cameras=r.hgetall(f"{PREFIX}:cameras"), info=info, at=time.time())
        except redis.RedisError:
            pass
    return _cam_meta


def cams_of(company):
    """Tập cam của 1 công ty; None = admin, thấy tất cả."""
    company = scope(company)
    if company is None:
        return None
    return {cam for cam, co in cam_meta()["companies"].items() if co == company}


def scope(company):
    """Công ty của request: web riêng (COMPANY) thì luôn là công ty đó, bỏ qua tham số."""
    return COMPANY or company


def check_company(company):
    company = scope(company)
    if company is not None and company not in set(cam_meta()["companies"].values()) | set(company_creds()):
        raise HTTPException(404, "Không có công ty này")


def media_url(path):
    """/outputs/w1_clips/x.mp4 -> media/w1_clips/x.mp4 (None nếu không có file).
    Đường dẫn tương đối: ở /c/<công ty>/ thành /c/<công ty>/media/... (có kiểm tra quyền)."""
    if not path or not path.startswith(OUTPUTS + "/"):
        return None
    if not os.path.isfile(path):
        return None
    return "media/" + path[len(OUTPUTS) + 1:]


def read_events(count, allowed=None):
    """Sự kiện mới nhất trước, dạng dict (type, cam, ts, data). allowed: chỉ lấy cam trong tập này."""
    out = []
    for msg_id, f in r.xrevrange(f"{PREFIX}:events", count=count):
        if allowed is not None and f.get("cam") not in allowed:
            continue
        try:
            data = json.loads(f.get("data", "{}"))
        except ValueError:
            data = {}
        out.append({"id": msg_id, "type": f.get("type"), "cam": f.get("cam"),
                    "ts": int(f.get("ts", 0)), "data": data})
    return out


def hls_path(uri):
    for prefix in HLS_RTSP_PREFIXES:
        if prefix and uri.startswith(prefix):
            return uri[len(prefix):]
    return None


def hls_url(uri):
    p = hls_path(uri)
    return f"hls/{p}/index.m3u8" if p else None


def cam_sort_key(cam):
    digits = "".join(ch for ch in cam if ch.isdigit())
    return (cam.rstrip("0123456789"), int(digits) if digits else 0)


def is_admin(request: Request):
    if not ADMIN_AUTH:
        return not COMPANY and not company_creds()    # chạy local không đăng nhập = admin
    return secrets.compare_digest(request.headers.get("authorization", "").encode(), ADMIN_AUTH.encode())


def state(request: Request, company: str | None = None):
    """Tổng quan + trạng thái từng cam. Frontend gọi mỗi 1 s."""
    # Admin xem dashboard 1 công ty từ master (/c/<công ty>/): hiện nút quay về master.
    # Link public của công ty (web riêng COMPANY) và tài khoản công ty thì không
    back = not COMPANY and company is not None and is_admin(request)
    check_company(company)
    try:
        cams = r.hgetall(f"{PREFIX}:cameras")
        n_frames = r.xlen(f"{PREFIX}:frames")
        n_events = r.xlen(f"{PREFIX}:events")
        redis_ok = True
    except redis.RedisError:
        cams, n_frames, n_events, redis_ok = {}, 0, 0, False
    meta = cam_meta()
    allowed = cams_of(company)
    lv = live.snapshot()
    rows = []
    names = set(cams) | set(lv)
    if allowed is not None:
        names &= allowed
    order = lambda c: (meta["companies"].get(c, ""), meta["buildings"].get(c, ""), cam_sort_key(c))
    for cam in sorted(names, key=order):
        s = lv.get(cam, {"fps": 0, "age_s": None, "online": False, "data": None})
        d = s["data"] or {}
        objs = d.get("objs", [])
        rows.append({"cam": cam, "company": meta["companies"].get(cam), "building": meta["buildings"].get(cam),
                     "info": meta["info"].get(cam, {}),
                     "uri": cams.get(cam, "") if allowed is None else "", "hls": hls_url(cams.get(cam, "")),
                     "online": s["online"], "fps": s["fps"],
                     "age_s": s["age_s"], "num_obj": d.get("num_obj", 0),
                     "persons": sum(1 for o in objs if o.get("label") == "person" and not o.get("gie")),
                     "vips": sum(1 for o in objs if o.get("tier") == "vip"),
                     "w": d.get("w", 1920), "h": d.get("h", 1080),
                     "ts": d.get("ts_ms"), "objs": objs if s["online"] else []})
    # Dashboard công ty không lộ thông tin hạ tầng chung (Redis, tổng stream, URI camera)
    company = scope(company)
    admin = company is None
    companies = sorted(set(meta["companies"].values())) if admin else []
    company_name = next((i.get("company_name") for i in meta["info"].values()
                         if company and i.get("company_id") == company), company)
    return {"company": company, "company_name": company_name, "companies": companies, "ws_token": ws_token(company),
            "master": bool(MASTER) and admin, "back": back,
            "redis": redis_ok,
            "redis_addr": f"{REDIS_HOST}:{REDIS_PORT}" if admin else "",
            "frames_len": n_frames if admin else None, "events_len": n_events if admin else None,
            "cams_online": sum(1 for x in rows if x["online"]), "cams_total": len(rows),
            "fps_total": round(sum(x["fps"] for x in rows), 1), "cameras": rows}


def alerts(limit: int = 100, company: str | None = None):
    """Ghép các sự kiện của một lượt ở yên thành 1 bản ghi cảnh báo."""
    check_company(company)
    events = read_events(EVENT_SCAN, cams_of(company))
    clips, annotated, ends, resumes = {}, {}, {}, {}
    for e in events:
        d = e["data"]
        if e["type"] == "clip_ready":
            clips.setdefault(d.get("tag"), d)
        elif e["type"] == "clip_annotated":
            annotated.setdefault(d.get("tag"), d)
        elif e["type"] == "dwell_end":
            ends.setdefault((e["cam"], d.get("tid"), d.get("start_ms")), d)
        elif e["type"] == "dwell_resume":
            # Người bị che rồi hiện lại với tracking ID mới: lượt ở yên vẫn là (cam, tid, start_ms) cũ
            resumes.setdefault((e["cam"], d.get("tid"), d.get("start_ms")), []).append(d.get("track_tid"))
    out = []
    for e in events:
        if e["type"] != "dwell_alert":
            continue
        d = e["data"]
        end = ends.get((e["cam"], d.get("tid"), d.get("start_ms")))
        clip_ref = d.get("clip") or ""
        tag = clip_ref[len("pending:"):] if clip_ref.startswith("pending:") else None
        snap = d.get("snapshot")
        crop = snap.replace("_frame.jpg", "_crop.jpg") if snap else None
        clip = clips.get(tag) if tag else None
        ann = annotated.get(tag) if tag else None
        out.append({
            "cam": e["cam"], "tid": d.get("tid"), "start_ms": d.get("start_ms"),
            "tids": [d.get("tid")] + resumes.get((e["cam"], d.get("tid"), d.get("start_ms")), []),
            "alert_ms": e["ts"], "dwell_s": d.get("dwell_s"), "bbox": d.get("bbox"),
            "end_ms": d["start_ms"] + int(end["dwell_s"] * 1000) if end and d.get("start_ms") else None,
            "total_s": end.get("dwell_s") if end else None,
            "end_reason": end.get("reason") if end else None,
            "snapshot": media_url(snap), "crop": media_url(crop),
            "clip": media_url(clip["file"]) if clip else None,
            "clip_s": round(clip["duration_ms"] / 1000, 1) if clip else None,
            "clip_bbox": media_url(ann["file"]) if ann else None,
            "clip_state": "bbox" if ann else "raw" if clip else "pending" if tag else "none",
        })
        if len(out) >= limit:
            break
    return {"alerts": out}


def events(limit: int = 200, company: str | None = None):
    check_company(company)
    allowed = cams_of(company)
    # Công ty: quét rộng hơn rồi lọc, vì sự kiện của các công ty nằm chung 1 stream
    evs = read_events(min(limit, 1000) if allowed is None else EVENT_SCAN, allowed)[:min(limit, 1000)]
    if allowed is not None:
        for e in evs:
            e["data"].pop("uri", None)   # URI RTSP có thể chứa mật khẩu camera
    return {"events": evs}


def frames(cam: str, since: int = 0, company: str | None = None):
    """Metadata (bbox) của 1 cam sau mốc since (epoch ms), để vẽ đè lên video live."""
    allowed = cams_of(company)
    if allowed is not None and cam not in allowed:
        raise HTTPException(404)
    return {"frames": live.frames_since(cam, since)}


hls_client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=3.0))


# ---------------- bbox qua WebSocket ----------------
# Mỗi trang mở 1 WebSocket cho mọi cam đang xem, server đẩy frame mới mỗi 250 ms (thay vì trình duyệt hỏi
# lại vài lần mỗi giây cho từng cam). Không dùng SSE vì quick tunnel trycloudflare giữ lại cả luồng SSE.
# WebSocket không đi qua middleware Basic Auth -> api/state (đã đăng nhập) cấp token ngắn hạn gắn với
# công ty, WebSocket tự kiểm tra token.
WS_KEY = secrets.token_bytes(32)
WS_TOKEN_TTL = 600


def ws_token(company):
    exp = int(time.time()) + WS_TOKEN_TTL
    msg = f"{company or '*'}.{exp}"
    return msg + "." + hmac.new(WS_KEY, msg.encode(), hashlib.sha256).hexdigest()[:32]


def ws_token_ok(token, company):
    try:
        sc, exp, sig = token.rsplit(".", 2)
    except ValueError:
        return False
    want = hmac.new(WS_KEY, f"{sc}.{exp}".encode(), hashlib.sha256).hexdigest()[:32]
    # token admin ("*") dùng được mọi nơi; token công ty chỉ dùng cho đúng công ty đó
    return (hmac.compare_digest(sig, want) and exp.isdigit() and int(exp) > time.time()
            and sc in ("*", company or "*"))


async def meta_ws(ws: WebSocket, company: str | None = None):
    """Query: token, cams=a,b, fps (ô nhỏ 10, trình phát lớn 30). Gửi {cam: [frame, ...]} các frame mới,
    giãn còn tối đa `fps` frame/giây mỗi cam. Đổi tập cam thì trình duyệt mở kết nối mới."""
    q = ws.query_params
    company = scope(company)
    if not ws_token_ok(q.get("token", ""), company):
        await ws.close(code=4401)
        return
    allowed = cams_of(company)
    names = [c for c in dict.fromkeys(q.get("cams", "").split(","))
             if c and (allowed is None or c in allowed)][:64]
    try:
        fps = float(q.get("fps", 10))
    except ValueError:
        fps = 10.0
    gap_ms = 1000 / max(1.0, min(fps, 30.0)) * 0.9
    await ws.accept()
    # Gói đầu: 8 s gần nhất (đủ bù độ trễ HLS), không phải cả bộ đệm 20 s
    start = int(time.time() * 1000) - 8000
    since, emitted = {c: start for c in names}, {c: 0 for c in names}
    try:
        while True:
            out = {}
            for c in names:
                frs = live.frames_since(c, since[c])
                if not frs:
                    continue
                since[c] = frs[-1]["ts"]
                keep = []
                for f in frs:
                    if f["ts"] - emitted[c] >= gap_ms:
                        keep.append(f)
                        emitted[c] = f["ts"]
                if keep:
                    out[c] = keep
            if out:
                await ws.send_text(json.dumps(out, separators=(",", ":")))
            await asyncio.sleep(0.25)
    except (WebSocketDisconnect, RuntimeError):
        pass


async def hls(path: str, request: Request, company: str | None = None):
    """Proxy HLS của MediaMTX (chỉ nghe 127.0.0.1) để đi chung đăng nhập và chung tunnel với web."""
    allowed = cams_of(company)
    if allowed is not None:
        cameras = cam_meta()["cameras"]
        mine = [p for p in (hls_path(cameras.get(c, "")) for c in allowed) if p]
        if not any(path.startswith(p + "/") for p in mine):
            raise HTTPException(404)
    try:
        up = await hls_client.send(
            hls_client.build_request("GET", f"{HLS_UPSTREAM}/{path}", params=request.query_params), stream=True)
    except httpx.HTTPError:
        return Response("Không có luồng HLS (MediaMTX chưa chạy?)".encode(), status_code=502)
    headers = {k: v for k, v in up.headers.items() if k.lower() in ("content-type", "cache-control")}
    return StreamingResponse(up.aiter_raw(), status_code=up.status_code, headers=headers,
                             background=BackgroundTask(up.aclose))


def media(path: str, company: str | None = None):
    """Ảnh/clip trong outputs/. Công ty chỉ lấy được file của cam mình (tên file bắt đầu bằng <cam>_)."""
    root = os.path.realpath(OUTPUTS)
    full = os.path.realpath(os.path.join(root, path))
    if not full.startswith(root + os.sep) or not os.path.isfile(full):
        raise HTTPException(404)
    allowed = cams_of(company)
    if allowed is not None:
        # Tên file = <cam>_...; camera_id có thể chứa "_" nên lấy cam dài nhất khớp tiền tố (vd. "a" và "a_b")
        name, meta = os.path.basename(full), cam_meta()
        owner = max((c for c in set(meta["cameras"]) | set(meta["companies"]) if name.startswith(c + "_")),
                    key=len, default=None)
        if owner not in allowed:
            raise HTTPException(404)
    return FileResponse(full)   # có hỗ trợ Range (tua video)


# ---------------- khuôn mặt: đăng ký, mặt lạ từ camera, thông báo nhận diện ----------------
# Admin đăng ký được cho mọi công ty (công ty rỗng = nhận ở mọi camera); tài khoản công ty chỉ thấy / sửa
# người của công ty mình. Ghi Redis (gallery), w1-app tự đọc lại sau ~3 s, không cần chạy lại app.
gallery = faces.Gallery(r, PREFIX)
FACE_EVENTS = ("face_recognized", "vip_detected")
MAX_PHOTOS = 6
MAX_PHOTO_CHARS = 8_000_000      # data URL ~6 MB ảnh


def face_fields(body):
    name, tier = body.get("name"), body.get("tier", "normal")
    if not isinstance(name, str) or not name.strip() or len(name) > 80 or any(ord(ch) < 32 for ch in name):
        raise HTTPException(400, "Tên: 1-80 ký tự, không xuống dòng")
    if tier not in faces.TIERS:
        raise HTTPException(400, "Hạng khách: vip hoặc normal")
    return name.strip(), tier


def face_owned(pid, company):
    f = gallery.get(pid)
    if not f or (scope(company) is not None and f.get("company") != scope(company)):
        raise HTTPException(404, "Không có người này")
    return f


def faces_get(company: str | None = None):
    check_company(company)
    co = scope(company)
    return {"people": gallery.list(co), "models": faces.models_ready(),
            "companies": sorted(set(cam_meta()["companies"].values())) if co is None else []}


def faces_post(body: dict, request: Request, company: str | None = None):
    """Thêm người (hoặc thêm mẫu cho person_id): từ ảnh (images: data URL) hoặc mặt lạ (unknown: id stream)."""
    require_xhr(request)
    check_company(company)
    co, allowed = scope(company), cams_of(company)
    images, unknown, pid = body.get("images") or [], body.get("unknown") or [], body.get("person_id")
    if not isinstance(images, list) or not isinstance(unknown, list) or not (images or unknown):
        raise HTTPException(400, "Cần ít nhất 1 ảnh, hoặc 1 mặt chụp từ camera")
    if len(images) > MAX_PHOTOS or len(unknown) > 20 or any(not isinstance(x, str) or len(x) > MAX_PHOTO_CHARS for x in images):
        raise HTTPException(400, f"Tối đa {MAX_PHOTOS} ảnh, mỗi ảnh ~6 MB")
    if pid:
        f = face_owned(pid, company)
        name, tier, owner = f["name"], f["tier"], f["company"]
    else:
        name, tier = face_fields(body)
        owner = co if co is not None else body.get("company") or ""
        if owner and not NAME_RE.match(owner):
            raise HTTPException(400, "Sai mã công ty")
    try:
        vecs, thumb = faces.vectors_from_photos(images) if images else ([], None)
        for uid in unknown:
            u = gallery.unknown(uid)
            if allowed is not None and u["cam"] not in allowed:
                raise faces.FaceError("Mặt này không thuộc camera của bạn")
            vecs.append(u["vec"])
            if not pid and not owner and co is None:
                owner = cam_meta()["companies"].get(u["cam"], "")   # admin đăng ký từ cam công ty nào -> công ty đó
            if not thumb and u.get("face_crop", "").startswith(OUTPUTS + "/") and os.path.isfile(u["face_crop"]):
                with open(u["face_crop"], "rb") as fp:
                    raw = fp.read()
                thumb = "data:image/jpeg;base64," + base64.b64encode(raw).decode() if len(raw) < 80_000 else None
        f = gallery.save(name, tier, owner, vecs, thumb, "camera" if unknown and not images else "photo", pid)
    except faces.FaceError as e:
        raise HTTPException(400, str(e))
    return {"ok": True, "person": f, "samples_added": len(vecs)}


def faces_patch(pid: str, body: dict, request: Request, company: str | None = None):
    require_xhr(request)
    face_owned(pid, company)
    name, tier = face_fields(body)
    return {"ok": True, "person": gallery.update(pid, name, tier)}


def faces_delete(pid: str, request: Request, company: str | None = None):
    require_xhr(request)
    face_owned(pid, company)
    gallery.delete(pid)
    return {"ok": True}


def faces_unknown(limit: int = 60, company: str | None = None):
    """Mặt lạ đủ rõ w1-app gặp gần đây (1 ảnh / track người), mới nhất trước."""
    check_company(company)
    allowed, info, out = cams_of(company), cam_meta()["info"], []
    try:
        items = r.xrevrange(f"{PREFIX}:faces_unknown", count=1000)
    except redis.RedisError:
        items = []
    for msg_id, f in items:
        if allowed is not None and f.get("cam") not in allowed:
            continue
        try:
            d = json.loads(f.get("data", "{}"))
        except ValueError:
            continue
        crop = media_url(d.get("face_crop"))
        if crop:
            out.append({"id": msg_id, "cam": f.get("cam"), "cam_name": info.get(f.get("cam"), {}).get("camera_name"),
                        "ts": int(f.get("ts", 0)), "tid": d.get("tid"), "crop": crop,
                        "snapshot": media_url(d.get("snapshot"))})
        if len(out) >= min(limit, 200):
            break
    return {"unknown": out}


def face_hits(since: int = 0, limit: int = 50, company: str | None = None):
    """face_recognized / vip_detected sau mốc since (ms, theo id stream), mới nhất trước: cho thông báo + nhật ký."""
    check_company(company)
    allowed, info, out = cams_of(company), cam_meta()["info"], []
    since = max(since, int(time.time() * 1000) - 24 * 3600 * 1000)
    try:
        items = r.xrevrange(f"{PREFIX}:events", min=f"{since + 1}-0", count=3000)
    except redis.RedisError:
        items = []
    for msg_id, f in items:
        if f.get("type") not in FACE_EVENTS or (allowed is not None and f.get("cam") not in allowed):
            continue
        try:
            d = json.loads(f.get("data", "{}"))
        except ValueError:
            continue
        out.append({"id": msg_id, "ms": int(msg_id.split("-")[0]), "type": f["type"], "cam": f.get("cam"),
                    "cam_name": info.get(f.get("cam"), {}).get("camera_name"), "ts": int(f.get("ts", 0)),
                    "tid": d.get("tid"), "person_id": d.get("person_id"), "name": d.get("name"),
                    "tier": d.get("tier"), "score": d.get("score"), "prob": d.get("prob"),
                    "snapshot": media_url(d.get("snapshot")), "crop": media_url(d.get("face_crop") or d.get("crop"))})
        if len(out) >= min(limit, 200):
            break
    return {"hits": out, "now": int(time.time() * 1000)}


# ---------------- dashboard master (chỉ admin) ----------------
MASTER = os.getenv("MASTER_CONFIGS") if not COMPANY else None
VIDEOS = os.getenv("VIDEOS", "/videos")
NAME_RE = re.compile(r"^[A-Za-z0-9_]{1,32}$")
CAM_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
MAX_CAMS = 64
TEXT_MAX = 120   # độ dài tối đa các trường mô tả (tên công ty, địa chỉ, ...)


def master_on():
    if not MASTER:
        raise HTTPException(404, "Dashboard master chưa bật (web.sh start với MASTER_CONFIGS)")


def require_xhr(request: Request):
    # Chặn CSRF: trình duyệt không cho trang web khác tự gắn header riêng khi gửi sang đây
    if request.headers.get("x-master") != "1":
        raise HTTPException(403)


def list_videos():
    try:
        return sorted(f for f in os.listdir(VIDEOS) if f.endswith(".mp4") and os.path.isfile(os.path.join(VIDEOS, f)))
    except OSError:
        return []


def read_sites():
    """configs/sites.json: {"companies": [{id, name, buildings: [{id, name, address, cameras: [...]}]}]}."""
    try:
        with open(os.path.join(MASTER, "sites.json"), encoding="utf-8") as f:
            return json.load(f).get("companies", [])
    except (OSError, ValueError):
        return []


def read_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def write_atomic(path, text):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


def master_get():
    master_on()
    accounts = {}
    try:
        with open(COMPANIES_AUTH, encoding="utf-8") as f:
            for line in f:
                p = line.split()
                if len(p) == 3:
                    accounts[p[0]] = {"user": p[1], "pass": p[2]}
    except OSError:
        pass
    status = read_json(os.path.join(MASTER, ".master/status.json"), {})
    # Trạng thái thật của từng cam: fps nhận được, timestamp frame mới nhất (ts_ms của metadata)
    live_cams = {cam: {"online": v["online"], "fps": v["fps"], "ts": (v["data"] or {}).get("ts_ms")}
                 for cam, v in live.snapshot().items()}
    return {"companies": read_sites(), "videos": list_videos(), "accounts": accounts, "live": live_cams,
            "app": read_json(os.path.join(MASTER, "app.json"), {"dwell_s": 120, "smart_record": True}),
            "status": status, "agent_alive": time.time() - status.get("updated", 0) < 30,
            "pending": os.path.exists(os.path.join(MASTER, ".master/request.json"))}


def master_put(body: dict, request: Request):
    """Lưu cấu hình (chưa áp dụng). Kiểm tra kỹ vì nội dung đi thẳng vào file mà script đọc."""
    master_on()
    require_xhr(request)
    videos = set(list_videos())

    def text(v, what):
        if not isinstance(v, str) or len(v) > TEXT_MAX or any(ord(ch) < 32 for ch in v):
            raise HTTPException(400, f"{what}: chữ tối đa {TEXT_MAX} ký tự, không xuống dòng")
        return v.strip()

    out, seen, cam_ids = [], set(), set()
    companies = body.get("companies")
    if not isinstance(companies, list):
        raise HTTPException(400, "Sai định dạng")
    for c in companies:
        co = str(c.get("id", ""))
        if not NAME_RE.match(co):
            raise HTTPException(400, f"Mã công ty '{co}' chỉ gồm chữ, số, _ (tối đa 32 ký tự)")
        if co in seen:
            raise HTTPException(400, f"Trùng mã công ty {co}")
        seen.add(co)
        if not c.get("buildings"):
            raise HTTPException(400, f"Công ty {co} chưa có toà nào")
        oc = {"id": co, "name": text(c.get("name", ""), f"{co}: tên công ty"), "buildings": []}
        for b in c["buildings"]:
            bid = str(b.get("id", ""))
            if not NAME_RE.match(bid):
                raise HTTPException(400, f"{co}: mã toà '{bid}' chỉ gồm chữ, số, _")
            if (co, bid) in seen:
                raise HTTPException(400, f"{co}: trùng toà {bid}")
            seen.add((co, bid))
            if not b.get("cameras"):
                raise HTTPException(400, f"{co}/{bid}: chưa có camera nào")
            ob = {"id": bid, "name": text(b.get("name", ""), f"{co}/{bid}: tên toà"),
                  "address": text(b.get("address", ""), f"{co}/{bid}: địa chỉ"), "cameras": []}
            for cam in b["cameras"]:
                cid, video, fps, start = str(cam.get("id", "")), cam.get("video"), cam.get("fps"), cam.get("start")
                if not CAM_RE.match(cid):
                    raise HTTPException(400, f"{co}/{bid}: camera_id '{cid}' chỉ gồm chữ, số, _ . -")
                if cid in cam_ids:
                    raise HTTPException(400, f"Trùng camera_id {cid}")
                cam_ids.add(cid)
                if video not in videos:
                    raise HTTPException(400, f"{cid}: chọn video có trong videos/")
                if not isinstance(fps, int) or not 0 <= fps <= 60:
                    raise HTTPException(400, f"{cid}: fps 0-60 (0 = mọi frame)")
                if not isinstance(start, int) or not 0 <= start <= 86400:
                    raise HTTPException(400, f"{cid}: giây bắt đầu 0-86400")
                roi = cam.get("roi") or None
                if roi is not None and not (isinstance(roi, list) and 3 <= len(roi) <= 32 and all(
                        isinstance(pt, list) and len(pt) == 2 and all(isinstance(v, (int, float)) and 0 <= v <= 1 for v in pt)
                        for pt in roi)):
                    raise HTTPException(400, f"{cid}: vùng ghế cần 3-32 điểm, toạ độ 0-1")
                ob["cameras"].append({"id": cid, "name": text(cam.get("name", ""), f"{cid}: tên camera"),
                                      "floor": text(cam.get("floor", ""), f"{cid}: tầng"),
                                      "zone": text(cam.get("zone", ""), f"{cid}: khu vực"),
                                      "video": video, "fps": fps, "start": start,
                                      **({"roi": [[round(x, 4), round(y, 4)] for x, y in roi]} if roi else {})})
            oc["buildings"].append(ob)
        out.append(oc)
    if not out:
        raise HTTPException(400, "Cần ít nhất 1 công ty")
    if len(cam_ids) > MAX_CAMS:
        raise HTTPException(400, f"Tổng {len(cam_ids)} camera, tối đa {MAX_CAMS}")
    app_cfg = body.get("app") or {}
    dwell = app_cfg.get("dwell_s", 120)
    if not isinstance(dwell, int) or not 10 <= dwell <= 3600:
        raise HTTPException(400, "Ngưỡng ở yên 10-3600 giây")
    write_atomic(os.path.join(MASTER, "sites.json"), json.dumps({"companies": out}, ensure_ascii=False, indent=2))
    face_thr = app_cfg.get("face_thr", 0.42)
    if not isinstance(face_thr, (int, float)) or not 0.2 <= face_thr <= 0.9:
        raise HTTPException(400, "Ngưỡng cosine khuôn mặt 0.2-0.9")
    # face: chạy pipeline YOLO COCO + khuôn mặt + VIP (master/agent.py thêm config nvinfer khi chạy app)
    write_atomic(os.path.join(MASTER, "app.json"),
                 json.dumps({"dwell_s": dwell, "smart_record": bool(app_cfg.get("smart_record", True)),
                             "face": bool(app_cfg.get("face", False)), "face_thr": round(face_thr, 2),
                             "auto_heal": bool(app_cfg.get("auto_heal", True))}))
    return {"ok": True, "total_cams": len(cam_ids)}


MASTER_ACTIONS = {"apply", "app_restart", "app_stop", "republish", "unpublish", "reset_password", "start", "start_all",
                  "clear_alerts"}
START_TARGETS = {"ds", "redis", "sim", "upload", "app", "web"}   # thành phần hệ thống, xem COMPONENTS trong master/agent.py


def master_action(body: dict, request: Request):
    master_on()
    require_xhr(request)
    action, co = body.get("action"), body.get("company")
    if action not in MASTER_ACTIONS:
        raise HTTPException(400, "Thao tác không hợp lệ")
    if action in ("republish", "unpublish", "reset_password") and not (co and NAME_RE.match(co)):
        raise HTTPException(400, "Thiếu công ty")
    target = body.get("target") if action == "start" else None
    if action == "start" and target not in START_TARGETS:
        raise HTTPException(400, "Thành phần không hợp lệ")
    st = read_json(os.path.join(MASTER, ".master/status.json"), {})
    req_path = os.path.join(MASTER, ".master/request.json")
    if st.get("state") == "running" or os.path.exists(req_path):
        raise HTTPException(409, "Agent đang làm việc khác, đợi xong rồi bấm lại")
    os.makedirs(os.path.dirname(req_path), exist_ok=True)
    write_atomic(req_path, json.dumps({"action": action, "company": co if action != "apply" else None, "target": target}))
    return {"ok": True}


if MASTER:
    app.add_api_route("/api/master", master_get, methods=["GET"])
    app.add_api_route("/api/master/config", master_put, methods=["PUT"])
    app.add_api_route("/api/master/action", master_action, methods=["POST"])


def index(company: str | None = None):
    check_company(company)
    return FileResponse(os.path.join(STATIC, "index.html"), headers={"Cache-Control": "no-cache"})


def cam_page(cam: str, company: str | None = None):
    """Link riêng của 1 camera: <dashboard>/cam/<camera_id> (cùng trang, JS tự chuyển sang chế độ 1 camera)."""
    check_company(company)
    allowed = cams_of(company)
    if allowed is not None and cam not in allowed:
        raise HTTPException(404, "Không có camera này")
    return index(company)


# Cùng 1 bộ API cho admin (/) và cho từng công ty (/c/<công ty>/, đã lọc theo công ty).
# Web riêng (COMPANY): chỉ có bộ ở /, đã khoá vào công ty đó
for base in ("",) if COMPANY else ("", "/c/{company}"):
    app.add_api_route(base + "/", index, methods=["GET"])
    app.add_api_route(base + "/api/state", state, methods=["GET"])
    app.add_api_route(base + "/api/alerts", alerts, methods=["GET"])
    app.add_api_route(base + "/api/events", events, methods=["GET"])
    app.add_api_websocket_route(base + "/api/ws", meta_ws)
    app.add_api_route(base + "/cam/{cam}", cam_page, methods=["GET"])
    app.add_api_route(base + "/api/frames/{cam}", frames, methods=["GET"])
    app.add_api_route(base + "/hls/{path:path}", hls, methods=["GET"])
    app.add_api_route(base + "/media/{path:path}", media, methods=["GET"])
    app.add_api_route(base + "/api/faces", faces_get, methods=["GET"])
    app.add_api_route(base + "/api/faces", faces_post, methods=["POST"])
    app.add_api_route(base + "/api/faces/unknown", faces_unknown, methods=["GET"])
    app.add_api_route(base + "/api/faces/hits", face_hits, methods=["GET"])
    app.add_api_route(base + "/api/faces/{pid}", faces_patch, methods=["PATCH"])
    app.add_api_route(base + "/api/faces/{pid}", faces_delete, methods=["DELETE"])


def company_no_slash(company: str):
    return RedirectResponse(f"/c/{company}/")   # link tương đối (api/..., media/...) cần dấu / cuối


if not COMPANY:
    app.add_api_route("/c/{company}", company_no_slash, methods=["GET"])


app.mount("/static", StaticFiles(directory=STATIC), name="static")
