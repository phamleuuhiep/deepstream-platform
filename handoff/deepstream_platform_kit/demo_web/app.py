"""Platform demo: xem kết quả DeepStream trên web thay vì đọc log.

Chỉ đọc (không ghi) Redis Streams của w1-app và thư mục outputs/:
    ds:frames   -> trạng thái live từng cam (fps, số object, bbox hiện tại)
    ds:events   -> danh sách cảnh báo (ghép dwell_alert + dwell_end + clip_ready + clip_annotated)
    ds:cameras  -> danh sách cam
    /outputs    -> ảnh snapshot, clip MP4 (phục vụ qua /media, có hỗ trợ tua video)

Chạy: ./scripts/web.sh start  ->  http://127.0.0.1:8095
"""
import json
import os
import threading
import time
from collections import defaultdict, deque

import redis
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

REDIS_HOST, REDIS_PORT = os.getenv("REDIS", "127.0.0.1:6390").rsplit(":", 1)
PREFIX = os.getenv("PREFIX", "ds")
OUTPUTS = os.getenv("OUTPUTS", "/outputs")
STATIC = os.path.join(os.path.dirname(__file__), "static")
LIVE_TIMEOUT_S = 5      # không có frame quá 5 s thì coi cam là mất hình
EVENT_SCAN = 5000       # số sự kiện gần nhất đọc để dựng danh sách cảnh báo

r = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), password=os.getenv("REDIS_PASSWORD"),
                decode_responses=True, socket_timeout=5)
app = FastAPI(title="DeepStream platform demo")


class Live:
    """Thread đọc đuôi ds:frames, giữ frame mới nhất + fps của từng cam."""

    def __init__(self):
        self.lock = threading.Lock()
        self.last = {}                            # cam -> data JSON frame mới nhất
        self.seen_at = {}                         # cam -> time.time() lúc nhận frame cuối
        self.arrivals = defaultdict(deque)        # cam -> thời điểm nhận các frame trong 5 s
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
                    newest[cam] = f["data"]       # chỉ parse JSON frame cuối của mỗi cam trong batch
                    self.arrivals[cam].append(now)
            with self.lock:
                for cam, raw in newest.items():
                    try:
                        self.last[cam] = json.loads(raw)
                        self.seen_at[cam] = now
                    except ValueError:
                        pass
                for q in self.arrivals.values():
                    while q and q[0] < now - LIVE_TIMEOUT_S:
                        q.popleft()

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


def media_url(path):
    """/outputs/w1_clips/x.mp4 -> /media/w1_clips/x.mp4 (None nếu không có file)."""
    if not path or not path.startswith(OUTPUTS + "/"):
        return None
    if not os.path.isfile(path):
        return None
    return "/media/" + path[len(OUTPUTS) + 1:]


def read_events(count):
    """Sự kiện mới nhất trước, dạng dict (type, cam, ts, data)."""
    out = []
    for msg_id, f in r.xrevrange(f"{PREFIX}:events", count=count):
        try:
            data = json.loads(f.get("data", "{}"))
        except ValueError:
            data = {}
        out.append({"id": msg_id, "type": f.get("type"), "cam": f.get("cam"),
                    "ts": int(f.get("ts", 0)), "data": data})
    return out


def cam_sort_key(cam):
    digits = "".join(ch for ch in cam if ch.isdigit())
    return (cam.rstrip("0123456789"), int(digits) if digits else 0)


@app.get("/api/state")
def state():
    """Tổng quan + trạng thái từng cam. Frontend gọi mỗi 1 s."""
    try:
        cams = r.hgetall(f"{PREFIX}:cameras")
        n_frames = r.xlen(f"{PREFIX}:frames")
        n_events = r.xlen(f"{PREFIX}:events")
        redis_ok = True
    except redis.RedisError:
        cams, n_frames, n_events, redis_ok = {}, 0, 0, False
    lv = live.snapshot()
    rows = []
    for cam in sorted(set(cams) | set(lv), key=cam_sort_key):
        s = lv.get(cam, {"fps": 0, "age_s": None, "online": False, "data": None})
        d = s["data"] or {}
        objs = d.get("objs", [])
        rows.append({"cam": cam, "uri": cams.get(cam, ""), "online": s["online"], "fps": s["fps"],
                     "age_s": s["age_s"], "num_obj": d.get("num_obj", 0),
                     "persons": sum(1 for o in objs if o.get("label") == "person"),
                     "w": d.get("w", 1920), "h": d.get("h", 1080),
                     "objs": objs if s["online"] else []})
    return {"redis": redis_ok, "redis_addr": f"{REDIS_HOST}:{REDIS_PORT}",
            "frames_len": n_frames, "events_len": n_events,
            "cams_online": sum(1 for x in rows if x["online"]), "cams_total": len(rows),
            "fps_total": round(sum(x["fps"] for x in rows), 1), "cameras": rows}


@app.get("/api/alerts")
def alerts(limit: int = 100):
    """Ghép các sự kiện của một lượt ở yên thành 1 bản ghi cảnh báo."""
    events = read_events(EVENT_SCAN)
    clips, annotated, ends = {}, {}, {}
    for e in events:
        d = e["data"]
        if e["type"] == "clip_ready":
            clips.setdefault(d.get("tag"), d)
        elif e["type"] == "clip_annotated":
            annotated.setdefault(d.get("tag"), d)
        elif e["type"] == "dwell_end":
            ends.setdefault((e["cam"], d.get("tid"), d.get("start_ms")), d)
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


@app.get("/api/events")
def events(limit: int = 200):
    return {"events": read_events(min(limit, 1000))}


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC, "index.html"))


app.mount("/media", StaticFiles(directory=OUTPUTS), name="media")
app.mount("/static", StaticFiles(directory=STATIC), name="static")
