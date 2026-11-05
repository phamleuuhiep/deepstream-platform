"""Exporter Prometheus cho pipeline DeepStream (Task 2.4). Chạy trong container ds_exporter (./scripts/monitor.sh start).

Nguồn số liệu:
  *:stats:g<GPU>         (mọi prefix: ds, soak test, ...) JSON w1-app ghi mỗi -s giây (hết hạn 60 s khi app chết): fps tổng/từng cam, cam có hình,
                         queue/sent/drop Redis, sự kiện đã tạo/đã gửi, track ở yên, RAM (RSS)
  <prefix>:uploader      hash của uploader: file đã upload, sự kiện pending, S3 up
  <prefix>:events        đọc nối đuôi, đếm sự kiện theo loại (dwell_alert, face_recognized, camera_offline, ...)
  nvidia-smi             VRAM của tiến trình w1-app trên từng GPU (container chạy --gpus all --pid host)
Mở: http://<host>:9466/metrics
"""
import json
import os
import subprocess
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import redis

REDIS_HOST, REDIS_PORT = os.getenv("REDIS", "127.0.0.1:6390").rsplit(":", 1)
PREFIX = os.getenv("PREFIX", "ds")
LISTEN = os.getenv("LISTEN", "0.0.0.0:9466")
r = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), password=os.getenv("REDIS_PASSWORD") or None,
                decode_responses=True, socket_timeout=5)
events_by_type = Counter()          # đếm từ lúc exporter chạy (Prometheus dùng increase()/rate())
lock = threading.Lock()


def tail_events():
    last = "$"
    while True:
        try:
            for _, msgs in r.xread({f"{PREFIX}:events": last}, count=1000, block=5000) or []:
                for msg_id, f in msgs:
                    last = msg_id
                    with lock:
                        events_by_type[(f.get("type", "?"), f.get("company", ""))] += 1
        except redis.RedisError:
            time.sleep(2)


def vram_by_pid():
    """{(gpu index, pid host): MiB} của các tiến trình w1-app."""
    out = {}
    try:
        gpus = dict(line.split(", ") for line in subprocess.run(
            ["nvidia-smi", "--query-gpu=gpu_uuid,index", "--format=csv,noheader"],
            capture_output=True, text=True, timeout=5).stdout.splitlines() if ", " in line)
        apps = subprocess.run(["nvidia-smi", "--query-compute-apps=gpu_uuid,pid,process_name,used_memory",
                               "--format=csv,noheader,nounits"], capture_output=True, text=True, timeout=5).stdout
        for line in apps.splitlines():
            uuid, pid, name, mem = [x.strip() for x in line.split(",")]
            if "w1-app" in name or _cmd(pid).startswith("/workspace/apps/w1_multistream/w1-app"):
                out[(gpus.get(uuid, "?"), pid)] = float(mem)
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    return out


def _cmd(pid):
    try:
        return open(f"/proc/{pid}/cmdline", "rb").read().replace(b"\0", b" ").decode(errors="replace")
    except OSError:
        return ""


def _nspid(pid):
    """pid của tiến trình trong container (dòng NSpid cuối) từ pid trên host."""
    try:
        for line in open(f"/proc/{pid}/status"):
            if line.startswith("NSpid:"):
                return line.split()[-1]
    except OSError:
        pass
    return pid


def esc(v):
    return str(v).replace("\\", "\\\\").replace('"', '\\"')


def metrics():
    out = []
    add = lambda name, value, **lb: out.append(
        f"{name}{{{','.join(f'{k}={chr(34)}{esc(v)}{chr(34)}' for k, v in lb.items())}}} {value}" if lb else f"{name} {value}")
    try:
        keys = sorted(r.scan_iter("*:stats:g*"))
        stats = [dict(json.loads(x), prefix=k.split(":")[0]) for k, x in zip(keys, r.mget(keys)) if x] if keys else []
        up_r = 1
    except (redis.RedisError, ValueError):
        stats, up_r = [], 0
    add("ds_exporter_redis_up", up_r)
    vram = vram_by_pid()
    now = time.time() * 1000
    for s in stats:
        g, pf = s.get("gpu", 0), s["prefix"]
        fresh = now - s.get("ts", 0) < 30000
        lb = {"prefix": pf, "gpu": g}
        add("ds_app_up", int(fresh), **lb)
        if not fresh:
            continue
        add("ds_fps_total", s["fps_total"], **lb)
        add("ds_cams_online", s["cams_online"], **lb)
        add("ds_cams_total", s["cams_total"], **lb)
        add("ds_redis_up", s["redis_up"], **lb)
        add("ds_redis_queue", s["queue"], **lb)
        add("ds_messages_sent_total", s["sent"], **lb)
        add("ds_messages_dropped_total", s["drop"], **lb)
        add("ds_events_created_total", s["events_created"], **lb)
        add("ds_events_sent_total", s["events"], **lb)
        add("ds_dwell_tracks", s["dwell"], **lb)
        add("ds_face_hits_total", s.get("face_hits", 0), **lb)
        add("ds_app_rss_bytes", int(s["rss_mb"] * 1048576), **lb)
        # VRAM: theo pid trong container của app; nvidia-smi (--pid host) cho pid host -> khớp qua NSpid
        mib = sum(v for (gi, hp), v in vram.items() if gi == str(g) and _nspid(hp) == str(s.get("pid")))
        add("ds_app_vram_bytes", int(mib * 1048576), **lb)
        for cam, fps in s.get("cams", {}).items():
            add("ds_cam_fps", fps, cam=cam, **lb)
            add("ds_cam_online", int(fps > 0), cam=cam, **lb)
    try:
        u = r.hgetall(f"{PREFIX}:uploader")
        if u:
            fresh = now - int(u.get("ts", 0)) < 60000
            add("ds_upload_up", int(fresh))
            add("ds_upload_s3_up", int(u.get("s3_up", 0)) if fresh else 0)
            add("ds_upload_files_total", u.get("uploaded", 0))
            add("ds_upload_bytes_total", u.get("bytes", 0))
            add("ds_upload_pending_events", u.get("pending", 0))
            add("ds_upload_errors_total", u.get("errors", 0))
    except redis.RedisError:
        pass
    with lock:
        for (typ, co), n in sorted(events_by_type.items()):
            add("ds_events_by_type_total", n, type=typ, company=co)
    return "\n".join(out) + "\n"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.split("?")[0] != "/metrics":
            self.send_error(404)
            return
        body = metrics().encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    threading.Thread(target=tail_events, daemon=True).start()
    host, port = LISTEN.rsplit(":", 1)
    print(f"ds_exporter: http://{LISTEN}/metrics (Redis {REDIS_HOST}:{REDIS_PORT}, prefix {PREFIX})", flush=True)
    ThreadingHTTPServer((host, int(port)), Handler).serve_forever()
