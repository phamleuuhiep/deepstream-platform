"""Upload ảnh/clip của w1-app lên S3/MinIO (Task 2.1). Chạy trong container ds_uploader (./scripts/upload.sh start).

Đọc <prefix>:events bằng consumer group "uploader" (giống platform đọc): mỗi field "/outputs/<đường dẫn>" của sự
kiện (snapshot, face_crop, file của clip_ready / clip_annotated, ...) được upload với key = <đường dẫn> vào bucket,
rồi ghi <file>.up cạnh file để w1-app (-M) biết file đã upload, được phép dọn. URL trong sự kiện do app tự ghép
(<URL gốc>/<đường dẫn>), khớp đúng key ở đây.

Không mất file khi S3 hoặc Redis chết:
  - Sự kiện chỉ được XACK khi mọi file của nó đã lên S3. S3 lỗi -> giữ trong danh sách pending của group, thử lại
    mỗi RETRY_SEC giây (đọc lại pending bằng XREADGROUP id 0).
  - File chưa ghi xong (ảnh JPEG ghi bất đồng bộ, clip đang ghi) -> cũng để pending, thử lại; quá GIVE_UP_SEC mà
    vẫn không có file thì bỏ (ghi log), không kẹt mãi.
  - Redis chết -> nối lại mỗi 2 s; group nhớ vị trí đã đọc nên không sót sự kiện nào.
Thống kê ghi vào <prefix>:uploader (hash) cho exporter Prometheus.
"""
import json
import mimetypes
import os
import re
import socket
import time

import boto3
import redis
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

REDIS_HOST, REDIS_PORT = os.getenv("REDIS", "127.0.0.1:6390").rsplit(":", 1)
PREFIX = os.getenv("PREFIX", "ds")
OUTPUTS = os.getenv("OUTPUTS", "/outputs")
BUCKET = os.environ["S3_BUCKET"]
GROUP = os.getenv("GROUP", "uploader")
CONSUMER = socket.gethostname()
RETRY_SEC = 10
GIVE_UP_SEC = 900           # clip ghi xong sau ~10 s; 15 phút không thấy file thì bỏ
PATH_RE = re.compile(r'"[a-z_]+":"(/outputs/[^"]+)"')

s3 = boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT") or None,
                  aws_access_key_id=os.environ["S3_ACCESS_KEY"], aws_secret_access_key=os.environ["S3_SECRET_KEY"],
                  region_name=os.getenv("S3_REGION", "us-east-1"),
                  config=Config(connect_timeout=3, read_timeout=30, retries={"max_attempts": 1}))
stats = {"uploaded": 0, "bytes": 0, "errors": 0, "gave_up": 0, "pending": 0, "s3_up": 1, "last_ok": 0}


def log(msg):
    print(time.strftime("%H:%M:%S ") + msg, flush=True)


def files_of(data):
    """Đường dẫn file của 1 sự kiện; ảnh toàn khung *_frame.jpg kèm ảnh crop *_crop.jpg."""
    out = []
    for p in PATH_RE.findall(data):
        out.append(p)
        if p.endswith("_frame.jpg"):
            out.append(p[:-len("_frame.jpg")] + "_crop.jpg")
    return list(dict.fromkeys(out))


def upload(path):
    """True = đã ở trên S3 (vừa upload hoặc có .up). None = chưa có file. Lỗi S3 -> raise."""
    if os.path.exists(path + ".up"):
        return True
    if not os.path.isfile(path):
        return None
    if time.time() - os.path.getmtime(path) < 2:
        return None          # JPEG / MP4 có thể còn đang ghi
    key = os.path.relpath(path, OUTPUTS)
    ctype = mimetypes.guess_type(path)[0] or "application/octet-stream"
    s3.upload_file(path, BUCKET, key, ExtraArgs={"ContentType": ctype})
    open(path + ".up", "w").close()
    stats["uploaded"] += 1
    stats["bytes"] += os.path.getsize(path)
    stats["last_ok"] = int(time.time())
    return True


def handle(r, msg_id, fields):
    """True = xong (XACK), False = để pending thử lại sau."""
    paths = [p for p in files_of(fields.get("data", "")) if p.startswith(OUTPUTS + "/")]
    age = time.time() - int(msg_id.split("-")[0]) / 1000
    done = True
    for p in paths:
        ok = upload(p)
        if ok is None:
            if p.endswith("_crop.jpg") and os.path.isfile(p[:-len("_crop.jpg")] + "_frame.jpg"):
                continue     # sự kiện không có crop (ảnh camera)
            if age > GIVE_UP_SEC:
                stats["gave_up"] += 1
                log(f"bỏ {p}: {int(age)} s vẫn không có file ({fields.get('type')} {fields.get('cam')})")
                continue
            done = False
    return done


def main():
    r = None
    last_retry = last_log = 0.0
    while True:
        try:
            if r is None:
                r = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), password=os.getenv("REDIS_PASSWORD") or None,
                                decode_responses=True, socket_timeout=10)
                try:
                    r.xgroup_create(f"{PREFIX}:events", GROUP, id="$", mkstream=True)
                    log(f"tạo consumer group {GROUP} trên {PREFIX}:events")
                except redis.ResponseError as e:
                    if "BUSYGROUP" not in str(e):
                        raise
                log(f"đã nối Redis {REDIS_HOST}:{REDIS_PORT}, bucket {BUCKET}")
            # Sự kiện mới, rồi định kỳ thử lại sự kiện còn pending (id 0 = pending của consumer này)
            batches = [">"]
            if time.time() - last_retry > RETRY_SEC:
                batches.insert(0, "0")
                last_retry = time.time()
            for start in batches:
                resp = r.xreadgroup(GROUP, CONSUMER, {f"{PREFIX}:events": start}, count=200,
                                    block=2000 if start == ">" else None)
                for _, msgs in resp or []:
                    for msg_id, fields in msgs:
                        try:
                            if handle(r, msg_id, fields):
                                r.xack(f"{PREFIX}:events", GROUP, msg_id)
                            stats["s3_up"] = 1
                        except (BotoCoreError, ClientError, OSError) as e:
                            stats["errors"] += 1
                            if stats["s3_up"]:
                                log(f"S3 lỗi ({type(e).__name__}: {str(e)[:120]}), giữ pending, thử lại mỗi {RETRY_SEC} s")
                            stats["s3_up"] = 0
                            break
            stats["pending"] = r.xpending(f"{PREFIX}:events", GROUP)["pending"]
            r.hset(f"{PREFIX}:uploader", mapping={**stats, "ts": int(time.time() * 1000)})
            if time.time() - last_log > 60:
                log(f"đã upload {stats['uploaded']} file ({stats['bytes'] / 1e6:.1f} MB), pending {stats['pending']} sự kiện, "
                    f"lỗi {stats['errors']}, bỏ {stats['gave_up']}, S3 {'UP' if stats['s3_up'] else 'DOWN'}")
                last_log = time.time()
        except redis.RedisError as e:
            if r is not None:
                log(f"mất Redis ({e}), nối lại mỗi 2 s")
            r = None
            time.sleep(2)


if __name__ == "__main__":
    main()
