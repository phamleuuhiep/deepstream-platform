"""Phát lại dữ liệu mẫu DeepStream vào Redis, giống hệt w1-app đang chạy thật.

Dùng để team platform phát triển / test mà không cần GPU, camera hay DeepStream.

    python replay.py --redis 127.0.0.1:6390 --data ../sample_data
    python replay.py --speed 4            # tua nhanh 4 lần: cảnh báo sau ~30 s thay vì ~120 s
    python replay.py --frames-every 6     # chỉ gửi 1/6 frame (5 fps/cam), giống w1-app -f 6

Cách phát:
  * ds:cameras  ghi 1 lần lúc đầu (HSET), như w1-app khi nối Redis.
  * ds:events   phát đúng thứ tự và khoảng cách thời gian gốc (chia cho --speed).
  * ds:frames   60 s metadata mẫu được phát lặp vòng liên tục (cam luôn "có hình").
  * Ctrl+C      gửi dwell_end reason=shutdown cho các lượt ở yên chưa kết thúc, như w1-app khi tắt.
Mọi timestamp (ts, *_ms, *_ts ISO) được dời về thời điểm hiện tại. Riêng dwell_s, duration_ms
giữ giá trị gốc, nên với --speed > 1 thì dwell_s (120) sẽ lớn hơn khoảng thời gian thực tế.
"""
import argparse
import json
import os
import signal
import time
from datetime import datetime, timezone

import redis

ISO_KEYS = ("start_ts", "alert_ts", "end_ts", "ts")
KEEP_MS_KEYS = ("duration_ms",)
MAXLEN = 100000


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(ms) % 1000:03d}Z"


def parse_iso(s):
    return int(datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc).timestamp() * 1000)


def load_jsonl(path):
    with open(path, encoding="utf-8") as fp:
        return [json.loads(line) for line in fp if line.strip()]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--redis", default=os.getenv("REDIS", "127.0.0.1:6390"), help="host:port")
    ap.add_argument("--prefix", default="ds")
    ap.add_argument("--data", default=os.path.join(os.path.dirname(__file__), "..", "sample_data"))
    ap.add_argument("--speed", type=float, default=1.0, help="tua nhanh N lần (mặc định 1 = thời gian thật)")
    ap.add_argument("--frames-every", type=int, default=1, help="chỉ gửi 1/N frame mỗi cam")
    ap.add_argument("--no-frames", action="store_true", help="chỉ phát sự kiện")
    ap.add_argument("--keep", action="store_true", help="không xoá ds:* cũ trước khi phát")
    args = ap.parse_args()

    host, port = args.redis.rsplit(":", 1)
    r = redis.Redis(host=host, port=int(port), password=os.getenv("REDIS_PASSWORD") or None, decode_responses=True)
    k_frames, k_events, k_cams = (f"{args.prefix}:{x}" for x in ("frames", "events", "cameras"))
    for _ in range(30):                                   # chờ Redis lên (docker compose)
        try:
            r.ping()
            break
        except redis.ConnectionError:
            time.sleep(1)
    if not args.keep:
        r.delete(k_frames, k_events, k_cams)

    events = load_jsonl(os.path.join(args.data, "events.jsonl"))
    frames = [] if args.no_frames else load_jsonl(os.path.join(args.data, "frames.jsonl"))
    with open(os.path.join(args.data, "cameras.json"), encoding="utf-8") as fp:
        r.hset(k_cams, mapping=json.load(fp))

    speed = args.speed
    now0 = time.time() * 1000
    t0 = events[0]["ts"]
    shift = lambda ms: int(now0 + (ms - t0) / speed)      # noqa: E731

    def shift_data(d):
        d = dict(d)
        for k, v in list(d.items()):
            if k.endswith("_ms") and k not in KEEP_MS_KEYS and isinstance(v, (int, float)):
                d[k] = shift(v)
            elif k in ISO_KEYS and isinstance(v, str):
                d[k] = iso(shift(parse_iso(v)))
        return d

    # Frame: lặp vòng cửa sổ mẫu; frame k của vòng c đến hạn lúc now0 + (c*P + offset)/speed
    frames = [f for i, f in enumerate(frames) if (f["data"]["frame"] % args.frames_every) == 0]
    f0 = frames[0]["ts"] if frames else 0
    period = (frames[-1]["ts"] - f0 + 34) if frames else 0
    per_cam = {}
    for f in frames:
        per_cam[f["cam"]] = per_cam.get(f["cam"], 0) + 1

    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))

    print(f"[replay] Redis {args.redis}: {len(events)} sự kiện, {len(frames)} frame/vòng "
          f"({period / 1000:.0f} s), tốc độ x{speed:g}. Ctrl+C để dừng.", flush=True)
    ei, fi, cycle, sent_frames, open_dwell = 0, 0, 0, 0, {}
    last_log = 0
    while not stop["flag"]:
        now = time.time() * 1000
        pipe = r.pipeline(transaction=False)
        n = 0
        while ei < len(events) and shift(events[ei]["ts"]) <= now:
            e = events[ei]
            d = shift_data(e["data"])
            ts = shift(e["ts"])
            pipe.xadd(k_events, {"cam": e["cam"], "type": e["type"], "ts": ts,
                                 "data": json.dumps(d, ensure_ascii=False)}, maxlen=MAXLEN, approximate=True)
            if e["type"] == "dwell_alert":
                open_dwell[(e["cam"], d["tid"])] = d
            elif e["type"] == "dwell_end":
                open_dwell.pop((e["cam"], d["tid"]), None)
            print(f"[replay] {iso(ts)} {e['cam']:>6} {e['type']}", flush=True)
            ei += 1
            n += 1
        while frames:
            f = frames[fi]
            due = now0 + (cycle * period + f["ts"] - f0) / speed
            if due > now:
                break
            d = dict(f["data"])
            d["ts_ms"] = int(due)
            d["frame"] = d["frame"] + cycle * per_cam[f["cam"]] * args.frames_every
            pipe.xadd(k_frames, {"cam": f["cam"], "type": "frame", "ts": int(due),
                                 "data": json.dumps(d, separators=(",", ":"))}, maxlen=MAXLEN, approximate=True)
            sent_frames += 1
            n += 1
            fi += 1
            if fi == len(frames):
                fi, cycle = 0, cycle + 1
        if n:
            pipe.execute()
        if now - last_log > 10000:
            print(f"[replay] sự kiện {ei}/{len(events)}, frame đã gửi {sent_frames}", flush=True)
            last_log = now
        time.sleep(0.01)

    # Giống w1-app khi tắt: đóng các lượt ở yên còn mở
    now = int(time.time() * 1000)
    for (cam, tid), d in open_dwell.items():
        end = dict(d)
        end.pop("alert_ts", None)
        end.update(type="dwell_end", end_ts=iso(now), reason="shutdown",
                   dwell_s=round((now - d["start_ms"]) / 1000 * speed, 1))
        r.xadd(k_events, {"cam": cam, "type": "dwell_end", "ts": now,
                          "data": json.dumps(end, ensure_ascii=False)}, maxlen=MAXLEN, approximate=True)
        print(f"[replay] {iso(now)} {cam:>6} dwell_end (shutdown)", flush=True)
    print("[replay] đã dừng", flush=True)


if __name__ == "__main__":
    main()
