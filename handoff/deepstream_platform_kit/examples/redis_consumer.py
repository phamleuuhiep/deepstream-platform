"""Consumer mẫu cho team platform: đọc sự kiện / metadata DeepStream từ Redis Streams.

Dùng consumer group nên chạy nhiều instance song song được, mỗi message chỉ 1 instance xử lý,
và message chưa ACK sẽ được giao lại nếu instance chết.

    pip install redis
    python redis_consumer.py --host 127.0.0.1 --port 6390                 # chỉ sự kiện
    python redis_consumer.py --frames --count 5                           # xem thử metadata frame

Key (prefix mặc định "ds"):
    ds:events   dwell_alert, dwell_end, clip_ready, camera_online, camera_offline
    ds:frames   metadata từng frame (bbox, class, track id), giới hạn ~100k bản ghi gần nhất
    ds:cameras  hash cam_id -> rtsp uri
Mỗi entry có các field: cam, type, ts (epoch ms), data (JSON).
"""
import argparse
import json
import socket

import redis


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6390)
    ap.add_argument("--password", default=None)
    ap.add_argument("--prefix", default="ds")
    ap.add_argument("--group", default="platform")
    ap.add_argument("--frames", action="store_true", help="đọc ds:frames thay vì ds:events")
    ap.add_argument("--count", type=int, default=0, help="dừng sau N message (0 = chạy mãi)")
    args = ap.parse_args()

    r = redis.Redis(host=args.host, port=args.port, password=args.password, decode_responses=True)
    stream = f"{args.prefix}:{'frames' if args.frames else 'events'}"
    consumer = socket.gethostname()
    try:
        # "0": nhóm mới đọc từ đầu stream; đổi thành "$" nếu chỉ cần message mới
        r.xgroup_create(stream, args.group, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    print(f"camera: {r.hgetall(f'{args.prefix}:cameras')}")
    seen = 0
    while not args.count or seen < args.count:
        resp = r.xreadgroup(args.group, consumer, {stream: ">"}, count=100, block=5000)
        for _, messages in resp or []:
            for msg_id, fields in messages:
                data = json.loads(fields["data"])
                handle(fields["type"], fields["cam"], int(fields["ts"]), data)
                r.xack(stream, args.group, msg_id)   # ACK sau khi xử lý xong
                seen += 1
                if args.count and seen >= args.count:
                    return


def handle(typ, cam, ts_ms, data):
    if typ == "dwell_alert":
        print(f"[ALERT] {cam} người #{data['tid']} ở yên {data['dwell_s']}s "
              f"từ {data['start_ts']} -> {data['alert_ts']}, ảnh {data['snapshot']}, clip {data['clip']}")
    elif typ == "dwell_end":
        print(f"[END]   {cam} người #{data['tid']} tổng {data['dwell_s']}s ({data.get('reason')})")
    elif typ == "clip_ready":
        print(f"[CLIP]  {cam} {data['file']} ({data['duration_ms'] / 1000:.1f}s) cho {data['tag']}")
    elif typ in ("camera_online", "camera_offline"):
        print(f"[CAM]   {cam} {typ}")
    elif typ == "frame":
        print(f"[FRAME] {cam} frame {data['frame']}: {data['num_obj']} obj")
    else:
        print(f"[?]     {typ} {data}")


if __name__ == "__main__":
    main()
