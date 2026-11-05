#!/usr/bin/env bash
# Chạy từng bài test trong docs/BAO_CAO_TUAN2.md để lấy minh chứng. Kết quả in ra màn hình và lưu vào
# outputs/bao_cao_tuan2/<bài>.txt (+ ảnh / file). Mỗi lần chỉ chạy 1 bài.
# Không đụng app đang chạy (prefix ds): app test dùng prefix Redis riêng (w2*), luồng RTSP riêng (w2-*, container
# w2pub_*), chạy trên GPU ${TEST_GPU:-1}. Riêng bài tu-hoi-phuc cố ý làm chết hệ thống thật để xem tự hồi phục.
# Dùng: ./scripts/w2_bc_test.sh <bài>
#   platform       2.1  consumer group của platform nhận dwell_alert -> dwell_end -> clip_ready, có schema_version (~2 phút)
#   upload         2.1  ảnh/clip lên MinIO, sự kiện mang URL mở được; chỉ dọn file đã upload            (~3 phút)
#   ngat-platform  2.1  ngắt Redis + S3 5 phút (OUTAGE=giây để đổi) không mất sự kiện, upload bù        (~8 phút)
#   model-nguoi    2.2  so TrafficCamNet / PeopleNet / YOLO11s bắt người ngồi                          (~3 phút, lần đầu ~8)
#   roi            2.2  vùng ghế: người ngồi trong vùng bị cảnh báo, người đứng ngoài vùng thì không      (~2 phút)
#   do-lech        2.2  19 lượt có gán nhãn (10 ngồi + 9 ngồi ngắn): độ lệch cảnh báo, cảnh báo nhầm     (~4 phút, lần đầu +3)
#   khuon-mat      2.3  đăng ký 1 người từ camera -> nhận ra; video người khác không nhận nhầm          (~3 phút)
#   tu-hoi-phuc    2.4  xoá ds_mediamtx + kill app THẬT: agent tự bật lại, về 19/19 cam                  (~2 phút)
#   grafana        2.4  số liệu đang có trong Prometheus + link Grafana                                  (~5 s)
#   soak start|status|report|stop [cam]  2.4  soak test 24 h, mặc định 32 cam trên GPU 1, ghi RAM/VRAM mỗi phút
#   gpu1           2.4  2 instance (GPU 0 + GPU 1) x 22 cam = 44 cam, không trùng dữ liệu                (~4 phút)
#   cam-that <rtsp main> [rtsp sub]  2.4  camera thật Hikvision/Dahua: lên hình + rút mạng tự hồi phục   (~3 phút)
#   don-test             xoá container, key Redis, ảnh/clip do các bài test tạo (giữ outputs/bao_cao_tuan2/)
set -euo pipefail
if ! command -v docker >/dev/null; then
  echo "Lỗi: chạy script này trên HOST (cd /home/hiepplh/deepstream), không chạy trong container." >&2
  exit 1
fi
cd "$(dirname "$0")/.."
ROOT=$PWD
. scripts/redis_env.sh

APP=/workspace/apps/w1_multistream/w1-app
RTSP=rtsp://127.0.0.1:8554
FFMPEG_IMAGE=linuxserver/ffmpeg:latest
PY_IMAGE=ds-uploader:1                      # python + redis (ảnh của uploader)
OUT=outputs/bao_cao_tuan2
# Ảnh/clip của app test vào thư mục riêng (-o): -k/-K của app chỉ dọn trong thư mục của chính nó. Thiếu -o thì app test
# quét chung outputs/w1_snapshots, w1_clips với app thật và XOÁ ảnh/clip thật (sự cố 05/11: mất 6.134 file)
TDIR=/outputs/w2test
GPU=${TEST_GPU:-1}
YOLO=(-c /workspace/configs/pgie_yolo11_coco.txt -p 0)
FACE=(-S /workspace/configs/sgie1_face_detect.txt -S /workspace/configs/sgie2_face_embed.txt -F 0.42)
# Vùng ghế mặc định cho dwell_sitting_180s.mp4: bao chỗ cô gái ngồi trên gờ tường (chân ở x=0.04, y=0.75),
# không bao người đàn ông đứng chờ ngay cạnh (chân ở x=0.074, y=0.71). Toạ độ 0..1 theo khung hình
ROI_DEFAULT='[[0,0.725],[0.065,0.725],[0.065,0.8],[0,0.8]]'
mkdir -p "$OUT" videos/.test

cli() { ./scripts/redis_dev.sh cli "$@"; }
header() { echo "=== $1 ==="; echo "Thời điểm: $(date '+%H:%M:%S %d/%m/%Y') · máy: $(hostname) · GPU test: $GPU"; echo; }

# pub <tên> <file trong videos/> [once]: phát lên MediaMTX rtsp://127.0.0.1:8554/w2-<tên> (lặp lại, trừ khi once)
pub() {
  local loop=(-stream_loop -1); [[ "${3:-}" == once ]] && loop=()
  docker rm -f "w2pub_$1" >/dev/null 2>&1 || true
  docker run -d --name "w2pub_$1" --network host -v "$ROOT/videos:/videos:ro" "$FFMPEG_IMAGE" -hide_banner \
    -loglevel error -re "${loop[@]}" -i "/videos/$2" -an -c:v copy -f rtsp -rtsp_transport tcp "$RTSP/w2-$1" >/dev/null
}
unpub() { docker ps -aq --filter "name=^/w2pub_" | xargs -r docker rm -f >/dev/null; }

# Engine YOLO cho GPU test: GPU 0 và 1 cùng loại (RTX 4070 Ti SUPER) nên dùng lại được engine đã build trên GPU 0
yolo_engine() {
  docker exec deepstream_dev bash -c "d=/workspace/models/yolo11s_engine; [[ -f \$d/model_b$1_gpu$GPU\_fp16.engine ]] \
    || { [[ -f \$d/model_b$1_gpu0_fp16.engine ]] && cp \$d/model_b$1_gpu0_fp16.engine \$d/model_b$1_gpu$GPU\_fp16.engine; } || true"
}

# run_app <giây> <log> <tham số...>: app test chạy rồi nhận SIGINT (chỉ tiến trình này), 30 s không tắt thì kill
run_app() {
  local sec=$1 log=$2; shift 2
  docker exec deepstream_dev rm -f "/outputs/logs/$log"
  echo "... app test chạy ${sec}s trên GPU $GPU (log: outputs/logs/$log)"
  docker exec -e LANG=C.UTF-8 -e REDIS_PASSWORD="${REDIS_PASSWORD:-}" deepstream_dev \
    timeout -s INT -k 30 "$sec" "$APP" -g "$GPU" -o "$TDIR" -l "/outputs/logs/$log" -k 0 -K 0 "$@" >/dev/null 2>&1 || true
}
stats_line() { grep '\[stats\]' "outputs/logs/$1" | tail -1 | cut -c1-230; }

# Python có redis (đọc stream / consumer group thay platform)
py() { docker run --rm -i --network host -e REDIS_PASSWORD="${REDIS_PASSWORD:-}" -e BIND="${REDIS_BIND:-127.0.0.1}" \
  -v "$ROOT/outputs:/outputs" "$PY_IMAGE" python - "$@"; }
ensure_py_image() { docker image inspect "$PY_IMAGE" >/dev/null 2>&1 || docker build -q -t "$PY_IMAGE" uploader >/dev/null; }

# ---------------------------------------------------------------- Task 2.1
test_platform() {
  header "2.1 Platform đọc sự kiện bằng consumer group (prefix w2plat)"
  ensure_py_image
  cli DEL w2plat:events w2plat:frames >/dev/null
  cli XGROUP CREATE w2plat:events platform '$' MKSTREAM >/dev/null
  echo "Consumer group 'platform' tạo trên w2plat:events TRƯỚC khi app chạy (như platform thật)"
  pub dwell dwell_sitting_180s.mp4; pub walk sample_1080p_h264.mp4; yolo_engine 2; sleep 3
  run_app 80 bc_w2_platform.log -P w2plat "${YOLO[@]}" -t 20 -r 40 -A "$RTSP/w2-dwell" "$RTSP/w2-walk"
  unpub
  stats_line bc_w2_platform.log; echo
  sleep 15   # clip_ready của cảnh báo cuối (ghi thêm 10 s sau cảnh báo) đã gửi lúc app tắt
  py <<'EOF'
import json, os, redis
r = redis.Redis(host=os.environ["BIND"], port=6390, password=os.environ["REDIS_PASSWORD"], decode_responses=True)
msgs = [m for _, ms in (r.xreadgroup("platform", "platform-1", {"w2plat:events": ">"}, count=100000) or []) for m in ms]
for mid, _ in msgs:
    r.xack("w2plat:events", "platform", mid)
ev = [(f["type"], json.loads(f["data"])) for _, f in msgs]
no_ver = [t for t, d in ev if d.get("schema_version") != 2]
from collections import Counter
print(f"Consumer 'platform-1' nhận {len(ev)} sự kiện: {dict(Counter(t for t, _ in ev))}")
print(f"Sự kiện thiếu schema_version=2: {len(no_ver)}")
print(f"Còn pending chưa ACK: {r.xpending('w2plat:events', 'platform')['pending']}")
print("\n--- Từng lượt ở yên (dwell_alert -> dwell_end -> clip_ready):")
ok = 0
for t, d in ev:
    if t != "dwell_alert":
        continue
    end = next((x for tt, x in ev if tt == "dwell_end" and x["cam"] == d["cam"] and x["tid"] == d["tid"] and x["start_ms"] == d["start_ms"]), None)
    tag = (d.get("clip") or "").replace("pending:", "")
    clip = next((x for tt, x in ev if tt == "clip_ready" and x.get("tag") == tag), None)
    full = bool(end and clip)
    ok += full
    print(f"  {d['cam']:9} người #{d['tid']:<5} alert {d['alert_ts'][11:19]}  end: {end['reason'] if end else 'THIẾU':9} "
          f"clip: {os.path.basename(clip['file']) if clip else 'THIẾU'}")
print(f"\n{ok} lượt đủ 3 sự kiện. {'ĐẠT' if ok and not no_ver else 'KHÔNG ĐẠT'}")
print("\n--- 1 sự kiện dwell_alert đầy đủ:")
print(json.dumps(next((d for t, d in ev if t == "dwell_alert"), {}), ensure_ascii=False, indent=1)[:1500])
EOF
}

test_upload() {
  header "2.1 Ảnh/clip upload lên S3/MinIO, sự kiện mang URL (prefix w2up)"
  [[ -f .s3.env ]] || { echo "Chưa có .s3.env: ./scripts/upload.sh minio && ./scripts/upload.sh start" >&2; exit 1; }
  set -a; . ./.s3.env; set +a
  ensure_py_image
  cli DEL w2up:events w2up:frames >/dev/null
  UPLOADER_NAME=ds_uploader_w2up PREFIX=w2up ./scripts/upload.sh start
  # File giả 40 ngày tuổi: 1 cái đã upload (.up), 1 cái chưa. App test (-k 30, -M) chỉ được xoá cái đã upload
  docker exec deepstream_dev bash -c 'mkdir -p /outputs/w2test/w1_snapshots && cd /outputs/w2test/w1_snapshots && for f in w2-up_gia_da_upload_frame.jpg w2-up_gia_chua_upload_frame.jpg; do
    head -c 2000 /dev/urandom > $f; touch -d "40 days ago" $f; done; touch w2-up_gia_da_upload_frame.jpg.up'
  pub dwell dwell_sitting_180s.mp4; pub walk sample_1080p_h264.mp4; yolo_engine 2; sleep 3
  docker exec deepstream_dev rm -f /outputs/logs/bc_w2_upload.log
  echo "... app test chạy 80s trên GPU $GPU với -M $MEDIA_BASE -k 30"
  docker exec -e LANG=C.UTF-8 -e REDIS_PASSWORD="${REDIS_PASSWORD:-}" deepstream_dev timeout -s INT -k 30 80 "$APP" \
    -g "$GPU" -o "$TDIR" -l /outputs/logs/bc_w2_upload.log -P w2up -M "$MEDIA_BASE" -k 30 -K 0 "${YOLO[@]}" -t 20 -r 40 -A \
    "$RTSP/w2-dwell" "$RTSP/w2-walk" >/dev/null 2>&1 || true
  unpub
  grep -E "Upload ảnh|dọn file" outputs/logs/bc_w2_upload.log || true
  stats_line bc_w2_upload.log; echo
  echo "... chờ uploader upload xong (tối đa 2 phút)"
  for _ in $(seq 60); do
    [[ "$(cli HGET w2up:uploader pending)" == 0 && -n "$(cli XLEN w2up:events | grep -v '^0$')" ]] && break; sleep 2
  done
  ./scripts/upload.sh logs 3 2>/dev/null | sed 's/^/  uploader: /' || true
  UPLOADER_NAME=ds_uploader_w2up ./scripts/upload.sh logs 3 | sed 's/^/  uploader test: /'
  py "/$OUT" <<'EOF'
import json, os, re, sys, urllib.request, redis
out = sys.argv[1]
r = redis.Redis(host=os.environ["BIND"], port=6390, password=os.environ["REDIS_PASSWORD"], decode_responses=True)
urls = []
for _, f in r.xrange("w2up:events"):
    d = json.loads(f["data"])
    for k, v in d.items():
        if k.endswith("_url"):
            urls.append((f["type"], k, v))
print(f"\n--- {len(urls)} URL trong sự kiện, mở thử từng URL:")
ok, saved = 0, {}
for t, k, u in urls:
    try:
        with urllib.request.urlopen(u, timeout=10) as resp:
            body = resp.read()
            code, ctype = resp.status, resp.headers.get("Content-Type")
    except Exception as e:
        code, ctype, body = getattr(e, "code", "LỖI"), "", b""
    ok += code == 200
    print(f"  {code} {ctype:16} {len(body):>9} B  {t:14} {k:15} {u}")
    ext = ".mp4" if u.endswith(".mp4") else ".jpg"
    if code == 200 and ext not in saved and (ext == ".jpg" and "frame" in u or ext == ".mp4"):
        saved[ext] = f"{out}/2_upload_tai_ve_qua_url{ext}"
        open(saved[ext], "wb").write(body)
print(f"\n{ok}/{len(urls)} URL mở được (HTTP 200). Đã tải qua URL: {', '.join(saved.values())}")
print(f"Uploader: {r.hgetall('w2up:uploader')}")
EOF
  echo
  echo "--- Dọn file local (-k 30 ngày, file giả 40 ngày tuổi):"
  for f in w2-up_gia_da_upload_frame.jpg w2-up_gia_chua_upload_frame.jpg; do
    [[ -f outputs/w2test/w1_snapshots/$f ]] && echo "  CÒN   $f" || echo "  ĐÃ XOÁ $f"
  done
  echo "  Đạt khi: file đã upload bị xoá, file chưa upload còn (thư mục riêng của bài test: outputs/w2test/)"
  UPLOADER_NAME=ds_uploader_w2up ./scripts/upload.sh stop
}

test_ngat_platform() {
  local outage=${OUTAGE:-300}
  header "2.1 Ngắt Redis + S3 ${outage}s không mất sự kiện (Redis test :6391 có AOF, MinIO test :9010, prefix w2ngat)"
  ensure_py_image
  local env="$OUT/.s3_ngat.env"
  docker rm -f ds_redis_w2ngat >/dev/null 2>&1 || true
  docker run -d --name ds_redis_w2ngat --network host redis:8.8-alpine redis-server --port 6391 --bind 127.0.0.1 \
    --requirepass "$REDIS_PASSWORD" --appendonly yes >/dev/null
  S3_ENV="$env" MINIO_NAME=ds_minio_w2ngat MINIO_PORT=9010 ./scripts/upload.sh minio
  sleep 2
  local rc=(docker exec -e REDISCLI_AUTH="$REDIS_PASSWORD" ds_redis_w2ngat redis-cli -p 6391 --no-auth-warning)
  "${rc[@]}" XGROUP CREATE w2ngat:events platform '$' MKSTREAM >/dev/null
  S3_ENV="$env" UPLOADER_NAME=ds_uploader_w2ngat PREFIX=w2ngat REDIS=127.0.0.1:6391 ./scripts/upload.sh start
  set -a; . "$env"; set +a
  pub dwell dwell_sitting_180s.mp4; pub walk sample_1080p_h264.mp4; yolo_engine 2; sleep 3
  local total=$((outage + 160)) log=bc_w2_ngat.log
  docker exec deepstream_dev rm -f /outputs/logs/$log
  docker exec -d -e LANG=C.UTF-8 -e REDIS_PASSWORD="$REDIS_PASSWORD" deepstream_dev timeout -s INT -k 30 $total "$APP" \
    -g "$GPU" -o "$TDIR" -l /outputs/logs/$log -R 127.0.0.1:6391 -P w2ngat -M "$MEDIA_BASE" -k 0 -K 0 "${YOLO[@]}" -t 20 -r 40 -A \
    "$RTSP/w2-dwell" "$RTSP/w2-walk"
  echo "... app test chạy ${total}s; ngắt sau 50s"
  sleep 50
  echo "$(date +%T) trước khi ngắt: $(stats_line $log | grep -o 'redis=.*events=[0-9/]*')"
  docker stop ds_redis_w2ngat ds_minio_w2ngat >/dev/null
  echo "$(date +%T) ĐÃ NGẮT Redis + MinIO (docker stop)"
  for ((s = 60; s <= outage; s += 60)); do
    sleep $((s + 60 > outage ? outage - s + 60 : 60)); echo "$(date +%T) đang ngắt: $(stats_line $log | grep -o 'redis=.*events=[0-9/]*')"
  done
  docker start ds_redis_w2ngat ds_minio_w2ngat >/dev/null
  echo "$(date +%T) ĐÃ BẬT LẠI Redis + MinIO"
  sleep 30; echo "$(date +%T) sau 30s: $(stats_line $log | grep -o 'redis=.*events=[0-9/]*')"
  while docker exec deepstream_dev pgrep -f "$log" >/dev/null; do sleep 3; done
  unpub
  echo "$(date +%T) app test đã tắt. Dòng [stats] cuối:"; stats_line $log
  grep -E "\[redis\]" outputs/logs/$log | head -4 | sed 's/^/  log: /'
  echo "... chờ uploader upload bù (tối đa 3 phút)"
  for _ in $(seq 90); do [[ "$("${rc[@]}" HGET w2ngat:uploader pending)" == 0 ]] && break; sleep 2; done
  S3_ENV="$env" UPLOADER_NAME=ds_uploader_w2ngat ./scripts/upload.sh logs 6 | sed 's/^/  uploader: /'
  local created sent
  created=$(stats_line $log | grep -o 'events=[0-9]*/[0-9]*' | cut -d/ -f2); sent=$(stats_line $log | grep -o 'events=[0-9]*' | cut -d= -f2)
  docker run --rm -i --network host -e P="$REDIS_PASSWORD" -e CREATED="$created" -e SENT="$sent" "$PY_IMAGE" python - <<'EOF'
import json, os, urllib.request, redis
r = redis.Redis(port=6391, password=os.environ["P"], decode_responses=True)
n = r.xlen("w2ngat:events")
got = [m for _, ms in (r.xreadgroup("platform", "p1", {"w2ngat:events": ">"}, count=100000) or []) for m in ms]
urls = [v for _, f in r.xrange("w2ngat:events") for k, v in json.loads(f["data"]).items() if k.endswith("_url")]
ok = 0
for u in urls:
    try:
        ok += urllib.request.urlopen(u, timeout=10).status == 200
    except Exception:
        pass
created = int(os.environ["CREATED"] or 0)
print(f"\nApp tạo {created} sự kiện, đã gửi {os.environ['SENT']}; trong Redis {n}; consumer group platform nhận {len(got)}")
print(f"Ảnh/clip: {ok}/{len(urls)} URL mở được sau khi upload bù; uploader {r.hgetall('w2ngat:uploader')}")
print("ĐẠT" if created and created == n == len(got) and ok == len(urls) else "KHÔNG ĐẠT")
print("(drop trong [stats] là metadata frame bị bỏ khi Redis chết lâu - thiết kế cố ý; sự kiện không bao giờ bị bỏ)")
EOF
  S3_ENV="$env" UPLOADER_NAME=ds_uploader_w2ngat ./scripts/upload.sh stop
  docker rm -f ds_redis_w2ngat ds_minio_w2ngat >/dev/null
}

# ---------------------------------------------------------------- Task 2.2
test_model_nguoi() {
  header "2.2 So model detect người: TrafficCamNet / PeopleNet / YOLO11s"
  docker exec deepstream_dev make -s -C /workspace/apps/task13_metadata
  # YOLO batch 1 cho task13: engine lưu vào models/yolo11s_engine (chạy trong thư mục đó)
  sed 's#model_b4_gpu0_fp16#model_b1_gpu0_fp16#' configs/pgie_yolo11_coco.txt > "$OUT/.pgie_yolo_b1.txt"
  local models=("TrafficCamNet|/workspace/configs/task13_pgie.txt|2" "PeopleNet|/workspace/configs/pgie_peoplenet.txt|0"
                "YOLO11s|/workspace/outputs/bao_cao_tuan2/.pgie_yolo_b1.txt|0")
  for m in "${models[@]}"; do
    IFS='|' read -r name cfg cls <<<"$m"
    for v in dwell_sitting_180s sample_office; do
      echo "... $name trên $v.mp4 (lần đầu build engine vài phút)"
      docker exec -w /workspace/models/yolo11s_engine -e LANG=C.UTF-8 deepstream_dev /workspace/apps/task13_metadata/task13-meta-app \
        -c "$cfg" -p 100000 -o "/outputs/bao_cao_tuan2/.det_${name}_$v.jsonl" "file:///workspace/videos/$v.mp4" >/dev/null 2>&1 || true
    done
  done
  python3 - "$OUT" <<'EOF'
import json, sys
from PIL import Image, ImageDraw, ImageFont
out = sys.argv[1]
models = [("TrafficCamNet", 2), ("PeopleNet", 0), ("YOLO11s", 0)]
def load(name, v):
    try:
        return [json.loads(l) for l in open(f"{out}/.det_{name}_{v}.jsonl")]
    except OSError:
        return []
# Người ngồi trên gờ tường ở góc trái dưới (chân x < 0.12 khung, y > 0.72): có trong video từ giây 0 đến hết
# (khung thấp < 260 px: người ngồi; người đứng chờ ngay cạnh chân ở y = 0.71, khung cao ~305 px nên không tính)
sit = lambda o: (o["bbox"][0] + o["bbox"][2] / 2) < 0.12 * 1920 and (o["bbox"][1] + o["bbox"][3]) > 0.72 * 1080 and o["bbox"][3] < 260
print(f"{'Model':14}| {'dwell_sitting_180s.mp4 (người ngồi có mặt suốt 180 s)':58}| sample_office.mp4")
print(f"{'':14}| {'% frame thấy người ngồi':24} {'thấy lần đầu':14} {'người/frame':12}| người/frame  % frame có người")
for name, cls in models:
    d, o = load(name, "dwell_sitting_180s"), load(name, "sample_office")
    if not d:
        print(f"{name:14}| LỖI: không có kết quả (xem log)"); continue
    hit = [f for f in d if any(x["cls"] == cls and sit(x) for x in f["objs"])]
    first = hit[0]["pts_ms"] / 1000 if hit else None
    per = sum(sum(x["cls"] == cls for x in f["objs"]) for f in d) / len(d)
    oper = sum(sum(x["cls"] == cls for x in f["objs"]) for f in o) / max(1, len(o))
    ohas = 100 * sum(any(x["cls"] == cls for x in f["objs"]) for f in o) / max(1, len(o))
    print(f"{name:14}| {100 * len(hit) / len(d):6.1f} %{'':16} {('%.1f s' % first) if first is not None else 'không thấy':14} {per:6.2f}{'':6}| {oper:6.2f}       {ohas:5.1f} %")
# Ảnh minh chứng: frame ở giây 10, khung người của từng model
import subprocess
subprocess.run(["docker", "run", "--rm", "-v", f"{sys.path[0] or '.'}:/x", "-v", "/home/hiepplh/deepstream/videos:/v:ro", "-v",
                f"/home/hiepplh/deepstream/{out}:/o", "--entrypoint", "ffmpeg", "linuxserver/ffmpeg:latest", "-v", "error", "-y", "-ss", "10",
                "-i", "/v/dwell_sitting_180s.mp4", "-frames:v", "1", "/o/.frame10.jpg"], check=False)
base = Image.open(f"{out}/.frame10.jpg").convert("RGB")
tiles = []
for name, cls in models:
    im = base.copy(); dr = ImageDraw.Draw(im)
    d = load(name, "dwell_sitting_180s")
    f = min(d, key=lambda f: abs(f["pts_ms"] - 10000)) if d else {"objs": []}
    for x in f["objs"]:
        if x["cls"] == cls:
            l, t, w, h = x["bbox"]; dr.rectangle([l, t, l + w, t + h], outline=(255, 40, 40) if sit(x) else (40, 255, 40), width=5)
    dr.rectangle([0, 0, 1920, 90], fill=(0, 0, 0)); dr.text((20, 12), f"{name}: {sum(x['cls'] == cls for x in f['objs'])} người (đỏ = người ngồi)", fill=(255, 255, 255),
            font=ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 56))
    tiles.append(im.resize((960, 540)))
canvas = Image.new("RGB", (960, 540 * 3)); [canvas.paste(t, (0, 540 * i)) for i, t in enumerate(tiles)]
canvas.save(f"{out}/4_model_nguoi_giay10.jpg", quality=85)
print(f"\nẢnh so sánh (giây 10, khung đỏ = người ngồi): {out}/4_model_nguoi_giay10.jpg")
EOF
}

# Danh sách cam cho -i: <công ty> <toà> <cam_id> <uri> <fps> <JSON thông tin, có "roi" nếu có vùng>
cam_line() { echo "w2 T $1 $2 0 {\"camera_id\":\"$1\",\"camera_name\":\"$1\"${3:+,\"roi\":$3}}"; }

roi_from_sites() {
  python3 - <<'EOF' 2>/dev/null || true
import json
for co in json.load(open("configs/sites.json"))["companies"]:
    for b in co["buildings"]:
        for c in b["cameras"]:
            if c.get("roi") and c.get("video") == "dwell_sitting_180s.mp4":
                print(json.dumps(c["roi"], separators=(",", ":"))); raise SystemExit
EOF
}

test_roi() {
  header "2.2 Vùng ghế (ROI): chỉ cảnh báo người trong vùng (prefix w2roi)"
  local roi; roi=$(roi_from_sites); local src="vẽ trên dashboard master (sites.json)"
  [[ -n "$roi" ]] || { roi=$ROI_DEFAULT; src="mặc định của bài test (chưa có camera nào dùng dwell_sitting_180s.mp4 có vùng ghế)"; }
  echo "Vùng ghế: $roi ($src)"
  { cam_line w2-coroi "$RTSP/w2-dwell" "$roi"; cam_line w2-khongroi "$RTSP/w2-dwell"; } > "$OUT/.roi_cams.txt"
  cli DEL w2roi:events w2roi:frames >/dev/null
  pub dwell dwell_sitting_180s.mp4; yolo_engine 2; sleep 3
  run_app 75 bc_w2_roi.log -P w2roi "${YOLO[@]}" -t 20 -A -i /workspace/outputs/bao_cao_tuan2/.roi_cams.txt
  unpub
  grep "Vùng ghế" outputs/logs/bc_w2_roi.log || true
  python3 - "$OUT" "$roi" <<'EOF'
import json, subprocess, sys
from PIL import Image, ImageDraw
out, roi = sys.argv[1], json.loads(sys.argv[2])
raw = subprocess.run(["./scripts/redis_dev.sh", "cli", "XRANGE", "w2roi:events", "-", "+"], capture_output=True, text=True).stdout
ev = [json.loads(l) for l in raw.splitlines() if l.startswith("{")]
who = lambda b: "người NGỒI (gờ tường)" if b[0] < 60 and b[3] < 260 else "người ĐỨNG chờ cạnh ghế" if b[0] < 160 and b[3] > 260 else "người khác"
res = {}
for cam in ("w2-coroi", "w2-khongroi"):
    al = [d for d in ev if d["type"] == "dwell_alert" and d["cam"] == cam]
    res[cam] = al
    print(f"\n{cam} ({'có' if cam == 'w2-coroi' else 'không có'} vùng ghế): {len(al)} cảnh báo")
    for d in al:
        print(f"  người #{d['tid']:<4} bbox {d['bbox']}  -> {who(d['bbox'])}  ở yên {d['dwell_s']}s")
inroi = [d for d in res["w2-coroi"]]
ok = inroi and all(who(d["bbox"]).startswith("người NGỒI") for d in inroi) and any(who(d["bbox"]).startswith("người ĐỨNG") for d in res["w2-khongroi"])
print("\nĐẠT: trong vùng chỉ cảnh báo người ngồi, người đứng chờ cạnh đó (bị cảnh báo khi không có vùng) không còn bị cảnh báo" if ok else "\nKHÔNG ĐẠT")
snap = next((d.get("snapshot") for d in inroi if d.get("snapshot")), None)
if snap:
    im = Image.open(snap.replace("/outputs/", "outputs/")).convert("RGB"); dr = ImageDraw.Draw(im, "RGBA")
    dr.polygon([(x * im.width, y * im.height) for x, y in roi], outline=(242, 179, 61, 255), fill=(242, 179, 61, 70), width=4)
    for d in inroi:
        l, t, w, h = d["bbox"]; dr.rectangle([l, t, l + w, t + h], outline=(240, 82, 79), width=5)
    im.crop((0, 340, 960, 880)).save(f"{out}/5_roi_canh_bao.jpg", quality=90)
    print(f"Ảnh: {out}/5_roi_canh_bao.jpg (vàng = vùng ghế, đỏ = người bị cảnh báo)")
EOF
}

test_do_lech() {
  local T=30 LEAD=40
  header "2.2 Độ lệch thời gian cảnh báo trên 19 lượt có gán nhãn (ngưỡng ${T}s, prefix w2lech)"
  local roi; roi=$(roi_from_sites); [[ -n "$roi" ]] || roi=$ROI_DEFAULT
  # Lượt có nhãn: màn đen (LEAD + k giây) rồi cảnh có người ngồi. 10 lượt dương: ngồi T+15 s -> phải cảnh báo đúng lúc
  # ngồi đủ T s. 9 lượt âm: ngồi T-12 s rồi đi (màn đen) -> không được cảnh báo. Người đứng ngoài vùng: không được cảnh báo
  echo "lượt,video,giây bắt đầu ngồi (trong video),thời gian ngồi (s),phải cảnh báo" > "$OUT/6_do_lech_nhan.csv"
  local list="$OUT/.lech_cams.txt"; : > "$list"
  for i in $(seq 1 19); do
    local k=$(( (i - 1) % 10 * 2 + 3 )) pos=1 dur=$((T + 15)) name
    (( i > 10 )) && { pos=0; dur=$((T - 12)); }
    name=lech_$( ((pos)) && echo p || echo n )$i.mp4
    if [[ ! -f videos/.test/$name ]]; then
      echo "... tạo videos/.test/$name (đen $((LEAD + k))s + ngồi ${dur}s + đen 15s)"
      docker run --rm -v "$ROOT/videos:/v" --entrypoint ffmpeg "$FFMPEG_IMAGE" -v error -y \
        -f lavfi -t $((LEAD + k)) -i color=black:s=1920x1080:r=30 -t $dur -i /v/dwell_sitting_180s.mp4 \
        -f lavfi -t 15 -i color=black:s=1920x1080:r=30 -filter_complex "[0:v][1:v][2:v]concat=n=3:v=1:a=0,format=yuv420p" \
        -r 30 -c:v libx264 -preset veryfast -g 30 "/v/.test/$name"
    fi
    echo "$i,$name,$((LEAD + k)),$dur,$( ((pos)) && echo có || echo không )" >> "$OUT/6_do_lech_nhan.csv"
    cam_line "w2-lech$i" "$RTSP/w2-lech$i" "$roi" >> "$list"
  done
  cli DEL w2lech:events w2lech:frames >/dev/null
  yolo_engine 19
  for i in $(seq 1 19); do pub "lech$i" ".test/$(cut -d, -f2 <<<"$(sed -n "$((i + 1))p" "$OUT/6_do_lech_nhan.csv")")" once; done
  sleep 2
  run_app $((LEAD + 21 + T + 15 + 30)) bc_w2_lech.log -P w2lech -L 300000 "${YOLO[@]}" -t $T -A -i /workspace/outputs/bao_cao_tuan2/.lech_cams.txt
  unpub
  stats_line bc_w2_lech.log
  python3 - "$OUT" "$T" <<'EOF'
import csv, json, subprocess, sys
out, T = sys.argv[1], int(sys.argv[2])
cli = lambda *a: subprocess.run(["./scripts/redis_dev.sh", "cli", *a], capture_output=True, text=True).stdout
ev = [json.loads(l) for l in cli("XRANGE", "w2lech:events", "-", "+").splitlines() if l.startswith("{")]
first = {}
for l in cli("XRANGE", "w2lech:frames", "-", "+").splitlines():
    if l.startswith("{"):
        f = json.loads(l)
        if f["num_obj"] and f["cam"] not in first:
            first[f["cam"]] = f["ts_ms"]     # khung đầu tiên hết màn đen (có object) = lúc người bắt đầu ngồi
rows = list(csv.DictReader(open(f"{out}/6_do_lech_nhan.csv")))
print(f"\n{'lượt':5}{'nhãn':10}{'bắt đầu ngồi':14}{'cảnh báo lúc':14}{'lệch (s)':10}kết quả")
bad = fa = 0; devs = []
for r in rows:
    cam = f"w2-lech{r['lượt']}"; must = r["phải cảnh báo"] == "có"
    al = [d for d in ev if d["type"] == "dwell_alert" and d["cam"] == cam]
    t0 = first.get(cam)
    if must:
        if not al or not t0:
            bad += 1; print(f"{r['lượt']:5}{'ngồi':10}{'-':14}{'KHÔNG CÓ':14}{'-':10}SAI (bỏ sót)"); continue
        a = al[0]
        dev = (a["start_ms"] + a["dwell_s"] * 1000 - t0) / 1000 - T     # lúc cảnh báo - lúc bắt đầu ngồi - ngưỡng
        devs.append(abs(dev)); extra = len(al) - 1; fa += extra
        ok = abs(dev) <= 5 and not extra; bad += not ok
        print(f"{r['lượt']:5}{'ngồi':10}{'0.0 s':14}{dev + T:8.1f} s{'':5}{dev:+7.1f}   {'ĐÚNG' if ok else 'SAI'}{f' (+{extra} cảnh báo thừa)' if extra else ''}")
    else:
        fa += len(al); bad += bool(al)
        print(f"{r['lượt']:5}{'ngồi ngắn':10}{'0.0 s':14}{(str(len(al)) + ' cảnh báo') if al else 'không':14}{'-':10}{'SAI (cảnh báo nhầm)' if al else 'ĐÚNG'}")
print(f"\nLệch lớn nhất {max(devs) if devs else 0:.1f} s, trung bình {sum(devs) / max(1, len(devs)):.1f} s (cần <= 5 s); cảnh báo nhầm: {fa}")
print("ĐẠT" if not bad and not fa and len(devs) >= 10 else "KHÔNG ĐẠT")
EOF
}

# ---------------------------------------------------------------- Task 2.3
test_khuon_mat() {
  header "2.3 Khuôn mặt: đăng ký từ camera -> nhận ra; video khác không nhận nhầm (prefix w2face, ngưỡng 0.42)"
  [[ -f videos/.test/sample_ride_bike.mov ]] || docker cp -q deepstream_dev:/opt/nvidia/deepstream/deepstream/samples/streams/sample_ride_bike.mov videos/.test/
  docker exec -i -e REDIS_PASSWORD="$REDIS_PASSWORD" -e BIND="${REDIS_BIND:-127.0.0.1}" ds_web python - <<'EOF'
import os, redis
r = redis.Redis(host=os.environ["BIND"], port=6390, password=os.environ["REDIS_PASSWORD"])
for k in r.scan_iter("w2face:*"):
    r.delete(k)
EOF
  pub bike .test/sample_ride_bike.mov; pub walk sample_walk.mov; yolo_engine 2; sleep 3
  echo "Lượt 1: chưa ai đăng ký -> app gửi mặt lạ lên w2face:faces_unknown"
  run_app 60 bc_w2_face1.log -P w2face "${YOLO[@]}" "${FACE[@]}" -A "$RTSP/w2-bike" "$RTSP/w2-walk"
  docker exec -i -e REDIS_PASSWORD="$REDIS_PASSWORD" -e BIND="${REDIS_BIND:-127.0.0.1}" ds_web python - <<'EOF'
import os, sys, redis
sys.path.insert(0, "/app")
import faces
r = redis.Redis(host=os.environ["BIND"], port=6390, password=os.environ["REDIS_PASSWORD"], decode_responses=True)
g = faces.Gallery(r, "w2face")
unk = [(mid, f) for mid, f in r.xrange("w2face:faces_unknown") if f["cam"] == "w2-bike"]
print(f"  mặt lạ: {r.xlen('w2face:faces_unknown')} (cam w2-bike: {len(unk)})")
if unk:
    p = g.save("Người đạp xe (test)", "vip", "", [g.unknown(unk[0][0])["vec"]], None, "camera")
    print(f"  đăng ký '{p['name']}' (VIP) từ mặt lạ {unk[0][0]}, id {p['id']}")
EOF
  echo "Lượt 2: phải nhận ra người đạp xe; cam w2-walk (người khác, mặt rõ) không được báo"
  run_app 60 bc_w2_face2.log -P w2face "${YOLO[@]}" "${FACE[@]}" -A "$RTSP/w2-bike" "$RTSP/w2-walk"
  unpub
  grep -E "\[face\]|KHÔNG MẶT|KHUÔN MẶT" outputs/logs/bc_w2_face2.log | head -5
  stats_line bc_w2_face2.log
  python3 - <<'EOF'
import json, subprocess
raw = subprocess.run(["./scripts/redis_dev.sh", "cli", "XRANGE", "w2face:events", "-", "+"], capture_output=True, text=True).stdout
ev = [json.loads(l) for l in raw.splitlines() if l.startswith("{") and '"face_recognized"' in l]
bike = [d for d in ev if d["cam"] == "w2-bike"]; cam6 = [d for d in ev if d["cam"] == "w2-walk"]
fr = subprocess.run(["./scripts/redis_dev.sh", "cli", "XRANGE", "w2face:frames", "-", "+"], capture_output=True, text=True).stdout
n6 = sum(l.count('"label":"face"') for l in fr.splitlines() if '"cam":"w2-walk"' in l)
print(f"Số mặt (gie 2) ở w2-walk đã đem so khớp: {n6}")
for d in ev:
    print(f"  {d['cam']:8} người #{d['tid']} = {d['name']} ({d['tier']}), cosine {d['score']}, ảnh {d.get('face_crop')}")
print(f"\nNhận ra ở w2-bike: {len(bike)}; nhận nhầm ở w2-walk: {len(cam6)}")
print("ĐẠT" if bike and not cam6 and n6 else "KHÔNG ĐẠT")
EOF
}

# ---------------------------------------------------------------- Task 2.4
test_tu_hoi_phuc() {
  header "2.4 Tự hồi phục: xoá ds_mediamtx + kill app (hệ thống THẬT, prefix ds)"
  grep -q '"auto_heal": false' configs/app.json && { echo "Tự hồi phục đang tắt (dashboard master)"; exit 1; }
  local t0 oldpid; t0=$(date +%s)
  oldpid=$(cli GET ds:stats:g0 | python3 -c "import sys,json; print(json.loads(sys.stdin.read() or '{}').get('pid', ''))")
  echo "$(date +%T) xoá ds_mediamtx (mọi camera giả lập mất luồng) + kill -9 w1-app (pid trong container $oldpid)"
  docker rm -f ds_mediamtx >/dev/null
  docker exec deepstream_dev pkill -KILL -f '^/workspace/apps/w1_multistream/w1-app -i /workspace/configs/cameras_sim.txt' || true
  local up_at="" comp_at="" last=""
  while (( $(date +%s) - t0 < 240 )); do
    sleep 3
    local now=$(( $(date +%s) - t0 ))
    local comps; comps=$(python3 -c "import json; s=json.load(open('configs/.master/status.json')); print(' '.join(c['key']+('=ok' if c['ok'] else '=CHẾT') for c in s.get('components', [])), '|', s.get('action'), s.get('state'))")
    # Chỉ tính thống kê của tiến trình app MỚI (pid khác), ghi sau lúc ngắt: thống kê cũ còn hạn 60 s trong Redis
    local st; st=$(cli GET ds:stats:g0 | python3 -c "import sys,json; d=json.loads(sys.stdin.read() or '{}'); print(f\"{d['cams_online']}/{d['cams_total']} cam\" if d and str(d.get('pid')) != '$oldpid' and d['ts'] > $t0 * 1000 else 'app mới chưa gửi thống kê')" 2>/dev/null)
    [[ "$comps $st" != "$last" ]] && echo "+${now}s  $comps  ·  $st"; last="$comps $st"
    [[ -z "$comp_at" && "$comps" != *CHẾT* && "$st" != *"chưa gửi"* ]] && comp_at=$now
    if [[ "$st" == "19/19 cam" ]]; then up_at=$now; break; fi
  done
  echo
  echo "--- Log agent (dashboard master, khung log):"
  python3 -c "import json; print('\n'.join(json.load(open('configs/.master/status.json'))['log'][-12:]))"
  echo; echo "--- Sự kiện system_heal trong ds:events:"
  cli XREVRANGE ds:events + - COUNT 50 | grep '"system_heal"' | head -2
  echo
  echo "Mọi thành phần chạy lại + app mới gửi thống kê: sau ${comp_at:-?}s (cần <= 60 s)"
  echo "Về 19/19 cam: $([[ -n "$up_at" ]] && echo "sau ${up_at}s" || echo "chưa (4 phút)"). 5 cam giả lập phát video 2 fps, GOP dài (sample_cam5/cam6) chờ khung hình đầu"
  echo "  thêm ~40 s sau khi nối lại; camera thật GOP 1-2 s thì lên cùng lúc các cam khác"
  [[ -n "$comp_at" && $comp_at -le 60 && -n "$up_at" ]] && echo "ĐẠT" || echo "KHÔNG ĐẠT"
}

test_grafana() {
  header "2.4 Metric Prometheus + dashboard Grafana"
  local b=${REDIS_BIND:-127.0.0.1}
  ./scripts/monitor.sh status
  echo; echo "--- Một phần /metrics (http://$b:9466/metrics):"
  curl -s "$b:9466/metrics" | grep -E '^ds_(app_up|fps_total|cams_online|cams_total|redis_queue|messages_dropped|events_created|app_rss|app_vram|upload_pending)' | grep 'prefix="ds"\|^ds_upload' | head -12
  echo; echo "--- Prometheus (http://$b:9091) đã lưu bao lâu:"
  curl -s "$b:9091/api/v1/query?query=count_over_time(ds_cams_online%7Bprefix%3D%22ds%22%7D%5B24h%5D)" | python3 -c "import sys,json; r=json.load(sys.stdin)['data']['result']; print(f'{int(r[0][\"value\"][1]) * 5 // 60} phút số liệu' if r else 'chưa có')"
  echo; ./scripts/monitor.sh info | sed 's#admin / .*)#mật khẩu admin: ./scripts/monitor.sh info)#'
  echo "Mở Grafana -> dashboard 'DeepStream pipeline' (Prefix Redis = ds), chụp màn hình"
}

test_soak() {
  local act=${1:-status} n=${2:-32} log=w2_soak.log pidf="$OUT/.soak_sampler.pid" csv="$OUT/9_soak.csv"
  case $act in
    start)
      header "2.4 Soak test: $n cam trên GPU $GPU, prefix w2soak (dừng: soak stop; kết quả: soak report)"
      local vids=(sample_1080p_h264.mp4 sample_1080p_h265.mp4 dwell_sitting_180s.mp4 sample_office.mp4)   # đều 30 fps
      for i in 1 2 3 4; do pub "soak$i" "${vids[i - 1]}"; done
      : > "$OUT/.soak_cams.txt"
      for i in $(seq 1 "$n"); do cam_line "soak-cam$i" "$RTSP/w2-soak$(( (i - 1) % 4 + 1 ))" >> "$OUT/.soak_cams.txt"; done
      yolo_engine "$n"
      docker exec deepstream_dev rm -f /outputs/logs/$log
      # Đúng pipeline đang chạy thật: YOLO + tìm mặt + embedding, ngưỡng ở yên 120 s, ghi clip
      # -o: ảnh/clip + dọn file (-k 1 -K 5) chỉ trong outputs/w2soak/, không đụng ảnh/clip của app thật
      docker exec -d -e LANG=C.UTF-8 -e REDIS_PASSWORD="$REDIS_PASSWORD" deepstream_dev "$APP" -g "$GPU" -P w2soak \
        -o /outputs/w2soak -l /outputs/logs/$log -k 1 -K 5 "${YOLO[@]}" "${FACE[@]}" -t 120 -r 135 -i /workspace/outputs/bao_cao_tuan2/.soak_cams.txt
      echo "thời gian,giây,app sống,cam có hình,FPS tổng,RAM MB,VRAM MiB,queue,drop,sự kiện,app chết (lần)" > "$csv"
      # Mỗi phút: RAM (RSS) + VRAM của tiến trình app (nvidia-smi trên host) + thống kê app. App chết -> ghi lại, bật lại
      nohup bash -c '
        t0=$(date +%s); crash=0
        while sleep 60; do
          pid=$(pgrep -f "^/workspace/apps/w1_multistream/w1-app .*-P w2soak" | head -1)   # tiến trình app (không phải docker exec)
          if [[ -z "$pid" ]]; then
            crash=$((crash + 1)); echo "$(date +%T) APP CHẾT lần $crash, bật lại" >> '"$OUT"'/9_soak_su_co.txt
            docker exec -d -e LANG=C.UTF-8 -e REDIS_PASSWORD="'"$REDIS_PASSWORD"'" deepstream_dev '"$APP"' -g '"$GPU"' -P w2soak \
              -o /outputs/w2soak -l /outputs/logs/'"$log"' -k 1 -K 5 '"${YOLO[*]} ${FACE[*]}"' -t 120 -r 135 -i /workspace/outputs/bao_cao_tuan2/.soak_cams.txt
            continue
          fi
          rss=$(awk "/VmRSS/{printf \"%.0f\", \$2/1024}" /proc/$pid/status)
          vram=$(nvidia-smi --query-compute-apps=pid,used_memory --format=csv,noheader,nounits | awk -F", " -v p=$pid "\$1==p{print \$2}")
          st=$(docker exec -e REDISCLI_AUTH="'"$REDIS_PASSWORD"'" ds_redis redis-cli --no-auth-warning GET w2soak:stats:g'"$GPU"')
          line=$(python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(d[\"cams_online\"], d[\"fps_total\"], d[\"queue\"], d[\"drop\"], d[\"events\"], sep=\",\")" "$st" 2>/dev/null || echo ",,,,")
          echo "$(date "+%F %T"),$(( $(date +%s) - t0 )),1,$line" | awk -F, -v r=$rss -v v=$vram -v c=$crash "BEGIN{OFS=\",\"}{print \$1,\$2,\$3,\$4,\$5,r,v,\$6,\$7,\$8,c}" >> '"$csv"'
        done' >/dev/null 2>&1 &
      echo $! > "$pidf"
      echo "Đã chạy: app test + bộ ghi mỗi phút ($csv). Lần đầu với $n cam: build engine YOLO ~5 phút."
      echo "Grafana: chọn Prefix Redis = w2soak. Sau 24 h: ./scripts/w2_bc_test.sh soak report" ;;
    status)
      [[ -f "$pidf" ]] && kill -0 "$(cat "$pidf")" 2>/dev/null && echo "Soak đang chạy" || echo "Soak không chạy"
      tail -3 "$csv" 2>/dev/null; stats_line $log 2>/dev/null || true ;;
    stop)
      [[ -f "$pidf" ]] && kill "$(cat "$pidf")" 2>/dev/null || true; rm -f "$pidf"
      docker exec deepstream_dev pkill -INT -f "w1-app.*-P w2soak" || true
      sleep 20; unpub; echo "Đã dừng soak" ;;
    report)
      header "2.4 Kết quả soak test (w2soak, $csv)"
      python3 - "$csv" "outputs/logs/$log" "$OUT/9_soak_su_co.txt" <<'EOF'
import csv, re, sys
rows = [r for r in csv.DictReader(open(sys.argv[1])) if r["RAM MB"]]
if not rows:
    sys.exit("chưa có số liệu")
h = int(rows[-1]["giây"]) / 3600
def slope(key):          # MB / giờ, hồi quy tuyến tính, bỏ 30 phút đầu (khởi động, build engine)
    pts = [(int(r["giây"]) / 3600, float(r[key])) for r in rows if int(r["giây"]) > 1800 and r[key]]
    if len(pts) < 3: return 0.0
    n = len(pts); mx = sum(x for x, _ in pts) / n; my = sum(y for _, y in pts) / n
    return sum((x - mx) * (y - my) for x, y in pts) / max(1e-9, sum((x - mx) ** 2 for x, _ in pts))
first = [r for r in rows if 1800 < int(r["giây"]) <= 5400]; last = rows[-60:]
avg = lambda rs, k: sum(float(r[k]) for r in rs if r[k]) / max(1, len(rs))
cams = [int(r["cam có hình"]) for r in rows if r["cam có hình"]]
print(f"Chạy {h:.1f} giờ, {len(rows)} mẫu (1 mẫu/phút). App chết: {rows[-1]['app chết (lần)']} lần")
print(f"RAM : giờ đầu {avg(first, 'RAM MB'):.0f} MB -> giờ cuối {avg(last, 'RAM MB'):.0f} MB, xu hướng {slope('RAM MB'):+.1f} MB/giờ")
print(f"VRAM: giờ đầu {avg(first, 'VRAM MiB'):.0f} MiB -> giờ cuối {avg(last, 'VRAM MiB'):.0f} MiB, xu hướng {slope('VRAM MiB'):+.1f} MiB/giờ")
print(f"Cam có hình: thấp nhất {min(cams)}, {100 * sum(c == max(cams) for c in cams) / len(cams):.1f}% mẫu đủ {max(cams)} cam")
print(f"drop cuối: {rows[-1]['drop']}, sự kiện: {rows[-1]['sự kiện']}")
log = open(sys.argv[2], encoding="utf-8", errors="replace").read()
off = re.findall(r"\[CAM\] camera_offline (\S+)", log); on = re.findall(r"\[CAM\] camera_online (\S+)", log)
print(f"Rớt cam: {len(off)} lần, có hình lại: {len(on)} lần" + (" (mọi lần rớt đều tự hồi phục)" if len(on) >= len(off) else " (CÒN CAM CHƯA LÊN LẠI)"))
ok = h >= 24 and rows[-1]["app chết (lần)"] == "0" and abs(slope("RAM MB")) < 20 and abs(slope("VRAM MiB")) < 20 and len(on) >= len(off) and rows[-1]["drop"] == "0"
print("ĐẠT" if ok else "CHƯA ĐẠT" + (" (chưa đủ 24 giờ)" if h < 24 else ""))
EOF
      ;;
  esac
}

test_gpu1() {
  local n=22 dur=150
  header "2.4 Hơn 40 cam: 2 instance (GPU 0 + GPU 1) x $n cam, model TrafficCamNet, prefix w2gpu"
  local vids=(sample_1080p_h264.mp4 sample_1080p_h265.mp4 dwell_sitting_180s.mp4 sample_office.mp4)   # đều 30 fps
  for i in 1 2 3 4; do pub "gpu$i" "${vids[i - 1]}"; done
  for g in 0 1; do
    : > "$OUT/.gpu${g}_cams.txt"
    for i in $(seq 1 $n); do cam_line "g$g-cam$i" "$RTSP/w2-gpu$(( (i + g) % 4 + 1 ))" >> "$OUT/.gpu${g}_cams.txt"; done
  done
  cli DEL w2gpu:frames w2gpu:events w2gpu:stats:g0 w2gpu:stats:g1 >/dev/null
  sleep 3
  echo "... 2 instance chạy song song ${dur}s (lần đầu build engine batch $n cho mỗi GPU ~2 phút: chạy lại bài nếu thiếu số liệu)"
  for g in 0 1; do
    docker exec deepstream_dev rm -f /outputs/logs/bc_w2_gpu$g.log
    docker exec -d -e LANG=C.UTF-8 -e REDIS_PASSWORD="$REDIS_PASSWORD" deepstream_dev timeout -s INT -k 30 $dur "$APP" -g $g \
      -o "$TDIR" -P w2gpu -l /outputs/logs/bc_w2_gpu$g.log -k 0 -K 0 -A -i /workspace/outputs/bao_cao_tuan2/.gpu${g}_cams.txt
  done
  sleep $((dur - 25))
  for g in 0 1; do cli GET w2gpu:stats:g$g > "$OUT/.gpu$g.json"; done
  # Số bản ghi metadata trong 10 s gần nhất (id stream = epoch ms); không dùng XLEN vì stream bị cắt ở MAXLEN
  local f0=0 f1; sleep 10; f1=$(( $(date +%s) * 1000 ))
  f1=$(cli XRANGE w2gpu:frames $((f1 - 10000)) $f1 COUNT 1000000 | grep -c '^{' || true)
  sleep 25; unpub
  for g in 0 1; do echo "GPU $g: $(stats_line bc_w2_gpu$g.log)"; done
  python3 - "$OUT" "$n" "$f0" "$f1" <<'EOF'
import json, sys
out, n, f0, f1 = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
st = [json.load(open(f"{out}/.gpu{g}.json")) for g in (0, 1)]
cams = [set(s["cams"]) for s in st]
fps = [v for s in st for v in s["cams"].values()]
print(f"\nGPU 0: {st[0]['cams_online']}/{st[0]['cams_total']} cam, {st[0]['fps_total']} FPS · GPU 1: {st[1]['cams_online']}/{st[1]['cams_total']} cam, {st[1]['fps_total']} FPS")
print(f"Tổng {len(fps)} cam có hình {sum(v > 0 for v in fps)}; fps mỗi cam: thấp nhất {min(fps)}, trung bình {sum(fps) / len(fps):.1f}")
print(f"Cam trùng giữa 2 instance: {len(cams[0] & cams[1])}")
print(f"Metadata vào w2gpu:frames trong 10 s: {f1 - f0} (~{(f1 - f0) / 10:.0f}/s, bằng tổng FPS {sum(fps):.0f} -> không gửi trùng)")
ok = len(fps) > 40 and min(fps) >= 25 and not cams[0] & cams[1]
print("ĐẠT" if ok else "KHÔNG ĐẠT")
EOF
}

test_cam_that() {
  local main=${1:-} sub=${2:-}
  header "2.4 Camera thật: $main ${sub:+/ $sub}"
  [[ -n "$main" ]] || { echo "Dùng: $0 cam-that rtsp://user:pass@<ip>:554/Streaming/Channels/101 [.../102]"; exit 1; }
  for u in $main $sub; do
    docker run --rm --network host --entrypoint ffprobe "$FFMPEG_IMAGE" -v error -rtsp_transport tcp -timeout 5000000 \
      -select_streams v:0 -show_entries stream=codec_name,width,height,avg_frame_rate -of csv=p=0 "$u" | sed "s#^#  ${u%%@*}@...: #" || echo "  LỖI: không mở được $u"
  done
  echo "App test chạy 180s. Sau khi thấy 'có hình' ~30s: RÚT dây mạng camera ~30s rồi cắm lại"
  run_app 180 bc_w2_camthat.log -P w2cam "${YOLO[@]}" -t 120 -A $main $sub &
  local p=$!
  for _ in $(seq 18); do sleep 10; echo "$(date +%T) $(stats_line bc_w2_camthat.log | cut -c1-120)"; done
  wait $p || true
  grep -E "\[CAM\]" outputs/logs/bc_w2_camthat.log | sed 's/^/  /'
  echo "Đạt khi: mọi luồng có hình, sau khi rút mạng có camera_offline rồi camera_online"
}

don_test() {
  unpub
  docker rm -f ds_redis_w2ngat ds_minio_w2ngat ds_uploader_w2ngat ds_uploader_w2up >/dev/null 2>&1 || true
  docker volume rm ds_minio_w2ngat_data >/dev/null 2>&1 || true
  test_soak stop >/dev/null 2>&1 || true
  docker exec deepstream_dev bash -c 'pkill -INT -f "w1-app.* -P w2" ; rm -f /tmp/w1-app-w2*.lock
    cd /outputs && rm -rf w2test w2soak && rm -f w1_snapshots/w2-* w1_clips/w2-* logs/bc_w2_*.log logs/w2_soak.log' || true
  local n=0
  for k in $(cli --scan --pattern 'w2*:*'); do cli DEL "$k" >/dev/null; n=$((n + 1)); done
  echo "Đã dọn: luồng test, container test, $n key Redis w2*, ảnh/clip/log test (giữ $OUT/)"
}

case "${1:-}" in
  platform) test_platform | tee "$OUT/1_platform.txt" ;;
  upload) test_upload | tee "$OUT/2_upload.txt" ;;
  ngat-platform) test_ngat_platform | tee "$OUT/3_ngat_platform.txt" ;;
  model-nguoi) test_model_nguoi | tee "$OUT/4_model_nguoi.txt" ;;
  roi) test_roi | tee "$OUT/5_roi.txt" ;;
  do-lech) test_do_lech | tee "$OUT/6_do_lech.txt" ;;
  khuon-mat) test_khuon_mat | tee "$OUT/10_khuon_mat.txt" ;;
  tu-hoi-phuc) test_tu_hoi_phuc | tee "$OUT/7_tu_hoi_phuc.txt" ;;
  grafana) test_grafana | tee "$OUT/8_grafana.txt" ;;
  soak) shift; if [[ "${1:-}" == report ]]; then test_soak report | tee "$OUT/9_soak.txt"; else test_soak "$@"; fi ;;
  gpu1) test_gpu1 | tee "$OUT/11_gpu1.txt" ;;
  cam-that) shift; test_cam_that "$@" | tee "$OUT/12_cam_that.txt" ;;
  don-test) don_test ;;
  *) sed -n '2,24p' "$0"; exit 1 ;;
esac
