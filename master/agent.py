"""Agent của dashboard master (chạy trong container ds_agent, có quyền docker, KHÔNG mở cổng mạng).

Web master (web/app.py, trang admin) không có quyền docker: nó chỉ ghi cấu hình (configs/sites.json,
configs/app.json) và 1 file yêu cầu configs/.master/request.json. Agent đọc yêu cầu, chạy đúng 1 trong các
thao tác cố định bên dưới bằng scripts/*.sh, rồi ghi tiến trình + log vào configs/.master/status.json.
Mỗi 10 s agent cập nhật thêm: link public từng công ty, app có đang chạy không.
Tự hồi phục (Task 2.4, app.json "auto_heal", mặc định bật): mỗi HEAL_SEC giây, thành phần nào chết thì bật lại theo
thứ tự như nút Bật tất cả, ghi log lên dashboard + sự kiện system_heal vào ds:events. App mà người dùng bấm Dừng
thì để yên. Server khởi động lại: ds_agent tự lên (--restart unless-stopped) rồi bật nốt phần còn lại.
"""
import json
import os
import re
import secrets
import subprocess
import time

ROOT = os.environ["ROOT"]                       # đường dẫn dự án, giống hệt trên host (cho docker -v)
STATE_DIR = os.path.join(ROOT, "configs/.master")
REQUEST = os.path.join(STATE_DIR, "request.json")
STATUS = os.path.join(STATE_DIR, "status.json")
APP_CFG = os.path.join(ROOT, "configs/app.json")
CAM_LIST = "configs/cameras_sim.txt"
COMPANIES_AUTH = os.path.join(ROOT, "web/companies.auth")
APP_BIN = "/workspace/apps/w1_multistream/w1-app"
# Chỉ app chính (danh sách cam của master); app test / soak (w2_bc_test.sh, prefix khác) không bị đếm hay dừng nhầm
APP_MATCH = f"^{APP_BIN} -i /workspace/{CAM_LIST}"
APP_ERR = "outputs/logs/w1_stderr.log"           # stderr của lần chạy app gần nhất
REDIS_ENV = os.path.join(ROOT, ".redis.env")      # REDIS_PASSWORD=... do scripts/redis_dev.sh tạo
# Pipeline nhiều tầng (app.json "face": true): YOLO COCO -> tracker -> mặt trong người -> ArcFace [-> VIP classifier]
FACE_MODELS = ("models/yolo11s.onnx", "models/libnvdsinfer_custom_impl_Yolo.so", "models/facenet/model.etlt",
               "models/buffalo_s/w600k_mbf.onnx")
FACE_ARGS = ["-c", "/workspace/configs/pgie_yolo11_coco.txt", "-p", "0",
             "-S", "/workspace/configs/sgie1_face_detect.txt", "-S", "/workspace/configs/sgie2_face_embed.txt"]
VIP_MODEL = "models/vip_classifier.onnx"          # có thì thêm tầng 4 (configs/sgie3_vip_classifier.txt)
S3_ENV = os.path.join(ROOT, ".s3.env")            # có thì app chạy -M MEDIA_BASE (./scripts/upload.sh)
APP_STOPPED = os.path.join(STATE_DIR, "app_stopped")   # người dùng bấm Dừng app: tự hồi phục không bật lại app
HEAL_SEC = 5                                      # kiểm rẻ (1 docker ps + 1 pgrep): phát hiện chết trong <= 5 s
HEAL_BACKOFF_SEC = 300                            # 1 thành phần bật lỗi 3 lần liên tiếp: nghỉ 5 phút mới thử lại
NAME_RE = re.compile(r"^[A-Za-z0-9_]{1,32}$")
URL_RE = re.compile(rb"https://[a-z0-9-]+\.trycloudflare\.com")

status = {"state": "idle", "action": None, "started": None, "finished": None, "ok": None, "log": [],
          "links": {}, "app_running": False, "updated": 0}


def save():
    status["updated"] = time.time()
    tmp = STATUS + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False)
    os.replace(tmp, STATUS)


def log(line):
    status["log"].append(time.strftime("%H:%M:%S ") + line.rstrip())
    del status["log"][:-300]


def run(*cmd, check=True, env=None):
    """Chạy lệnh, đưa từng dòng output vào log của status."""
    log("$ " + " ".join(cmd))
    save()
    p = subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
    last = 0.0
    for line in p.stdout:
        log(line)
        if time.time() - last > 0.5:
            save()
            last = time.time()
    rc = p.wait()
    save()
    if check and rc != 0:
        raise RuntimeError(f"lệnh lỗi (mã {rc}): {' '.join(cmd)}")
    return rc


def redis_env():
    """env cho docker exec app: thêm REDIS_PASSWORD từ .redis.env (truyền qua env, không lộ trong log lệnh)."""
    env = dict(os.environ)
    try:
        for line in open(REDIS_ENV, encoding="utf-8"):
            k, _, v = line.strip().partition("=")
            if k == "REDIS_PASSWORD":
                env[k] = v
    except OSError:
        pass
    return env


def s3_env():
    try:
        return dict(line.strip().split("=", 1) for line in open(S3_ENV, encoding="utf-8") if "=" in line)
    except OSError:
        return {}


def quiet(*cmd):
    return subprocess.run(cmd, capture_output=True, timeout=30)


# ---------------- app w1 trong container deepstream_dev ----------------
def app_running():
    return quiet("docker", "exec", "deepstream_dev", "pgrep", "-f", APP_MATCH).returncode == 0


APP_LOCK = "/tmp/w1-app-ds.lock"                  # khoá chạy 1 bản của w1-app (prefix Redis ds), xem acquire_instance_lock


def app_lock_free():
    """Khoá được nhả = app cũ đã thoát HẲN. pgrep không đủ: process bị kill còn vài giây trả GPU cho driver, lúc đó
    pgrep không thấy nữa nhưng process vẫn giữ khoá -> bản mới báo "đã có w1-app khác đang chạy"."""
    return quiet("docker", "exec", "deepstream_dev", "flock", "-n", APP_LOCK, "true").returncode == 0


def app_stop():
    if quiet("docker", "inspect", "-f", "{{.State.Running}}", "deepstream_dev").stdout.strip() != b"true":
        log("deepstream_dev không chạy, app đã dừng")
        return
    if not app_running() and app_lock_free():
        log("app không chạy")
        return
    # SIGINT: app dừng sạch (đóng clip đang ghi). YOLO + 3 SGIE + 19 cam mất 20-40 s; quá 45 s thì kill
    # (vd. app đang build engine TensorRT lần đầu, không nhận SIGINT)
    run("docker", "exec", "deepstream_dev", "pkill", "-INT", "-f", APP_MATCH, check=False)
    for i in range(45):
        if app_lock_free():
            log(f"app đã dừng sau {i} s")
            return
        time.sleep(1)
    run("docker", "exec", "deepstream_dev", "pkill", "-KILL", "-f", APP_MATCH, check=False)
    for i in range(60):
        if app_lock_free():
            log(f"app đã dừng (kill, nhả GPU sau {i} s)")
            return
        time.sleep(1)
    raise RuntimeError("app cũ chưa thoát hẳn sau khi kill (vẫn giữ khoá), đợi 1-2 phút rồi bấm Chạy lại app")


def app_start():
    if os.path.exists(APP_STOPPED):
        os.remove(APP_STOPPED)
    try:
        with open(APP_CFG, encoding="utf-8") as f:
            cfg = json.load(f)
    except (OSError, ValueError):
        cfg = {}
    dwell = int(cfg.get("dwell_s", 120))
    args = ["-i", f"/workspace/{CAM_LIST}", "-t", str(dwell)]
    if cfg.get("smart_record", True):
        args += ["-r", str(dwell + 15)]       # cache phải >= ngưỡng + 2 s để clip phủ từ lúc bắt đầu ở yên
    if cfg.get("face"):
        missing = [m for m in FACE_MODELS if not os.path.isfile(os.path.join(ROOT, m))]
        if missing:
            raise RuntimeError(f"thiếu model {', '.join(missing)}: chạy ./scripts/prepare_models.sh trên host")
        args += FACE_ARGS + ["-F", str(cfg.get("face_thr", 0.42))]
        if os.path.isfile(os.path.join(ROOT, VIP_MODEL)):
            args += ["-S", "/workspace/configs/sgie3_vip_classifier.txt"]
    base = s3_env().get("MEDIA_BASE")
    if base:
        args += ["-M", base]                  # sự kiện mang URL ảnh/clip, chỉ dọn file đã upload
    run("docker", "start", "deepstream_dev")
    if quiet("docker", "exec", "deepstream_dev", "pkg-config", "--exists", "gstreamer-1.0", "hiredis").returncode:
        run("scripts/setup_build_deps.sh")
    run("docker", "exec", "deepstream_dev", "make", "-s", "-C", os.path.dirname(APP_BIN))
    # -d: chạy nền trong container, không phụ thuộc terminal. Log: outputs/logs/w1_<giờ>.log
    # stderr ra file riêng: lỗi trước khi app mở log (chạy trùng, sai tham số, ...) vẫn xem được
    run("docker", "exec", "-d", "-e", "LANG=C.UTF-8", "-e", "REDIS_PASSWORD", "deepstream_dev", "bash", "-c",
        f"exec {APP_BIN} {' '.join(args)} >/dev/null 2>/workspace/{APP_ERR}", env=redis_env())
    time.sleep(3)
    if not app_running():
        try:
            for line in open(os.path.join(ROOT, APP_ERR), encoding="utf-8", errors="replace").read().splitlines()[-15:]:
                log("  " + line)
        except OSError:
            pass
        raise RuntimeError(f"app không khởi động được (lỗi ở trên, đủ trong {APP_ERR})")
    log(f"app đang chạy: w1-app {' '.join(args)}")
    log("lần đầu với số cam mới, nvinfer build engine (" + ("YOLO ~5 phút" if cfg.get("face") else "~1-2 phút")
        + "): camera chưa có hình trong lúc này, ĐỪNG bấm chạy lại app")


# ---------------- thành phần hệ thống (nút Bật trên dashboard) ----------------
# Thứ tự bật: mỗi bước cần bước trước. Web admin (ds_web) + agent (ds_agent) bật trên host
# (./scripts/web.sh start, ./scripts/master.sh start): dashboard cần chúng để có nút bấm.
COMPONENTS = ("ds", "redis", "sim", "upload", "app", "web")


def containers():
    """{tên container: đang chạy?} của mọi container (1 lệnh docker ps)."""
    out = quiet("docker", "ps", "-a", "--format", "{{.Names}}\t{{.State}}").stdout.decode()
    return {n: s == "running" for n, _, s in (line.partition("\t") for line in out.splitlines())}


def cam_list():
    """[(công ty, cam_id)] trong danh sách cam của app (configs/cameras_sim.txt)."""
    rows = []
    try:
        for line in open(os.path.join(ROOT, CAM_LIST), encoding="utf-8"):
            p = line.split()
            if len(p) >= 4 and not p[0].startswith("#"):
                rows.append((p[0], p[2]))
    except OSError:
        pass
    return rows


def components():
    """Trạng thái từng thành phần theo thứ tự bật: {key, ok, detail}."""
    c, cams = containers(), cam_list()
    ds = c.get("deepstream_dev", False)
    sims = sum(c.get(f"ds_sim_{cam}", False) for _, cam in cams)
    cos = sorted({co for co, _ in cams})
    webs = sum(c.get(f"ds_web_{co}", False) and c.get(f"ds_web_tunnel_{co}", False) for co in cos)
    app = ds and app_running()
    s3 = os.path.exists(S3_ENV)
    upload = c.get("ds_uploader", False) and (c.get("ds_minio", False) or "ds_minio" not in c)
    return [
        {"key": "ds", "ok": ds, "detail": "deepstream_dev (GPU, app chạy trong này)"},
        {"key": "redis", "ok": c.get("ds_redis", False), "detail": "ds_redis (app gửi metadata, sự kiện)"},
        {"key": "sim", "ok": c.get("ds_mediamtx", False) and bool(cams) and sims == len(cams),
         "detail": f"ds_mediamtx {'chạy' if c.get('ds_mediamtx') else 'TẮT'} + {sims}/{len(cams)} cam (ds_sim_*)"},
        *([{"key": "upload", "ok": upload, "detail": f"ds_uploader {'chạy' if c.get('ds_uploader') else 'TẮT'}"
              + (f", ds_minio {'chạy' if c.get('ds_minio') else 'TẮT'}" if "ds_minio" in c else "")
              + " (ảnh/clip lên S3)"}] if s3 else []),
        {"key": "app", "ok": app, "detail": "w1-app " + ("đang chạy" if app else "đang dừng")
         + (" (người dùng đã bấm Dừng, không tự bật)" if not app and os.path.exists(APP_STOPPED) else "")},
        {"key": "web", "ok": bool(cos) and webs == len(cos), "detail": f"{webs}/{len(cos)} công ty có web + link (ds_web_<cty>)"},
    ]


def start(key):
    if key == "ds":
        run("docker", "start", "deepstream_dev")
    elif key == "redis":
        run("scripts/redis_dev.sh", "start")
    elif key == "sim":
        # Tạo lại MediaMTX + mọi cam theo sites.json (cam đang chạy cũng bị nối lại vài giây)
        run("scripts/rtsp_sim.sh", "sites", "configs/sites.json")
    elif key == "upload":
        if "ds_minio" in containers():
            run("docker", "start", "ds_minio")
        run("scripts/upload.sh", "start")
    elif key == "app":
        if app_running():
            log("app đang chạy, bỏ qua (muốn chạy lại thì bấm Chạy lại app)")
        else:
            app_start()
    elif key == "web":
        run("scripts/web.sh", "publish", CAM_LIST)     # công ty đang có link thì giữ nguyên link


def start_all():
    """Bật lần lượt những thành phần đang tắt; thành phần đã chạy thì giữ nguyên."""
    for comp in components():
        if comp["ok"]:
            log(f"{comp['key']}: đã chạy, bỏ qua")
        else:
            log(f"{comp['key']}: bật")
            start(comp["key"])
            if comp["key"] == "sim":
                time.sleep(5)   # cho cam phát lên MediaMTX trước khi app nối vào


# ---------------- xoá cảnh báo cũ ----------------
KEEP_MIN = 10   # giữ cảnh báo, ảnh, clip trong 10 phút gần nhất (clip có thể đang ghi dở)


def clear_alerts():
    """Xoá cảnh báo cũ của mọi công ty: sự kiện trong Redis (ds:events) + ảnh, clip trong outputs/."""
    minid = int((time.time() - KEEP_MIN * 60) * 1000)     # id của stream Redis = thời điểm ghi (ms)
    run("bash", "-c", f'echo "đã xoá $(scripts/redis_dev.sh cli XTRIM ds:events MINID {minid}) sự kiện trong Redis"')
    # Ảnh/clip do app ghi bằng root nên xoá từ trong container
    run("docker", "exec", "deepstream_dev", "bash", "-c",
        "find /workspace/outputs/w1_snapshots /workspace/outputs/w1_clips -type f "
        f"-mmin +{KEEP_MIN} -printf '%s\\n' -delete "
        "| awk '{n++; s+=$1} END {printf \"đã xoá %d file ảnh/clip, giải phóng %.1f GB\\n\", n, s/1e9}'")


# ---------------- tài khoản công ty ----------------
def reset_password(company):
    lines = open(COMPANIES_AUTH, encoding="utf-8").read().splitlines()
    out, found = [], False
    for line in lines:
        parts = line.split()
        if len(parts) == 3 and parts[0] == company:
            line, found = f"{company} {parts[1]} {secrets.token_urlsafe(12)}", True
        out.append(line)
    if not found:
        raise RuntimeError(f"chưa có tài khoản cho {company} (bấm Áp dụng trước)")
    tmp = COMPANIES_AUTH + ".tmp"
    with open(os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")
    os.replace(tmp, COMPANIES_AUTH)
    log(f"đã đổi mật khẩu {company} (web công ty tự đọc lại, không cần khởi động lại)")


# ---------------- thao tác ----------------
def do(req):
    action, co = req.get("action"), req.get("company")
    if co is not None and not NAME_RE.match(str(co)):
        raise RuntimeError("tên công ty không hợp lệ")
    if action == "apply":
        # Camera giả lập theo configs/sites.json -> app đọc lại danh sách cam -> link cho công ty mới,
        # tắt link của công ty đã xoá (công ty cũ giữ nguyên link)
        run("scripts/rtsp_sim.sh", "sites", "configs/sites.json")
        app_stop()
        app_start()
        run("scripts/web.sh", "publish", CAM_LIST)
    elif action == "app_restart":
        app_stop()
        app_start()
    elif action == "app_stop":
        app_stop()
        open(APP_STOPPED, "w").close()
    elif action == "clear_alerts":
        clear_alerts()
    elif action == "start_all":
        start_all()
    elif action == "start" and req.get("target") in COMPONENTS:
        start(req["target"])
    elif action == "republish" and co:
        run("scripts/web.sh", "republish", co)
    elif action == "unpublish" and co:
        run("scripts/web.sh", "unpublish", co)
    elif action == "reset_password" and co:
        reset_password(co)
    else:
        raise RuntimeError(f"thao tác không hợp lệ: {action}")


def heal_event(text, comps):
    """Sự kiện system_heal vào ds:events: dashboard (tab Sự kiện) và platform thấy hệ thống đã tự hồi phục."""
    data = json.dumps({"schema_version": 2, "type": "system_heal", "detail": text, "components": comps},
                      ensure_ascii=False)
    quiet(os.path.join(ROOT, "scripts/redis_dev.sh"), "cli", "XADD", "ds:events", "MAXLEN", "~", "100000", "*", "cam", "system",
          "type", "system_heal", "ts", str(int(time.time() * 1000)), "data", data)


heal_fails = {}     # thành phần -> (số lần bật lỗi liên tiếp, lần thử cuối)


def auto_heal():
    """Bật lại thành phần đã chết (đúng thứ tự). Chạy khi agent rảnh, mỗi HEAL_SEC giây."""
    try:
        cfg = json.load(open(APP_CFG, encoding="utf-8"))
    except (OSError, ValueError):
        cfg = {}
    if not cfg.get("auto_heal", True):
        return
    dead = [c for c in components() if not c["ok"] and not (c["key"] == "app" and os.path.exists(APP_STOPPED))]
    now = time.time()
    dead = [c for c in dead if not (heal_fails.get(c["key"], (0, 0))[0] >= 3
                                    and now - heal_fails[c["key"]][1] < HEAL_BACKOFF_SEC)]
    if not dead:
        return
    names = ", ".join(c["key"] for c in dead)
    status.update(state="running", action="auto_heal", company=None, started=now, finished=None, ok=None, log=[])
    log(f"TỰ HỒI PHỤC: {names} đã chết -> bật lại theo thứ tự")
    heal_event(f"phát hiện chết: {names}", [c["key"] for c in dead])
    ok = True
    for c in components():          # đúng thứ tự, kiểm lại vì bật bước trước có thể kéo bước sau lên
        if c["ok"] or c["key"] not in {d["key"] for d in dead}:
            continue
        try:
            log(f"{c['key']}: bật ({c['detail']})")
            start(c["key"])
            if c["key"] == "sim":
                time.sleep(5)       # cho cam phát lên MediaMTX trước khi app nối vào
            heal_fails.pop(c["key"], None)
        except Exception as e:
            ok = False
            n = heal_fails.get(c["key"], (0, 0))[0] + 1
            heal_fails[c["key"]] = (n, time.time())
            log(f"LỖI bật {c['key']} (lần {n}): {e}" + (f"; nghỉ {HEAL_BACKOFF_SEC // 60} phút" if n >= 3 else ""))
    took = time.time() - now
    log(f"tự hồi phục xong sau {took:.0f} s" if ok else "tự hồi phục chưa xong, thử lại ở vòng sau")
    heal_event(f"{'đã bật lại' if ok else 'chưa bật lại được'}: {names} ({took:.0f} s)", [c["key"] for c in dead])
    status.update(state="idle", finished=time.time(), ok=ok)


def refresh_info():
    links = {}
    try:
        for line in open(COMPANIES_AUTH, encoding="utf-8"):
            parts = line.split()
            if len(parts) != 3 or not NAME_RE.match(parts[0]):
                continue
            name = f"ds_web_tunnel_{parts[0]}"
            running = quiet("docker", "inspect", "-f", "{{.State.Running}}", name).stdout.strip() == b"true"
            urls = URL_RE.findall(quiet("docker", "logs", name).stderr) if running else []
            links[parts[0]] = urls[-1].decode() if urls else None
    except OSError:
        pass
    status["links"] = links
    status["components"] = components()
    status["app_running"] = next(c["ok"] for c in status["components"] if c["key"] == "app")
    save()


def main():
    os.makedirs(STATE_DIR, exist_ok=True)
    if os.path.exists(STATUS):
        try:
            old = json.load(open(STATUS, encoding="utf-8"))
            status.update(log=old.get("log", []), action=old.get("action"), ok=old.get("ok"),
                          started=old.get("started"), finished=old.get("finished"))
        except ValueError:
            pass
    status["state"] = "idle"
    last_info = 0.0
    last_heal = time.time()          # vòng đầu sau HEAL_SEC: chờ thành phần tự lên (restart policy) khi server vừa bật
    while True:
        if os.path.exists(REQUEST):
            try:
                req = json.load(open(REQUEST, encoding="utf-8"))
            except ValueError:
                req = {}
            os.remove(REQUEST)
            status.update(state="running", action=req.get("action"), company=req.get("company"),
                          started=time.time(), finished=None, ok=None, log=[])
            log(f"bắt đầu: {req.get('action')} {req.get('target') or req.get('company') or ''}")
            try:
                do(req)
                status["ok"] = True
                log("xong")
            except Exception as e:  # báo lỗi lên dashboard, agent chạy tiếp
                status["ok"] = False
                log(f"LỖI: {e}")
            status.update(state="idle", finished=time.time())
            refresh_info()
            last_info = time.time()
        elif time.time() - last_info > 10:
            refresh_info()
            last_info = time.time()
        if time.time() - last_heal > HEAL_SEC and not os.path.exists(REQUEST):
            auto_heal()
            last_heal = time.time()
            if status["action"] == "auto_heal" and status["finished"] and status["finished"] > last_info:
                refresh_info()
        time.sleep(1)


if __name__ == "__main__":
    main()
