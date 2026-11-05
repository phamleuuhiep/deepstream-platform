#!/usr/bin/env python3
"""Đọc configs/sites.json (công ty -> toà -> camera, sửa ở dashboard master) cho scripts/rtsp_sim.sh.

  sites.py sim <sites.json>                    in mỗi cam 1 dòng "<công ty> <toà> <cam> <video> <fps> <giây bắt đầu>"
  sites.py app <sites.json> <rtsp base> <out>  ghi danh sách cam cho w1-app -i:
                                               "<công ty> <toà> <cam> <uri> <fps> <JSON thông tin camera>"
JSON thông tin camera được w1-app đẩy nguyên vào Redis (hash <prefix>:camera_info, sự kiện camera_info)
cho team platform. Lỗi cấu hình: in lý do ra stderr, thoát mã 1.
"""
import json
import re
import sys

NAME_RE = re.compile(r"^[A-Za-z0-9_]{1,32}$")
CAM_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
MAX_CAMS = 64


def fail(msg):
    print(f"Lỗi {sys.argv[2]}: {msg}", file=sys.stderr)
    sys.exit(1)


def cameras(path):
    """Kiểm tra cấu hình, trả về list (company, building, camera) theo thứ tự file."""
    try:
        with open(path, encoding="utf-8") as f:
            cfg = json.load(f)
    except (OSError, ValueError) as e:
        fail(e)
    out, ids = [], set()
    for co in cfg.get("companies", []):
        if not NAME_RE.match(str(co.get("id", ""))):
            fail(f"mã công ty '{co.get('id')}' chỉ gồm chữ, số, _")
        for b in co.get("buildings", []):
            if not NAME_RE.match(str(b.get("id", ""))):
                fail(f"{co['id']}: mã toà '{b.get('id')}' chỉ gồm chữ, số, _")
            for cam in b.get("cameras", []):
                cid = str(cam.get("id", ""))
                if not CAM_RE.match(cid) or cid in ids:
                    fail(f"camera_id '{cid}' sai định dạng hoặc bị trùng")
                ids.add(cid)
                if not re.match(r"^[A-Za-z0-9_.-]+\.mp4$", str(cam.get("video", ""))):
                    fail(f"{cid}: video '{cam.get('video')}' không hợp lệ")
                for k in ("fps", "start"):
                    if not isinstance(cam.get(k, 0), int) or cam.get(k, 0) < 0:
                        fail(f"{cid}: {k} phải là số nguyên >= 0")
                out.append((co, b, cam))
    if not out:
        fail("chưa có camera nào")
    if len(out) > MAX_CAMS:
        fail(f"{len(out)} cam, app chỉ nhận tối đa {MAX_CAMS}")
    return out


def info(co, b, cam):
    """Các trường mô tả camera gửi cho platform (không có URI: URI cam thật có thể chứa mật khẩu)."""
    return {"camera_id": cam["id"], "camera_name": cam.get("name") or cam["id"],
            "company_id": co["id"], "company_name": co.get("name") or co["id"],
            "building_id": b["id"], "building_name": b.get("name") or b["id"],
            "building_address": b.get("address", ""), "floor": cam.get("floor", ""),
            "zone": cam.get("zone", ""), "fps_limit": cam.get("fps", 0),
            # Vùng ghế/khu chờ (vẽ trên dashboard master): w1-app chỉ tính ở yên cho người đứng chân trong vùng
            **({"roi": cam["roi"]} if cam.get("roi") else {})}


def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "sim":
        for co, b, cam in cameras(sys.argv[2]):
            print(co["id"], b["id"], cam["id"], cam["video"], cam.get("fps", 0), cam.get("start", 0))
    elif len(sys.argv) == 5 and sys.argv[1] == "app":
        base, out = sys.argv[3].rstrip("/"), sys.argv[4]
        lines = ["# Sinh tự động bởi rtsp_sim.sh sites từ " + sys.argv[2] + ". Dùng: ./scripts/run_w1.sh -i " + out,
                 "# <công ty> <toà> <cam_id> <uri> <fps xử lý, 0 = mọi frame> <JSON thông tin camera>"]
        for co, b, cam in cameras(sys.argv[2]):
            lines.append(f"{co['id']} {b['id']} {cam['id']} {base}/{cam['id']} {cam.get('fps', 0)} "
                         + json.dumps(info(co, b, cam), ensure_ascii=False, separators=(",", ":")))
        with open(out, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    else:
        print(__doc__, file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
