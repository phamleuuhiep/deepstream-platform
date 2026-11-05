# videos/

Video test cho camera RTSP giả lập (`scripts/rtsp_sim.sh`). Không commit vào git (~1 GB).
`videos/.sim/` là bản lệch giờ do `rtsp_sim.sh` tự tạo.

| File | Nguồn |
|------|-------|
| `sample_1080p_h264.mp4`, `sample_1080p_h265.mp4`, `sample_cam5.mp4`, `sample_cam6.mp4` | Mẫu của DeepStream 7.0 (lệnh dưới) |
| `dwell_sitting_180s.mp4` | Video riêng: người ngồi yên 3 phút, test cảnh báo ở yên. Không có trong mẫu DeepStream, lấy từ server |

```bash
docker exec deepstream_dev bash -c 'cp /opt/nvidia/deepstream/deepstream/samples/streams/{sample_1080p_h264,sample_1080p_h265,sample_cam5,sample_cam6}.mp4 /workspace/videos/'
```
