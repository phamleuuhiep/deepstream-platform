# Image chỉ chứa thư viện; ds_exporter.py được mount vào lúc chạy (./scripts/monitor.sh start)
FROM python:3.13-slim
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
WORKDIR /app
CMD ["python", "-u", "ds_exporter.py"]
