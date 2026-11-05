# Image chỉ chứa thư viện; uploader.py được mount vào lúc chạy (./scripts/upload.sh start)
FROM python:3.13-slim
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
WORKDIR /app
CMD ["python", "-u", "uploader.py"]
