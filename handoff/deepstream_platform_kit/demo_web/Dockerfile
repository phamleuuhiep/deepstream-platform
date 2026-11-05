# Image chỉ chứa thư viện; code (web/) được mount vào lúc chạy, sửa code xong chỉ cần restart
FROM python:3.12-slim
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
WORKDIR /app
CMD ["sh", "-c", "uvicorn app:app --host ${HOST:-127.0.0.1} --port ${PORT:-8095} --log-level warning"]
