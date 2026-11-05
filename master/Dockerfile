# Agent của dashboard master: thực hiện các thao tác web master yêu cầu (bật camera giả lập, chạy lại app,
# publish link công ty). Có docker CLI + bash để gọi scripts/*.sh. Không mở cổng mạng nào.
FROM python:3.12-slim
RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends iproute2 >/dev/null \
 && rm -rf /var/lib/apt/lists/*
COPY --from=docker:latest /usr/local/bin/docker /usr/local/bin/docker
ENV PYTHONUNBUFFERED=1
