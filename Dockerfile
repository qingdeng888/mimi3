FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Asia/Shanghai \
    SERVER_HOST=0.0.0.0 \
    SERVER_PORT=18619 \
    MIMO_METRICS_DB_PATH=/app/data/gateway_metrics.db \
    MIMO_METRICS_SNAPSHOT_PATH=/app/data/gateway_snapshot.json \
    MIMO_PROCESS_LOCK_PATH=/app/data/mimo2api.lock

WORKDIR /app

# 安装运行所需的系统依赖：
#  - tini  : PID 1 信号转发，确保 SIGTERM 能正常终止 uvicorn / 子任务
#  - curl  : HEALTHCHECK 使用
#  - tzdata: 让上面的 TZ=Asia/Shanghai 生效
RUN apt-get update \
 && apt-get install -y --no-install-recommends tini curl ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/*

# 先单独拷贝依赖清单，最大化利用 docker 层缓存
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 拷贝项目代码（.dockerignore 已排除 .env / 运行时数据 / 缓存等）
COPY . .

# 预创建运行时目录，方便 compose 直接挂载到这里
RUN mkdir -p /app/users /app/logs /app/data

EXPOSE 18619

# 使用 /api/auth/session（在公共白名单中，鉴权开关无关）做健康检查
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -fsS "http://127.0.0.1:${SERVER_PORT:-18619}/api/auth/session" >/dev/null || exit 1

# tini 作为 PID 1，避免 Python 子任务在容器退出时变成僵尸进程
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-u", "main.py"]
