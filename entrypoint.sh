#!/bin/bash
set -e

echo "Starting entrypoint script..."

# 设置环境变量 - 确保在整个脚本中可用
export env=dev
export IS_LOCAL=false

# 创建必要的目录
mkdir -p /var/log/celery /var/run/celery
chown -R appuser:appuser /var/log/celery /var/run/celery

# 启动Redis服务器
echo "Starting Redis server..."
redis-server --daemonize yes

# 等待Redis启动
echo "Waiting for Redis to start..."
until redis-cli ping &>/dev/null; do
  sleep 1
done
echo "Redis server started successfully"

# 使用非root用户启动 Celery Worker - 明确传递环境变量
echo "Starting Celery worker..."
su - appuser -c "cd /app && export env=dev && export IS_LOCAL=false && celery -A tasks worker \
    --loglevel=info \
    --concurrency=2 \
    --pidfile=/var/run/celery/celery.pid \
    --logfile=/var/log/celery/celery.log" &

# 等待 Celery Worker 启动 - 增加等待时间
sleep 5
echo "Celery worker started"

# 检查Celery是否真的启动了
echo "Checking Celery worker status..."
su - appuser -c "cd /app && celery -A tasks inspect ping" || {
    echo "WARNING: Celery worker may not have started properly. Check logs at /var/log/celery/celery.log"
}

# 使用非root用户启动 FastAPI 应用 - 明确传递环境变量
echo "Starting FastAPI application..."
exec su - appuser -c "cd /app && export env=dev && export IS_LOCAL=false && uvicorn app:app --host 0.0.0.0 --port 5001 --workers 1"