FROM python:3.9

# 更新系统并安装必要的包
RUN apt-get update \
    && apt-get install -y \
        apt-utils \
        python3-dev \
        build-essential \
        libgl1-mesa-glx \
        libglib2.0-0 \
        iproute2 \
        redis-server \
    && apt-get clean \
    && apt-get autoclean \
    && rm -rf /var/lib/apt/lists/*

# 创建非 root 用户
RUN groupadd -r appuser && useradd -r -g appuser -m appuser

# 设置工作目录
WORKDIR /app

# 复制项目文件
COPY . /app/

# 设置目录权限
RUN chown -R appuser:appuser /app

# 安装项目依赖
RUN pip install -i http://mirrors.i.brainpp.cn/pypi/simple/ \
    --extra-index-url http://pypi.i.brainpp.cn/brain/dev/+simple \
    --extra-index-url https://pypi.megvii-inc.com/simple \
    --trusted-host mirrors.i.brainpp.cn \
    --trusted-host pypi.i.brainpp.cn \
    -r requirements.txt

# 创建必要的目录并设置权限
RUN mkdir -p /home/appuser/.config/rclone \
    && cp /app/rclone/rclone.conf /home/appuser/.config/rclone/ \
    && chown -R appuser:appuser /home/appuser/.config \
    && mkdir -p /var/log/celery /var/run/celery \
    && chown -R appuser:appuser /var/log/celery /var/run/celery

# 复制启动脚本
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# 设置环境变量
ENV env=env

# 暴露端口
EXPOSE 5001 6379

# 使用启动脚本
ENTRYPOINT ["/entrypoint.sh"]