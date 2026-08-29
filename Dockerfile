# nexus-gateway — 容器化打包
# 构建: docker build -t nexus-gateway:v3.9 .
# 运行: docker run -d -p 8646:8646 nexus-gateway:v3.9
#
# 支持两种模式:
#   API 服务模式（默认）: python3 gateway.py
#   CLI 模式:           docker run --rm nexus-gateway:v3.9 python3 gateway.py check-deps

FROM python:3.11-slim

# 设置时区
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 安装 docker CLI（用于容器发现和日志）
RUN apt-get update && apt-get install -y --no-install-recommends \
    docker.io-cli \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 复制项目文件
COPY gateway.py .
COPY ops-gateway-core/ ./ops-gateway-core/
COPY nexus-gateway/ ./nexus-gateway/
COPY hermes_cfg/ ./hermes_cfg/
COPY hermes_fiber/ ./hermes_fiber/
COPY hermes_api/ ./hermes_api/
COPY hermes_ops/ ./hermes_ops/
COPY provider_router/ ./provider_router/
COPY fiber_tree/ ./fiber_tree/

# 安装依赖 — 以可编辑方式安装所有包（核心包 + 外部路由包 + shim 兼容包）
RUN pip install --no-cache-dir -e ./fiber_tree && \
    pip install --no-cache-dir -e ./provider_router && \
    pip install --no-cache-dir --no-deps -e ./ops-gateway-core && \
    pip install --no-cache-dir --no-deps -e ./nexus-gateway

# 配置挂载点
VOLUME ["/app/data", "/app/config"]

# 暴露 API 端口
EXPOSE 8646

# 默认以 API 服务模式启动
CMD ["python3", "gateway.py"]