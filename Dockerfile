# nexus-gateway — 容器化打包
# 构建: docker build -t nexus-gateway:v3.4 .
# 运行: docker run -d -p 8646:8646 nexus-gateway:v3.4
#
# 支持两种模式:
#   API 服务模式（默认）: python3 gateway.py
#   CLI 模式:           docker run --rm nexus-gateway:v3.4 python3 gateway.py check-deps

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
COPY hermes_cfg/ ./hermes_cfg/
COPY hermes_fiber/ ./hermes_fiber/
COPY hermes_api/ ./hermes_api/
COPY hermes_ops/ ./hermes_ops/
COPY provider_router/ ./provider_router/
COPY fiber_tree/ ./fiber_tree/

# 为每个子包创建最小 pyproject.toml（生产环境正式安装）
COPY hermes_cfg/pyproject.toml ./hermes_cfg/
COPY hermes_fiber/pyproject.toml ./hermes_fiber/
COPY hermes_api/pyproject.toml ./hermes_api/
COPY hermes_ops/pyproject.toml ./hermes_ops/
COPY provider_router/pyproject.toml ./provider_router/
COPY fiber_tree/pyproject.toml ./fiber_tree/

# 安装依赖 — 以可编辑方式安装所有子包
RUN pip install --no-cache-dir -e ./fiber_tree && \
    pip install --no-cache-dir -e ./provider_router && \
    pip install --no-cache-dir -e ./hermes_cfg && \
    pip install --no-cache-dir -e ./hermes_fiber && \
    pip install --no-cache-dir -e ./hermes_ops && \
    pip install --no-cache-dir -e ./hermes_api

# 配置挂载点
VOLUME ["/app/data", "/app/config"]

# 暴露 API 端口
EXPOSE 8646

# 默认以 API 服务模式启动
CMD ["python3", "gateway.py"]