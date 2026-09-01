# nexus-gateway — 容器化打包
# 构建: docker build -t nexus-gateway:v3.11 .
# 运行: docker run -d -p 8646:8646 nexus-gateway:v3.11
#
# 支持三种模式:
#   API 服务模式（默认）: python3 gateway.py
#   CLI 模式:           docker run --rm nexus-gateway:v3.11 python3 gateway.py check-deps
#   基准评分:           docker run --rm -v ./data:/app/data nexus-gateway:v3.11 python3 gateway.py benchmark --all
#
# 注意: shim 包（hermes_cfg/fiber/api/ops）已合并至 ops-gateway-core，不再单独复制

FROM python:3.12-slim

# 使用 Debian 中国镜像加速 apt
RUN sed -i 's/deb.debian.org/mirrors.ustc.edu.cn/g' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    && rm -rf /var/lib/apt/lists/*

# 使用 pip 中国镜像 + 安装 setuptools 68.x（>=84 移除了 _legacy 后端，fiber_tree 依赖）
RUN pip config set global.index-url https://mirrors.ustc.edu.cn/pypi/web/simple && \
    pip config set global.timeout 120 && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir "setuptools<84" wheel

WORKDIR /app

# 复制核心项目文件（不复制 gateway.yaml / gateway.db — 密钥在 .env 中，运行时挂载）
COPY gateway.py .
COPY ops-gateway-core/ ./ops-gateway-core/
COPY nexus-gateway/ ./nexus-gateway/
COPY provider_router/ ./provider_router/
COPY fiber_tree/ ./fiber_tree/

# 安装依赖 — 以可编辑方式安装所有包
# ops-gateway-core 声明了核心依赖（PyYAML、httpx、fastapi 等），必须安装其依赖
RUN pip install --no-cache-dir -e ./fiber_tree && \
    pip install --no-cache-dir -e ./provider_router && \
    pip install --no-cache-dir -e ./ops-gateway-core && \
    pip install --no-cache-dir --no-deps -e ./nexus-gateway

# 数据持久化通过 docker-compose volumes 挂载实现（不在镜像中声明 VOLUME，
# 避免 docker-compose v1 与新 Docker 镜像元数据不兼容）

# 暴露 API 端口
EXPOSE 8646

# 默认以 API 服务模式启动
CMD ["python3", "gateway.py"]