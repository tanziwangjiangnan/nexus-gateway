# nexus-gateway 部署

## 目录结构

```
deploy/
├── docker-compose.yaml          # Docker Compose 一键部署
├── .env.example                 # 环境变量模板（密钥引用）
├── config/
│   └── gateway.yaml.example     # 配置模板（${VAR} 引用，无硬编码）
└── k8s/
    ├── kustomization.yaml       # Kustomize 入口
    ├── namespace.yaml           # 命名空间
    ├── configmap.yaml           # 配置（不含密钥）
    ├── deployment.yaml          # 工作负载（滚动更新 + 探针）
    ├── service.yaml             # ClusterIP 服务
    └── ingress.yaml             # 对外暴露（ingress-nginx）
```

## 部署方式

### 方式一：Docker Compose（本地/单机）

```bash
# 1. 准备环境
cp deploy/.env.example .env              # 编辑填入真实密钥
cp deploy/config/gateway.yaml.example deploy/config/gateway.yaml  # 按需修改

# 2. 构建镜像
docker build -t nexus-gateway:v3.9 .

# 3. 启动
docker compose -f deploy/docker-compose.yaml up -d
```

### 方式二：K8s / ACK（集群）

```bash
# 1. 创建命名空间和密钥
kubectl create secret generic nexus-gateway-secrets \
  --namespace nexus-gateway \
  --from-literal=DEEPSEEK_API_KEY='ds-xxx' \
  --from-literal=QFG_GPT_KEY='qfg-xxx' \
  --from-literal=SCNET_TP_KEY='scnet-xxx' \
  --from-literal=XIAOMI_KEY='xiaomi-xxx' \
  --from-literal=GATEWAY_KEY='gw-xxx'

# 2. 部署全部资源
kubectl apply -k deploy/k8s/

# 3. 验证
kubectl get pods -n nexus-gateway
kubectl get svc -n nexus-gateway

# 4. 扩容（可选）
kubectl scale deployment nexus-gateway -n nexus-gateway --replicas=3
```

### 方式三：现有组件部署（不变）

```bash
pip install --no-deps -e ./ops-gateway-core
python3 gateway.py
```

## 关键设计

- **无硬编码密钥**: 所有 `api_key` 统一使用 `${VAR}` 环境变量引用，密钥通过 Secret（K8s）或 .env（Compose）注入
- **健康检查**: K8s 使用 liveness + readiness 探针；Compose 使用 healthcheck
- **滚动更新**: K8s 配置 RollingUpdate（maxUnavailable=0），确保零停机
- **配置热加载**: 网关支持 SIGHUP 热加载配置，无需重启 Pod