# 网关容器化迁移方案（v3.10 → 老机/ACK）

## 架构目标

```
                    ┌─────────────────────────┐
                    │   新机（京东云 4C8G）     │
                    │  ┌───────────────────┐  │
                    │  │ OpenHands         │  │
                    │  │ (agent-canvas栈)  │  │
                    │  └───────────────────┘  │
                    └──────────┬──────────────┘
                               │
                    DNS: hermes.jiangnande.cloud
                    ┌──────────┴──────────────┐
                    │   老机（阿里云 2C2G）     │
                    │  ┌───────────────────┐  │
                    │  │ nginx (SSL)       │  │
                    │  │  ├ 8648 → gateway │  │
                    │  │  ├ 8649 → direct  │  │
                    │  │  ├ 443  → 管理面板│  │
                    │  │  └ 8643 → api     │  │
                    │  ├───────────────────┤  │
                    │  │ nexus-gateway     │  │
                    │  │ (容器 :8646)      │  │
                    │  ├───────────────────┤  │
                    │  │ AstrBot + NapCat  │  │
                    │  │ (已有容器)         │  │
                    │  └───────────────────┘  │
                    └─────────────────────────┘
```

## 迁移步骤

### 阶段一：老机准备 ✅ 可立即执行

1. 安装 docker-compose-v2
2. 创建目录结构
3. 传输镜像
4. 复制配置和数据
5. 启动网关验证
6. 添加 systemd 管理

### 阶段二：DNS 切换（需择机执行）

1. 将 `hermes.jiangnande.cloud` 从 `117.72.220.114` 改为 `106.14.40.189`
2. 等待 DNS 生效（TTL 视配置而定）
3. 验证新机/老机均可达

### 阶段三：nginx 迁移（阶段二后执行）

1. 老机 nginx 增加 hermes 站点配置（SSL + 8648 + 8649）
2. 从新机复制 SSL 证书到老机（或老机重新申请 Let's Encrypt）
3. 验证 HTTPS 可达

## 回滚方案

### 如果 DNS 切后老机服务异常

```bash
# 1. DNS 切回新机 IP（117.72.220.114）
# 2. 老机停容器
ssh -p 2222 root@106.14.40.189 "cd /opt/containers/gateway && docker compose down"
# 3. 新机恢复网关
systemctl start gateway-docker  # 或 systemctl start gateway
```

### 如果老机容器启动失败

```bash
# 1. 检查日志
docker logs nexus-gateway
# 2. 确认配置文件和 .env 路径正确
# 3. 修复后重启
docker compose -f /opt/containers/gateway/docker-compose.yaml restart
```

## 端口规划

| 端口 | 服务 | 老机 | 新机 |
|---|---|---|---|
| 8646 | nexus-gateway (API) | ✅ 容器（已运行） | 停 |
| 8648 | 三池路由（SSL） | 待 nginx 迁移 | 停 |
| 8649 | 直连（SSL） | 待 nginx 迁移 | 停 |
| 8643 | api_server（SSL） | 待 nginx 迁移 | 停 |
| 443 | HTTPS 管理面板 | 待 nginx 迁移 | 停 |
| 8865 | OpenHands ingress | — | 保留 |
| 3001 | OpenHands 前端 | — | 保留 |
| 18000 | agent-server | — | 保留 |
| 18001 | automation | — | 保留 |

## 数据文件清单

| 文件 | 来源 | 目标 |
|---|---|---|
| `nexus-gateway:v3.10` 镜像 | 新机 docker images | 老机 docker load 已完成 |
| `gateway.yaml` | `/root/experiments/gateway/gateway.yaml` | `/opt/containers/gateway/config/gateway.yaml` 已完成 |
| `gateway.db` | `/root/experiments/gateway/gateway.db` | `/opt/containers/gateway/data/gateway.db` 已完成 |
| `.env`（密钥） | `/root/.hermes/.env` | `/opt/containers/gateway/.env` 已完成 |

## nginx 迁移（待执行）

### 前置条件

DNS 解析 `hermes.jiangnande.cloud → 106.14.40.189` 已生效。

### SSL 证书来源

| 证书 | 用途 | 来源 | 位置 |
|---|---|---|---|
| Let's Encrypt（域名） | hermes 443/8443/8643 | 新机 `/etc/letsencrypt/live/hermes.jiangnande.cloud/` | 到期 2026-11-12 |
| 自签 IP 证书 | 网关 8648/8649 | 新机 `/etc/nginx/ssl/hermes-ip/` | 自签 |

> 注：老机已有 certbot（证书 `astrbot.jiangnande.cloud`），域名切换后可执行
> `certbot certonly --nginx -d hermes.jiangnande.cloud` 在本地签发新证书，
> 或直接复制新机证书。

### 迁移步骤

```bash
# 1. 复制证书到老机
scp -P 2222 -r /etc/letsencrypt/live/hermes.jiangnande.cloud root@106.14.40.189:/etc/letsencrypt/live/
scp -P 2222 -r /etc/nginx/ssl/hermes-ip root@106.14.40.189:/etc/nginx/ssl/

# 2. 复制 nginx 配置
scp -P 2222 /etc/nginx/sites-enabled/hermes root@106.14.40.189:/etc/nginx/sites-enabled/

# 3. 老机重载 nginx
ssh -p 2222 root@106.14.40.189 "nginx -t && systemctl reload nginx"

# 4. 验证
curl -s -o /dev/null -w "%{http_code}" https://hermes.jiangnande.cloud/health
```

### 回滚（nginx 迁移失败）

```bash
ssh -p 2222 root@106.14.40.189 "rm /etc/nginx/sites-enabled/hermes && systemctl reload nginx"
# DNS 切回 117.72.220.114
```