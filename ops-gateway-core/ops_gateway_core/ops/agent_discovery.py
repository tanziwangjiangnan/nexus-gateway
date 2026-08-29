"""容器化智能体发现 — Docker 容器目标解析。

v3.4: 支持声明式接入 Docker 容器中的智能体（OpenHands / AstrBot 等）。
解析优先级:
  1. base_url 显式指定（最高优先级，跳过自动发现）
  2. container_name / container_id — docker CLI 查询容器 IP 和端口
  3. compose_project + compose_service — docker compose 解析
  4. workspace — 走原有本地文件系统逻辑（由上层处理）

使用 docker CLI（subprocess）而非 python docker SDK，
避免额外依赖和容器 socket 权限问题。
"""

import json
import subprocess


def _docker(*args, timeout=10):
    """执行 docker CLI，返回 (ok, stdout)。"""
    try:
        r = subprocess.run(
            ["docker", *args],
            capture_output=True, text=True, timeout=timeout,
        )
        if r.returncode != 0:
            return False, r.stderr.strip()
        return True, r.stdout.strip()
    except FileNotFoundError:
        return False, "docker CLI 不可用"
    except subprocess.TimeoutExpired:
        return False, f"docker {args[0]} 超时"


def resolve_by_docker(container, port=None):
    """通过容器名/ID 查询可达的 HTTP base_url。

    Args:
        container: 容器名或容器 ID
        port: 可选，容器内 HTTP 端口（默认取暴露端口或 80）

    Returns:
        (base_url, method) — 如 ("http://172.17.0.2:8863", "container")
        失败时返回 (None, error)
    """
    ok, out = _docker("inspect", "--format", "{{json .NetworkSettings}}", container)
    if not ok:
        return None, f"container inspect: {out}"

    try:
        nets = json.loads(out)
    except json.JSONDecodeError:
        return None, "container inspect 输出解析失败"

    ip = None
    for net in nets.get("Networks", {}).values():
        if net.get("IPAddress"):
            ip = net["IPAddress"]
            break

    # 端口解析：显式指定 > 端口映射 > 暴露端口
    if not port:
        exposed = nets.get("ExposedPorts") or {}
        if exposed:
            port = int(next(iter(exposed)).split("/")[0])
        else:
            port = 80
    if not ip:
        return None, "容器未连接网络（无 IP）"

    return f"http://{ip}:{port}", "container"


def resolve_by_compose(project, service, port=None):
    """通过 compose 项目 + 服务名解析容器。

    Returns:
        (base_url, method) — ("http://<service>:<port>", "compose")
    """
    ok, out = _docker("compose", "-p", project, "ps", "-q", service)
    if not ok:
        return None, f"compose ps: {out}"
    container_id = out.splitlines()[0] if out.strip() else ""
    if not container_id:
        return None, "compose 服务未运行"

    if not port:
        # 尝试从容器推断端口
        ok2, out2 = _docker("inspect", "--format", "{{json .NetworkSettings.Ports}}", container_id)
        if ok2 and out2.strip():
            try:
                ports = json.loads(out2)
                if ports:
                    first = next(iter(ports))
                    port = int(first.split("/")[0])
            except (json.JSONDecodeError, ValueError):
                pass
        if not port:
            port = 80

    # compose 网络内使用服务名直接访问
    return f"http://{service}:{port}", "compose"


def resolve_agent_target(agent_cfg):
    """解析 agent 配置为可达 HTTP 目标。

    Args:
        agent_cfg: gateway.yaml 中 agents 段的一个条目

    Returns:
        (base_url, method) 或 (None, error)
        method: explicit / container / compose / None
    """
    base_url = agent_cfg.get("base_url")
    if base_url:
        return base_url.rstrip("/"), "explicit"

    container = agent_cfg.get("container_name") or agent_cfg.get("container_id")
    if container:
        return resolve_by_docker(container, agent_cfg.get("port"))

    if agent_cfg.get("compose_project") and agent_cfg.get("compose_service"):
        return resolve_by_compose(
            agent_cfg["compose_project"],
            agent_cfg["compose_service"],
            agent_cfg.get("port"),
        )

    return None, None
