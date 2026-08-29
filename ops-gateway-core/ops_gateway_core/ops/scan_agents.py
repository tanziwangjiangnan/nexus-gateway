"""智能体自动发现 — scan-agents 命令。

v3.3: 从 gateway.py 拆分。扫描指定目录寻找 Agent 特征文件，
交互式确认后写入 gateway.yaml 并热加载。
"""

import os
import signal
import subprocess

import yaml


def _detect_agents(scan_dir):
    """扫描目录，返回发现的候选 agent 列表。"""
    found = []
    print(f"🔍 扫描 {scan_dir} ...")
    for root, dirs, files in os.walk(scan_dir):
        rel = os.path.relpath(root, scan_dir)
        if rel.startswith(".") or rel.startswith("_"):
            continue
        basename = os.path.basename(root)
        if basename in ("node_modules", "__pycache__", ".git", ".venv", "venv", "env", ".tox"):
            dirs[:] = []
            continue

        if "config.toml" in files:
            try:
                content = open(os.path.join(root, "config.toml")).read()
                if "[core]" in content:
                    found.append({
                        "id": f"openhands-{len(found)}",
                        "display_name": f"OpenHands ({rel})",
                        "type": "openhands",
                        "workspace": root,
                        "capabilities": ["read", "write", "execute"],
                        "confidence": "high",
                        "evidence": "config.toml → [core]",
                    })
                    continue
            except Exception:
                pass
        lock_files = [f for f in files if f.endswith(".lock")]
        if lock_files:
            found.append({
                "id": f"openhands-{len(found)}",
                "display_name": f"OpenHands ({rel})",
                "type": "openhands",
                "workspace": root,
                "capabilities": ["read", "write", "execute"],
                "confidence": "medium",
                "evidence": f"lock 文件: {', '.join(lock_files[:3])}",
            })
            continue

        if "main.py" in files:
            try:
                content = open(os.path.join(root, "main.py")).read()
                if "AstrBot" in content or "astrbot" in content.lower():
                    found.append({
                        "id": f"astrbot-{len(found)}",
                        "display_name": f"AstrBot ({rel})",
                        "type": "astrbot",
                        "base_url": "http://127.0.0.1:12345",
                        "capabilities": ["read"],
                        "confidence": "high",
                        "evidence": "main.py → AstrBot",
                    })
                    continue
            except Exception:
                pass
        if "config.yaml" in files:
            try:
                content = open(os.path.join(root, "config.yaml")).read()
                if "adapters" in content:
                    found.append({
                        "id": f"astrbot-{len(found)}",
                        "display_name": f"AstrBot ({rel})",
                        "type": "astrbot",
                        "base_url": "http://127.0.0.1:12345",
                        "capabilities": ["read"],
                        "confidence": "medium",
                        "evidence": "config.yaml → adapters",
                    })
                    continue
            except Exception:
                pass

        pid_files = [f for f in files if f.endswith(".pid")]
        if pid_files:
            found.append({
                "id": f"agent-{len(found)}",
                "display_name": f"Agent ({rel})",
                "type": "generic",
                "command": f"python3 {os.path.join(root, 'main.py')}" if "main.py" in files else "",
                "pid_file": os.path.join(root, pid_files[0]),
                "capabilities": ["read"],
                "confidence": "medium",
                "evidence": f"pid 文件: {pid_files[0]}",
            })
            continue

    return found


def _discover_plugins(found):
    """扫描每个 Agent 目录下的 plugins.yaml。"""
    discovered = []
    for agent in found:
        agent_dir = agent.get("workspace") or os.path.dirname(agent.get("pid_file", ""))
        if not agent_dir:
            continue
        plugin_file = os.path.join(agent_dir, "plugins.yaml")
        if os.path.isfile(plugin_file):
            try:
                with open(plugin_file) as f:
                    raw = f.read()
                plugin_list = yaml.safe_load(raw) or []
                if isinstance(plugin_list, list):
                    for p in plugin_list:
                        p["provider"] = agent["id"]
                        if "id" not in p:
                            p["id"] = f"{agent['id']}-{p.get('display_name', 'plugin')}"
                        discovered.append(p)
            except Exception as e:
                print(f"  ⚠️  解析 {agent['id']} 的 plugins.yaml 失败: {e}")
    return discovered


def _print_candidates(found, discovered_plugins):
    print(f"\n📋 发现 {len(found)} 个智能体候选:\n")
    for i, agent in enumerate(found, 1):
        icon = {"openhands": "🤖", "astrbot": "💬", "generic": "⚙️"}.get(agent["type"], "❓")
        print(f"  {i}. {icon} {agent['display_name']}")
        print(f"     类型: {agent['type']} | 置信度: {agent['confidence']}")
        print(f"     证据: {agent['evidence']}")
        if agent.get("workspace"):
            print(f"     路径: {agent['workspace']}")
        if agent.get("pid_file"):
            print(f"     PID: {agent['pid_file']}")
        agent_plugins = [p for p in discovered_plugins if p.get("provider") == agent["id"]]
        if agent_plugins:
            for p in agent_plugins:
                print(f"     📦 插件: {p.get('display_name', p['id'])} ({p.get('execution', '?')})")
        print()


def _diff_candidates(found, existing_agents):
    """与已存在 agents 对比，返回 to_add 列表。"""
    existing_ids = {a["id"] for a in existing_agents}
    to_add = []
    for agent in found:
        if agent["id"] in existing_ids:
            print(f"  ⏭️  {agent['display_name']} 已存在，跳过")
            continue
        entry = {
            "id": agent["id"],
            "display_name": agent["display_name"],
            "type": agent["type"],
            "capabilities": agent["capabilities"],
        }
        for key in ("workspace", "base_url", "command", "pid_file"):
            if agent.get(key):
                entry[key] = agent[key]
        to_add.append(entry)
    return to_add


def _write_yaml_blocks(raw, to_add, discovered_plugins, existing_plugins):
    """把新 agents + 新 plugins 写入 YAML 文本。"""
    if to_add:
        agents_yaml = yaml.dump(to_add, default_flow_style=False, allow_unicode=True, sort_keys=False)
        agents_yaml = "\n".join("  " + line if line.strip() else "" for line in agents_yaml.strip().split("\n"))
        if "# ── 智能体声明" in raw:
            insert_pos = raw.rfind("\n  - id:")
            raw = raw[:insert_pos] + "\n" + agents_yaml + raw[insert_pos:]
        else:
            marker = "# ── 验证模式"
            agents_block = f"\n# ── 智能体声明 ──\n# 自动发现: scan-agents 命令生成\n# 不迁移、不复制任何用户文件，只需声明路径/地址。\nagents:\n{agents_yaml}\n\n"
            if marker in raw:
                raw = raw.replace(marker, agents_block + marker)
            else:
                raw += "\n" + agents_block

    if discovered_plugins:
        existing_ids = {p["id"] for p in existing_plugins}
        new_plugins = [p for p in discovered_plugins if p["id"] not in existing_ids]
        if new_plugins:
            plugins_yaml = yaml.dump(new_plugins, default_flow_style=False, allow_unicode=True, sort_keys=False)
            plugins_yaml = "\n".join("  " + line if line.strip() else "" for line in plugins_yaml.strip().split("\n"))
            if "# ── 插件声明" in raw:
                plugins_section = raw[raw.find("# ── 插件声明"):]
                last_id_in_plugins = plugins_section.rfind("\n  - id:")
                if last_id_in_plugins >= 0:
                    abs_pos = raw.find("# ── 插件声明") + last_id_in_plugins
                    raw = raw[:abs_pos] + "\n" + plugins_yaml + raw[abs_pos:]
                else:
                    raw = raw.replace("# ── 插件声明", f"# ── 插件声明 ──\nplugins:\n{plugins_yaml}\n")
            else:
                agents_block = f"\n# ── 插件声明 ──\n# 自动发现: scan-agents 命令生成\nplugins:\n{plugins_yaml}\n\n"
                marker = "# ── 验证模式"
                if marker in raw:
                    raw = raw.replace(marker, agents_block + marker)
                else:
                    raw += "\n" + agents_block

    return raw


def cmd_scan_agents(cfg, base, config_path, scan_dir="~/agents"):
    """自动发现并接入智能体。"""
    scan_dir = os.path.expanduser(scan_dir)
    if not os.path.isdir(scan_dir):
        print(f"⚠️  目录不存在: {scan_dir}")
        print(f"   创建后重试，或指定: python3 gateway.py scan-agents --dir /path/to/agents")
        return

    found = _detect_agents(scan_dir)
    if not found:
        print(f"  未发现已知的智能体。")
        print(f"  提示: 将智能体放在 {scan_dir} 下的子目录中，或手动编辑 gateway.yaml 的 agents 段。")
        return

    discovered_plugins = _discover_plugins(found)
    _print_candidates(found, discovered_plugins)

    print(f"是否接入以上 {len(found)} 个智能体到 gateway.yaml？")
    existing = cfg.get("agents", [])
    to_add = _diff_candidates(found, existing)

    if not to_add:
        print("  没有新增的智能体需要接入。")
        return

    print(f"  将接入 {len(to_add)} 个新智能体。确认？[Y/n] ", end="")
    try:
        resp = input().strip().lower()
    except Exception:
        resp = "y"
    if resp not in ("", "y", "yes"):
        print("  已取消")
        return

    with open(config_path) as f:
        raw = f.read()
    raw = _write_yaml_blocks(raw, to_add, discovered_plugins, cfg.get("plugins", []))
    with open(config_path, "w") as f:
        f.write(raw)

    print(f"  ✅ 已写入 {config_path}")
    try:
        subprocess.run(["systemctl", "reload", "gateway"], capture_output=True, timeout=10)
        pid_file = os.path.join(base, "gateway.pid")
        if os.path.exists(pid_file):
            with open(pid_file) as f:
                pid = int(f.read().strip())
            os.kill(pid, signal.SIGHUP)
        print(f"  🔄 gateway 已热加载")
    except Exception as e:
        print(f"  ⚠️  热加载失败: {e}，手动执行: systemctl reload gateway")
    print(f"\n✨ 已完成。新增 {len(to_add)} 个智能体:")
    for a in to_add:
        print(f"   • {a['display_name']} ({a['type']})")