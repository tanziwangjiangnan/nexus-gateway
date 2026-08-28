"""反向依赖检查 — check-deps 命令。

v3.3: 从 gateway.py 拆分。扫描本地 .env / OpenHands 配置 / 环境变量
以及远程 SSH 主机上的 Key/URL 引用。
"""

import os
import shlex
import subprocess


def collect_all_keys(config):
    """从 config 中提取所有 Key（gateway_key + 各 provider 的 api_key + 外部 URL）。"""
    keys = {}
    gw_key = config.get("gateway_key", "")
    if gw_key:
        keys["gateway_key"] = gw_key
        for prefix in ("gw-", "gw_", "hermes-", "hermes_"):
            if gw_key.startswith(prefix):
                variant = gw_key[len(prefix):]
                keys[f"gateway_key_variant.{prefix}"] = variant
                break
    for pname, pcfg in config.get("providers", {}).items():
        ak = pcfg.get("api_key", "")
        if ak and not ak.startswith("${"):
            keys[f"providers.{pname}.api_key"] = ak
        api = pcfg.get("api", "")
        if api:
            keys[f"providers.{pname}.api"] = api
    host = config.get("host", "127.0.0.1")
    port = config.get("port", 8646)
    keys["_self_url"] = f"http://{host}:{port}"
    keys["_self_url_https"] = "https://117.72.220.114:8643"
    keys["_self_url_domain"] = "https://hermes.jiangnande.cloud:8643"
    return keys


def scan_local(index, config, base):
    """扫描本地依赖：.env, 已知 Agent 配置文件, 环境变量。"""
    env_path = os.path.join(base, ".env")
    if os.path.isfile(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                for key_name, key_val in config.items():
                    if key_val in v:
                        index.append({
                            "component": "本机 .env",
                            "file": env_path,
                            "key_name": key_name,
                            "current_value": key_val,
                            "found_at": f"{k} = {v[:60]}",
                            "fixable": True,
                            "fix_type": "sed",
                        })

    oh_paths = [
        os.path.expanduser("~/.openhands/config.toml"),
        os.path.expanduser("~/.config/oh/config.toml"),
    ]
    for oh_path in oh_paths:
        if os.path.isfile(oh_path):
            with open(oh_path) as f:
                content = f.read()
                for key_name, key_val in config.items():
                    if key_val in content:
                        index.append({
                            "component": "OpenHands",
                            "file": oh_path,
                            "key_name": key_name,
                            "current_value": key_val,
                            "found_at": "配置文件中引用",
                            "fixable": True,
                            "fix_type": "sed",
                        })

    for key_name, key_val in config.items():
        for env_name, env_val in sorted(os.environ.items()):
            if key_val == env_val or (key_val and key_val in env_val):
                if env_name in ("XIAOMI_API_KEY", "DEEPSEEK_API_KEY", "OPENHANDS_API_KEY", "GATEWAY_KEY"):
                    index.append({
                        "component": f"环境变量 {env_name}",
                        "file": "进程环境变量",
                        "key_name": key_name,
                        "current_value": key_val,
                        "found_at": f"{env_name} = {env_val[:60]}",
                        "fixable": False,
                        "fix_type": "env",
                    })


def scan_remote(host, port, cmd, config, label):
    """通过 SSH 在远程主机上扫描 Key 引用。"""
    try:
        full_cmd = f'ssh -o StrictHostKeyChecking=no -o ConnectTimeout=8 -p {port} root@{host} {shlex.quote(cmd)}'
        result = subprocess.run(full_cmd, shell=True, capture_output=True, timeout=15, text=True)
        if result.returncode != 0:
            return []
        content = result.stdout
        hits = []
        for key_name, key_val in config.items():
            if key_val in content:
                for i, line in enumerate(content.split("\n"), 1):
                    if key_val in line:
                        hits.append({
                            "component": label,
                            "file": f"{host}:{port} — 远程配置",
                            "key_name": key_name,
                            "current_value": key_val,
                            "found_at": f"第 {i} 行: {line.strip()[:80]}",
                            "fixable": True,
                            "fix_type": "ssh_sed",
                            "_remote": {
                                "host": host,
                                "port": port,
                                "file": cmd.split("cat ")[-1] if "cat " in cmd else "",
                            },
                        })
                        break
        return hits
    except Exception as e:
        return [{"component": label, "file": f"{host}:{port}", "key_name": "—",
                 "current_value": "—", "found_at": f"SSH 连接失败: {e}",
                 "fixable": False, "fix_type": "unreachable"}]


def cmd_check_deps(config, base, target_key=None, auto_sync=False):
    """check-deps 命令：反向依赖扫描 + 可选自动同步。"""
    all_keys = collect_all_keys(config)
    if target_key:
        filtered = {}
        for kn, kv in all_keys.items():
            if target_key in kv or target_key in kn:
                filtered[kn] = kv
        if not filtered:
            print(f"🔍 未找到匹配 '{target_key}' 的 Key")
            return
        all_keys = filtered

    print(f"\n🔍 反向依赖扫描 — 共 {len(all_keys)} 个配置项\n")
    for kn, kv in all_keys.items():
        if kn.startswith("_"):
            continue
        print(f"  📌 {kn}: {kv[:60]}...")
    print()

    index = []
    scan_local(index, all_keys, base)
    index += scan_remote("106.14.40.189", "2222",
                         "cat /opt/qq-bot/bot/astrbot/data/cmd_config.json",
                         all_keys, "AstrBot（老机）")
    index += scan_remote("106.14.20.149", "2222",
                         "cat /opt/kb-agent/config.json 2>/dev/null || cat /app/config.json 2>/dev/null || echo 'NO_CONFIG'",
                         all_keys, "kb_agent（新机）")

    if not index:
        print("✅ 未发现任何外部依赖，配置变更安全。")
        return

    fixable_deps = [d for d in index if d.get("fixable")]
    unfixable_deps = [d for d in index if not d.get("fixable")]

    if unfixable_deps:
        print("⚠️  以下依赖无法自动修复：\n")
        for d in unfixable_deps:
            print(f"   ❌ {d['component']}")
            print(f"      {d['found_at']}")
        print()

    if fixable_deps:
        print(f"🔧 以下 {len(fixable_deps)} 个依赖可自动同步：\n")
        for d in fixable_deps:
            print(f"   📎 {d['component']} ({d['file']})")
            print(f"      {d['found_at']}")
        print()

    if auto_sync and fixable_deps:
        print("=" * 50)
        print("🔄 自动同步模式启用\n")
        failed = False
        for d in fixable_deps:
            if d["fix_type"] == "sed" and os.path.isfile(d["file"]):
                old_val = d["current_value"]
                new_val = all_keys.get(d["key_name"], "")
                if not new_val:
                    continue
                bak = d["file"] + ".bak"
                try:
                    subprocess.run(f"cp {d['file']} {bak}", shell=True, capture_output=True, timeout=5)
                    esc_old = old_val.replace("/", "\\/").replace("'", "'\\''")
                    esc_new = new_val.replace("/", "\\/").replace("'", "'\\''")
                    r = subprocess.run(
                        f"sed -i 's/{esc_old}/{esc_new}/g' {d['file']}",
                        shell=True, capture_output=True, timeout=10, text=True)
                    if r.returncode == 0:
                        print(f"   ✅ {d['component']} — 已同步")
                    else:
                        print(f"   ❌ {d['component']} — sed 失败: {r.stderr[:80]}")
                        failed = True
                except Exception as e:
                    print(f"   ❌ {d['component']} — 异常: {e}")
                    failed = True

            elif d["fix_type"] == "ssh_sed" and d.get("_remote"):
                rhost = d["_remote"]["host"]
                rport = d["_remote"]["port"]
                rfile = d["_remote"]["file"]
                old_val = d["current_value"]
                new_val = all_keys.get(d["key_name"], "")
                if not new_val or not rfile:
                    continue
                esc_old = old_val.replace("/", "\\/").replace("'", "'\\''")
                esc_new = new_val.replace("/", "\\/").replace("'", "'\\''")
                try:
                    bak_cmd = f"ssh -o StrictHostKeyChecking=no -p {rport} root@{rhost} 'cp {rfile} {rfile}.depsync.bak' 2>/dev/null"
                    subprocess.run(bak_cmd, shell=True, capture_output=True, timeout=10)
                    sed_cmd = shlex.quote(f"sed -i 's/{esc_old}/{esc_new}/g' {rfile}")
                    full = f"ssh -o StrictHostKeyChecking=no -p {rport} root@{rhost} {sed_cmd}"
                    r = subprocess.run(full, shell=True, capture_output=True, timeout=15, text=True)
                    if r.returncode == 0:
                        print(f"   ✅ {d['component']} ({rhost}) — 已同步")
                    else:
                        print(f"   ❌ {d['component']} ({rhost}) — 同步失败: {r.stderr[:80]}")
                        failed = True
                except Exception as e:
                    print(f"   ❌ {d['component']} ({rhost}) — 异常: {e}")
                    failed = True

        if failed:
            print("\n   ❌ 部分依赖同步失败，请检查日志。")
        else:
            print("\n   ✅ 所有依赖已更新，变更安全。")

    elif auto_sync and not fixable_deps:
        print("🔄 没有可自动修复的依赖。")

    print()