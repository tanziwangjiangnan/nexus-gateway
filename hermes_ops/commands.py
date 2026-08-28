"""CLI 命令 — 模型/用量/质量/反馈/日志/撤销/Fiber 查看。

v3.3: 从 gateway.py 拆分。所有命令通过 cfg 参数获取配置。
"""

import datetime
import json
import os
import subprocess
import signal
import sys
import shlex

from hermes_cfg import get_db


def cmd_models(cfg):
    conn = get_db(cfg.get("config", {}).get("db_path"))
    rows = conn.execute("""SELECT r.model, r.pool, r.provider, r.tier, r.status,
                                  COALESCE(SUM(u.prompt_tokens+u.completion_tokens), 0) as tokens
                           FROM registry r LEFT JOIN usage u ON u.model=r.model
                           GROUP BY r.model ORDER BY r.pool, r.model""").fetchall()
    conn.close()
    print(f"{'模型名':<28s} {'池':<8s} {'Provider':<14s} {'档位':<4s} {'状态':<10s} {'今日用量'}")
    print("─" * 85)
    for r in rows:
        print(f"{r['model']:<28s} {r['pool']:<8s} {r['provider']:<14s} {r['tier']:<4s} {r['status']:<10s} {r['tokens']:>8d} tokens")
    print(f"\n共 {len(rows)} 个模型")


def cmd_usage(cfg):
    conn = get_db(cfg.get("config", {}).get("db_path"))
    today = datetime.date.today().isoformat()
    rows = conn.execute("""SELECT model, pool, provider, SUM(prompt_tokens) as p, SUM(completion_tokens) as c,
                                  COUNT(*) as n, SUM(ok) as ok, SUM(CASE WHEN ok=0 THEN 1 ELSE 0 END) as fail
                           FROM usage WHERE date(called_at)=?
                           GROUP BY model ORDER BY (p+c) DESC""", (today,)).fetchall()
    total = conn.execute("""SELECT SUM(prompt_tokens) as p, SUM(completion_tokens) as c
                            FROM usage WHERE date(called_at)=?""", (today,)).fetchone()
    conn.close()
    if not rows:
        print("今日无用量")
        return
    print(f"📊 今日用量 ({today})")
    print(f"{'模型名':<28s} {'池':<8s} {'Provider':<14s} {'Prompt':>8s} {'Completion':>10s} {'调用':>5s} {'成功':>5s}")
    print("─" * 85)
    for r in rows:
        print(f"{r['model']:<28s} {r['pool']:<8s} {r['provider']:<14s} {r['p']:>8d} {r['c']:>10d} {r['n']:>5d} {r['ok']:>5d}")
    if total:
        print(f"\n总计: {total['p']} prompt + {total['c']} completion = {total['p']+total['c']} tokens")


def cmd_quality(cfg):
    """查看每个 Provider 的质量排名（基于检查者评分）。"""
    conn = get_db(cfg.get("config", {}).get("db_path"))
    rows = conn.execute("""
        SELECT provider, COUNT(*) as n, ROUND(AVG(checker_score), 1) as avg_score
        FROM usage WHERE checker_score IS NOT NULL
        GROUP BY provider ORDER BY avg_score DESC
    """).fetchall()
    conn.close()
    if not rows:
        print("暂无检查者评分数据")
    else:
        print(f"\n📊 质量排名（检查者评分）")
        print(f"  {'Provider':<20s} {'样本数':>6s} {'平均分':>6s}")
        print(f"  {'─'*35}")
        for r in rows:
            print(f"  {r['provider']:<20s} {r['n']:>6d} {r['avg_score']:>6.1f}")
        print()


def cmd_feedback_stats(cfg):
    """查看每个 Provider 的用户反馈统计。"""
    conn = get_db(cfg.get("config", {}).get("db_path"))
    rows = conn.execute("""
        SELECT provider,
               COUNT(*) as n,
               SUM(CASE WHEN user_feedback=1 THEN 1 ELSE 0 END) as likes,
               SUM(CASE WHEN user_feedback=-1 THEN 1 ELSE 0 END) as dislikes
        FROM usage WHERE user_feedback != 0
        GROUP BY provider ORDER BY (likes - dislikes) DESC
    """).fetchall()
    conn.close()
    if not rows:
        print("暂无用户反馈数据")
    else:
        print(f"\n👍 用户反馈统计")
        print(f"  {'Provider':<20s} {'样本':>4s} {'点赞':>4s} {'点踩':>4s} {'净分':>5s}")
        print(f"  {'─'*42}")
        for r in rows:
            net = r["likes"] - r["dislikes"]
            print(f"  {r['provider']:<20s} {r['n']:>4d} {r['likes']:>4d} {r['dislikes']:>4d} {net:>+4d}")
        print()


def cmd_git_log(cfg, base):
    subprocess.run(["git", "log", "--oneline", "-20"], cwd=base)


def cmd_git_diff(cfg, base):
    subprocess.run(["git", "diff", "HEAD", "--", "gateway.yaml"], cwd=base)


def cmd_sync_runtime(cfg, base):
    """向运行中进程发 SIGHUP → 触发 reload_config()"""
    pid_file = os.path.join(base, "gateway.pid")
    if os.path.exists(pid_file):
        with open(pid_file) as f:
            pid = int(f.read().strip())
        os.kill(pid, signal.SIGHUP)
        print(f"✅ 已向 PID {pid} 发送 SIGHUP，运行时同步中")
    else:
        print(f"⚠️  未找到 pid 文件，尝试 systemctl reload gateway")
        subprocess.run(["systemctl", "reload", "gateway"])


def cmd_undo_remote(cfg, port=8646):
    """撤销运行时逆栈的最后一条操作（通过 Admin API）"""
    import urllib.request
    gw_key = cfg.get("gateway_key", "")
    req = urllib.request.Request(f"http://127.0.0.1:{port}/admin/undo",
                                 headers={"Authorization": f"Bearer {gw_key}"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            icon = "✅" if data.get("ok") else "❌"
            print(f"{icon} {data.get('message', '')}")
    except Exception as e:
        print(f"❌ 调用失败: {e}")


def cmd_undo_list_remote(cfg, port=8646):
    """查看运行时逆栈（通过 Admin API）"""
    import urllib.request
    gw_key = cfg.get("gateway_key", "")
    req = urllib.request.Request(f"http://127.0.0.1:{port}/admin/undo-list",
                                 headers={"Authorization": f"Bearer {gw_key}"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            stack = data.get("stack", [])
            if not stack:
                print("运行时逆栈为空")
            else:
                for i, desc in enumerate(stack, 1):
                    print(f"  {i}. {desc}")
    except Exception as e:
        print(f"❌ 调用失败: {e}")


def cmd_fiber_view(cfg, port=8646):
    """查看 fiber 树（通过 Admin API）"""
    import urllib.request
    gw_key = cfg.get("gateway_key", "")
    req = urllib.request.Request(f"http://127.0.0.1:{port}/admin/fiber/tree",
                                 headers={"Authorization": f"Bearer {gw_key}"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            fibers = data.get("fibers", {})
            if not fibers:
                print("fiber 森林为空")
            else:
                def _print_tree(fid, indent=0):
                    f = fibers.get(str(fid))
                    if not f:
                        return
                    prefix = "  " * indent + ("└─ " if indent > 0 else "")
                    icon = {"active": "🟢", "committed": "✅", "failed": "❌"}.get(f["status"], "⚪")
                    print(f"{prefix}{icon} #{f['id']} {f['description']} [{f['status']}] agent={f['agent_id']} undo={f['undo_count']}")
                    for child_id in sorted(f["children"]):
                        _print_tree(child_id, indent + 1)
                for fid, f in sorted(fibers.items()):
                    if f["parent_id"] is None:
                        _print_tree(int(fid))
    except Exception as e:
        print(f"❌ 调用失败: {e}")