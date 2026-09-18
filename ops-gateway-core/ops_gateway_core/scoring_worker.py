"""监督者评分后台任务（IO 层：发 HTTP + 写库）

职责：拿主 provider 的问题与回答，请第二名检查者打分，写入 usage.checker_score 并更新质量因子。
被谁调用：api/app.py 的 chat 主链（asyncio.create_task）；触发判定在 provider_router/scoring.py。

依赖：Router.resolve_env_key + httpx + 模块级 db_conn（这里在 build_app 之外，只能用默认连接）。
v3.17（2026-09-18）从 api/app.py 抽出（docs/模块约定.md 第 5 步）。
"""
import json
import time

import httpx

from provider_router import Router
from .cfg import db_conn as _default_db_conn


def get_provider_from_last_usage(conn):
    """返回最近一次调用使用的 provider 名（用于检查者评分关联）。

    conn 由调用方注入（与 app 内其余 db 访问走同一条连接来源）。
    """
    row = conn.execute("SELECT provider FROM usage ORDER BY id DESC LIMIT 1").fetchone()
    return row["provider"] if row else None


# ── 监督者（Supervisor）评分机制 ──
# 触发判定（should_score / supervisor_cfg / read_force_on）已抽到 provider_router/scoring.py（纯函数 + 单测）；
# 后台打分任务 _score_by_runner_up 仍在下方（它要发 HTTP + 写库，属于下一步拆分）。
# 设计文档：.openhands/memory/designs/supervisor-scoring.md
# 默认开启但不每次都审：冷启动 100% → 稳态概率衰减 → 大变量强制复审。


async def score_by_runner_up(cfg, provider, runner_up,
                               provider_model, messages, resp_body,
                               quality_factors, scoring_state):
    """后台任务：调第二名给第一名的回答打分。"""
    try:
        # 构造评分 prompt
        data = json.loads(resp_body)
        assistant_reply = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        if not assistant_reply:
            return

        scoring_cfg = cfg.get("supervisor", {}).get("scoring", {})
        scoring_prompt = scoring_cfg.get("prompt") or (
            "你是一个质量评分员。请根据用户的提问和 AI 的回答，"
            "给回答的质量评分（0-100，整数）。"
            "考虑：准确性、完整性、逻辑性、语言质量。"
            "只返回数字，不要其他文字。"
        )
        scoring_messages = [
            {"role": "system", "content": scoring_prompt},
            {
                "role": "user",
                "content": f"问题：{messages[-1]['content'] if messages else ''}\n\n回答：{assistant_reply[:2000]}"
            },
        ]

        # 确定评分模型
        scoring_model = scoring_cfg.get("model")
        # 找 runner-up 的 API 配置
        rp_cfg = cfg.get("providers", {}).get(runner_up["name"])
        if not rp_cfg:
            return
        rp_key = Router.resolve_env_key(rp_cfg.get("api_key", ""))
        if not rp_key:
            return
        rp_api = rp_cfg["api"].rstrip("/")
        # 如果配置了评分模型，用评分模型；否则用 runner_up 的第一个模型
        if scoring_model:
            scoring_model_used = scoring_model
        else:
            scoring_model_used = runner_up.get("models", [provider_model])[0]

        # 发起评分请求
        async with httpx.AsyncClient(timeout=30) as client:
            score_resp = await client.post(
                f"{rp_api}/chat/completions",
                json={
                    "model": scoring_model_used,
                    "messages": scoring_messages,
                    "max_tokens": 50,
                    "temperature": 0,
                },
                headers={"Authorization": f"Bearer {rp_key}"},
            )
            if score_resp.status_code != 200:
                return
            score_data = score_resp.json()
            score_text = (score_data.get("choices", [{}])[0]
                          .get("message", {}).get("content", ""))
            # 解析数字
            score = None
            for token in score_text.strip().split():
                try:
                    s = int(''.join(c for c in token if c.isdigit() or c == '-'))
                    if 0 <= s <= 100:
                        score = s
                        break
                except ValueError:
                    continue
            if score is None:
                return
            # 写入 DB
            # [2026-09-18] 本函数在 build_app 之外，看不到 deps 注入的 db_conn，
            # 只能用模块级别名（762e7da 把 import 改成别名后这里漏改，评分一直 NameError）。
            with _default_db_conn() as conn:
                conn.execute(
                    "UPDATE usage SET checker_score = ? WHERE id = (SELECT id FROM usage WHERE provider = ? ORDER BY id DESC LIMIT 1)",
                    (score, provider))
                conn.commit()
            # 更新运行时质量因子
            quality_window = cfg.get("quality_feedback", {}).get("quality_window", 20)
            with _default_db_conn() as conn2:
                rows = conn2.execute(
                    "SELECT checker_score FROM usage WHERE provider = ? AND checker_score IS NOT NULL ORDER BY id DESC LIMIT ?",
                    (provider, quality_window)
                ).fetchall()
            if rows:
                avg = sum(r[0] for r in rows) / len(rows)
                quality_factors[provider] = max(0.5, min(1.0, avg / 100.0))
            # 更新评分状态
            if provider not in scoring_state:
                scoring_state[provider] = {}
            st = scoring_state[provider]
            now = time.time()
            st["last"] = now
            st["count"] = st.get("count", 0) + 1
            st["last_score_value"] = score
            # 更新 last_runners（最多保留 3 个）
            last_runners = st.get("last_runners", [])
            if runner_up["name"] not in last_runners:
                last_runners.append(runner_up["name"])
                if len(last_runners) > 3:
                    last_runners.pop(0)
            st["last_runners"] = last_runners
            # 更新 recent_scores（最多 5 个，用于方差检测）
            recent = st.get("recent_scores", [])
            recent.append(score)
            if len(recent) > 5:
                recent.pop(0)
            st["recent_scores"] = recent
            # 方差爆发期递减
            if st.get("variance_boost_remaining", 0) > 0:
                st["variance_boost_remaining"] -= 1
            # 如果这次评分确实触发了方差突变，设置爆发期
            stddev = 0.0
            if len(recent) >= 3:
                mean = sum(recent) / len(recent)
                variance = sum((s - mean) ** 2 for s in recent) / len(recent)
                stddev = variance ** 0.5
                if stddev > 15:
                    st["variance_boost_remaining"] = 5
            print(f"📋 监督者评分 [{runner_up['name']}]→{provider}: {score}分 "
                  f"(第{st['count']}次, 标准差={stddev:.1f})")
    except Exception:
        import traceback
        traceback.print_exc()
