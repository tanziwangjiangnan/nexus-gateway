"""benchmark — 离线基准评分工具。

阶段 1:
  - 固定测试题集（10 题，覆盖代码/数学/翻译/对话/创意）
  - 发送给每个 provider 评分，结果写 quality_benchmark.yaml
  - 生产环境启动时读取基准，零 Token 开销

使用:
  python3 gateway.py benchmark --all          # 全量评分
  python3 gateway.py benchmark --all --model deepseek-v4-flash  # 指定评分模型
  python3 gateway.py benchmark --provider scnet-tp  # 单 provider

输出:
  {BASE}/quality_benchmark.yaml
"""

import os
import sys
import json
import time
import datetime
import yaml

from ..cfg import get_db
from ..cfg.loader import ConfigLoader

BENCHMARK_FILENAME = "quality_benchmark.yaml"

# ── 固定测试题集（10 题，覆盖 5 个维度） ──
BENCHMARK_QUESTIONS = [
    # 代码 (2)
    {
        "id": "code_1",
        "category": "code",
        "question": "用 Python 写一个快速排序算法，包含注释。",
    },
    {
        "id": "code_2",
        "category": "code",
        "question": "写一个 SQL 查询：找出每个部门工资最高的员工，返回部门名、员工名、工资。",
    },
    # 数学推理 (2)
    {
        "id": "math_1",
        "category": "math",
        "question": "一个圆柱体底面半径 3cm，高 5cm，求体积和表面积。（π取 3.14）",
    },
    {
        "id": "math_2",
        "category": "math",
        "question": "如果 3x + 7 = 22，求 x 的值，并验证。",
    },
    # 翻译 (2)
    {
        "id": "trans_1",
        "category": "translation",
        "question": "将以下英文翻译成中文：'The only way to do great work is to love what you do. Stay hungry, stay foolish.'",
    },
    {
        "id": "trans_2",
        "category": "translation",
        "question": "将以下中文翻译成英文：'人工智能正在深刻改变各行各业，从医疗到教育，从金融到制造业都离不开 AI 的赋能。'",
    },
    # 通用对话 (2)
    {
        "id": "chat_1",
        "category": "general_chat",
        "question": "请介绍一下你自己，你能做什么？",
    },
    {
        "id": "chat_2",
        "category": "general_chat",
        "question": "我最近工作压力很大，有什么建议可以帮助我缓解压力？",
    },
    # 创意写作 (2)
    {
        "id": "creative_1",
        "category": "creative_writing",
        "question": "以'深夜的便利店'为题，写一段 100 字左右的短文，营造氛围。",
    },
    {
        "id": "creative_2",
        "category": "creative_writing",
        "question": "为一个科幻小说构思一个引人入胜的开头段落，主题是'意识上传'。",
    },
]

# ── 评分 prompt ──
DEFAULT_SCORING_PROMPT = (
    "你是一个质量评分员。请根据用户的提问和 AI 的回答，"
    "给回答的质量评分（0-100，整数）。"
    "考虑：准确性、完整性、逻辑性、语言质量。"
    "只返回数字，不要其他文字。"
)


def _resolve_env_key(raw: str) -> str | None:
    """解析 ${VAR} 环境变量引用。"""
    if not raw:
        return None
    raw = raw.strip()
    if raw.startswith("${") and raw.endswith("}"):
        return os.environ.get(raw[2:-1])
    return raw


def _call_llm(api_url: str, api_key: str, model: str, messages: list, timeout: int = 30) -> dict | None:
    """同步调用 LLM API。"""
    import httpx
    try:
        resp = httpx.post(
            f"{api_url.rstrip('/')}/chat/completions",
            json={"model": model, "messages": messages, "max_tokens": 2048, "temperature": 0.7},
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout,
        )
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"    ⚠️  API 返回 {resp.status_code}: {resp.text[:200]}")
            return None
    except Exception as e:
        print(f"    ⚠️  请求失败: {e}")
        return None


def _score_response(api_url: str, api_key: str, scoring_model: str,
                    question: str, response: str, scoring_prompt: str) -> int | None:
    """调评分模型给回答打分。"""
    messages = [
        {"role": "system", "content": scoring_prompt},
        {"role": "user", "content": f"问题：{question}\n\n回答：{response[:2000]}"},
    ]
    result = _call_llm(api_url, api_key, scoring_model, messages, timeout=30)
    if not result:
        return None
    score_text = (result.get("choices", [{}])[0].get("message", {}).get("content", ""))
    for token in score_text.strip().split():
        try:
            s = int("".join(c for c in token if c.isdigit() or c == "-"))
            if 0 <= s <= 100:
                return s
        except ValueError:
            continue
    return None


def cmd_benchmark(cfg, base: str, provider_filter: str | None = None,
                  scoring_model: str | None = None):
    """运行离线基准测试，生成 quality_benchmark.yaml。"""
    providers_cfg = cfg.get("providers", {})
    pools_cfg = cfg.get("pools", {})

    if not providers_cfg:
        print("❌ 配置中未定义任何 provider")
        return

    # 收集所有活跃 provider（从池中找到的）
    active_providers = set()
    for pc in pools_cfg.values():
        for pv in pc.get("providers", []):
            if pv["name"] in providers_cfg:
                active_providers.add(pv["name"])

    if not active_providers:
        print("❌ 池中未找到活跃 provider")
        return

    # 过滤
    if provider_filter:
        if provider_filter not in active_providers:
            print(f"❌ Provider '{provider_filter}' 不在活跃列表中")
            return
        active_providers = {provider_filter}

    # 确定评分模型
    if not scoring_model:
        # 默认用第一个池的第一个 provider 的第二个模型（如果有），否则第一个
        for pc in pools_cfg.values():
            for pv in pc.get("providers", []):
                models = pv.get("models", [])
                if len(models) >= 2:
                    scoring_model = models[1]
                    break
                elif models:
                    scoring_model = models[0]
                break
            break
        if not scoring_model:
            scoring_model = "deepseek-v4-flash"

    scoring_prompt = cfg.get("supervisor", {}).get("scoring", {}).get("prompt") or DEFAULT_SCORING_PROMPT

    print(f"\n📊 离线基准评分")
    print(f"   评分模型: {scoring_model}")
    print(f"   测试题数: {len(BENCHMARK_QUESTIONS)}")
    print(f"   Provider: {', '.join(sorted(active_providers))}")
    print(f"   {'─' * 50}")

    results = {}  # {provider: {category: [scores]}}

    for pname in sorted(active_providers):
        rp_cfg = providers_cfg[pname]
        api_key = _resolve_env_key(rp_cfg.get("api_key", ""))
        api_url = rp_cfg.get("api", "")
        if not api_key or not api_url:
            print(f"\n⚠️  {pname}: 跳过（缺少 api_key 或 api）")
            continue

        models = rp_cfg.get("models", [])
        # 找第一个池中该 provider 声明的模型
        for pc in pools_cfg.values():
            for pv in pc.get("providers", []):
                if pv["name"] == pname:
                    models = pv.get("models", [])
                    break
        if not models:
            print(f"\n⚠️  {pname}: 跳过（未找到模型）")
            continue

        model = models[0]  # 用第一个模型回答问题
        print(f"\n🔍 {pname} (model={model})")

        # 先用模型回答所有问题
        answers = []
        for q in BENCHMARK_QUESTIONS:
            print(f"  提问: {q['category']}...", end=" ")
            sys.stdout.flush()
            result = _call_llm(api_url, api_key, model, [
                {"role": "user", "content": q["question"]},
            ], timeout=60)
            if result:
                content = (result.get("choices", [{}])[0].get("message", {}).get("content", ""))
                answers.append((q, content))
                print(f"✅ ({len(content)} chars)")
            else:
                answers.append((q, None))
                print("❌")

        # 然后用评分模型给每个回答打分
        print(f"  评分中...")
        provider_scores = {}
        for q, resp_content in answers:
            if resp_content is None:
                continue
            score = _score_response(api_url, api_key, scoring_model,
                                    q["question"], resp_content, scoring_prompt)
            if score is not None:
                cat = q["category"]
                if cat not in provider_scores:
                    provider_scores[cat] = []
                provider_scores[cat].append(score)
                print(f"    {q['id']}: {score}")

        if provider_scores:
            results[pname] = provider_scores
            # 打印汇总
            all_scores = [s for scores in provider_scores.values() for s in scores]
            overall = sum(all_scores) / len(all_scores)
            print(f"  📊 {pname} 总评分: {overall:.0f}/100 ({len(all_scores)} 样本)")
        else:
            print(f"  ⚠️  {pname}: 无有效评分")

    if not results:
        print("\n❌ 未生成任何有效评分")
        return

    # 生成 quality_benchmark.yaml
    quality_factors = {}
    benchmark_data = {
        "version": "3.9",
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "scoring_model": scoring_model,
        "question_count": len(BENCHMARK_QUESTIONS),
        "benchmarks": [],
    }

    for pname in sorted(results.keys()):
        provider_scores = results[pname]
        all_scores = [s for scores in provider_scores.values() for s in scores]
        overall = round(sum(all_scores) / len(all_scores), 1)
        quality_factors[pname] = max(0.5, min(1.0, overall / 100.0))

        entry = {
            "provider": pname,
            "overall": overall,
            "sample_count": len(all_scores),
            "scores": {cat: round(sum(s) / len(s), 1) for cat, s in provider_scores.items()},
        }
        benchmark_data["benchmarks"].append(entry)

    benchmark_data["quality_factors"] = quality_factors

    # 写入文件
    bench_path = os.path.join(base, BENCHMARK_FILENAME)
    with open(bench_path, "w") as f:
        yaml.dump(benchmark_data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    print(f"\n✅ 基准评分已写入: {bench_path}")
    print(f"   Quality Factors:")
    for pname, qf in sorted(quality_factors.items()):
        print(f"     {pname}: {qf:.3f}")
    print(f"\n   生产环境启动时自动加载，无需在线评分消耗 Token。")


def load_quality_benchmark(base: str) -> dict | None:
    """加载 quality_benchmark.yaml，返回 {provider: quality_factor}。

    生产环境启动时调用，填充 quality_factors 后在线监督者不再评分。
    """
    bench_path = os.path.join(base, BENCHMARK_FILENAME)
    if not os.path.exists(bench_path):
        return None

    try:
        with open(bench_path) as f:
            data = yaml.safe_load(f)
        if not data or "quality_factors" not in data:
            return None

        qf = data["quality_factors"]
        generated_at = data.get("generated_at", "unknown")
        print(f"📋 加载基准评分 (生成于 {generated_at})")
        for pname, factor in sorted(qf.items()):
            print(f"   {pname}: {factor:.3f}")
        print(f"   在线监督者评分已禁用（零 Token 开销）")
        return qf
    except Exception as e:
        print(f"⚠️  加载基准评分失败: {e}")
        return None