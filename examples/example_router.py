#!/usr/bin/env python3
"""Provider Router 使用示例 — 独立于网关，展示路由引擎核心功能。"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from provider_router import Router, RouterState, load_config


def main():
    # 1. 加载配置
    cfg = load_config(os.path.join(os.path.dirname(__file__), "..", "gateway.yaml"))
    print(f"✅ 加载配置: {len(cfg.get('pools', {}))} 个池")

    # 2. 创建路由状态（由调用方维护）
    state = RouterState()

    # 3. 模型查找
    pool_name, pool_cfg, provider, canonical = Router.find_model(cfg, "deepseek-v4-flash")
    print(f"🔍 查找模型 'deepseek-v4-flash': pool={pool_name}, provider={provider['name'] if provider else 'N/A'}, canonical={canonical}")

    pool_name, *_ = Router.find_model(cfg, "nonexistent-model")
    print(f"🔍 查找模型 'nonexistent-model': {pool_name}")

    # 4. 关键词路由
    text = "我需要进行代码审查，请帮我 review 这段代码"
    pool = Router.select_pool_by_keywords(cfg, text)
    print(f"🔑 关键词路由: '{text[:20]}...' → pool={pool}")

    text = "今天天气怎么样"
    pool = Router.select_pool_by_keywords(cfg, text)
    print(f"🔑 关键词路由: '{text}' → pool={pool}")

    # 5. 权重轮询选 provider
    for pool_name in cfg.get("pools", {}):
        providers = cfg["pools"][pool_name].get("providers", [])
        print(f"\n⚖️  池 '{pool_name}' 的 providers:")
        for pv in providers:
            w = pv.get("weight", 1)
            dw = state.dynamic_weights.get(pv["name"], w)
            print(f"   {pv['name']}: 静态权重={w}, 动态权重={dw:.2f}")

        # 模拟多次选择观察分布
        counts = {}
        for _ in range(1000):
            p = Router.select_provider(providers, state)
            if p:
                counts[p["name"]] = counts.get(p["name"], 0) + 1
        print(f"   1000 次权重轮询分布: {counts}")

    # 6. 限流测试
    provider_name = list(cfg.get("providers", {}).keys())[0]
    max_rps = 5
    allowed = sum(1 for _ in range(10) if Router.check_rate_limit(provider_name, max_rps, state))
    print(f"\n⏱️  限流: {provider_name} max_rps={max_rps}, 10 次请求中通过 {allowed} 次")

    # 7. 模拟熔断效果
    print(f"\n🔌 模拟熔断: 禁用 {provider_name}")
    state.disabled_providers.add(provider_name)
    p = Router.select_provider(cfg["pools"]["pool_a"]["providers"], state)
    print(f"   禁用后选 provider: {p['name'] if p else 'None (无可用 provider)'}")
    state.disabled_providers.clear()

    # 8. 质量因子和用户因子
    state.quality_factors["test-provider"] = 0.8
    state.user_factors["test-provider"] = 1.2
    print(f"\n📊 质量因子影响: quality=0.8, user=1.2 → 综合倍数=0.96")

    print("\n✅ Router 示例完成")


if __name__ == "__main__":
    main()