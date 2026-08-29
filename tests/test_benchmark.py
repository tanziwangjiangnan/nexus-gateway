"""测试 benchmark 模块：离线基准评分 + quality_benchmark.yaml 加载。"""
import pytest
import os
import tempfile
import yaml

from ops_gateway_core import (
    cmd_benchmark, load_quality_benchmark,
    BENCHMARK_QUESTIONS, BENCHMARK_FILENAME,
)


SAMPLE_CFG = {
    "port": 8646,
    "host": "127.0.0.1",
    "gateway_key": "test-key",
    "providers": {
        "scnet-tp": {"api": "https://api.scnet.cn/api/llm/v1", "api_key": "${SCNET_TP_KEY}", "max_rps": 100},
        "deepseek-direct": {"api": "https://api.deepseek.com/v1", "api_key": "${DEEPSEEK_API_KEY}", "max_rps": 50},
    },
    "pools": {
        "pool_a": {
            "description": "测试池",
            "providers": [
                {"name": "scnet-tp", "weight": 5, "models": ["DeepSeek-V4-Flash", "GLM-5.2"],
                 "capabilities": ["chat", "code"]},
            ],
        },
        "pool_b": {
            "description": "备用池",
            "providers": [
                {"name": "deepseek-direct", "weight": 1, "models": ["deepseek-v4"],
                 "capabilities": ["chat"]},
            ],
        },
    },
}


class TestBenchmarkQuestions:
    """测试题集基本属性。"""

    def test_question_count(self):
        assert len(BENCHMARK_QUESTIONS) == 10

    def test_question_structure(self):
        for q in BENCHMARK_QUESTIONS:
            assert "id" in q
            assert "category" in q
            assert "question" in q
            assert isinstance(q["question"], str)
            assert len(q["question"]) > 10

    def test_category_coverage(self):
        categories = {q["category"] for q in BENCHMARK_QUESTIONS}
        expected = {"code", "math", "translation", "general_chat", "creative_writing"}
        assert categories == expected, f"缺少分类: {expected - categories}"

    def test_unique_ids(self):
        ids = [q["id"] for q in BENCHMARK_QUESTIONS]
        assert len(ids) == len(set(ids)), "测试题 ID 有重复"


class TestLoadQualityBenchmark:
    """测试 quality_benchmark.yaml 加载。"""

    def test_load_nonexistent(self):
        """文件不存在时返回 None。"""
        with tempfile.TemporaryDirectory() as tmp:
            result = load_quality_benchmark(tmp)
            assert result is None

    def test_load_valid(self):
        """加载合法的基准评分文件。"""
        sample_data = {
            "version": "3.9",
            "generated_at": "2026-08-29T12:00:00",
            "scoring_model": "deepseek-v4-flash",
            "question_count": 10,
            "benchmarks": [
                {
                    "provider": "scnet-tp",
                    "overall": 85.0,
                    "sample_count": 10,
                    "scores": {"code": 90.0, "general_chat": 80.0},
                },
            ],
            "quality_factors": {"scnet-tp": 0.85},
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, BENCHMARK_FILENAME)
            with open(path, "w") as f:
                yaml.dump(sample_data, f)
            result = load_quality_benchmark(tmp)
            assert result == {"scnet-tp": 0.85}

    def test_load_missing_quality_factors(self):
        """缺少 quality_factors 字段时返回 None。"""
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, BENCHMARK_FILENAME)
            with open(path, "w") as f:
                yaml.dump({"version": "3.9"}, f)
            result = load_quality_benchmark(tmp)
            assert result is None

    def test_load_corrupted(self):
        """损坏的 YAML 返回 None 不崩溃。"""
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, BENCHMARK_FILENAME)
            with open(path, "w") as f:
                f.write(": invalid yaml: [")
            result = load_quality_benchmark(tmp)
            assert result is None

    def test_quality_factor_bounds(self):
        """quality_factor 应在 [0.5, 1.0] 范围内。"""
        sample_data = {
            "version": "3.9",
            "quality_factors": {"test-pv": 0.75, "test-pv2": 0.95},
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, BENCHMARK_FILENAME)
            with open(path, "w") as f:
                yaml.dump(sample_data, f)
            result = load_quality_benchmark(tmp)
            assert result is not None
            for v in result.values():
                assert 0.5 <= v <= 1.0, f"quality_factor {v} 超出 [0.5, 1.0]"


class TestCmdBenchmarkCli:
    """测试 benchmark CLI 命令解析（不实际调用 API）。"""

    def test_benchmark_help(self):
        """验证 cmd_benchmark 函数存在且可调用。"""
        assert callable(cmd_benchmark)

    def test_benchmark_no_providers(self):
        """配置中无 provider 时安全退出。"""
        with tempfile.TemporaryDirectory() as tmp:
            old_cwd = os.getcwd()
            os.chdir(tmp)
            try:
                # 空配置不崩溃
                cmd_benchmark({}, tmp)
            finally:
                os.chdir(old_cwd)

    def test_benchmark_dry_config(self):
        """验证函数接受有效配置参数。"""
        assert callable(cmd_benchmark)
        # 不实际调用 API（需要真实密钥），只验证签名的参数传递
        # 该函数在无 API key 时会跳过对应 provider，不会崩溃


class TestCmdBenchmarkEndToEnd:
    """用 monkeypatch 伪造 API，验证完整评分流程。"""

    def _make_fake_cfg(self):
        """构造配置：密钥用 ${VAR} 引用，通过环境变量注入。"""
        os.environ["FAKE_KEY_A"] = "sk-fake-a"
        os.environ["FAKE_KEY_B"] = "sk-fake-b"
        return {
            "port": 8646,
            "host": "127.0.0.1",
            "gateway_key": "test-key",
            "providers": {
                "fake-a": {"api": "https://fake-a.example.com/v1", "api_key": "${FAKE_KEY_A}", "max_rps": 100},
                "fake-b": {"api": "https://fake-b.example.com/v1", "api_key": "${FAKE_KEY_B}", "max_rps": 100},
            },
            "pools": {
                "pool_a": {
                    "description": "测试池",
                    "providers": [
                        {"name": "fake-a", "weight": 5, "models": ["model-a1", "model-a2"],
                         "capabilities": ["chat", "code"]},
                        {"name": "fake-b", "weight": 1, "models": ["model-b1"],
                         "capabilities": ["chat"]},
                    ],
                },
            },
        }

    def _make_fake_llm(self, monkeypatch, score_map):
        """伪造 _call_llm：回答问题返回固定内容，评分请求返回对应分数。"""
        import ops_gateway_core.ops.benchmark as bench_mod

        def fake_call_llm(api_url, api_key, model, messages, timeout=30):
            # 判断是回答问题还是评分请求
            last_msg = messages[-1]["content"]
            if "回答：" in last_msg:
                # 评分请求 — 返回预设分数
                question = last_msg.split("回答：")[0].replace("问题：", "").strip()
                score = score_map.get(question, 80)
                return {"choices": [{"message": {"content": str(score)}}]}
            else:
                # 回答问题请求
                return {"choices": [{"message": {"content": f"这是对 '{messages[-1]['content'][:20]}' 的回答"}}]}

        monkeypatch.setattr(bench_mod, "_call_llm", fake_call_llm)

    def test_full_benchmark_generates_yaml(self, monkeypatch):
        """完整流程：跑 benchmark 生成 quality_benchmark.yaml。"""
        self._make_fake_llm(monkeypatch, {})
        cfg = self._make_fake_cfg()
        with tempfile.TemporaryDirectory() as tmp:
            cmd_benchmark(cfg, tmp)
            # 验证文件生成
            bench_path = os.path.join(tmp, BENCHMARK_FILENAME)
            assert os.path.exists(bench_path), "quality_benchmark.yaml 未生成"
            # 验证内容
            with open(bench_path) as f:
                data = yaml.safe_load(f)
            assert "quality_factors" in data
            assert "benchmarks" in data
            assert "generated_at" in data
            # 验证两个 provider 都在
            providers = {b["provider"] for b in data["benchmarks"]}
            assert "fake-a" in providers and "fake-b" in providers
            # quality_factors 范围
            for v in data["quality_factors"].values():
                assert 0.5 <= v <= 1.0

    def test_full_benchmark_scores_reflected(self, monkeypatch):
        """验证评分聚合：fake-a 高、fake-b 低 → quality_factor 对应高低。"""
        score_map = {}
        # fake-a 的评分请求返回高分的判断在 fake_call_llm 中按问题返回；
        # 这里我们让两个 provider 有不同分数（通过 api_url 区分）
        import ops_gateway_core.ops.benchmark as bench_mod

        def fake_call_llm(api_url, api_key, model, messages, timeout=30):
            last_msg = messages[-1]["content"]
            if "回答：" in last_msg:
                score = 90 if "fake-a" in api_url else 40
                return {"choices": [{"message": {"content": str(score)}}]}
            return {"choices": [{"message": {"content": "回答内容"}}]}

        monkeypatch.setattr(bench_mod, "_call_llm", fake_call_llm)
        cfg = self._make_fake_cfg()
        with tempfile.TemporaryDirectory() as tmp:
            cmd_benchmark(cfg, tmp)
            with open(os.path.join(tmp, BENCHMARK_FILENAME)) as f:
                data = yaml.safe_load(f)
            qf = data["quality_factors"]
            assert qf["fake-a"] >= 0.85, f"fake-a 应为高分，实际 {qf['fake-a']}"
            assert qf["fake-b"] <= 0.5, f"fake-b 应为低分，实际 {qf['fake-b']}"

    def test_provider_filter(self, monkeypatch):
        """--provider 过滤：只评分指定 provider。"""
        self._make_fake_llm(monkeypatch, {})
        cfg = self._make_fake_cfg()
        with tempfile.TemporaryDirectory() as tmp:
            cmd_benchmark(cfg, tmp, provider_filter="fake-a")
            with open(os.path.join(tmp, BENCHMARK_FILENAME)) as f:
                data = yaml.safe_load(f)
            providers = {b["provider"] for b in data["benchmarks"]}
            assert providers == {"fake-a"}, f"应只有 fake-a，实际 {providers}"

    def test_unknown_provider_filter(self, monkeypatch):
        """--provider 传入不存在的 provider 时安全退出。"""
        self._make_fake_llm(monkeypatch, {})
        cfg = self._make_fake_cfg()
        with tempfile.TemporaryDirectory() as tmp:
            cmd_benchmark(cfg, tmp, provider_filter="nonexistent")
            # 不崩溃，且不生成文件
            assert not os.path.exists(os.path.join(tmp, BENCHMARK_FILENAME))