"""测试 hermes_ops 包：CLI 命令"""
import pytest, io, contextlib
from hermes_ops import cmd_models, cmd_usage, cmd_quality, cmd_feedback_stats, cmd_check_deps, cmd_scan_agents
from hermes_ops.check_deps import collect_all_keys, scan_local
from hermes_cfg import get_db

SAMPLE_CFG = {
    "port": 8646,
    "host": "127.0.0.1",
    "gateway_key": "test-key",
    "providers": {
        "test-pv": {"api": "https://test.api", "api_key": "sk-test", "max_rps": 10},
    },
    "pools": {
        "pool_a": {
            "description": "测试池",
            "providers": [
                {"name": "test-pv", "weight": 1, "models": ["test-model"],
                 "capabilities": ["chat"]},
            ],
        },
    },
    "agents": [],
    "plugins": {},
    "config": {"db_path": ":memory:", "db_type": "sqlite"},
}


class TestCmdModels:
    def test_cmd_models_output(self):
        # 注册模型到 registry 表，让 cmd_models 有数据可显示
        conn = get_db()
        conn.execute("INSERT OR IGNORE INTO registry (model, pool, provider, tier, status, notes) VALUES (?,?,?,?,?,?)",
                     ("test-model", "pool_a", "test-pv", "1", "unknown", ""))
        conn.commit()
        conn.close()
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            cmd_models(SAMPLE_CFG)
        output = buf.getvalue()
        assert "test-model" in output
        assert "test-pv" in output
        assert "pool_a" in output


class TestCmdUsage:
    def test_cmd_usage_empty(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            cmd_usage(SAMPLE_CFG)
        output = buf.getvalue()
        assert "今日无用量" in output


class TestCmdQuality:
    def test_cmd_quality_empty(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            cmd_quality(SAMPLE_CFG)
        output = buf.getvalue()
        assert "暂无" in output


class TestCmdFeedbackStats:
    def test_cmd_feedback_stats_empty(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            cmd_feedback_stats(SAMPLE_CFG)
        output = buf.getvalue()
        assert "暂无" in output


class TestCheckDeps:
    def test_collect_all_keys(self):
        keys = collect_all_keys(SAMPLE_CFG)
        assert "gateway_key" in keys
        assert keys["gateway_key"] == "test-key"

    def test_scan_local_empty_dir(self, tmp_path):
        cfg = {"gateway_key": "test-key",
               "providers": {"test-pv": {"api_key": "sk-test"}}}
        keys = collect_all_keys(cfg)
        index = []
        scan_local(index, keys, str(tmp_path))
        assert len(index) == 0  # 空目录无匹配

    def test_scan_local_finds_key(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("MY_KEY=sk-test\n")
        cfg = {"gateway_key": "test-key",
               "providers": {"test-pv": {"api_key": "sk-test"}}}
        keys = collect_all_keys(cfg)
        index = []
        scan_local(index, keys, str(tmp_path))
        assert len(index) == 1
        assert index[0]["current_value"] == "sk-test"

    def test_cmd_check_deps_no_crash(self, tmp_path):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            cmd_check_deps(SAMPLE_CFG, base="/tmp", target_key=None)
        output = buf.getvalue()
        assert output is not None


class TestCmdScanAgents:
    def test_cmd_scan_agents_no_crash(self, tmp_path):
        config_path = tmp_path / "gateway.yaml"
        config_path.write_text("")
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            cmd_scan_agents(SAMPLE_CFG, base="/tmp", config_path=str(config_path), scan_dir="/tmp")
        output = buf.getvalue()
        assert output is not None