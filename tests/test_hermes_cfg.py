"""测试 hermes_cfg 包：ConfigLoader + get_db"""
import pytest, os, tempfile, yaml
from ops_gateway_core import ConfigLoader, get_db, init_registry

MINIMAL_CONFIG = {
    "port": 8646,
    "host": "127.0.0.1",
    "gateway_key": "test-key",
    "providers": {
        "test-provider": {
            "api": "https://test.api",
            "api_key": "sk-test",
            "max_rps": 10,
        },
    },
    "pools": {
        "pool_a": {
            "description": "测试池",
            "providers": [
                {"name": "test-provider", "weight": 1, "models": ["test-model"]},
            ],
        },
    },
}


@pytest.fixture
def config_file():
    """创建临时 YAML 配置文件"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(MINIMAL_CONFIG, f)
        path = f.name
    yield path
    os.unlink(path)


class TestConfigLoader:
    def test_load(self, config_file):
        loader = ConfigLoader(path=config_file)
        cfg = loader.load()
        assert cfg["port"] == 8646
        assert "test-provider" in cfg["providers"]
        assert len(cfg["pools"]) == 1

    def test_config_property(self, config_file):
        loader = ConfigLoader(path=config_file)
        loader.load()
        cfg = loader.config
        assert cfg["gateway_key"] == "test-key"

    def test_reload(self, config_file):
        loader = ConfigLoader(path=config_file)
        loader.load()
        # 修改文件
        with open(config_file, "r") as f:
            data = yaml.safe_load(f)
        data["port"] = 9999
        with open(config_file, "w") as f:
            yaml.dump(data, f)
        loader.reload()
        assert loader.config["port"] == 9999

    def test_reload_triggers_callback(self, config_file):
        triggered = []

        def on_reload():
            triggered.append(1)

        loader = ConfigLoader(path=config_file, on_reload=on_reload)
        loader.load()
        loader.reload()
        assert len(triggered) == 1

    def test_load_non_existent_file(self):
        loader = ConfigLoader(path="/no/such/file.yaml")
        with pytest.raises(FileNotFoundError):
            loader.load()

    def test_load_invalid_yaml(self, config_file):
        with open(config_file, "w") as f:
            f.write("{invalid: yaml: broken\n")
        loader = ConfigLoader(path=config_file)
        with pytest.raises(Exception):
            loader.load()


class TestGetDb:
    def test_get_db_returns_connection(self):
        conn = get_db()
        assert conn is not None
        conn.close()

    def test_init_registry_creates_table(self):
        conn = get_db()
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        names = [r[0] for r in tables]
        assert "registry" in names
        conn.close()