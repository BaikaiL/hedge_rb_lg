from pathlib import Path

from auto_hedge.config import load_config


def test_example_configuration_automatically_follows_all_markets(monkeypatch):
    for k, v in {"RH_ACCOUNT_INDEX": "11", "CORE_ACCOUNT_INDEX": "22",
                 "RH_HEDGE_API_KEY_INDEX": "5", "CORE_HEDGE_API_KEY_INDEX": "4"}.items():
        monkeypatch.setenv(k, v)
    cfg = load_config(Path(__file__).parents[1] / "config.example.toml")
    assert cfg.auto_markets
    assert not cfg.routes
    assert not cfg.enabled
    assert cfg.source.chain_id == 466324
    assert cfg.target.chain_id == 304
