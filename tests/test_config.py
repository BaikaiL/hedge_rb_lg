from pathlib import Path
import pytest

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
    assert cfg.monitor.flatten_buffer_usd == 75
    assert cfg.monitor.critical_buffer_usd == 100
    assert cfg.monitor.warning_buffer_usd == 150
    assert cfg.monitor.recovery_buffer_usd == 175
    assert cfg.margin_execution.max_slippage_pct == 2
    assert cfg.margin_execution.close_retry_rounds == 3
    assert cfg.notifications.live_status_interval_seconds == 10


@pytest.mark.parametrize("before,after", [
    ("flatten_buffer_usd = 75.0", "flatten_buffer_usd = 125.0"),
    ("isolated_flatten_distance_pct = 3.0", "isolated_flatten_distance_pct = 5.0"),
    ("poll_seconds = 10", "poll_seconds = 40"),
    ("close_retry_rounds = 3", "close_retry_rounds = 3.5"),
    ("cancel_open_orders = true", 'cancel_open_orders = "true"'),
])
def test_invalid_margin_config_is_rejected_before_start(tmp_path, monkeypatch, before, after):
    for k, v in {"RH_ACCOUNT_INDEX": "11", "CORE_ACCOUNT_INDEX": "22",
                 "RH_HEDGE_API_KEY_INDEX": "5", "CORE_HEDGE_API_KEY_INDEX": "4"}.items():
        monkeypatch.setenv(k, v)
    example = (Path(__file__).parents[1] / "config.example.toml").read_text(encoding="utf-8")
    path = tmp_path / "config.toml"
    path.write_text(example.replace(before, after), encoding="utf-8")
    with pytest.raises(ValueError):
        load_config(path)
