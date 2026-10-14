from unittest.mock import patch

import pytest

from fundbot.mvp import Settings


def test_missing_dry_run_setting_defaults_to_simulation() -> None:
    with patch.dict("os.environ", {"VOOI_BEARER_TOKEN": "test-placeholder"}, clear=True):
        assert Settings.from_env().dry_run is True


@pytest.mark.parametrize("value", ["false", "0", "no"])
def test_live_mode_requires_an_explicit_setting(value: str) -> None:
    with patch.dict(
        "os.environ", {"VOOI_BEARER_TOKEN": "test-placeholder", "BOT_DRY_RUN": value}, clear=True,
    ):
        assert Settings.from_env().dry_run is False


@pytest.mark.parametrize("value", ["", "tru", "disabled"])
def test_invalid_dry_run_setting_cannot_enable_live_mode(value: str) -> None:
    with patch.dict(
        "os.environ", {"VOOI_BEARER_TOKEN": "test-placeholder", "BOT_DRY_RUN": value}, clear=True,
    ), pytest.raises(RuntimeError, match="BOT_DRY_RUN"):
        Settings.from_env()
