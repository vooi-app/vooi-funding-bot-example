from dataclasses import replace
from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest

from fundbot.mvp import Settings, VooiClient, _close_leg_alo_then_market


@pytest.mark.asyncio
async def test_timed_out_passive_close_uses_the_client_delete_method() -> None:
    with patch.dict("os.environ", {"VOOI_BEARER_TOKEN": "test-placeholder"}, clear=True):
        settings = Settings.from_env()
    settings = replace(settings, limit_alo_close_timeout_sec=0)
    client = VooiClient("https://example.invalid", "test-placeholder")
    market_result = httpx.Response(200, json={"orderId": "market-close"})
    client.post = AsyncMock(side_effect=[httpx.Response(200, json={"orderId": "passive-close"}), market_result])
    client.delete = AsyncMock(return_value=httpx.Response(200, json={}))
    body = {"exchange": "hyperliquid", "asset": "BTC", "side": "sell", "size": "1", "reduceOnly": True}
    with patch("fundbot.mvp.quote_leg", AsyncMock(return_value={"avgPrice": "100"})), patch(
        "fundbot.mvp.asyncio.sleep", AsyncMock(),
    ):
        result = await _close_leg_alo_then_market(client, Mock(), settings, body, "arb", "long", 1)
    client.delete.assert_awaited_once_with(
        "/exchange/orders", body={"exchange": "hyperliquid", "asset": "BTC", "orderId": "passive-close"},
    )
    assert result is market_result
