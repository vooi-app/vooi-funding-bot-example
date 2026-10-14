"""Tests for Patches A-G implemented in fundbot/mvp.py (v2 review).

Coverage:
  Patch A — calculate_sl_trigger
  Patch C — round_size, round_price, MarketMeta
  Patch D — effective_leverage
  Patch F — smart-exit: safe_floor, decline-window, negative-window, funding breakeven
  Patch G — adverse basis check (via filter_opportunities integration)
  Core   — save_snapshot / load_snapshot round-trip (incl. Patch F fields)
  Core   — filter_opportunities (APR caps, blacklist, ratio filters)
  Core   — Settings defaults
"""

from __future__ import annotations

import json
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from fundbot.mvp import (
    DEFAULT_DECLINE_WINDOW,
    DEFAULT_ESTIMATED_FRICTION_BPS,
    DEFAULT_EXCHANGE_SL_BUFFER_PCT,
    DEFAULT_EXCHANGE_SL_ENABLED,
    DEFAULT_FUNDING_BREAKEVEN_SKIP_SAFE_FLOOR,
    DEFAULT_LEVERAGE_TARGET,
    DEFAULT_LIMIT_ALO_DRIFT_BPS,
    DEFAULT_LIMIT_ALO_ENABLED,
    DEFAULT_LIMIT_ALO_LEG_SIDE,
    DEFAULT_LIMIT_ALO_OFFSET_BPS,
    DEFAULT_LIMIT_ALO_POLL_SEC,
    DEFAULT_LIMIT_ALO_TIMEOUT_SEC,
    DEFAULT_MAX_ADVERSE_BASIS_BPS,
    DEFAULT_MAX_HOLD_HOURS,
    DEFAULT_MAX_MARGIN_PER_EX_USD,
    DEFAULT_MAX_SLIPPAGE_BPS,
    DEFAULT_MIN_HOLD_HOURS,
    DEFAULT_MIN_NET_APR,
    DEFAULT_MIN_VOLUME_24H_USD,
    DEFAULT_NEGATIVE_WINDOW,
    DEFAULT_PAIR_COOLDOWN_AFTER_LOSSES,
    DEFAULT_PAIR_COOLDOWN_HOURS,
    DEFAULT_SAFE_FLOOR_MULT,
    DEFAULT_SMART_NEG_VALUE_FLOOR,
    DEFAULT_STOP_LOSS_PCT,
    MarketMeta,
    OpenPosition,
    Opportunity,
    Settings,
    calculate_sl_trigger,
    cooldown_blocked_assets,
    filter_opportunities,
    load_cooldown_state,
    load_snapshot,
    prune_cooldown,
    round_price,
    round_size,
    save_cooldown_state,
    save_snapshot,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_settings(**overrides) -> Settings:
    """Build a minimal Settings object (dry_run=True, no real token needed)."""
    base = dict(
        base_url="https://test.example",
        bearer_token="test-token",
        target_exchanges=("hyperliquid", "lighter"),
        leg_collat_usd=Decimal("10"),
        max_margin_per_ex_usd=DEFAULT_MAX_MARGIN_PER_EX_USD,
        min_net_apr=DEFAULT_MIN_NET_APR,
        max_net_apr=Decimal("0"),
        leverage_target=DEFAULT_LEVERAGE_TARGET,
        leverage_cap=0,
        max_notional_per_position_usd=Decimal("0"),
        max_hold_hours=DEFAULT_MAX_HOLD_HOURS,
        min_volume_24h_usd=DEFAULT_MIN_VOLUME_24H_USD,
        per_leg_volume_min_usd=Decimal("0"),
        apr_upper_cap=Decimal("0"),
        apr_ratio_1h_to_24h_max=Decimal("0"),
        apr_ratio_24h_to_7d_max=Decimal("0"),
        asset_whitelist=(),
        asset_blacklist=(),
        stop_loss_pct=DEFAULT_STOP_LOSS_PCT,
        negative_apr_trading_streak_threshold=4,
        max_slippage_bps=DEFAULT_MAX_SLIPPAGE_BPS,
        loop_interval_sec=300,
        trading_cycle_sec=3600,
        dry_run=True,
        state_file=Path("/tmp/test-state.ndjson"),
        snapshot_file=Path("/tmp/test-snapshot.json"),
        instance_uuid="test-uuid-01",
        include_hl_non_crypto=False,
        safe_floor_mult=DEFAULT_SAFE_FLOOR_MULT,
        decline_window=DEFAULT_DECLINE_WINDOW,
        negative_window=DEFAULT_NEGATIVE_WINDOW,
        funding_breakeven_skip_safe_floor=DEFAULT_FUNDING_BREAKEVEN_SKIP_SAFE_FLOOR,
        estimated_friction_bps=DEFAULT_ESTIMATED_FRICTION_BPS,
        exchange_sl_buffer_pct=DEFAULT_EXCHANGE_SL_BUFFER_PCT,
        exchange_sl_enabled=DEFAULT_EXCHANGE_SL_ENABLED,
        limit_alo_enabled=DEFAULT_LIMIT_ALO_ENABLED,
        limit_alo_offset_bps=DEFAULT_LIMIT_ALO_OFFSET_BPS,
        limit_alo_timeout_sec=DEFAULT_LIMIT_ALO_TIMEOUT_SEC,
        limit_alo_drift_bps=DEFAULT_LIMIT_ALO_DRIFT_BPS,
        limit_alo_poll_sec=DEFAULT_LIMIT_ALO_POLL_SEC,
        limit_alo_leg_side=DEFAULT_LIMIT_ALO_LEG_SIDE,
        limit_alo_close_enabled=False,
        limit_alo_close_timeout_sec=60,
        limit_alo_close_offset_bps=5,
        max_adverse_basis_bps=DEFAULT_MAX_ADVERSE_BASIS_BPS,
        low_apr_threshold=Decimal("0.20"),
        low_apr_window=3,
        post_close_settle_sec=20,
        trading_minute=5,
        margin_headroom_pct=Decimal("0.005"),
        # Tuning 2026-05-13: smart-exit hardening
        smart_neg_value_floor=Decimal("0"),
        min_hold_hours=0,
        pair_cooldown_after_losses=0,
        pair_cooldown_hours=48,
        cooldown_file=Path("/tmp/test-cooldown.json"),
        # Patch A3: Survivor watcher (disabled in tests)
        survivor_watch_enabled=False,
        survivor_watch_sec=1.0,
        survivor_watch_idle_sec=30.0,
        # SSE event stream (disabled in tests)
        sse_enabled=False,
        sse_heartbeat_timeout_sec=60.0,
        sse_reconnect_max_attempts=10,
    )
    base.update(overrides)
    return Settings(**base)


def _make_opportunity(**overrides) -> Opportunity:
    defaults = dict(
        asset="BTC",
        long_exchange="lighter",
        short_exchange="hyperliquid",
        long_base_symbol="BTC",
        short_base_symbol="BTC",
        long_quote_symbol="USDC",
        short_quote_symbol="USDC",
        net_apr=Decimal("0.50"),
        apr1h=Decimal("0.50"),
        apr24h=Decimal("0.40"),
        apr7d=Decimal("0.35"),
        gross_spread_hourly=Decimal("0.0001"),
        long_funding_rate=Decimal("0.0002"),
        short_funding_rate=Decimal("-0.0002"),
        volume_24h_usd=Decimal("500000"),
        long_max_leverage=10,
        short_max_leverage=10,
        raw={},
    )
    defaults.update(overrides)
    return Opportunity(**defaults)


def _make_open_position(
    arb_id: str = "test-01",
    asset: str = "BTC",
    open_net_apr: Decimal = Decimal("0.50"),
    leg_collat_usd: Decimal = Decimal("10"),
    leg_notional_usd: Decimal = Decimal("100"),
    opened_at: datetime | None = None,
    **overrides,
) -> OpenPosition:
    return OpenPosition(
        arb_id=arb_id,
        asset=asset,
        long_exchange="lighter",
        short_exchange="hyperliquid",
        long_base_symbol=asset,
        short_base_symbol=asset,
        opened_at=opened_at or datetime.now(UTC),
        open_net_apr=open_net_apr,
        leg_collat_usd=leg_collat_usd,
        leg_notional_usd=leg_notional_usd,
        effective_leverage=10,
        long_client_order_id="coid-long",
        short_client_order_id="coid-short",
        **overrides,
    )


# =============================================================================
# Patch A — calculate_sl_trigger
# =============================================================================


class TestCalculateSlTrigger:
    def test_long_sl_above_liq_below_entry(self) -> None:
        """LONG: SL = liq + (entry - liq) * buffer. Should be between liq and entry."""
        sl = calculate_sl_trigger(
            entry_price=Decimal("100"),
            liquidation_price=Decimal("80"),
            side="buy",
            buffer_pct=Decimal("0.05"),
            price_decimals=2,
        )
        # liq=80, entry=100, buffer=0.05 → SL = 80 + 20*0.05 = 81
        assert sl == Decimal("81.00")
        assert sl > Decimal("80")  # above liq
        assert sl < Decimal("100")  # below entry

    def test_short_sl_below_liq_above_entry(self) -> None:
        """SHORT: SL = liq - (liq - entry) * buffer. Should be between entry and liq."""
        sl = calculate_sl_trigger(
            entry_price=Decimal("100"),
            liquidation_price=Decimal("120"),
            side="sell",
            buffer_pct=Decimal("0.05"),
            price_decimals=2,
        )
        # liq=120, entry=100, buffer=0.05 → SL = 120 - 20*0.05 = 119
        assert sl == Decimal("119.00")
        assert sl < Decimal("120")  # below liq
        assert sl > Decimal("100")  # above entry

    def test_long_fallback_when_liq_ge_entry(self) -> None:
        """Bizarre case: liq >= entry for LONG → fallback 5% below entry."""
        sl = calculate_sl_trigger(
            entry_price=Decimal("100"),
            liquidation_price=Decimal("100"),  # liq == entry
            side="buy",
            buffer_pct=Decimal("0.05"),
            price_decimals=2,
        )
        assert sl == Decimal("95.00")  # 100 * 0.95

    def test_short_fallback_when_liq_le_entry(self) -> None:
        """Bizarre case: liq <= entry for SHORT → fallback 5% above entry."""
        sl = calculate_sl_trigger(
            entry_price=Decimal("100"),
            liquidation_price=Decimal("100"),  # liq == entry
            side="sell",
            buffer_pct=Decimal("0.05"),
            price_decimals=2,
        )
        assert sl == Decimal("105.00")  # 100 * 1.05

    def test_price_decimals_respected(self) -> None:
        """SL trigger rounded to price_decimals."""
        sl = calculate_sl_trigger(
            entry_price=Decimal("100"),
            liquidation_price=Decimal("80"),
            side="buy",
            buffer_pct=Decimal("0.05"),
            price_decimals=0,  # integer prices
        )
        # 80 + 20*0.05 = 81 → already integer, no rounding needed
        assert sl == Decimal("81")

    def test_sl_never_equals_liq(self) -> None:
        """Buffer > 0 ensures SL is never exactly at liquidation."""
        sl_long = calculate_sl_trigger(
            Decimal("1000"), Decimal("800"), "buy", Decimal("0.05"), 4
        )
        sl_short = calculate_sl_trigger(
            Decimal("1000"), Decimal("1200"), "sell", Decimal("0.05"), 4
        )
        assert sl_long != Decimal("800")
        assert sl_short != Decimal("1200")

    def test_buffer_zero_puts_sl_at_liq(self) -> None:
        """Buffer=0: SL placed exactly at liquidation price."""
        sl = calculate_sl_trigger(
            Decimal("100"), Decimal("80"), "buy", Decimal("0"), 4
        )
        assert sl == Decimal("80.0000")

    def test_high_leverage_tight_spread(self) -> None:
        """High leverage (10x): entry=1000, liq=900. SL = 900 + 100*0.05 = 905."""
        sl = calculate_sl_trigger(
            Decimal("1000"), Decimal("900"), "buy", Decimal("0.05"), 2
        )
        assert sl == Decimal("905.00")

    def test_long_round_down(self) -> None:
        """Long SL rounds DOWN (conservative — stays above liq)."""
        # entry=100.1, liq=80.1, buffer=0.05 → SL = 80.1 + 20*0.05 = 81.1
        sl = calculate_sl_trigger(
            Decimal("100.1"), Decimal("80.1"), "buy", Decimal("0.05"), 1
        )
        # 80.1 + 1.0 = 81.1, rounded down to 1 decimal = 81.1 (exact)
        assert sl == Decimal("81.1")

    def test_short_round_up(self) -> None:
        """Short SL rounds UP (conservative — stays below liq)."""
        sl = calculate_sl_trigger(
            Decimal("100"), Decimal("120"), "sell", Decimal("0.333"), 2
        )
        # 120 - 20*0.333 = 120 - 6.66 = 113.34, sell side rounds UP
        assert sl == Decimal("113.34")


# =============================================================================
# Patch A3 — Cross-leg symmetric TP (TP одной ноги = SL противоположной)
# =============================================================================


class TestCrossLegTp:
    """Сценарий: silver-arb LONG hyp:km:SILVER (entry $83.67, liq $59.98) /
    SHORT lig:XAG (entry $83.71, liq $163.23). Cross-leg TP должен дать:
    long_tp ≈ short_sl ≈ $151, short_tp ≈ long_sl ≈ $60.
    """

    def test_cross_leg_matches_opposite_sl(self) -> None:
        long_entry = Decimal("83.67")
        long_liq = Decimal("59.98")
        short_entry = Decimal("83.71")
        short_liq = Decimal("163.2348")
        buffer = Decimal("0.05")
        price_decimals = 4

        long_sl = calculate_sl_trigger(long_entry, long_liq, "buy", buffer, price_decimals)
        short_sl = calculate_sl_trigger(short_entry, short_liq, "sell", buffer, price_decimals)

        # Cross-leg TP проекция через _project_tp_for_exchange
        from fundbot.mvp import _project_tp_for_exchange
        long_tp = _project_tp_for_exchange(short_sl, price_decimals, "sell")
        short_tp = _project_tp_for_exchange(long_sl, price_decimals, "buy")

        # long_tp ≈ short_sl (одна и та же USD-цена в верхнем уровне).
        # short_tp ≈ long_sl (одна и та же USD-цена в нижнем уровне).
        tol = Decimal("0.0001")
        assert abs(long_tp - short_sl) <= tol, (
            f"long_tp={long_tp} should ≈ short_sl={short_sl}"
        )
        assert abs(short_tp - long_sl) <= tol, (
            f"short_tp={short_tp} should ≈ long_sl={long_sl}"
        )

    def test_long_tp_above_long_entry(self) -> None:
        """Sanity: long_tp = short_sl должен быть выше long entry."""
        long_entry = Decimal("83.67")
        short_sl = Decimal("159.26")
        from fundbot.mvp import _project_tp_for_exchange
        long_tp = _project_tp_for_exchange(short_sl, 4, "sell")
        assert long_tp > long_entry

    def test_short_tp_below_short_entry(self) -> None:
        """Sanity: short_tp = long_sl должен быть ниже short entry."""
        short_entry = Decimal("83.71")
        long_sl = Decimal("61.16")
        from fundbot.mvp import _project_tp_for_exchange
        short_tp = _project_tp_for_exchange(long_sl, 4, "buy")
        assert short_tp < short_entry

    def test_round_direction_for_long_tp(self) -> None:
        """long_tp округляется в 'sell' direction (вверх), не уходит ниже cross-leg SL."""
        from fundbot.mvp import _project_tp_for_exchange
        # Edge: 81.234 c price_decimals=2 → "sell" round = 81.24 (вверх)
        tp = _project_tp_for_exchange(Decimal("81.234"), 2, "sell")
        assert tp == Decimal("81.24")

    def test_round_direction_for_short_tp(self) -> None:
        """short_tp округляется в 'buy' direction (вниз), не уходит выше cross-leg SL."""
        from fundbot.mvp import _project_tp_for_exchange
        # Edge: 81.234 c price_decimals=2 → "buy" round = 81.23 (вниз)
        tp = _project_tp_for_exchange(Decimal("81.234"), 2, "buy")
        assert tp == Decimal("81.23")

    def test_hl_max_sig_figs_applied(self) -> None:
        """HL ограничение 5 sig figs применяется и к проекции TP."""
        from fundbot.mvp import _project_tp_for_exchange
        tp = _project_tp_for_exchange(
            Decimal("12345.6789"), 4, "sell", max_sig_figs=5,
        )
        s = format(tp.normalize(), "f").replace("-", "").replace(".", "").lstrip("0")
        assert len(s.rstrip("0")) <= 5, f"tp={tp} has too many sig figs"

    def test_silver_xag_real_scenario(self) -> None:
        """Real silver/XAG: cross-leg даёт цены вокруг entry ~$83 (NOT $6 и $106)."""
        long_entry = Decimal("83.67")
        long_liq = Decimal("59.98")
        short_entry = Decimal("83.71")
        short_liq = Decimal("163.2348")
        buffer = Decimal("0.05")

        long_sl = calculate_sl_trigger(long_entry, long_liq, "buy", buffer, 4)
        short_sl = calculate_sl_trigger(short_entry, short_liq, "sell", buffer, 4)
        from fundbot.mvp import _project_tp_for_exchange
        long_tp = _project_tp_for_exchange(short_sl, 4, "sell")
        short_tp = _project_tp_for_exchange(long_sl, 4, "buy")

        # Бывшая ошибочная формула давала long_tp=$106, short_tp=$6 (на entry-frame).
        # Cross-leg даёт long_tp ≈ $159 (= short_sl), short_tp ≈ $61 (= long_sl).
        # Самое важное: оба уровня "вокруг $83" в смысле общего USD-уровня для обеих ног.
        assert long_tp > Decimal("150"), f"long_tp={long_tp} should be ≈ short_sl (≈159)"
        assert short_tp < Decimal("65"), f"short_tp={short_tp} should be ≈ long_sl (≈61)"
        # SL и TP пары совпадают по price level:
        assert abs(long_tp - short_sl) < Decimal("0.001")
        assert abs(short_tp - long_sl) < Decimal("0.001")


# =============================================================================
# Patch C — round_size, round_price
# =============================================================================


class TestRoundSize:
    def test_basic_round_down(self) -> None:
        assert round_size(Decimal("0.12345"), 4) == Decimal("0.1234")

    def test_zero_decimals_floor(self) -> None:
        assert round_size(Decimal("1.9"), 0) == Decimal("1")

    def test_exact_value_unchanged(self) -> None:
        assert round_size(Decimal("0.1234"), 4) == Decimal("0.1234")

    def test_small_value_rounds_to_zero(self) -> None:
        # $5 / $76421 ≈ 0.0000654 → 0 at base_decimals=4
        raw = Decimal("5") / Decimal("76421")
        result = round_size(raw, 4)
        assert result == Decimal("0")

    def test_sufficient_value_non_zero(self) -> None:
        # $100 / $76421 ≈ 0.001308 → 0.001 at base_decimals=3
        raw = Decimal("100") / Decimal("76421")
        result = round_size(raw, 3)
        assert result == Decimal("0.001")
        assert result > 0

    def test_does_not_exceed_raw(self) -> None:
        for decimals in range(6):
            raw = Decimal("1.999999")
            result = round_size(raw, decimals)
            assert result <= raw, f"round_size exceeded raw at decimals={decimals}"

    def test_eight_decimals(self) -> None:
        """Bitcoin precision: 8 decimals."""
        raw = Decimal("0.00000001")
        assert round_size(raw, 8) == Decimal("0.00000001")
        assert round_size(Decimal("0.000000019"), 8) == Decimal("0.00000001")


class TestRoundPrice:
    def test_buy_rounds_down(self) -> None:
        assert round_price(Decimal("100.5559"), 3, "buy") == Decimal("100.555")

    def test_sell_rounds_up(self) -> None:
        assert round_price(Decimal("100.5551"), 3, "sell") == Decimal("100.556")

    def test_exact_value_unchanged_buy(self) -> None:
        assert round_price(Decimal("100.500"), 3, "buy") == Decimal("100.500")

    def test_exact_value_unchanged_sell(self) -> None:
        assert round_price(Decimal("100.500"), 3, "sell") == Decimal("100.500")

    def test_zero_decimals_buy(self) -> None:
        assert round_price(Decimal("100.9"), 0, "buy") == Decimal("100")

    def test_zero_decimals_sell(self) -> None:
        assert round_price(Decimal("100.1"), 0, "sell") == Decimal("101")

    def test_buy_price_never_exceeds_input(self) -> None:
        p = round_price(Decimal("1234.5678"), 4, "buy")
        assert p <= Decimal("1234.5678")

    def test_sell_price_never_below_input(self) -> None:
        p = round_price(Decimal("1234.5678"), 4, "sell")
        assert p >= Decimal("1234.5678")


# =============================================================================
# Patch D — effective_leverage (Opportunity method)
# =============================================================================


class TestEffectiveLeverage:
    def test_target_le_market_max(self) -> None:
        opp = _make_opportunity(long_max_leverage=20, short_max_leverage=20)
        assert opp.effective_leverage(10) == 10

    def test_market_max_lower_than_target(self) -> None:
        opp = _make_opportunity(long_max_leverage=5, short_max_leverage=3)
        assert opp.effective_leverage(10) == 3  # min(10, 5, 3)

    def test_cap_applied(self) -> None:
        opp = _make_opportunity(long_max_leverage=20, short_max_leverage=20)
        assert opp.effective_leverage(10, cap=7) == 7

    def test_minimum_is_one(self) -> None:
        opp = _make_opportunity(long_max_leverage=0, short_max_leverage=0)
        assert opp.effective_leverage(10) == 1

    def test_cap_zero_means_no_cap(self) -> None:
        opp = _make_opportunity(long_max_leverage=5, short_max_leverage=5)
        assert opp.effective_leverage(10, cap=0) == 5


# =============================================================================
# filter_opportunities — APR + volume + blacklist + ratio filters
# =============================================================================


class TestFilterOpportunities:
    def _settings(self, **kw) -> Settings:
        return _make_settings(**kw)

    def test_basic_pass(self) -> None:
        opp = _make_opportunity()
        result = filter_opportunities([opp], self._settings(), set())
        assert len(result) == 1

    def test_min_volume_filter(self) -> None:
        opp = _make_opportunity(volume_24h_usd=Decimal("50000"))
        result = filter_opportunities([opp], self._settings(min_volume_24h_usd=Decimal("100000")), set())
        assert len(result) == 0

    def test_min_apr_filter(self) -> None:
        opp = _make_opportunity(net_apr=Decimal("0.05"))
        result = filter_opportunities([opp], self._settings(min_net_apr=Decimal("0.10")), set())
        assert len(result) == 0

    def test_max_apr_filter(self) -> None:
        opp = _make_opportunity(net_apr=Decimal("0.50"))
        result = filter_opportunities([opp], self._settings(max_net_apr=Decimal("0.30")), set())
        assert len(result) == 0

    def test_apr_upper_cap_removes_spike(self) -> None:
        opp = _make_opportunity(net_apr=Decimal("3.00"))
        result = filter_opportunities([opp], self._settings(apr_upper_cap=Decimal("2.00")), set())
        assert len(result) == 0

    def test_apr_upper_cap_zero_means_no_cap(self) -> None:
        opp = _make_opportunity(net_apr=Decimal("50.00"))
        result = filter_opportunities([opp], self._settings(apr_upper_cap=Decimal("0")), set())
        assert len(result) == 1

    def test_apr_ratio_1h_24h_filter(self) -> None:
        # apr1h/apr24h = 6 > max 5 → should be filtered
        opp = _make_opportunity(apr1h=Decimal("0.60"), apr24h=Decimal("0.10"))
        result = filter_opportunities([opp], self._settings(apr_ratio_1h_to_24h_max=Decimal("5")), set())
        assert len(result) == 0

    def test_apr_ratio_1h_24h_passes_below_max(self) -> None:
        opp = _make_opportunity(apr1h=Decimal("0.40"), apr24h=Decimal("0.10"))
        result = filter_opportunities([opp], self._settings(apr_ratio_1h_to_24h_max=Decimal("5")), set())
        assert len(result) == 1

    def test_apr_ratio_24h_7d_filter(self) -> None:
        # apr24h/apr7d = 4 > max 3 → filtered
        opp = _make_opportunity(apr24h=Decimal("0.40"), apr7d=Decimal("0.10"))
        result = filter_opportunities([opp], self._settings(apr_ratio_24h_to_7d_max=Decimal("3")), set())
        assert len(result) == 0

    def test_blacklist_filters_asset(self) -> None:
        opp = _make_opportunity(asset="YZY")
        result = filter_opportunities([opp], self._settings(asset_blacklist=("YZY",)), set())
        assert len(result) == 0

    def test_blacklist_case_insensitive(self) -> None:
        opp = _make_opportunity(asset="yzy")
        result = filter_opportunities([opp], self._settings(asset_blacklist=("YZY",)), set())
        assert len(result) == 0

    def test_whitelist_allows_only_listed(self) -> None:
        btc_opp = _make_opportunity(asset="BTC")
        eth_opp = _make_opportunity(asset="ETH")
        result = filter_opportunities([btc_opp, eth_opp], self._settings(asset_whitelist=("BTC",)), set())
        assets = [o.asset for o in result]
        assert "BTC" in assets
        assert "ETH" not in assets

    def test_blocked_assets_skip(self) -> None:
        opp = _make_opportunity(asset="SOL")
        result = filter_opportunities([opp], self._settings(), {"SOL"})
        assert len(result) == 0

    def test_negative_apr1h_filtered(self) -> None:
        opp = _make_opportunity(apr1h=Decimal("-0.10"))
        result = filter_opportunities([opp], self._settings(), set())
        assert len(result) == 0

    def test_zero_gross_spread_filtered(self) -> None:
        opp = _make_opportunity(gross_spread_hourly=Decimal("0"))
        result = filter_opportunities([opp], self._settings(), set())
        assert len(result) == 0

    def test_results_sorted_by_net_apr_desc(self) -> None:
        low = _make_opportunity(asset="A", net_apr=Decimal("0.20"))
        high = _make_opportunity(asset="B", net_apr=Decimal("0.80"))
        mid = _make_opportunity(asset="C", net_apr=Decimal("0.50"))
        result = filter_opportunities([low, high, mid], self._settings(), set())
        assert [o.asset for o in result] == ["B", "C", "A"]

    def test_per_leg_volume_filter(self) -> None:
        opp = _make_opportunity(volume_24h_usd=Decimal("200000"))
        result = filter_opportunities(
            [opp], self._settings(per_leg_volume_min_usd=Decimal("250000")), set()
        )
        assert len(result) == 0

    def test_max_leverage_below_2_filtered(self) -> None:
        opp = _make_opportunity(long_max_leverage=1, short_max_leverage=10)
        result = filter_opportunities([opp], self._settings(), set())
        assert len(result) == 0


# =============================================================================
# Patch F — Smart-exit logic helpers (unit-level, no HTTP)
# =============================================================================


class TestSmartExitSafeFloor:
    """Verify the safe_floor calculation matches TZ: max(open_apr * mult, min_net_apr)."""

    def test_safe_floor_from_open_apr(self) -> None:
        settings = _make_settings(safe_floor_mult=Decimal("0.70"), min_net_apr=Decimal("0.10"))
        pos = _make_open_position(open_net_apr=Decimal("0.50"))
        safe_floor = max(pos.open_net_apr * settings.safe_floor_mult, settings.min_net_apr)
        assert safe_floor == Decimal("0.35")  # 0.50 * 0.70

    def test_safe_floor_floor_by_min_net_apr(self) -> None:
        settings = _make_settings(safe_floor_mult=Decimal("0.70"), min_net_apr=Decimal("0.30"))
        pos = _make_open_position(open_net_apr=Decimal("0.20"))
        # 0.20 * 0.70 = 0.14 < 0.30 → floor wins
        safe_floor = max(pos.open_net_apr * settings.safe_floor_mult, settings.min_net_apr)
        assert safe_floor == Decimal("0.30")

    def test_funding_breakeven_skips_floor_check(self) -> None:
        settings = _make_settings(
            funding_breakeven_skip_safe_floor=True,
            safe_floor_mult=Decimal("0.70"),
            min_net_apr=Decimal("0.10"),
        )
        pos = _make_open_position(
            open_net_apr=Decimal("0.50"),
            last_seen_net_apr=Decimal("0.60"),  # above floor, breakeven achieved
        )
        pos.funding_breakeven_achieved = True
        # check_floor = not (True AND True) = False → floor bypassed
        check_floor = not (
            settings.funding_breakeven_skip_safe_floor and pos.funding_breakeven_achieved
        )
        assert check_floor is False

    def test_funding_breakeven_not_yet(self) -> None:
        settings = _make_settings(funding_breakeven_skip_safe_floor=True)
        pos = _make_open_position()
        pos.funding_breakeven_achieved = False
        check_floor = not (
            settings.funding_breakeven_skip_safe_floor and pos.funding_breakeven_achieved
        )
        assert check_floor is True

    def test_funding_breakeven_disabled_always_checks(self) -> None:
        settings = _make_settings(funding_breakeven_skip_safe_floor=False)
        pos = _make_open_position()
        pos.funding_breakeven_achieved = True
        check_floor = not (
            settings.funding_breakeven_skip_safe_floor and pos.funding_breakeven_achieved
        )
        assert check_floor is True


class TestSmartExitNegativeWindow:
    """Verify N-consecutive-negative trigger logic."""

    def _would_trigger_neg(self, hist: list[float], window: int) -> bool:
        h = [Decimal(str(x)) for x in hist]
        return len(h) >= window and all(x < 0 for x in h[-window:])

    def test_two_negatives_triggers_with_window_2(self) -> None:
        assert self._would_trigger_neg([-0.1, 0.2, -0.3, -0.4], 2)

    def test_one_negative_does_not_trigger_window_2(self) -> None:
        assert not self._would_trigger_neg([0.2, 0.3, -0.1], 2)

    def test_positive_breaks_streak(self) -> None:
        assert not self._would_trigger_neg([-0.5, 0.1, -0.3], 2)

    def test_zero_apr_is_not_negative(self) -> None:
        assert not self._would_trigger_neg([-0.1, 0.0], 2)

    def test_empty_history_does_not_trigger(self) -> None:
        assert not self._would_trigger_neg([], 2)


class TestSmartExitDeclineWindow:
    """Verify N-consecutive-declines trigger logic."""

    def _would_trigger_decl(self, hist: list[float], window: int) -> bool:
        h = [Decimal(str(x)) for x in hist]
        if len(h) < window + 1:
            return False
        return all(h[-1 - i] < h[-2 - i] for i in range(window))

    def test_three_declines_triggers_window_3(self) -> None:
        # 0.5, 0.4, 0.3, 0.2 → 3 consecutive declines
        assert self._would_trigger_decl([0.5, 0.4, 0.3, 0.2], 3)

    def test_two_declines_not_enough_for_window_3(self) -> None:
        # only 2 declines
        assert not self._would_trigger_decl([0.5, 0.4, 0.3], 3)

    def test_plateau_breaks_streak(self) -> None:
        # same value twice = not declining
        assert not self._would_trigger_decl([0.5, 0.4, 0.4, 0.3], 3)

    def test_recovery_breaks_streak(self) -> None:
        assert not self._would_trigger_decl([0.5, 0.4, 0.3, 0.35], 3)

    def test_needs_window_plus_one_data_points(self) -> None:
        # decline_window=3 needs 4 data points minimum
        assert not self._would_trigger_decl([0.3, 0.2, 0.1], 3)


class TestFundingBreakevenCalculation:
    """Verify funding_total > friction_est × 1.5 triggers breakeven."""

    def test_breakeven_achieved_when_above_threshold(self) -> None:
        settings = _make_settings(estimated_friction_bps=15)
        pos = _make_open_position(leg_notional_usd=Decimal("100"))
        # friction_est = 15/10000 * 100 * 2 * 2 = 0.60
        # (4 legs total: long-open, short-open, long-close, short-close)
        friction_est = (
            Decimal(settings.estimated_friction_bps) / Decimal(10000)
            * pos.leg_notional_usd * Decimal(2) * Decimal(2)
        )
        assert friction_est == Decimal("0.6000")
        # funding_total must exceed 0.60 * 1.5 = 0.90 to trigger breakeven
        assert Decimal("1.00") > friction_est * Decimal("1.5")  # 1.00 > 0.90 ✓
        assert Decimal("0.85") < friction_est * Decimal("1.5")  # 0.85 < 0.90 ✗

    def test_breakeven_not_achieved_below_threshold(self) -> None:
        settings = _make_settings(estimated_friction_bps=15)
        pos = _make_open_position(leg_notional_usd=Decimal("100"))
        friction_est = (
            Decimal(settings.estimated_friction_bps) / Decimal(10000)
            * pos.leg_notional_usd * Decimal(2) * Decimal(2)
        )
        # funding_total = 0.08 < 0.06 * 1.5 = 0.09
        assert not (Decimal("0.08") > friction_est * Decimal("1.5"))


# =============================================================================
# Patch G — Adverse basis check (unit logic)
# =============================================================================


class TestAdverseBasicCheck:
    """Validate adverse_bps calculation matches TZ formula."""

    def _adverse_bps(self, long_price: Decimal, short_price: Decimal) -> Decimal:
        return (long_price - short_price) / long_price * Decimal(10000)

    def test_adverse_basis_positive_when_long_more_expensive(self) -> None:
        # long_price=100, short_price=99: adverse_bps = 1/100 * 10000 = 100 bps
        bps = self._adverse_bps(Decimal("100"), Decimal("99"))
        assert bps == Decimal("100")

    def test_adverse_basis_negative_when_long_cheaper(self) -> None:
        # long_price=99, short_price=100: negative → we're buying cheap and selling expensive
        bps = self._adverse_bps(Decimal("99"), Decimal("100"))
        assert bps < 0

    def test_adverse_basis_zero_same_price(self) -> None:
        bps = self._adverse_bps(Decimal("100"), Decimal("100"))
        assert bps == Decimal("0")

    def test_30bps_cap_triggers_at_31bps(self) -> None:
        # 31 bps adverse basis should exceed cap of 30
        bps = self._adverse_bps(Decimal("10000"), Decimal("9969"))
        assert bps > Decimal("30")

    def test_30bps_cap_passes_at_29bps(self) -> None:
        bps = self._adverse_bps(Decimal("10000"), Decimal("9971"))
        assert bps < Decimal("30")


# =============================================================================
# Snapshot round-trip (save_snapshot / load_snapshot) — includes Patch F fields
# =============================================================================


class TestSnapshotRoundTrip:
    def test_empty_state_produces_empty_json(self, tmp_path: Path) -> None:
        path = tmp_path / "snap.json"
        save_snapshot({}, path)
        loaded = load_snapshot(path)
        assert loaded == {}

    def test_basic_position_survives_round_trip(self, tmp_path: Path) -> None:
        path = tmp_path / "snap.json"
        pos = _make_open_position()
        save_snapshot({"test-01": pos}, path)
        loaded = load_snapshot(path)
        assert "test-01" in loaded
        out = loaded["test-01"]
        assert out.arb_id == pos.arb_id
        assert out.asset == pos.asset
        assert out.open_net_apr == pos.open_net_apr

    def test_patch_f_fields_survive_round_trip(self, tmp_path: Path) -> None:
        """apr_history, funding_breakeven_achieved, peak_funding_cum serialised correctly."""
        path = tmp_path / "snap.json"
        pos = _make_open_position()
        pos.apr_history = [Decimal("0.50"), Decimal("0.45"), Decimal("0.40")]
        pos.funding_breakeven_achieved = True
        pos.peak_funding_cum = Decimal("0.12")
        save_snapshot({"test-01": pos}, path)
        loaded = load_snapshot(path)
        out = loaded["test-01"]
        assert out.apr_history == [Decimal("0.50"), Decimal("0.45"), Decimal("0.40")]
        assert out.funding_breakeven_achieved is True
        assert out.peak_funding_cum == Decimal("0.12")

    def test_closed_position_excluded_from_snapshot(self, tmp_path: Path) -> None:
        path = tmp_path / "snap.json"
        pos = _make_open_position(arb_id="closed-01")
        pos.closed = True
        save_snapshot({"closed-01": pos}, path)
        loaded = load_snapshot(path)
        assert "closed-01" not in loaded

    def test_missing_file_returns_empty_dict(self, tmp_path: Path) -> None:
        path = tmp_path / "nonexistent.json"
        assert load_snapshot(path) == {}

    def test_corrupted_json_returns_empty_dict(self, tmp_path: Path) -> None:
        path = tmp_path / "bad.json"
        path.write_text("{corrupted!", encoding="utf-8")
        assert load_snapshot(path) == {}

    def test_partial_corrupt_entry_skipped(self, tmp_path: Path) -> None:
        """If one entry is broken, rest are still loaded."""
        path = tmp_path / "snap.json"
        pos = _make_open_position(arb_id="good-01")
        save_snapshot({"good-01": pos}, path)
        # Inject a broken entry directly
        data = json.loads(path.read_text())
        data["bad-02"] = {"arb_id": "bad-02"}  # missing required fields
        path.write_text(json.dumps(data), encoding="utf-8")
        loaded = load_snapshot(path)
        assert "good-01" in loaded
        assert "bad-02" not in loaded

    def test_multiple_positions_round_trip(self, tmp_path: Path) -> None:
        path = tmp_path / "snap.json"
        pos1 = _make_open_position(arb_id="p1", asset="BTC")
        pos2 = _make_open_position(arb_id="p2", asset="ETH")
        save_snapshot({"p1": pos1, "p2": pos2}, path)
        loaded = load_snapshot(path)
        assert len(loaded) == 2
        assert loaded["p1"].asset == "BTC"
        assert loaded["p2"].asset == "ETH"

    def test_opened_at_timezone_preserved(self, tmp_path: Path) -> None:
        path = tmp_path / "snap.json"
        ts = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        pos = _make_open_position(opened_at=ts)
        save_snapshot({"p1": pos}, path)
        loaded = load_snapshot(path)
        assert loaded["p1"].opened_at == ts


# =============================================================================
# Settings defaults — sanity checks
# =============================================================================


class TestSettingsDefaults:
    def test_stop_loss_pct_default_is_5pct(self) -> None:
        """Default stop_loss_pct=0.05 (5%) — выровнен с ТЗ NOTE-2 и .env.example."""
        assert DEFAULT_STOP_LOSS_PCT == Decimal("0.05")

    def test_max_adverse_basis_bps_default_30(self) -> None:
        assert DEFAULT_MAX_ADVERSE_BASIS_BPS == 30

    def test_safe_floor_mult_default_070(self) -> None:
        assert DEFAULT_SAFE_FLOOR_MULT == Decimal("0.70")

    def test_decline_window_default_3(self) -> None:
        assert DEFAULT_DECLINE_WINDOW == 3

    def test_negative_window_default_2(self) -> None:
        assert DEFAULT_NEGATIVE_WINDOW == 2

    def test_exchange_sl_enabled_default_true(self) -> None:
        assert DEFAULT_EXCHANGE_SL_ENABLED is True

    def test_limit_alo_enabled_default_true(self) -> None:
        assert DEFAULT_LIMIT_ALO_ENABLED is True

    def test_limit_alo_leg_side_default_auto(self) -> None:
        assert DEFAULT_LIMIT_ALO_LEG_SIDE == "auto"

    def test_from_env_reads_token(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VOOI_BEARER_TOKEN", "test-token-xyz")
        s = Settings.from_env()
        assert s.bearer_token == "test-token-xyz"

    def test_from_env_missing_token_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("VOOI_BEARER_TOKEN", raising=False)
        with pytest.raises(RuntimeError, match="VOOI_BEARER_TOKEN"):
            Settings.from_env()

    def test_from_env_bad_exchange_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VOOI_BEARER_TOKEN", "tok")
        monkeypatch.setenv("BOT_TARGET_EXCHANGES", "hyperliquid,unknown_exchange")
        with pytest.raises(RuntimeError, match="non-tradable"):
            Settings.from_env()

    def test_from_env_dry_run_default_true(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VOOI_BEARER_TOKEN", "tok")
        monkeypatch.delenv("BOT_DRY_RUN", raising=False)
        s = Settings.from_env()
        assert s.dry_run is True

    def test_from_env_asset_blacklist_parsed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VOOI_BEARER_TOKEN", "tok")
        monkeypatch.setenv("BOT_ASSET_BLACKLIST", "YZY,STBL,2Z")
        s = Settings.from_env()
        assert "YZY" in s.asset_blacklist
        assert "STBL" in s.asset_blacklist
        assert "2Z" in s.asset_blacklist

    def test_from_env_limit_alo_settings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VOOI_BEARER_TOKEN", "tok")
        monkeypatch.setenv("BOT_LIMIT_ALO_ENABLED", "false")
        monkeypatch.setenv("BOT_LIMIT_ALO_DRIFT_BPS", "75")
        s = Settings.from_env()
        assert s.limit_alo_enabled is False
        assert s.limit_alo_drift_bps == 75

    def test_from_env_exchange_sl_settings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VOOI_BEARER_TOKEN", "tok")
        monkeypatch.setenv("BOT_EXCHANGE_SL_ENABLED", "false")
        monkeypatch.setenv("BOT_EXCHANGE_SL_BUFFER_PCT", "0.10")
        s = Settings.from_env()
        assert s.exchange_sl_enabled is False
        assert s.exchange_sl_buffer_pct == Decimal("0.10")


# =============================================================================
# Regression: hard stop-loss threshold formula
# =============================================================================


class TestHardStopLossThreshold:
    def test_threshold_uses_collat_times_2(self) -> None:
        """Threshold = -stop_loss_pct * leg_collat_usd * 2 (both legs)."""
        settings = _make_settings(stop_loss_pct=Decimal("0.05"))
        pos = _make_open_position(leg_collat_usd=Decimal("10"))
        threshold = -settings.stop_loss_pct * pos.leg_collat_usd * 2
        # -0.05 * 10 * 2 = -1.0
        assert threshold == Decimal("-1.0")

    def test_threshold_with_2pct_default(self) -> None:
        """Default 2% stop-loss on $10 collat each leg = -$0.40 threshold."""
        settings = _make_settings(stop_loss_pct=Decimal("0.02"))
        pos = _make_open_position(leg_collat_usd=Decimal("10"))
        threshold = -settings.stop_loss_pct * pos.leg_collat_usd * 2
        assert threshold == Decimal("-0.40")

    def test_net_above_threshold_no_close(self) -> None:
        settings = _make_settings(stop_loss_pct=Decimal("0.05"))
        pos = _make_open_position(leg_collat_usd=Decimal("10"))
        pos.long_upnl_usd = Decimal("-0.50")
        pos.short_upnl_usd = Decimal("0.30")
        pos.long_funding_usd = Decimal("0.10")
        pos.short_funding_usd = Decimal("0.20")
        net_usd = sum(
            [pos.long_upnl_usd, pos.short_upnl_usd, pos.long_funding_usd, pos.short_funding_usd],
            Decimal(0),
        )
        threshold = -settings.stop_loss_pct * pos.leg_collat_usd * 2
        assert net_usd > threshold  # -0.50+0.30+0.10+0.20 = 0.10 > -1.0

    def test_net_below_threshold_triggers_close(self) -> None:
        settings = _make_settings(stop_loss_pct=Decimal("0.05"))
        pos = _make_open_position(leg_collat_usd=Decimal("10"))
        pos.long_upnl_usd = Decimal("-2.00")
        pos.short_upnl_usd = Decimal("0.50")
        pos.long_funding_usd = Decimal("0.10")
        pos.short_funding_usd = Decimal("0.10")
        net_usd = sum(
            [pos.long_upnl_usd, pos.short_upnl_usd, pos.long_funding_usd, pos.short_funding_usd],
            Decimal(0),
        )
        threshold = -settings.stop_loss_pct * pos.leg_collat_usd * 2
        assert net_usd < threshold  # -1.30 < -1.0


# =============================================================================
# Tuning — smart-exit hardening + per-asset cooldown
# =============================================================================


class TestTuning2026_05_13_Defaults:
    """Defaults остаются «выключенными» — поведение меняется только если .env задал значения."""

    def test_smart_neg_value_floor_default_zero(self) -> None:
        # 0 = legacy: любой x<0 квалифицируется как negative-reading.
        assert DEFAULT_SMART_NEG_VALUE_FLOOR == Decimal("0")

    def test_min_hold_hours_default_zero(self) -> None:
        # 0 = soft-exits не блокируются — обратная совместимость.
        assert DEFAULT_MIN_HOLD_HOURS == 0

    def test_pair_cooldown_after_losses_default_zero(self) -> None:
        # 0 = cooldown отключён.
        assert DEFAULT_PAIR_COOLDOWN_AFTER_LOSSES == 0

    def test_pair_cooldown_hours_default_48(self) -> None:
        assert DEFAULT_PAIR_COOLDOWN_HOURS == 48

    def test_from_env_reads_smart_neg_value_floor(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VOOI_BEARER_TOKEN", "tok")
        monkeypatch.setenv("BOT_SMART_NEG_VALUE_FLOOR", "-0.05")
        s = Settings.from_env()
        assert s.smart_neg_value_floor == Decimal("-0.05")

    def test_from_env_reads_min_hold_hours(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VOOI_BEARER_TOKEN", "tok")
        monkeypatch.setenv("BOT_MIN_HOLD_HOURS", "6")
        s = Settings.from_env()
        assert s.min_hold_hours == 6

    def test_from_env_reads_cooldown_settings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VOOI_BEARER_TOKEN", "tok")
        monkeypatch.setenv("BOT_PAIR_COOLDOWN_AFTER_LOSSES", "2")
        monkeypatch.setenv("BOT_PAIR_COOLDOWN_HOURS", "24")
        monkeypatch.setenv("BOT_COOLDOWN_FILE", "/tmp/custom-cooldown.json")
        s = Settings.from_env()
        assert s.pair_cooldown_after_losses == 2
        assert s.pair_cooldown_hours == 24
        assert s.cooldown_file == Path("/tmp/custom-cooldown.json")


class TestCooldownState:
    def test_save_load_round_trip(self, tmp_path: Path) -> None:
        path = tmp_path / "cooldown.json"
        ts1 = datetime(2026, 5, 13, 10, 0, tzinfo=UTC).isoformat()
        ts2 = datetime(2026, 5, 13, 11, 0, tzinfo=UTC).isoformat()
        save_cooldown_state({"XMR": [ts1, ts2], "NEAR": [ts1]}, path)
        loaded = load_cooldown_state(path)
        assert loaded == {"XMR": [ts1, ts2], "NEAR": [ts1]}

    def test_missing_file_returns_empty(self, tmp_path: Path) -> None:
        assert load_cooldown_state(tmp_path / "missing.json") == {}

    def test_corrupt_file_returns_empty(self, tmp_path: Path) -> None:
        path = tmp_path / "bad.json"
        path.write_text("{not json")
        assert load_cooldown_state(path) == {}

    def test_load_normalises_asset_keys_upper(self, tmp_path: Path) -> None:
        path = tmp_path / "cooldown.json"
        ts = datetime(2026, 5, 13, 10, 0, tzinfo=UTC).isoformat()
        path.write_text(json.dumps({"xmr": [ts]}), encoding="utf-8")
        loaded = load_cooldown_state(path)
        assert "XMR" in loaded

    def test_prune_drops_old_entries(self) -> None:
        now = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)
        fresh = (now - timedelta(hours=10)).isoformat()
        stale = (now - timedelta(hours=72)).isoformat()
        losses = {"XMR": [stale, fresh], "NEAR": [stale]}
        prune_cooldown(losses, cooldown_hours=48, now=now)
        assert losses == {"XMR": [fresh]}  # NEAR полностью удалён, stale выкинут

    def test_prune_zero_hours_clears_everything(self) -> None:
        losses = {"XMR": [datetime.now(UTC).isoformat()]}
        prune_cooldown(losses, cooldown_hours=0)
        assert losses == {}

    def test_blocked_below_threshold_returns_empty(self) -> None:
        now = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)
        ts = (now - timedelta(hours=10)).isoformat()
        # only 1 loss but threshold is 2
        blocks = cooldown_blocked_assets({"XMR": [ts]}, after_losses=2, cooldown_hours=48, now=now)
        assert blocks == {}

    def test_blocked_at_threshold_returns_asset(self) -> None:
        now = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)
        ts1 = (now - timedelta(hours=10)).isoformat()
        ts2 = (now - timedelta(hours=20)).isoformat()
        blocks = cooldown_blocked_assets(
            {"XMR": [ts1, ts2]}, after_losses=2, cooldown_hours=48, now=now,
        )
        assert "XMR" in blocks
        assert "cooldown" in blocks["XMR"]
        assert "losses=2/2" in blocks["XMR"]

    def test_blocked_ignores_stale_entries(self) -> None:
        now = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)
        fresh = (now - timedelta(hours=10)).isoformat()
        stale = (now - timedelta(hours=72)).isoformat()
        # 1 fresh + 1 stale → fresh count = 1 < threshold 2
        blocks = cooldown_blocked_assets(
            {"XMR": [fresh, stale]}, after_losses=2, cooldown_hours=48, now=now,
        )
        assert blocks == {}

    def test_blocked_when_threshold_zero_returns_empty(self) -> None:
        now = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)
        ts = (now - timedelta(hours=10)).isoformat()
        blocks = cooldown_blocked_assets({"XMR": [ts, ts]}, after_losses=0, cooldown_hours=48, now=now)
        assert blocks == {}


class TestSmartNegValueFloorLogic:
    """Логика: каждое чтение в окне должно быть < value_floor."""

    def test_default_floor_zero_matches_legacy_behavior(self) -> None:
        hist = [Decimal("-0.005"), Decimal("-0.002")]
        floor = Decimal("0")  # legacy
        # Все < 0 → triggers
        assert all(x < floor for x in hist[-2:])

    def test_floor_neg_5pct_rejects_noise(self) -> None:
        # Шум ±1% APR: -0.01, -0.005 — legacy триггерил, новый не должен.
        hist = [Decimal("-0.01"), Decimal("-0.005")]
        floor = Decimal("-0.05")
        assert not all(x < floor for x in hist[-2:])

    def test_floor_neg_5pct_accepts_real_negative(self) -> None:
        # Реально-отрицательный APR: -10%, -8% — должен сработать.
        hist = [Decimal("-0.10"), Decimal("-0.08")]
        floor = Decimal("-0.05")
        assert all(x < floor for x in hist[-2:])

    def test_floor_neg_5pct_window_3_requires_all_below(self) -> None:
        # Окно 3: один из трёх не прошёл floor → не триггерит.
        hist = [Decimal("-0.10"), Decimal("-0.02"), Decimal("-0.08")]
        floor = Decimal("-0.05")
        assert not all(x < floor for x in hist[-3:])


class TestMinHoldHoursGate:
    """held_h >= min_hold_hours блокирует soft-exits."""

    def test_held_under_min_hold_blocks_soft_exit(self) -> None:
        s = _make_settings(min_hold_hours=6)
        # held_h = 3.5 < 6 → soft_exit_allowed = False
        held_h = 3.5
        assert (held_h >= s.min_hold_hours) is False

    def test_held_above_min_hold_allows_soft_exit(self) -> None:
        s = _make_settings(min_hold_hours=6)
        held_h = 8.0
        assert (held_h >= s.min_hold_hours) is True

    def test_min_hold_zero_disables_gate(self) -> None:
        # Default = 0 → all soft-exits allowed from t=0.
        s = _make_settings(min_hold_hours=0)
        assert (0.1 >= s.min_hold_hours) is True
