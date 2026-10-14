"""vooi-funding-arb-bot — delta-neutral funding-rate arbitrage engine.

⚠️  ВНИМАНИЕ: Это **MVP**, НЕ production-ready.

Намеренно отсутствуют (см. docs/plan.md §4.A — это блокеры production):

- C2: при 5xx на POST orders НЕ делается reconcile через `clientOrderId` filter.
  Может задвоить ордер. Каждый запуск перед opener'ом проверяет существующие
  позиции, но между двумя POSTs внутри одного opener'а защиты нет.
- C3: SSE не используется вообще. Все события через REST poll каждые 5 мин.
- C4: SSE watchdog не нужен — нет SSE.
- C6: single-instance gate отсутствует. Если запустить два процесса под
  одним токеном — оба откроют дубль. ОПЕРАТОР ОТВЕЧАЕТ за единственный экземпляр.
- C8: intent/effect events упрощены до append-only лога без replay-recovery.
  При crash в момент opener'а — possible half-legged state, требует ручного fix.
- V1: net funding flip без grace period — закрываем сразу при `current_netApr < 0`.
- V4: mutex per (exchange, asset) есть, но per-exchange cap проверяется до lock.

Что есть (минимум):
- C5: hard-reject `extended` exchange.
- C7: broker config пробрасывается (опциональный для HL — fail-soft если 4xx).
- C9: по умолчанию hard-reject `xyz:`/`alias:` (`probe.markets`); при
  `BOT_INCLUDE_HL_NON_CRYPTO=1` — HIP-3 пары (xyz/alias/km) допускаются,
  margin/caps учитываются в общем bucket `hyperliquid:perps:<quote>`
  (xyz/alias делят USDC-пул с crypto-perps — verified live 2026-05-22;
  см. `probe.markets` для эмпирики).
- V8: clock drift sanity (вынесено в startup).
- $cap per exchange + min net APR threshold.

Запуск:
    uv run python -m fundbot

Параметры — через `.env` (см. секции ниже).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from pathlib import Path
from typing import Any

import httpx
from probe.markets import CRYPTO_PERPS_ACCOUNT_TYPE, is_non_crypto_prefix, margin_bucket_key

from fundbot import sse as _sse_mod

# =============================================================================
# Constants & defaults
# =============================================================================

ALLOWED_TRADING_EXCHANGES: frozenset[str] = frozenset({"hyperliquid", "lighter", "aster"})

# Broker / integrator attribution is handled server-side by the VOOI API:
# the bot does not need to set a `broker` field on outgoing orders.

DEFAULT_LOOP_INTERVAL_SEC = 300  # monitor (report) cycle
DEFAULT_TRADING_CYCLE_SEC = 3600  # trading (open/close) cycle
DEFAULT_LEG_COLLAT_USD = Decimal("5")

# Все биржи которые мы умеем обрабатывать. Используется для проверки
# external positions при принятии решения об открытии новой стратегии:
# даже если торгуем только на части (target_exchanges), но на ДРУГОЙ бирже
# уже руками открыта поза по этому asset — стратегию по нему не открываем
# (защита от двойных позиций по одному asset).
ALL_SUPPORTED_EXCHANGES: tuple[str, ...] = ("hyperliquid", "lighter", "aster")
DEFAULT_MAX_MARGIN_PER_EX_USD = Decimal("15")
# Из ТЗ: min volume на стратегию = $100k.
DEFAULT_MIN_VOLUME_24H_USD = Decimal("100000")
# Из ТЗ §1.2: `netApr` сейчас = grossSpread (fees не вычтены, в будущем будут).
# Минимальный APR для входа в стратегию. Утверждён 2026-05-01 в strategy-tuning-2026-05-01.md.
# 10% — отсекает шум, окупится за <4 дня даже при friction.
DEFAULT_MIN_NET_APR = Decimal("0.10")
# В ТЗ нет max_net_apr cap. Оставлено как опциональный safety override через .env.
# 0 = no cap. По умолчанию выключен.
DEFAULT_MAX_NET_APR = Decimal("0")
# Target leverage = ВЕРХНИЙ потолок безопасности. Фактический = min(target, maxLev_long, maxLev_short).
DEFAULT_LEVERAGE_TARGET = 10
# Максимум hold перед force-close. Утверждён 2026-05-01 (96h = 4 дня).
DEFAULT_MAX_HOLD_HOURS = 96

# Stop-loss: если NET позы < -X% × collat × 2 → close на ЛЮБОМ цикле.
# v2: обновлено до 5% (Patch F threshold = collat*2, а не notional).
# На $10 collat × 2 ног = -$1.00 при 5%.
DEFAULT_STOP_LOSS_PCT = Decimal("0.05")
# Сколько TRADING циклов подряд (по 1 часу) с net_apr<0 ждём перед close.
# Утверждён 2026-05-01 (4 цикла = 4 часа грейса). Защита от funding-rate flip-tick'ов.
DEFAULT_NEGATIVE_APR_TRADING_STREAK = 4
# Максимально допустимый slippage на одной ноге при entry. > → reject open.
# Утверждён 2026-05-01 (200bps = 2%). Защита от тонкого book'а (типа YZY).
DEFAULT_MAX_SLIPPAGE_BPS = 200
# Asset blacklist: assets, по которым НИКОГДА не открываем стратегию (даже если netApr идеальный).
# По умолчанию пустой — фильтры решают.
DEFAULT_ASSET_BLACKLIST: tuple[str, ...] = ()

# Whitelist по умолчанию ОТКЛЮЧЁН (в ТЗ нет). Можно включить через .env
# `BOT_ASSET_WHITELIST=BTC,ETH,...` для опционального ограничения.
DEFAULT_ASSET_WHITELIST: tuple[str, ...] = ()

# APR upper cap: отсекаем spike-APR с высокой вероятностью drift. 0 = нет cap.
DEFAULT_APR_UPPER_CAP = Decimal("0")
# Минимальный volume на каждой ноге (per leg) для входа. 0 = нет фильтра.
DEFAULT_PER_LEG_VOLUME_MIN_USD = Decimal("0")
# Ratio apr1h/apr24h максимальный (spike-детектор). 0 = нет фильтра.
DEFAULT_APR_RATIO_1H_TO_24H_MAX = Decimal("0")
# Ratio apr24h/apr7d максимальный (среднесрочный spike-детектор). 0 = нет фильтра.
DEFAULT_APR_RATIO_24H_TO_7D_MAX = Decimal("0")
# Leverage cap: жёсткий потолок поверх market maxLeverage. 0 = нет доп. cap.
DEFAULT_LEVERAGE_CAP = 0
# Максимальный нотионал на одну позицию (2 ноги). 0 = нет ограничения.
DEFAULT_MAX_NOTIONAL_PER_POSITION_USD = Decimal("0")

# Patch F: Smart-exit defaults
DEFAULT_SAFE_FLOOR_MULT = Decimal("0.70")
DEFAULT_DECLINE_WINDOW = 3
DEFAULT_NEGATIVE_WINDOW = 2
DEFAULT_FUNDING_BREAKEVEN_SKIP_SAFE_FLOOR = True
DEFAULT_ESTIMATED_FRICTION_BPS = 15

# Tuning note: # Каждое чтение в smart_neg окне должно быть < этого значения (а не любое <0,
# которое включает шум ±1% APR на двух тиках подряд). Default 0 = legacy
# поведение (любое <0). Рекомендованное значение: -0.05 (-5% APR).
DEFAULT_SMART_NEG_VALUE_FLOOR = Decimal("0")
# Блокирует soft-exits (smart_neg/decl/low_apr) пока held_h < этого значения.
# hard_stop_loss и max_hold продолжают работать. Default 0 = отключено.
DEFAULT_MIN_HOLD_HOURS = 0
# После N убыточных закрытий по одному asset в окне PAIR_COOLDOWN_HOURS → asset
# попадает в blocked_assets как cooldown. Default 0 = отключено.
DEFAULT_PAIR_COOLDOWN_AFTER_LOSSES = 0
DEFAULT_PAIR_COOLDOWN_HOURS = 48

# Patch A: Exchange SL defaults
DEFAULT_EXCHANGE_SL_BUFFER_PCT = Decimal("0.05")
DEFAULT_EXCHANGE_SL_ENABLED = True

# Patch B: LIMIT alo defaults
DEFAULT_LIMIT_ALO_ENABLED = True
DEFAULT_LIMIT_ALO_OFFSET_BPS = 5
DEFAULT_LIMIT_ALO_TIMEOUT_SEC = 600
DEFAULT_LIMIT_ALO_DRIFT_BPS = 50
DEFAULT_LIMIT_ALO_POLL_SEC = 5
DEFAULT_LIMIT_ALO_LEG_SIDE = "auto"

# Patch J (2026-05-20): close-side ALO to reduce close-slippage on soft-exits.
# On hard_stop_loss / max_hold these are skipped (urgency > slippage savings).
DEFAULT_LIMIT_ALO_CLOSE_ENABLED = False  # opt-in; market-only behaviour by default
DEFAULT_LIMIT_ALO_CLOSE_TIMEOUT_SEC = 60
DEFAULT_LIMIT_ALO_CLOSE_OFFSET_BPS = 5

# Patch G: Adverse basis check default
DEFAULT_MAX_ADVERSE_BASIS_BPS = 30

# Patch H: Low-APR sustained exit
DEFAULT_LOW_APR_THRESHOLD = Decimal("0.20")  # 20%
DEFAULT_LOW_APR_WINDOW = 3

# Patch I: Post-close settlement wait
DEFAULT_POST_CLOSE_SETTLE_SEC = 20

# Patch J: Trading cycle offset from hour boundary
DEFAULT_TRADING_MINUTE = 5

# Явно включённые в книгу пары (asset из snapshot), помимо обычного scanner flow.
# Должны присутствовать в `state-snapshot.json`, иначе при старте будет `BOOK_MANAGED_MISSING`.
MANAGED_BOOK_EXTRA_ASSETS: frozenset[str] = frozenset({"CHIP", "KAITO", "TON"})

# Биржи где POST /exchange/margin-mode не реализован — пропускаем без warning.
NO_MARGIN_MODE_API: frozenset[str] = frozenset({"lighter"})


# =============================================================================
# Settings (минимально, всё из .env)
# =============================================================================


@dataclass(frozen=True)
class Settings:
    base_url: str
    bearer_token: str
    target_exchanges: tuple[str, ...]
    leg_collat_usd: Decimal
    max_margin_per_ex_usd: Decimal
    min_net_apr: Decimal
    max_net_apr: Decimal
    leverage_target: int
    leverage_cap: int
    max_notional_per_position_usd: Decimal
    max_hold_hours: int
    min_volume_24h_usd: Decimal
    per_leg_volume_min_usd: Decimal
    apr_upper_cap: Decimal
    apr_ratio_1h_to_24h_max: Decimal
    apr_ratio_24h_to_7d_max: Decimal
    asset_whitelist: tuple[str, ...]
    asset_blacklist: tuple[str, ...]
    stop_loss_pct: Decimal
    negative_apr_trading_streak_threshold: int
    max_slippage_bps: int
    loop_interval_sec: int
    trading_cycle_sec: int
    dry_run: bool
    state_file: Path
    snapshot_file: Path
    instance_uuid: str
    include_hl_non_crypto: bool
    # Patch F: Smart-exit
    safe_floor_mult: Decimal
    decline_window: int
    negative_window: int
    funding_breakeven_skip_safe_floor: bool
    estimated_friction_bps: int
    # Tuning 2026-05-13: smart-exit hardening
    smart_neg_value_floor: Decimal
    min_hold_hours: int
    pair_cooldown_after_losses: int
    pair_cooldown_hours: int
    cooldown_file: Path
    # Patch A: Exchange SL
    exchange_sl_buffer_pct: Decimal
    exchange_sl_enabled: bool
    # Patch A3: Survivor watcher (1s poll, closes naked leg on bracket trigger)
    survivor_watch_enabled: bool
    survivor_watch_sec: float
    survivor_watch_idle_sec: float
    # Patch B: LIMIT alo
    limit_alo_enabled: bool
    limit_alo_offset_bps: int
    limit_alo_timeout_sec: int
    limit_alo_drift_bps: int
    limit_alo_poll_sec: int
    limit_alo_leg_side: str
    # Patch J: close-side ALO
    limit_alo_close_enabled: bool
    limit_alo_close_timeout_sec: int
    limit_alo_close_offset_bps: int
    # Patch G: Adverse basis check
    max_adverse_basis_bps: int
    # Patch H: Low-APR sustained exit
    low_apr_threshold: Decimal
    low_apr_window: int
    # Patch I: Post-close settlement wait
    post_close_settle_sec: int
    # Patch J: Trading cycle offset from hour boundary
    trading_minute: int
    # Б7: запас на комиссию/слиппедж при расчёте notional (0.5% = 50 bps).
    # Защищает от HL "Insufficient margin" когда биржа считает initial margin
    # с учётом fee/slippage чуть строже, чем наш номинальный leg_collat × leverage.
    margin_headroom_pct: Decimal
    # SSE event-stream client (Phase 1 — default disabled; latency optimisation
    # over REST polling, REST stays the source of truth). See fundbot/sse.py.
    sse_enabled: bool
    sse_heartbeat_timeout_sec: float
    sse_reconnect_max_attempts: int

    @classmethod
    def from_env(cls) -> Settings:
        token = os.environ.get("VOOI_BEARER_TOKEN")
        if not token:
            raise RuntimeError("VOOI_BEARER_TOKEN required")
        dry_run_value = os.environ.get("BOT_DRY_RUN", "true").strip().lower()
        if dry_run_value not in ("1", "true", "yes", "0", "false", "no"):
            raise RuntimeError("BOT_DRY_RUN must be an explicit true/false value")
        target = tuple(
            e.strip() for e in os.environ.get("BOT_TARGET_EXCHANGES", "hyperliquid,lighter").split(",")
            if e.strip()
        )
        bad = [e for e in target if e not in ALLOWED_TRADING_EXCHANGES]
        if bad:
            raise RuntimeError(
                f"BOT_TARGET_EXCHANGES contains non-tradable: {bad}. "
                f"Allowed: {sorted(ALLOWED_TRADING_EXCHANGES)}",
            )
        wl_raw = os.environ.get("BOT_ASSET_WHITELIST")
        if wl_raw is None:
            whitelist = DEFAULT_ASSET_WHITELIST
        elif wl_raw.strip() == "":
            whitelist = ()  # empty = no whitelist filter
        else:
            whitelist = tuple(s.strip().upper() for s in wl_raw.split(",") if s.strip())

        bl_raw = os.environ.get("BOT_ASSET_BLACKLIST")
        if bl_raw is None or bl_raw.strip() == "":
            blacklist = DEFAULT_ASSET_BLACKLIST
        else:
            blacklist = tuple(s.strip().upper() for s in bl_raw.split(",") if s.strip())
        return cls(
            base_url=os.environ.get("VOOI_API_BASE_URL", "https://perps-api.vooi.io"),
            bearer_token=token,
            target_exchanges=target,
            leg_collat_usd=Decimal(os.environ.get("BOT_LEG_COLLAT_USD", str(DEFAULT_LEG_COLLAT_USD))),
            max_margin_per_ex_usd=Decimal(
                os.environ.get("BOT_MAX_MARGIN_PER_EXCHANGE_USD", str(DEFAULT_MAX_MARGIN_PER_EX_USD)),
            ),
            min_net_apr=Decimal(os.environ.get("BOT_MIN_NET_APR", str(DEFAULT_MIN_NET_APR))),
            max_net_apr=Decimal(os.environ.get("BOT_MAX_NET_APR", str(DEFAULT_MAX_NET_APR))),
            leverage_target=int(os.environ.get("BOT_LEVERAGE_TARGET", str(DEFAULT_LEVERAGE_TARGET))),
            leverage_cap=int(os.environ.get("BOT_LEVERAGE_CAP", str(DEFAULT_LEVERAGE_CAP))),
            max_notional_per_position_usd=Decimal(
                os.environ.get("BOT_MAX_NOTIONAL_PER_POSITION_USD", str(DEFAULT_MAX_NOTIONAL_PER_POSITION_USD)),
            ),
            max_hold_hours=int(os.environ.get("BOT_MAX_HOLD_HOURS", str(DEFAULT_MAX_HOLD_HOURS))),
            min_volume_24h_usd=Decimal(
                os.environ.get("BOT_MIN_VOLUME_24H_USD", str(DEFAULT_MIN_VOLUME_24H_USD)),
            ),
            per_leg_volume_min_usd=Decimal(
                os.environ.get("BOT_PER_LEG_VOLUME_MIN_USD", str(DEFAULT_PER_LEG_VOLUME_MIN_USD)),
            ),
            apr_upper_cap=Decimal(os.environ.get("BOT_APR_UPPER_CAP", str(DEFAULT_APR_UPPER_CAP))),
            apr_ratio_1h_to_24h_max=Decimal(
                os.environ.get("BOT_APR_RATIO_1H_TO_24H_MAX", str(DEFAULT_APR_RATIO_1H_TO_24H_MAX)),
            ),
            apr_ratio_24h_to_7d_max=Decimal(
                os.environ.get("BOT_APR_RATIO_24H_TO_7D_MAX", str(DEFAULT_APR_RATIO_24H_TO_7D_MAX)),
            ),
            asset_whitelist=whitelist,
            asset_blacklist=blacklist,
            stop_loss_pct=Decimal(
                os.environ.get("BOT_STOP_LOSS_PCT", str(DEFAULT_STOP_LOSS_PCT)),
            ),
            negative_apr_trading_streak_threshold=int(
                os.environ.get(
                    "BOT_NEGATIVE_APR_TRADING_STREAK",
                    str(DEFAULT_NEGATIVE_APR_TRADING_STREAK),
                ),
            ),
            max_slippage_bps=int(
                os.environ.get("BOT_MAX_SLIPPAGE_BPS", str(DEFAULT_MAX_SLIPPAGE_BPS)),
            ),
            loop_interval_sec=int(os.environ.get("BOT_LOOP_INTERVAL_SEC", str(DEFAULT_LOOP_INTERVAL_SEC))),
            trading_cycle_sec=int(os.environ.get("BOT_TRADING_CYCLE_SEC", str(DEFAULT_TRADING_CYCLE_SEC))),
            dry_run=dry_run_value in ("1", "true", "yes"),
            state_file=Path(os.environ.get("BOT_STATE_FILE", "state.ndjson")),
            snapshot_file=Path(os.environ.get("BOT_SNAPSHOT_FILE", "state-snapshot.json")),
            instance_uuid=uuid.uuid4().hex[:12],
            include_hl_non_crypto=os.environ.get("BOT_INCLUDE_HL_NON_CRYPTO", "false").lower()
            in ("1", "true", "yes"),
            # Patch F: Smart-exit
            safe_floor_mult=Decimal(os.environ.get("BOT_SAFE_FLOOR_MULT", str(DEFAULT_SAFE_FLOOR_MULT))),
            decline_window=int(os.environ.get("BOT_DECLINE_WINDOW", str(DEFAULT_DECLINE_WINDOW))),
            negative_window=int(os.environ.get("BOT_NEGATIVE_WINDOW", str(DEFAULT_NEGATIVE_WINDOW))),
            funding_breakeven_skip_safe_floor=os.environ.get(
                "BOT_FUNDING_BREAKEVEN_SKIP_SAFE_FLOOR", "true"
            ).lower() in ("1", "true", "yes"),
            estimated_friction_bps=int(
                os.environ.get("BOT_ESTIMATED_FRICTION_BPS", str(DEFAULT_ESTIMATED_FRICTION_BPS))
            ),
            # Tuning 2026-05-13: smart-exit hardening
            smart_neg_value_floor=Decimal(
                os.environ.get("BOT_SMART_NEG_VALUE_FLOOR", str(DEFAULT_SMART_NEG_VALUE_FLOOR))
            ),
            min_hold_hours=int(
                os.environ.get("BOT_MIN_HOLD_HOURS", str(DEFAULT_MIN_HOLD_HOURS))
            ),
            pair_cooldown_after_losses=int(
                os.environ.get("BOT_PAIR_COOLDOWN_AFTER_LOSSES", str(DEFAULT_PAIR_COOLDOWN_AFTER_LOSSES))
            ),
            pair_cooldown_hours=int(
                os.environ.get("BOT_PAIR_COOLDOWN_HOURS", str(DEFAULT_PAIR_COOLDOWN_HOURS))
            ),
            cooldown_file=Path(os.environ.get("BOT_COOLDOWN_FILE", "state-cooldown.json")),
            # Patch A: Exchange SL
            exchange_sl_buffer_pct=Decimal(
                os.environ.get("BOT_EXCHANGE_SL_BUFFER_PCT", str(DEFAULT_EXCHANGE_SL_BUFFER_PCT))
            ),
            exchange_sl_enabled=os.environ.get("BOT_EXCHANGE_SL_ENABLED", "true").lower()
            in ("1", "true", "yes"),
            # Patch A3: Survivor watcher
            survivor_watch_enabled=os.environ.get("BOT_SURVIVOR_WATCH_ENABLED", "true").lower()
            in ("1", "true", "yes"),
            survivor_watch_sec=float(os.environ.get("BOT_SURVIVOR_WATCH_SEC", "1")),
            survivor_watch_idle_sec=float(os.environ.get("BOT_SURVIVOR_WATCH_IDLE_SEC", "30")),
            # Patch B: LIMIT alo
            limit_alo_enabled=os.environ.get("BOT_LIMIT_ALO_ENABLED", "true").lower()
            in ("1", "true", "yes"),
            limit_alo_offset_bps=int(
                os.environ.get("BOT_LIMIT_ALO_OFFSET_BPS", str(DEFAULT_LIMIT_ALO_OFFSET_BPS))
            ),
            limit_alo_timeout_sec=int(
                os.environ.get("BOT_LIMIT_ALO_TIMEOUT_SEC", str(DEFAULT_LIMIT_ALO_TIMEOUT_SEC))
            ),
            limit_alo_drift_bps=int(
                os.environ.get("BOT_LIMIT_ALO_DRIFT_BPS", str(DEFAULT_LIMIT_ALO_DRIFT_BPS))
            ),
            limit_alo_poll_sec=int(
                os.environ.get("BOT_LIMIT_ALO_POLL_SEC", str(DEFAULT_LIMIT_ALO_POLL_SEC))
            ),
            limit_alo_leg_side=os.environ.get("BOT_LIMIT_ALO_LEG_SIDE", DEFAULT_LIMIT_ALO_LEG_SIDE),
            # Patch J: close-side ALO
            limit_alo_close_enabled=os.environ.get(
                "BOT_LIMIT_ALO_CLOSE_ENABLED",
                "true" if DEFAULT_LIMIT_ALO_CLOSE_ENABLED else "false",
            ).lower() in ("1", "true", "yes"),
            limit_alo_close_timeout_sec=int(
                os.environ.get(
                    "BOT_LIMIT_ALO_CLOSE_TIMEOUT_SEC", str(DEFAULT_LIMIT_ALO_CLOSE_TIMEOUT_SEC)
                )
            ),
            limit_alo_close_offset_bps=int(
                os.environ.get(
                    "BOT_LIMIT_ALO_CLOSE_OFFSET_BPS", str(DEFAULT_LIMIT_ALO_CLOSE_OFFSET_BPS)
                )
            ),
            # Patch G: Adverse basis
            max_adverse_basis_bps=int(
                os.environ.get("BOT_MAX_ADVERSE_BASIS_BPS", str(DEFAULT_MAX_ADVERSE_BASIS_BPS))
            ),
            # Patch H: Low-APR sustained exit
            low_apr_threshold=Decimal(
                os.environ.get("BOT_LOW_APR_THRESHOLD", str(DEFAULT_LOW_APR_THRESHOLD))
            ),
            low_apr_window=int(
                os.environ.get("BOT_LOW_APR_WINDOW", str(DEFAULT_LOW_APR_WINDOW))
            ),
            # Patch I: Post-close settlement wait
            post_close_settle_sec=int(
                os.environ.get("BOT_POST_CLOSE_SETTLE_SEC", str(DEFAULT_POST_CLOSE_SETTLE_SEC))
            ),
            # Patch J: Trading cycle offset
            trading_minute=int(
                os.environ.get("BOT_TRADING_MINUTE", str(DEFAULT_TRADING_MINUTE))
            ),
            # Б7: headroom для initial margin (по умолчанию 0.5%).
            margin_headroom_pct=Decimal(
                os.environ.get("BOT_MARGIN_HEADROOM_PCT", "0.005")
            ),
            # SSE event stream (default on — pure latency optimisation;
            # callers still REST-confirm every decision. Set BOT_SSE_ENABLED=false
            # in .env to force pure-REST behaviour.)
            sse_enabled=os.environ.get("BOT_SSE_ENABLED", "true").lower()
                in ("1", "true", "yes"),
            sse_heartbeat_timeout_sec=float(
                os.environ.get("BOT_SSE_HEARTBEAT_TIMEOUT_SEC", "60")
            ),
            sse_reconnect_max_attempts=int(
                os.environ.get("BOT_SSE_RECONNECT_MAX_ATTEMPTS", "10")
            ),
        )


# =============================================================================
# Patch C — Market metadata cache + precision rounding helpers
# =============================================================================


@dataclass
class MarketMeta:
    base_decimals: int
    price_decimals: int
    quote_decimals: int
    open: bool
    funding_interval_h: int
    max_leverage: int
    max_sig_figs: int | None = None  # Hyperliquid enforces max 5 significant figures on prices


# Global markets cache, обновляется в начале каждого cycle().
markets_cache: dict[tuple[str, str], MarketMeta] = {}
markets_cache_updated_at: datetime | None = None


async def refresh_markets_cache(client: VooiClient, log: NDJsonLog, settings: Settings) -> None:
    """Дёргаем GET /exchange/markets для всех target_exchanges, обновляем cache.
    При ошибке оставляем старый cache — не прерываем цикл.
    """
    global markets_cache_updated_at
    try:
        r = await client.get(
            "/exchange/markets",
            params={"exchanges": list(settings.target_exchanges)},
        )
        if r.status_code != 200:
            log.emit("MARKETS_FETCH_HTTP_ERR", status=r.status_code)
            return
        items = r.json()
        new_cache: dict[tuple[str, str], MarketMeta] = {}
        for m in items:
            key = (m["exchange"], m["baseSymbol"])
            # Б2: HL всегда требует max 5 sig figs — никогда не оставляем None для HL.
            # Для не-HL — None: каждая биржа имеет свои tick rules, лишний round может потерять точность.
            exchange_name = m["exchange"]
            sig_figs_limit = 5 if exchange_name == "hyperliquid" else None
            new_cache[key] = MarketMeta(
                base_decimals=int(m.get("baseDecimals", 8)),
                price_decimals=int(m.get("priceDecimals", 6)),
                quote_decimals=int(m.get("quoteDecimals", 6)),
                open=bool(m.get("open", True)),
                funding_interval_h=int(m.get("fundingInterval", 1)),
                max_leverage=int(m.get("maxLeverage", 1)),
                max_sig_figs=sig_figs_limit,
            )
        markets_cache.clear()
        markets_cache.update(new_cache)
        markets_cache_updated_at = datetime.now(UTC)
        log.emit("MARKETS_CACHE_REFRESHED", count=len(new_cache))
    except (httpx.HTTPError, KeyError, ValueError, TypeError) as e:
        log.emit("MARKETS_FETCH_ERROR", error=str(e))


def market_meta(exchange: str, asset: str) -> MarketMeta | None:
    return markets_cache.get((exchange, asset))


def round_size(size: Decimal, base_decimals: int) -> Decimal:
    """Округление размера вниз (не превышаем целевой размер)."""
    factor = Decimal(10) ** base_decimals
    return (size * factor).quantize(Decimal("1"), rounding=ROUND_DOWN) / factor


def round_price(price: Decimal, price_decimals: int, side: str) -> Decimal:
    """Округление цены: buy — вниз (не превышаем max), sell — вверх."""
    factor = Decimal(10) ** price_decimals
    if side == "buy":
        return (price * factor).quantize(Decimal("1"), rounding=ROUND_DOWN) / factor
    return (price * factor).quantize(Decimal("1"), rounding=ROUND_UP) / factor


def round_sig_figs(price: Decimal, n: int, side: str) -> Decimal:
    """Round price to at most N significant figures (Hyperliquid enforces max 5)."""
    if price <= 0:
        return price
    import math as _math
    magnitude = int(_math.floor(_math.log10(float(price))))
    factor = Decimal(10) ** (magnitude - n + 1)
    rounding = ROUND_DOWN if side == "buy" else ROUND_UP
    return (price / factor).to_integral_value(rounding=rounding) * factor


# =============================================================================
# Patch A — SL trigger calculation helper
# =============================================================================


def calculate_sl_trigger(
    entry_price: Decimal,
    liquidation_price: Decimal,
    side: str,
    buffer_pct: Decimal,
    price_decimals: int,
    max_sig_figs: int | None = None,
) -> Decimal:
    """SL trigger price рядом с liq но с buffer'ом, чтобы не доходить до неё.

    LONG: entry > liq. SL = liq + (entry - liq) * buffer  (близко к liq снизу)
    SHORT: liq > entry. SL = liq - (liq - entry) * buffer  (близко к liq сверху)

    max_sig_figs: если задан — дополнительно округляем до N значащих цифр
                  (Hyperliquid требует max 5 sig figs; rejections → retry без SL).
    """
    if side == "buy":  # long
        if liquidation_price >= entry_price:
            sl = round_price(entry_price * Decimal("0.95"), price_decimals, "buy")
        else:
            sl = round_price(
                liquidation_price + (entry_price - liquidation_price) * buffer_pct,
                price_decimals, "buy",
            )
    else:  # short
        if liquidation_price <= entry_price:
            sl = round_price(entry_price * Decimal("1.05"), price_decimals, "sell")
        else:
            sl = round_price(
                liquidation_price - (liquidation_price - entry_price) * buffer_pct,
                price_decimals, "sell",
            )
    if max_sig_figs is not None:
        sl = round_sig_figs(sl, max_sig_figs, side)
    return sl


# =============================================================================
# Patch A3 — Symmetric TP (cross-leg same-USD level)
# =============================================================================
#
# Старая mirror-through-entry формула (Patch A2) была концептуально неверной:
# обе ноги торгуют один underlying и движутся в одну сторону, не зеркально.
# Правильная логика — TP одной ноги = SL противоположной (тот же USD уровень).
# Реализовано inline в _open_two_markets / _open_limit_then_market (а не как
# отдельная функция), потому что cross-leg TP требует SL обеих ног как input.
# См. docs/exchange-tp-symmetric.md.


def _project_tp_for_exchange(
    cross_sl: Decimal,
    target_price_decimals: int,
    direction: str,  # "sell" для long-TP (выше entry), "buy" для short-TP (ниже entry)
    max_sig_figs: int | None = None,
) -> Decimal:
    """Округлить cross-leg SL для использования как TP на противоположной бирже.

    direction="sell" для long_tp = short_sl (округлить вверх, чтобы trigger
        не «упал» под entry из-за мелких знаков).
    direction="buy" для short_tp = long_sl (округлить вниз, симметрично).
    """
    tp = round_price(cross_sl, target_price_decimals, direction)
    if max_sig_figs is not None:
        tp = round_sig_figs(tp, max_sig_figs, direction)
    return tp


# =============================================================================
# Patch D — Leverage verify helper
# =============================================================================


async def setup_leverage_with_verify(
    client: VooiClient,
    log: NDJsonLog,
    exchange: str,
    asset: str,
    target_leverage: int,
) -> bool:
    """Устанавливает leverage и верифицирует через GET /exchange/market-settings.

    Б5: при transient ошибках verify-шага — 3 ретрая × 1s, затем fallback на
    leverage из GET /exchange/positions, и при полном fail — PROCEED with warning
    (POST set вернул 201, скорее всего leverage применён; финальный sanity-check
    выполнится при попытке open и при leverage mismatch вернётся 503 от биржи).

    Returns True если leverage применён или есть достаточные основания полагать что да.
    """
    try:
        r = await client.post("/exchange/leverage", body={
            "exchange": exchange,
            "asset": asset,
            "leverage": target_leverage,
        })
        if r.status_code >= 400:
            log.emit(
                "LEVERAGE_SET_FAIL",
                exchange=exchange,
                asset=asset,
                target=target_leverage,
                status=r.status_code,
                body=r.text[:200],
            )
            return False
    except httpx.HTTPError as e:
        log.emit("LEVERAGE_SET_ERROR", exchange=exchange, asset=asset, error=str(e))
        return False

    # SET прошёл успешно. Verify через market-settings с ретраями.
    last_verify_err: str | None = None
    for attempt in range(1, 4):
        try:
            v = await client.get("/exchange/market-settings", params={
                "exchange": exchange,
                "asset": asset,
            })
            if v.status_code == 200:
                actual = v.json()
                actual_lev = float(actual.get("leverage", 0))
                if abs(actual_lev - target_leverage) < 0.5:
                    return True
                log.emit(
                    "LEVERAGE_MISMATCH",
                    exchange=exchange,
                    asset=asset,
                    target=target_leverage,
                    actual=actual_lev,
                )
                return False
            last_verify_err = f"status={v.status_code}"
            log.emit(
                "LEVERAGE_VERIFY_HTTP_ERR",
                exchange=exchange,
                asset=asset,
                status=v.status_code,
                attempt=attempt,
            )
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as e:
            last_verify_err = str(e)
            log.emit(
                "LEVERAGE_VERIFY_ERROR",
                exchange=exchange,
                asset=asset,
                error=str(e),
                attempt=attempt,
            )
        if attempt < 3:
            await asyncio.sleep(1.0)

    # Все ретраи verify провалились. Fallback: достаём leverage из /exchange/positions.
    try:
        rp = await client.get("/exchange/positions", params={"exchange": exchange})
        if rp.status_code == 200:
            for p in (rp.json() or []):
                if (p.get("baseSymbol") or p.get("asset") or "").upper() == asset.upper():
                    pos_lev = float(p.get("leverage") or 0)
                    if abs(pos_lev - target_leverage) < 0.5:
                        log.emit(
                            "LEVERAGE_VERIFIED_VIA_POSITIONS",
                            exchange=exchange,
                            asset=asset,
                            target=target_leverage,
                            actual=pos_lev,
                        )
                        return True
                    break
    except httpx.HTTPError:
        pass

    # Verify полностью fail. POST set вернул 201 — proceed-with-warning.
    log.emit(
        "LEVERAGE_VERIFY_INCONCLUSIVE",
        exchange=exchange,
        asset=asset,
        target=target_leverage,
        last_error=last_verify_err,
        hint="POST /exchange/leverage succeeded, verify unavailable — proceeding optimistically",
    )
    return True


def opportunity_passes_non_crypto_rules(
    include_hl_non_crypto: bool,
    long_base: str,
    short_base: str,
    asset: str,
    long_ex: str,
    short_ex: str,
) -> bool:
    """C9 / HL non-crypto gate for ``/funding-strategies`` rows.

    When ``include_hl_non_crypto`` is False: reject any ``xyz:`` / ``alias:`` leg or asset.
    When True: allow those symbols only if Hyperliquid is one of the exchanges (cross-venue).
    """
    if not include_hl_non_crypto:
        if is_non_crypto_prefix(long_base) or is_non_crypto_prefix(short_base):
            return False
        return not is_non_crypto_prefix(asset)
    if (
        is_non_crypto_prefix(long_base)
        or is_non_crypto_prefix(short_base)
        or is_non_crypto_prefix(asset)
    ):
        return "hyperliquid" in (long_ex, short_ex)
    return True


# =============================================================================
# NDJSON logger (stdout + state.ndjson)
# =============================================================================


class NDJsonLog:
    def __init__(self, state_file: Path, instance_uuid: str) -> None:
        self.state_file = state_file
        self.instance_uuid = instance_uuid
        self.state_file.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event: str, **data: Any) -> None:
        payload = {
            "ts": datetime.now(UTC).isoformat(),
            "instance": self.instance_uuid,
            "event": event,
            **data,
        }
        line = json.dumps(payload, ensure_ascii=False, default=str)
        sys.stdout.write(line + "\n")
        sys.stdout.flush()
        with self.state_file.open("a", encoding="utf-8") as f:
            f.write(line + "\n")


# =============================================================================
# HTTP client (минимальный, без tenacity для MVP)
# =============================================================================


class VooiClient:
    def __init__(self, base_url: str, token: str, timeout_sec: float = 30.0) -> None:
        self._base = base_url.rstrip("/")
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        self._timeout = httpx.Timeout(timeout_sec, connect=10.0)
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> VooiClient:
        self._client = httpx.AsyncClient(
            base_url=self._base,
            headers=self._headers,
            timeout=self._timeout,
            http2=True,
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @property
    def cl(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("VooiClient not entered")
        return self._client

    async def get(self, path: str, params: dict[str, Any] | None = None) -> httpx.Response:
        return await self.cl.get(path, params=params)

    async def post(self, path: str, body: dict[str, Any]) -> httpx.Response:
        return await self.cl.post(path, json=body)

    async def delete(self, path: str, body: dict[str, Any] | None = None) -> httpx.Response:
        if body is None:
            return await self.cl.delete(path)
        return await self.cl.request("DELETE", path, json=body)


# =============================================================================
# Domain
# =============================================================================


@dataclass
class Opportunity:
    asset: str
    long_exchange: str
    short_exchange: str
    long_base_symbol: str
    short_base_symbol: str
    long_quote_symbol: str
    short_quote_symbol: str
    net_apr: Decimal
    apr1h: Decimal
    apr24h: Decimal
    apr7d: Decimal | None
    gross_spread_hourly: Decimal
    long_funding_rate: Decimal
    short_funding_rate: Decimal
    volume_24h_usd: Decimal
    long_max_leverage: int
    short_max_leverage: int
    raw: dict[str, Any]

    def effective_leverage(self, target: int, cap: int = 0) -> int:
        candidates = [target, self.long_max_leverage, self.short_max_leverage]
        if cap > 0:
            candidates.append(cap)
        return max(1, min(candidates))


@dataclass
class OpenPosition:
    arb_id: str
    asset: str
    long_exchange: str
    short_exchange: str
    long_base_symbol: str
    short_base_symbol: str
    opened_at: datetime
    open_net_apr: Decimal
    leg_collat_usd: Decimal
    leg_notional_usd: Decimal
    effective_leverage: int
    long_client_order_id: str
    short_client_order_id: str
    last_seen_net_apr: Decimal | None = None
    last_seen_at: datetime | None = None
    # Сколько TRADING-циклов подряд last_seen_net_apr был отрицательным (legacy, оставлено).
    negative_apr_trading_streak: int = 0
    closed: bool = False
    # Patch A3: in-flight orphan close flag (для координации watcher ↔ main-cycle reconcile,
    # чтобы не делать double close при одновременной detection'е).
    close_in_progress: bool = False
    # Patch F: smart-exit fields
    apr_history: list[Decimal] = field(default_factory=list)  # last 24 hourly APR snapshots
    funding_breakeven_achieved: bool = False
    peak_funding_cum: Decimal = Decimal(0)
    # Runtime: последние известные значения с биржи (обновляются каждый цикл).
    long_upnl_usd: Decimal | None = None
    short_upnl_usd: Decimal | None = None
    long_funding_usd: Decimal | None = None
    short_funding_usd: Decimal | None = None
    long_position_size: Decimal | None = None
    short_position_size: Decimal | None = None
    long_mark_price: Decimal | None = None
    short_mark_price: Decimal | None = None

    @property
    def arb_upnl_usd(self) -> Decimal:
        return (self.long_upnl_usd or Decimal(0)) + (self.short_upnl_usd or Decimal(0))

    @property
    def arb_funding_usd(self) -> Decimal:
        return (self.long_funding_usd or Decimal(0)) + (self.short_funding_usd or Decimal(0))

    @property
    def arb_total_pnl_usd(self) -> Decimal:
        return self.arb_upnl_usd + self.arb_funding_usd


@dataclass
class CycleReport:
    cycle: int
    started_at: datetime
    elapsed_sec: float
    accounts: dict[str, dict[str, Decimal]] = field(default_factory=dict)
    positions: list[OpenPosition] = field(default_factory=list)
    opportunities: list[Opportunity] = field(default_factory=list)
    actions: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    blocked_assets: dict[str, str] = field(default_factory=dict)


# =============================================================================
# Scanner
# =============================================================================


async def fetch_opportunities(
    client: VooiClient,
    settings: Settings,
) -> tuple[list[Opportunity], list[str]]:
    errors: list[str] = []
    try:
        resp = await client.get("/funding-strategies", params={"limit": 100})
    except httpx.HTTPError as e:
        errors.append(f"GET /funding-strategies failed: {e}")
        return [], errors
    if resp.status_code != 200:
        errors.append(f"GET /funding-strategies HTTP {resp.status_code}: {resp.text[:200]}")
        return [], errors
    items = resp.json()
    if not isinstance(items, list):
        errors.append(f"funding-strategies: expected list, got {type(items).__name__}")
        return [], errors

    out: list[Opportunity] = []
    for raw in items:
        try:
            long_md = raw.get("longMarketData") or {}
            short_md = raw.get("shortMarketData") or {}
            long_ex = long_md.get("exchange")
            short_ex = short_md.get("exchange")
            long_base = long_md.get("baseSymbol") or ""
            short_base = short_md.get("baseSymbol") or ""

            # C5: оба leg должны быть в торгуемых биржах + в target.
            if long_ex not in ALLOWED_TRADING_EXCHANGES or short_ex not in ALLOWED_TRADING_EXCHANGES:
                continue
            if long_ex not in settings.target_exchanges or short_ex not in settings.target_exchanges:
                continue

            # C9 / HL non-crypto: см. ``opportunity_passes_non_crypto_rules``.
            asset = (raw.get("asset") or "").strip()
            if not opportunity_passes_non_crypto_rules(
                settings.include_hl_non_crypto,
                long_base,
                short_base,
                asset,
                long_ex,
                short_ex,
            ):
                continue

            long_quote = str(long_md.get("quoteSymbol") or "USDC")
            short_quote = str(short_md.get("quoteSymbol") or "USDC")
            net_apr = Decimal(str(raw.get("netApr", "0")))
            apr1h = Decimal(str(raw.get("apr1h", "0")))
            apr24h = Decimal(str(raw.get("apr24h", "0")))
            apr7d_raw = raw.get("apr7d")
            apr7d = Decimal(str(apr7d_raw)) if apr7d_raw is not None else None
            gross_hourly = Decimal(str(raw.get("grossSpreadHourly", "0")))
            volume = Decimal(str(raw.get("volume24h") or "0"))

            out.append(
                Opportunity(
                    asset=asset,
                    long_exchange=long_ex,
                    short_exchange=short_ex,
                    long_base_symbol=long_base,
                    short_base_symbol=short_base,
                    long_quote_symbol=long_quote,
                    short_quote_symbol=short_quote,
                    net_apr=net_apr,
                    apr1h=apr1h,
                    apr24h=apr24h,
                    apr7d=apr7d,
                    gross_spread_hourly=gross_hourly,
                    long_funding_rate=Decimal(str(long_md.get("fundingRate", "0"))),
                    short_funding_rate=Decimal(str(short_md.get("fundingRate", "0"))),
                    volume_24h_usd=volume,
                    long_max_leverage=int(long_md.get("maxLeverage") or 0),
                    short_max_leverage=int(short_md.get("maxLeverage") or 0),
                    raw=raw,
                ),
            )
        except (ValueError, ArithmeticError, KeyError, TypeError) as e:
            errors.append(f"parse opportunity: {e}; raw={raw!r}")
            continue
    return out, errors


def filter_opportunities(
    opps: list[Opportunity],
    settings: Settings,
    blocked_assets: set[str],
) -> list[Opportunity]:
    """Фильтрация по ТЗ §5 (`/funding-strategies` consume → filter):

    1. exchanges in target (уже сделано в fetch_opportunities)
    2. non-crypto / HL gate — C9 (см. ``fetch_opportunities`` + ``BOT_INCLUDE_HL_NON_CRYPTO``)
    3. volume24h >= MIN
    4. grossSpreadHourly > 0
    5. APR sanity: apr1h > 0 AND apr24h > 0 AND apr7d > 0 (apr7d опционально)
    6. maxLeverage пары > 1
    7. (опц.) net_apr >= MIN если задан в .env
    8. (опц.) net_apr <= MAX если задан и > 0 в .env
    9. (опц.) asset whitelist если задан
    10. asset не заблокирован уже открытой позицией
    """
    out = []
    wl = {a.upper() for a in settings.asset_whitelist}
    bl = {a.upper() for a in settings.asset_blacklist}
    for o in opps:
        if o.volume_24h_usd < settings.min_volume_24h_usd:
            continue
        if settings.per_leg_volume_min_usd > 0 and o.volume_24h_usd < settings.per_leg_volume_min_usd:
            continue
        if o.gross_spread_hourly <= 0:
            continue
        if o.apr1h <= 0 or o.apr24h <= 0:
            continue
        if o.apr7d is not None and o.apr7d <= 0:
            continue
        if o.long_max_leverage < 2 or o.short_max_leverage < 2:
            continue
        if settings.min_net_apr > 0 and o.net_apr < settings.min_net_apr:
            continue
        if settings.max_net_apr > 0 and o.net_apr > settings.max_net_apr:
            continue
        # APR upper cap: отсекаем spike-APR (вероятный basis drift, см. YZY case).
        if settings.apr_upper_cap > 0 and o.net_apr > settings.apr_upper_cap:
            continue
        # Ratio apr1h / apr24h: защита от краткосрочных спайков.
        if settings.apr_ratio_1h_to_24h_max > 0 and o.apr24h > 0:
            if o.apr1h / o.apr24h > settings.apr_ratio_1h_to_24h_max:
                continue
        # Ratio apr24h / apr7d: защита от среднесрочных спайков.
        if settings.apr_ratio_24h_to_7d_max > 0 and o.apr7d is not None and o.apr7d > 0:
            if o.apr24h / o.apr7d > settings.apr_ratio_24h_to_7d_max:
                continue
        asset_upper = o.asset.upper()
        if wl and asset_upper not in wl:
            continue
        if bl and asset_upper in bl:
            continue
        if asset_upper in blocked_assets or o.asset in blocked_assets:
            continue
        out.append(o)
    out.sort(key=lambda x: x.net_apr, reverse=True)
    return out


# =============================================================================
# Account / position state
# =============================================================================


async def fetch_accounts(
    client: VooiClient,
    exchanges: tuple[str, ...],
    log: NDJsonLog | None = None,
) -> dict[str, dict[str, Decimal]]:
    """``{margin_bucket_key …} -> balances`` — для Hyperliquid отдельно ``perps`` и ``xyz``."""
    out: dict[str, dict[str, Decimal]] = {}
    for ex in exchanges:
        try:
            resp = await client.get("/exchange/accounts", params={"exchanges": ex})
        except httpx.HTTPError as e:
            if log is not None:
                log.emit("ACCOUNTS_FETCH_ERROR", exchange=ex, error=str(e))
            continue
        # Б9: явно логируем auth/non-2xx. Особенно 401 — токен истёк,
        # все последующие fetch'и будут тихо проваливаться без этого.
        if resp.status_code != 200:
            if log is not None:
                log.emit(
                    "ACCOUNTS_FETCH_FAIL" if resp.status_code != 401 else "ACCOUNTS_AUTH_FAIL",
                    exchange=ex,
                    status=resp.status_code,
                    body=resp.text[:200],
                )
            continue
        body = resp.json()
        if not isinstance(body, list):
            continue
        for a in body:
            typ = str(a.get("type") or CRYPTO_PERPS_ACCOUNT_TYPE)
            token = str(a.get("token") or "USDC")
            # Hyperliquid VOOI account-balance reports unified collateral as type="spot"
            # (or sometimes "perps") for the crypto-perps margin pool; "xyz" remains
            # a separate bucket for non-crypto venues. Opener keys via margin_bucket_key
            # which hardcodes "perps" for crypto → normalise non-xyz to perps so the
            # bucket lookup matches.
            if ex == "hyperliquid" and typ != "xyz":
                typ = CRYPTO_PERPS_ACCOUNT_TYPE
            key = f"hyperliquid:{typ}:{token}" if ex == "hyperliquid" else ex
            out[key] = {
                "total": Decimal(str(a.get("totalBalance", "0"))),
                "available": Decimal(str(a.get("availableMargin", "0"))),
                "in_use": Decimal(str(a.get("marginInUse", "0"))),
                "withdrawable": Decimal(str(a.get("withdrawable", "0"))),
            }
    return out


async def fetch_positions(
    client: VooiClient,
    exchanges: tuple[str, ...],
    settings: Settings,
) -> dict[str, list[dict[str, Any]] | None]:
    """{exchange: positions or None if fetch failed}.

    None means the fetch failed (timeout / non-200) and positions for that
    exchange are unknown.  Callers that need a safe empty list should use
    ``result.get(ex) or []``.  The reconciler uses None to skip reconciliation
    for positions whose exchange is unreachable.
    """
    out: dict[str, list[dict[str, Any]] | None] = {ex: None for ex in exchanges}
    for ex in exchanges:
        try:
            resp = await client.get("/exchange/positions", params={"exchanges": ex})
        except httpx.HTTPError:
            continue
        if resp.status_code != 200:
            continue
        body = resp.json() or []
        positions: list[dict[str, Any]] = []
        for p in body:
            base = p.get("baseSymbol") or ""
            if not settings.include_hl_non_crypto and is_non_crypto_prefix(base):
                continue
            positions.append(p)
        out[ex] = positions
    return out


def _find_real_pos(
    positions: list[dict[str, Any]],
    base_symbol: str,
    side: str,
) -> dict[str, Any] | None:
    """Найти реальную позицию по (baseSymbol, side). side='buy' для long, 'sell' для short."""
    target = base_symbol.upper()
    for p in positions:
        if (p.get("baseSymbol") or "").upper() == target and p.get("side") == side:
            return p
    return None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (ValueError, ArithmeticError):
        return None


def _enrich_position_with_real(
    pos: OpenPosition,
    real_long: dict[str, Any] | None,
    real_short: dict[str, Any] | None,
) -> None:
    if real_long is not None:
        pos.long_upnl_usd = _decimal_or_none(real_long.get("unrealizedPnl"))
        pos.long_funding_usd = _decimal_or_none(
            real_long.get("fundingPnl") or real_long.get("fundingFee") or real_long.get("cumFunding"),
        )
        pos.long_position_size = _decimal_or_none(real_long.get("size"))
        pos.long_mark_price = _decimal_or_none(real_long.get("markPrice") or real_long.get("entryPrice"))
    if real_short is not None:
        pos.short_upnl_usd = _decimal_or_none(real_short.get("unrealizedPnl"))
        pos.short_funding_usd = _decimal_or_none(
            real_short.get("fundingPnl") or real_short.get("fundingFee") or real_short.get("cumFunding"),
        )
        pos.short_position_size = _decimal_or_none(real_short.get("size"))
        pos.short_mark_price = _decimal_or_none(real_short.get("markPrice") or real_short.get("entryPrice"))


# =============================================================================
# Snapshot persistence — позволяет рестартовать без потери открытых позиций.
# =============================================================================


def save_snapshot(state: dict[str, OpenPosition], path: Path) -> None:
    """Сохранить текущий state в JSON. Перезаписывает файл целиком."""
    data: dict[str, dict[str, Any]] = {}
    for arb_id, pos in state.items():
        if pos.closed:
            continue
        data[arb_id] = {
            "arb_id": pos.arb_id,
            "asset": pos.asset,
            "long_exchange": pos.long_exchange,
            "short_exchange": pos.short_exchange,
            "long_base_symbol": pos.long_base_symbol,
            "short_base_symbol": pos.short_base_symbol,
            "opened_at": pos.opened_at.isoformat(),
            "open_net_apr": str(pos.open_net_apr),
            "leg_collat_usd": str(pos.leg_collat_usd),
            "leg_notional_usd": str(pos.leg_notional_usd),
            "effective_leverage": pos.effective_leverage,
            "long_client_order_id": pos.long_client_order_id,
            "short_client_order_id": pos.short_client_order_id,
            "negative_apr_trading_streak": pos.negative_apr_trading_streak,
            # Patch F
            "apr_history": [str(x) for x in pos.apr_history],
            "funding_breakeven_achieved": pos.funding_breakeven_achieved,
            "peak_funding_cum": str(pos.peak_funding_cum),
        }
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def load_snapshot(path: Path) -> dict[str, OpenPosition]:
    """Подгружает open positions из snapshot JSON. Возвращает пустой dict если файла нет."""
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    state: dict[str, OpenPosition] = {}
    for arb_id, d in raw.items():
        try:
            state[arb_id] = OpenPosition(
                arb_id=d["arb_id"],
                asset=d["asset"],
                long_exchange=d["long_exchange"],
                short_exchange=d["short_exchange"],
                long_base_symbol=d["long_base_symbol"],
                short_base_symbol=d["short_base_symbol"],
                opened_at=datetime.fromisoformat(d["opened_at"]),
                open_net_apr=Decimal(d["open_net_apr"]),
                leg_collat_usd=Decimal(d["leg_collat_usd"]),
                leg_notional_usd=Decimal(d["leg_notional_usd"]),
                effective_leverage=int(d["effective_leverage"]),
                long_client_order_id=d["long_client_order_id"],
                short_client_order_id=d["short_client_order_id"],
                negative_apr_trading_streak=int(d.get("negative_apr_trading_streak", 0)),
                # Patch F
                apr_history=[Decimal(x) for x in d.get("apr_history", [])],
                funding_breakeven_achieved=bool(d.get("funding_breakeven_achieved", False)),
                peak_funding_cum=Decimal(d.get("peak_funding_cum", "0")),
            )
        except (KeyError, ValueError, ArithmeticError):
            continue
    return state


# Tuning 2026-05-13: per-asset losing-close cooldown.
# Файл хранит ``{asset: [iso_timestamp, ...]}``. Записываются ТОЛЬКО убыточные
# закрытия. Окно ретенции = pair_cooldown_hours (старые записи отбрасываются
# при load и при prune).


def save_cooldown_state(losses: dict[str, list[str]], path: Path) -> None:
    """Сохранить per-asset losses-log в JSON. Пустой dict — пустой файл."""
    path.write_text(json.dumps(losses, indent=2, sort_keys=True), encoding="utf-8")


def load_cooldown_state(path: Path) -> dict[str, list[str]]:
    """Подгружает per-asset losses-log. Пустой dict если файла нет или повреждён."""
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, list[str]] = {}
    for asset, stamps in raw.items():
        if not isinstance(stamps, list):
            continue
        clean = [s for s in stamps if isinstance(s, str)]
        if clean:
            out[str(asset).upper()] = clean
    return out


def prune_cooldown(
    losses: dict[str, list[str]],
    cooldown_hours: int,
    now: datetime | None = None,
) -> dict[str, list[str]]:
    """Удаляет записи старше cooldown_hours. Мутирует и возвращает тот же dict."""
    if cooldown_hours <= 0:
        losses.clear()
        return losses
    now_dt = now or datetime.now(UTC)
    cutoff = now_dt - timedelta(hours=cooldown_hours)
    for asset in list(losses.keys()):
        kept: list[str] = []
        for s in losses[asset]:
            try:
                ts = datetime.fromisoformat(s)
            except ValueError:
                continue
            if ts >= cutoff:
                kept.append(s)
        if kept:
            losses[asset] = kept
        else:
            del losses[asset]
    return losses


def cooldown_blocked_assets(
    losses: dict[str, list[str]],
    after_losses: int,
    cooldown_hours: int,
    now: datetime | None = None,
) -> dict[str, str]:
    """Возвращает ``{asset_upper: human-readable reason}`` для cooldown-блокировок."""
    if after_losses <= 0 or cooldown_hours <= 0:
        return {}
    now_dt = now or datetime.now(UTC)
    cutoff = now_dt - timedelta(hours=cooldown_hours)
    out: dict[str, str] = {}
    for asset, stamps in losses.items():
        recent: list[datetime] = []
        for s in stamps:
            try:
                ts = datetime.fromisoformat(s)
            except ValueError:
                continue
            if ts >= cutoff:
                recent.append(ts)
        if len(recent) >= after_losses:
            oldest = min(recent).isoformat()
            out[asset.upper()] = (
                f"cooldown losses={len(recent)}/{after_losses} "
                f"window={cooldown_hours}h since={oldest}"
            )
    return out


# =============================================================================
# Opener
# =============================================================================


def make_arb_id(instance_uuid: str) -> str:
    return f"{instance_uuid}-{uuid.uuid4().hex[:8]}"


def make_coid(instance_uuid: str, arb_id: str, leg: str, exchange: str) -> str:
    """Per-exchange clientOrderId formatting.

    - HL: `0x` + 32 hex chars (= 34 total). Deterministic from (instance, arb, leg)
      so retry of same intent yields same coid (HL дедупликация).
    - Lighter/Aster/etc: human-readable `vooi-funding-arb-...`.
    """
    if exchange == "hyperliquid":
        digest = hashlib.sha256(f"{instance_uuid}-{arb_id}-{leg}".encode()).hexdigest()[:32]
        return "0x" + digest
    return f"vooi-funding-arb-{instance_uuid}-{arb_id}-{leg}"


async def setup_leverage_only(
    client: VooiClient,
    log: NDJsonLog,
    exchange: str,
    asset: str,
    leverage: int,
) -> None:
    """Устанавливает leverage (margin всегда cross — set_margin_mode удалён)."""
    await setup_leverage_with_verify(client, log, exchange, asset, leverage)


# Backward-compat alias (удалить после рефакторинга всех вызовов)
setup_margin_and_leverage = setup_leverage_only


async def quote_leg(
    client: VooiClient,
    log: NDJsonLog,
    exchange: str,
    asset: str,
    side: str,
    quote_size_usd: Decimal,
    leverage: int,
    margin_mode: str,
) -> dict[str, Any] | None:
    """GET /exchange/quotes для получения baseSize и averageExecutionPrice.
    Возвращает quote dict или None при ошибке.
    """
    try:
        r = await client.get(
            "/exchange/quotes",
            params={
                "exchanges": exchange,
                "asset": asset,
                "side": side,
                "quoteSize": str(quote_size_usd),
                "leverage": leverage,
                "marginMode": margin_mode,
            },
        )
    except httpx.HTTPError as e:
        log.emit("QUOTE_ERROR", exchange=exchange, asset=asset, error=str(e))
        return None
    if r.status_code != 200:
        log.emit("QUOTE_HTTP_ERR", exchange=exchange, asset=asset, status=r.status_code, body=r.text[:200])
        return None
    body = r.json()
    if not body or not isinstance(body, list):
        return None
    if not isinstance(body[0], dict):
        return None
    quote = body[0].get("quote")
    return quote if isinstance(quote, dict) else None


async def open_strategy(
    client: VooiClient,
    log: NDJsonLog,
    settings: Settings,
    opp: Opportunity,
) -> OpenPosition | None:
    """Open оба leg через MARKET (или LIMIT alo + MARKET если Patch B включён).
    Возвращает OpenPosition либо None при провале (best-effort cleanup при partial fail).

    ⚠️ MVP: между двумя POSTs есть окно. Если crash — half-legged.
    """
    arb_id = make_arb_id(settings.instance_uuid)
    coid_long = make_coid(settings.instance_uuid, arb_id, "long", opp.long_exchange)
    coid_short = make_coid(settings.instance_uuid, arb_id, "short", opp.short_exchange)
    effective_lev = opp.effective_leverage(settings.leverage_target, settings.leverage_cap)
    leg_notional_usd = settings.leg_collat_usd * effective_lev

    log.emit(
        "OPEN_INTENT",
        arb_id=arb_id,
        asset=opp.asset,
        long_exchange=opp.long_exchange,
        short_exchange=opp.short_exchange,
        net_apr=str(opp.net_apr),
        collat_usd=str(settings.leg_collat_usd),
        notional_usd=str(leg_notional_usd),
        effective_leverage=effective_lev,
        target_leverage=settings.leverage_target,
        market_max_long=opp.long_max_leverage,
        market_max_short=opp.short_max_leverage,
    )

    if settings.dry_run:
        log.emit("OPEN_DRY_RUN_SKIP", arb_id=arb_id)
        return None

    # Patch D: verify leverage через market-settings.
    ok_long = await setup_leverage_with_verify(
        client, log, opp.long_exchange, opp.long_base_symbol, effective_lev,
    )
    ok_short = await setup_leverage_with_verify(
        client, log, opp.short_exchange, opp.short_base_symbol, effective_lev,
    )
    if not (ok_long and ok_short):
        log.emit(
            "OPEN_ABORT_LEVERAGE_NOT_SET",
            arb_id=arb_id,
            ok_long=ok_long,
            ok_short=ok_short,
        )
        return None

    # Получаем baseSize через quotes (HL не вычисляет из quoteSize для market).
    # Margin всегда cross.
    long_quote = await quote_leg(
        client, log, opp.long_exchange, opp.long_base_symbol,
        "buy", leg_notional_usd, effective_lev, "cross",
    )
    short_quote = await quote_leg(
        client, log, opp.short_exchange, opp.short_base_symbol,
        "sell", leg_notional_usd, effective_lev, "cross",
    )
    if long_quote is None or short_quote is None:
        log.emit("OPEN_ABORT_NO_QUOTE", arb_id=arb_id)
        return None

    long_size_raw = long_quote.get("baseSize")
    short_size_raw = short_quote.get("baseSize")
    if (
        not long_size_raw or not short_size_raw
        or Decimal(str(long_size_raw)) <= 0 or Decimal(str(short_size_raw)) <= 0
    ):
        log.emit(
            "OPEN_ABORT_BAD_SIZE",
            arb_id=arb_id,
            long_size=long_size_raw,
            short_size=short_size_raw,
        )
        return None

    # Slippage cap: защита от тонкого order book'а.
    long_slip = _decimal_or_none(long_quote.get("slippageBps"))
    short_slip = _decimal_or_none(short_quote.get("slippageBps"))
    slip_cap = Decimal(str(settings.max_slippage_bps))
    if (long_slip is not None and abs(long_slip) > slip_cap) or (
        short_slip is not None and abs(short_slip) > slip_cap
    ):
        log.emit(
            "OPEN_ABORT_SLIPPAGE_CAP",
            arb_id=arb_id,
            asset=opp.asset,
            long_slipBps=str(long_slip) if long_slip is not None else None,
            short_slipBps=str(short_slip) if short_slip is not None else None,
            cap_bps=settings.max_slippage_bps,
        )
        return None

    # Patch G: Adverse basis check — если long_price > short_price (мы покупаем дороже, чем продаём),
    # это гарантированный drag. Отказываемся от opportunity.
    long_price = _decimal_or_none(long_quote.get("averageExecutionPrice"))
    short_price = _decimal_or_none(short_quote.get("averageExecutionPrice"))
    if long_price and short_price and long_price > 0:
        adverse_bps = (long_price - short_price) / long_price * Decimal(10000)
        if adverse_bps > Decimal(settings.max_adverse_basis_bps):
            matched_size_raw = min(Decimal(str(long_size_raw)), Decimal(str(short_size_raw)))
            log.emit(
                "OPEN_ABORT_ADVERSE_BASIS",
                arb_id=arb_id,
                asset=opp.asset,
                long_exchange=opp.long_exchange,
                short_exchange=opp.short_exchange,
                long_price=str(long_price),
                short_price=str(short_price),
                adverse_bps=str(adverse_bps),
                cap_bps=settings.max_adverse_basis_bps,
                expected_drag_usd=str(matched_size_raw * (long_price - short_price)),
            )
            return None
        log.emit("OPEN_BASIS_OK", arb_id=arb_id, adverse_bps=str(adverse_bps))

    # Patch C: Precision rounding — используем market meta для округления size.
    meta_long = market_meta(opp.long_exchange, opp.long_base_symbol)
    meta_short = market_meta(opp.short_exchange, opp.short_base_symbol)
    if not meta_long or not meta_short:
        log.emit("OPEN_ABORT_NO_MARKET_META", arb_id=arb_id,
                 long_exchange=opp.long_exchange, short_exchange=opp.short_exchange)
        return None

    raw_matched = min(Decimal(str(long_size_raw)), Decimal(str(short_size_raw)))
    # Б7: режем notional на margin_headroom_pct (default 0.5%) ДО precision-rounding,
    # чтобы initial margin requirement биржи помещался в leg_collat_usd с запасом
    # на fee/slippage. Защита от HL "Insufficient margin to place order".
    headroom = settings.margin_headroom_pct
    if headroom > 0:
        raw_matched = raw_matched * (Decimal(1) - headroom)
    matched_size = min(
        round_size(raw_matched, meta_long.base_decimals),
        round_size(raw_matched, meta_short.base_decimals),
    )
    if matched_size <= 0:
        log.emit("OPEN_ABORT_SIZE_ROUND_ZERO", arb_id=arb_id, raw_size=str(raw_matched))
        return None

    log.emit(
        "OPEN_QUOTES_OK",
        arb_id=arb_id,
        long_base_symbol=opp.long_base_symbol,
        short_base_symbol=opp.short_base_symbol,
        coid_long=coid_long,
        coid_short=coid_short,
        matched_size=str(matched_size),
        long_baseSize_quoted=str(long_size_raw),
        long_avgPrice=long_quote.get("averageExecutionPrice"),
        long_feeBps=long_quote.get("feesBps"),
        long_slipBps=long_quote.get("slippageBps"),
        long_liqPrice=long_quote.get("liquidationPrice"),
        short_baseSize_quoted=str(short_size_raw),
        short_avgPrice=short_quote.get("averageExecutionPrice"),
        short_feeBps=short_quote.get("feesBps"),
        short_slipBps=short_quote.get("slippageBps"),
        short_liqPrice=short_quote.get("liquidationPrice"),
    )

    # Patch B: LIMIT alo + MARKET ioc вместо двух MARKET'ов.
    if settings.limit_alo_enabled:
        return await _open_limit_then_market(
            client, log, settings, opp,
            arb_id, coid_long, coid_short,
            matched_size, effective_lev, leg_notional_usd,
            long_quote, short_quote, meta_long, meta_short,
        )

    # Fallback: два MARKET'а (используется если limit_alo_enabled=False).
    return await _open_two_markets(
        client, log, settings, opp,
        arb_id, coid_long, coid_short,
        matched_size, effective_lev, leg_notional_usd,
        long_quote, short_quote, meta_long, meta_short,
    )


async def _open_two_markets(
    client: VooiClient,
    log: NDJsonLog,
    settings: Settings,
    opp: Opportunity,
    arb_id: str,
    coid_long: str,
    coid_short: str,
    matched_size: Decimal,
    effective_lev: int,
    leg_notional_usd: Decimal,
    long_quote: dict[str, Any],
    short_quote: dict[str, Any],
    meta_long: MarketMeta,
    meta_short: MarketMeta,
) -> OpenPosition | None:
    """Open через два MARKET'а (Patch A: SL bracket + Patch E: PnL snapshot)."""

    # Patch A: расчёт SL triggers
    long_liq = _decimal_or_none(long_quote.get("liquidationPrice"))
    short_liq = _decimal_or_none(short_quote.get("liquidationPrice"))
    long_entry = _decimal_or_none(long_quote.get("averageExecutionPrice"))
    short_entry = _decimal_or_none(short_quote.get("averageExecutionPrice"))

    # Б2: для HL max_sig_figs обязателен (биржа enforce'ит max 5 sig figs).
    # Если cache почему-то не содержит max_sig_figs для HL — аборт, лучше пропустить
    # opportunity чем послать кривой ордер и получить 503.
    for leg_name, meta, exchange in (
        ("long", meta_long, opp.long_exchange),
        ("short", meta_short, opp.short_exchange),
    ):
        if exchange == "hyperliquid" and meta.max_sig_figs is None:
            log.emit(
                "OPEN_ABORT_NO_SIG_FIGS_META",
                arb_id=arb_id,
                leg=leg_name,
                exchange=exchange,
                hint="HL market in cache without max_sig_figs — refusing to POST",
            )
            return None

    long_sl_trigger: Decimal | None = None
    short_sl_trigger: Decimal | None = None
    long_tp_trigger: Decimal | None = None
    short_tp_trigger: Decimal | None = None
    if settings.exchange_sl_enabled:
        if long_liq and long_entry:
            long_sl_trigger = calculate_sl_trigger(
                long_entry, long_liq, "buy",
                settings.exchange_sl_buffer_pct, meta_long.price_decimals,
                max_sig_figs=meta_long.max_sig_figs,
            )
            # Defensive: ещё раз прогоним через sig_figs если HL.
            if opp.long_exchange == "hyperliquid":
                long_sl_trigger = round_sig_figs(long_sl_trigger, 5, "buy")
        else:
            log.emit("OPEN_NO_LIQ_PRICE_WARN", arb_id=arb_id, leg="long")
        if short_liq and short_entry:
            short_sl_trigger = calculate_sl_trigger(
                short_entry, short_liq, "sell",
                settings.exchange_sl_buffer_pct, meta_short.price_decimals,
                max_sig_figs=meta_short.max_sig_figs,
            )
            if opp.short_exchange == "hyperliquid":
                short_sl_trigger = round_sig_figs(short_sl_trigger, 5, "sell")
        else:
            log.emit("OPEN_NO_LIQ_PRICE_WARN", arb_id=arb_id, leg="short")

        # Patch A3: cross-leg TP. TP одной ноги = SL противоположной (тот же USD-уровень).
        # Ставим оба TP только если оба SL посчитаны и порядок цен консистентен.
        if (
            long_sl_trigger is not None and short_sl_trigger is not None
            and long_entry is not None and short_entry is not None
        ):
            long_tp_candidate = _project_tp_for_exchange(
                short_sl_trigger, meta_long.price_decimals, "sell",
                max_sig_figs=meta_long.max_sig_figs,
            )
            short_tp_candidate = _project_tp_for_exchange(
                long_sl_trigger, meta_short.price_decimals, "buy",
                max_sig_figs=meta_short.max_sig_figs,
            )
            # Sanity: long_tp > long_entry AND short_tp < short_entry.
            # Иначе entry-spread inverted — лучше no-TP, чем неправильный TP.
            if long_tp_candidate > long_entry and short_tp_candidate < short_entry:
                long_tp_trigger = long_tp_candidate
                short_tp_trigger = short_tp_candidate
            else:
                log.emit(
                    "OPEN_TP_SKIP_INVERTED",
                    arb_id=arb_id,
                    long_entry=str(long_entry),
                    short_entry=str(short_entry),
                    long_sl=str(long_sl_trigger),
                    short_sl=str(short_sl_trigger),
                    long_tp_candidate=str(long_tp_candidate),
                    short_tp_candidate=str(short_tp_candidate),
                )

    # Patch E: snapshot балансов ДО open
    accounts_before = await _snapshot_accounts(client, settings)

    long_body: dict[str, Any] = {
        "exchange": opp.long_exchange,
        "asset": opp.long_base_symbol,
        "side": "buy",
        "size": str(matched_size),
        "reduceOnly": False,
        "clientOrderId": coid_long,
    }
    short_body: dict[str, Any] = {
        "exchange": opp.short_exchange,
        "asset": opp.short_base_symbol,
        "side": "sell",
        "size": str(matched_size),
        "reduceOnly": False,
        "clientOrderId": coid_short,
    }
    if long_sl_trigger is not None:
        long_body["stopLoss"] = {"triggerPrice": str(long_sl_trigger)}
    if long_tp_trigger is not None:
        long_body["takeProfit"] = {"triggerPrice": str(long_tp_trigger)}
    if short_sl_trigger is not None:
        short_body["stopLoss"] = {"triggerPrice": str(short_sl_trigger)}
    if short_tp_trigger is not None:
        short_body["takeProfit"] = {"triggerPrice": str(short_tp_trigger)}

    # POST long leg
    try:
        r1 = await client.post("/exchange/orders", body=long_body)
    except httpx.HTTPError as e:
        log.emit("OPEN_LONG_ERROR", arb_id=arb_id, error=str(e))
        return None
    log.emit(
        "OPEN_LONG_RESP",
        arb_id=arb_id,
        status=r1.status_code,
        body=r1.json() if r1.headers.get("content-type", "").startswith("application/json") else r1.text[:200],
    )
    if r1.status_code >= 400:
        return None

    # POST short leg
    try:
        r2 = await client.post("/exchange/orders", body=short_body)
    except httpx.HTTPError as e:
        log.emit("OPEN_SHORT_ERROR", arb_id=arb_id, error=str(e), warning="long leg may be open — manual cleanup")
        return None
    log.emit(
        "OPEN_SHORT_RESP",
        arb_id=arb_id,
        status=r2.status_code,
        body=r2.json() if r2.headers.get("content-type", "").startswith("application/json") else r2.text[:200],
    )
    if r2.status_code >= 400:
        # If failure looks like "price out of range" (bracket trigger rejected), retry without stopLoss+takeProfit.
        if r2.status_code in (400, 503) and ("stopLoss" in short_body or "takeProfit" in short_body):
            short_body_no_bracket = {
                k: v for k, v in short_body.items() if k not in ("stopLoss", "takeProfit")
            }
            log.emit("OPEN_SHORT_RETRY_NO_BRACKET", arb_id=arb_id, original_status=r2.status_code)
            try:
                r2 = await client.post("/exchange/orders", body=short_body_no_bracket)
                log.emit(
                    "OPEN_SHORT_RESP",
                    arb_id=arb_id,
                    status=r2.status_code,
                    body=r2.json() if r2.headers.get("content-type", "").startswith("application/json") else r2.text[:200],
                )
                if r2.status_code < 400:
                    short_sl_trigger = None  # bracket not submitted; OPEN_OK must reflect this
                    short_tp_trigger = None
            except httpx.HTTPError as e:
                log.emit("OPEN_SHORT_ERROR", arb_id=arb_id, error=str(e))
                r2 = type("_R", (), {"status_code": 599})()  # force rollback

        if r2.status_code >= 400:
            log.emit("OPEN_PARTIAL_FAIL_ROLLBACK", arb_id=arb_id)
            rollback_body: dict[str, Any] = {
                "exchange": opp.long_exchange,
                "asset": opp.long_base_symbol,
                "side": "sell",
                "size": str(matched_size),
                "reduceOnly": True,
                "clientOrderId": make_coid(settings.instance_uuid, arb_id, "rollback", opp.long_exchange),
            }
            for rb_attempt in range(1, 4):
                try:
                    rb = await client.post("/exchange/orders", body=rollback_body)
                    log.emit("OPEN_ROLLBACK_RESP", arb_id=arb_id, attempt=rb_attempt,
                             status=rb.status_code, body=rb.text[:200])
                    if rb.status_code < 400:
                        break
                except httpx.HTTPError as e:
                    log.emit("OPEN_ROLLBACK_ERROR", arb_id=arb_id, attempt=rb_attempt, error=str(e))
                if rb_attempt < 3:
                    await asyncio.sleep(2 ** rb_attempt)
            else:
                log.emit("OPEN_ROLLBACK_GAVE_UP", arb_id=arb_id, warning="MANUAL CLEANUP REQUIRED")
            return None

    # Patch E: snapshot балансов ПОСЛЕ open (через 2s)
    await asyncio.sleep(2.0)
    accounts_after = await _snapshot_accounts(client, settings)
    _log_open_friction(log, arb_id, opp, accounts_before, accounts_after)

    pos = OpenPosition(
        arb_id=arb_id,
        asset=opp.asset,
        long_exchange=opp.long_exchange,
        short_exchange=opp.short_exchange,
        long_base_symbol=opp.long_base_symbol,
        short_base_symbol=opp.short_base_symbol,
        opened_at=datetime.now(UTC),
        open_net_apr=opp.net_apr,
        leg_collat_usd=settings.leg_collat_usd,
        leg_notional_usd=leg_notional_usd,
        effective_leverage=effective_lev,
        long_client_order_id=coid_long,
        short_client_order_id=coid_short,
        last_seen_net_apr=opp.net_apr,
        last_seen_at=datetime.now(UTC),
    )
    log.emit(
        "OPEN_OK",
        arb_id=arb_id,
        asset=opp.asset,
        long_sl_trigger=str(long_sl_trigger) if long_sl_trigger else None,
        short_sl_trigger=str(short_sl_trigger) if short_sl_trigger else None,
        long_tp_trigger=str(long_tp_trigger) if long_tp_trigger else None,
        short_tp_trigger=str(short_tp_trigger) if short_tp_trigger else None,
        long_liq_price=str(long_liq) if long_liq else None,
        short_liq_price=str(short_liq) if short_liq else None,
    )
    return pos


async def _open_limit_then_market(
    client: VooiClient,
    log: NDJsonLog,
    settings: Settings,
    opp: Opportunity,
    arb_id: str,
    coid_long: str,
    coid_short: str,
    matched_size: Decimal,
    effective_lev: int,
    leg_notional_usd: Decimal,
    long_quote: dict[str, Any],
    short_quote: dict[str, Any],
    meta_long: MarketMeta,
    meta_short: MarketMeta,
) -> OpenPosition | None:
    """Patch B: leg-1 → LIMIT alo (post-only), polling, leg-2 → MARKET ioc.
    SHORT-нога всегда идёт LIMIT'ом (default auto = protect adverse basis on short side).
    """
    # Определяем limit/market ноги
    leg_side = settings.limit_alo_leg_side
    if leg_side == "auto" or leg_side == "short":
        limit_is_short = True
    else:
        limit_is_short = False

    if limit_is_short:
        limit_exchange = opp.short_exchange
        limit_base = opp.short_base_symbol
        limit_side = "sell"
        limit_coid = coid_short
        limit_meta = meta_short
        limit_liq = _decimal_or_none(short_quote.get("liquidationPrice"))
        limit_entry = _decimal_or_none(short_quote.get("averageExecutionPrice"))
        market_exchange = opp.long_exchange
        market_base = opp.long_base_symbol
        market_side = "buy"
        market_coid = coid_long
        market_meta_obj = meta_long
        market_liq = _decimal_or_none(long_quote.get("liquidationPrice"))
        market_entry = _decimal_or_none(long_quote.get("averageExecutionPrice"))
    else:
        limit_exchange = opp.long_exchange
        limit_base = opp.long_base_symbol
        limit_side = "buy"
        limit_coid = coid_long
        limit_meta = meta_long
        limit_liq = _decimal_or_none(long_quote.get("liquidationPrice"))
        limit_entry = _decimal_or_none(long_quote.get("averageExecutionPrice"))
        market_exchange = opp.short_exchange
        market_base = opp.short_base_symbol
        market_side = "sell"
        market_coid = coid_short
        market_meta_obj = meta_short
        market_liq = _decimal_or_none(short_quote.get("liquidationPrice"))
        market_entry = _decimal_or_none(short_quote.get("averageExecutionPrice"))

    # BUG-2 fix: без entry price нельзя рассчитать limit price — abort, не POST мусорный ордер.
    if limit_entry is None:
        log.emit("OPEN_ABORT_NO_LIMIT_ENTRY_PRICE", arb_id=arb_id,
                 limit_exchange=limit_exchange, limit_base=limit_base)
        return None
    limit_mid = limit_entry

    # Б2: для HL max_sig_figs обязателен. Защита от cache-miss / race на refresh.
    if limit_exchange == "hyperliquid" and limit_meta.max_sig_figs is None:
        log.emit(
            "OPEN_ABORT_NO_SIG_FIGS_META",
            arb_id=arb_id,
            leg="limit",
            exchange=limit_exchange,
            hint="HL market in cache without max_sig_figs — refusing to POST",
        )
        return None
    if market_exchange == "hyperliquid" and market_meta_obj.max_sig_figs is None:
        log.emit(
            "OPEN_ABORT_NO_SIG_FIGS_META",
            arb_id=arb_id,
            leg="market",
            exchange=market_exchange,
            hint="HL market in cache without max_sig_figs — refusing to POST",
        )
        return None

    # Расчёт limit price: для sell (short) — выше mid, для buy (long) — ниже mid.
    offset = Decimal(settings.limit_alo_offset_bps) / Decimal(10000)
    if limit_side == "sell":
        limit_price = round_price(limit_mid * (1 + offset), limit_meta.price_decimals, "sell")
    else:
        limit_price = round_price(limit_mid * (1 - offset), limit_meta.price_decimals, "buy")
    # Б1: HL enforces max 5 significant figures независимо от priceDecimals
    # (e.g. ZEC priceDecimals=4 разрешает "1261.78", но 6 sig figs → reject).
    if limit_meta.max_sig_figs is not None:
        limit_price = round_sig_figs(limit_price, limit_meta.max_sig_figs, limit_side)

    # SL/TP trigger для обеих ног (Patch A + Patch A3 cross-leg).
    # Считаем оба SL **сейчас** (до POST limit), чтобы вычислить cross-leg TP:
    # limit_tp = market_sl (тот же USD-уровень), market_tp = limit_sl.
    # Если биржа не вернула liqPrice (кросс-маржа) — считаем приближённо:
    # LONG liq ≈ entry × (1 − 0.9/leverage),  SHORT liq ≈ entry × (1 + 0.9/leverage)

    def _eff_liq_or_estimate(
        leg_name: str,
        liq: Decimal | None,
        entry: Decimal | None,
        side: str,
    ) -> Decimal | None:
        if liq:
            return liq
        if entry is None or not effective_lev:
            return None
        factor = Decimal("0.9") / Decimal(str(effective_lev))
        est = entry * (1 - factor) if side == "buy" else entry * (1 + factor)
        log.emit("OPEN_LIQ_PRICE_ESTIMATED", arb_id=arb_id, leg=leg_name,
                 estimated_liq=str(round(est, 8)))
        return est

    limit_sl_trigger: Decimal | None = None
    limit_tp_trigger: Decimal | None = None
    market_sl_trigger_pre: Decimal | None = None  # pre-computed for cross-leg TP
    market_tp_trigger_pre: Decimal | None = None
    if settings.exchange_sl_enabled and limit_entry and market_entry:
        eff_limit_liq = _eff_liq_or_estimate("limit", limit_liq, limit_entry, limit_side)
        eff_market_liq = _eff_liq_or_estimate("market_pre", market_liq, market_entry, market_side)
        if eff_limit_liq:
            limit_sl_trigger = calculate_sl_trigger(
                limit_entry, eff_limit_liq, limit_side,
                settings.exchange_sl_buffer_pct, limit_meta.price_decimals,
                max_sig_figs=limit_meta.max_sig_figs,
            )
            if limit_exchange == "hyperliquid":
                limit_sl_trigger = round_sig_figs(limit_sl_trigger, 5, limit_side)
        if eff_market_liq:
            market_sl_trigger_pre = calculate_sl_trigger(
                market_entry, eff_market_liq, market_side,
                settings.exchange_sl_buffer_pct, market_meta_obj.price_decimals,
                max_sig_figs=market_meta_obj.max_sig_figs,
            )
            if market_exchange == "hyperliquid":
                market_sl_trigger_pre = round_sig_figs(market_sl_trigger_pre, 5, market_side)

        # Cross-leg TP: для KAЖДОЙ ноги TP = SL противоположной ноги (тот же USD-уровень).
        # Sanity: для long-стороны (TP выше entry) — проверяем tp > entry; для short — tp < entry.
        if limit_sl_trigger is not None and market_sl_trigger_pre is not None:
            # Какая нога long, какая short? Длинная сторона = "buy".
            long_entry_l = limit_entry if limit_side == "buy" else market_entry
            short_entry_l = market_entry if limit_side == "buy" else limit_entry
            long_sl_l = limit_sl_trigger if limit_side == "buy" else market_sl_trigger_pre
            short_sl_l = market_sl_trigger_pre if limit_side == "buy" else limit_sl_trigger
            # TP для long ноги = short_sl (округление в "sell"-direction вверх).
            # TP для short ноги = long_sl (округление в "buy"-direction вниз).
            long_meta = limit_meta if limit_side == "buy" else market_meta_obj
            short_meta = market_meta_obj if limit_side == "buy" else limit_meta
            long_tp_candidate = _project_tp_for_exchange(
                short_sl_l, long_meta.price_decimals, "sell",
                max_sig_figs=long_meta.max_sig_figs,
            )
            short_tp_candidate = _project_tp_for_exchange(
                long_sl_l, short_meta.price_decimals, "buy",
                max_sig_figs=short_meta.max_sig_figs,
            )
            if long_tp_candidate > long_entry_l and short_tp_candidate < short_entry_l:
                if limit_side == "buy":
                    limit_tp_trigger = long_tp_candidate
                    market_tp_trigger_pre = short_tp_candidate
                else:
                    limit_tp_trigger = short_tp_candidate
                    market_tp_trigger_pre = long_tp_candidate
            else:
                log.emit(
                    "OPEN_TP_SKIP_INVERTED",
                    arb_id=arb_id,
                    long_entry=str(long_entry_l),
                    short_entry=str(short_entry_l),
                    long_sl=str(long_sl_l),
                    short_sl=str(short_sl_l),
                    long_tp_candidate=str(long_tp_candidate),
                    short_tp_candidate=str(short_tp_candidate),
                )

    limit_body: dict[str, Any] = {
        "exchange": limit_exchange,
        "asset": limit_base,
        "side": limit_side,
        "size": str(matched_size),
        "price": str(limit_price),
        "timeInForce": "alo",
        "reduceOnly": False,
        "clientOrderId": limit_coid,
    }
    if limit_sl_trigger is not None:
        limit_body["stopLoss"] = {"triggerPrice": str(limit_sl_trigger)}
    if limit_tp_trigger is not None:
        limit_body["takeProfit"] = {"triggerPrice": str(limit_tp_trigger)}

    log.emit(
        "OPEN_LIMIT_INTENT",
        arb_id=arb_id,
        limit_exchange=limit_exchange,
        limit_base=limit_base,
        limit_side=limit_side,
        limit_price=str(limit_price),
        limit_size=str(matched_size),
        limit_sl=str(limit_sl_trigger) if limit_sl_trigger else None,
        limit_tp=str(limit_tp_trigger) if limit_tp_trigger else None,
    )

    # POST limit order
    try:
        r_limit = await client.post("/exchange/orders", body=limit_body)
    except httpx.HTTPError as e:
        log.emit("OPEN_LIMIT_POST_ERROR", arb_id=arb_id, error=str(e))
        return None

    if r_limit.status_code >= 400:
        log.emit("OPEN_LIMIT_REJECTED", arb_id=arb_id, status=r_limit.status_code, body=r_limit.text[:200])
        log.emit("OPEN_LIMIT_FALLBACK_TO_MARKET", arb_id=arb_id)
        return await _open_two_markets(
            client, log, settings, opp,
            arb_id, coid_long, coid_short,
            matched_size, effective_lev, leg_notional_usd,
            long_quote, short_quote, meta_long, meta_short,
        )

    # Сохраняем orderId — нужен для cancel (clientOrderId игнорируется Lighter при SL, BUG-3).
    # POST /exchange/orders возвращает только {"status":"ok"}, поэтому берём orderId из
    # open-orders через 1s после поста, когда ордер уже виден в книге.
    limit_order_id: str | None = None
    try:
        resp_body = r_limit.json()
        limit_order_id = str(resp_body.get("orderId") or resp_body.get("id") or "") or None
    except Exception:
        pass

    if limit_order_id is None:
        await asyncio.sleep(1)
        try:
            r_oo = await client.get("/exchange/open-orders", params={"exchange": limit_exchange})
            if r_oo.status_code == 200:
                for o in (r_oo.json() or []):
                    if (
                        (o.get("baseSymbol") or "").upper() == limit_base.upper()
                        and o.get("side") == limit_side
                        and abs(Decimal(str(o.get("size", "0"))) - matched_size) < Decimal("0.01")
                    ):
                        limit_order_id = str(o.get("orderId") or "")
                        break
        except Exception:
            pass

    log.emit("OPEN_LIMIT_POSTED", arb_id=arb_id, status=r_limit.status_code, order_id=limit_order_id)

    # Polling: ждём fill или timeout/drift
    initial_mid = limit_mid
    deadline = time.monotonic() + settings.limit_alo_timeout_sec
    filled = False

    # SSE wake: register one order-waiter for this leg (Phase 2).
    # REST below remains the source of truth; SSE only races the sleep.
    # We track the last live waiter so we can unregister on early exit.
    _stream = _sse_mod.get_active_stream()
    _sse_wake: asyncio.Event | None = None

    async def _arm_sse_wake() -> asyncio.Event | None:
        if _stream is not None and _stream.is_healthy():
            return await _stream.register_order_waiter(
                exchange=limit_exchange, asset=limit_base, side=limit_side,
            )
        return None

    async def _cleanup_sse_wake(ev: asyncio.Event | None) -> None:
        if _stream is not None and ev is not None and not ev.is_set():
            await _stream.unregister(ev)

    _sse_wake = await _arm_sse_wake()

    while time.monotonic() < deadline:
        woke_early = await _sse_mod.wait_first_or_timeout(
            [_sse_wake] if _sse_wake is not None else [],
            timeout=settings.limit_alo_poll_sec,
        )
        if woke_early and _sse_wake is not None and _sse_wake.is_set():
            log.emit("OPEN_LIMIT_SSE_WAKE", arb_id=arb_id)
            # Re-arm in case this was a spurious / partial event;
            # next iteration's REST check is canonical either way.
            _sse_wake = await _arm_sse_wake()

        # Проверяем статус limit ордера через open-orders.
        # BUG-3 workaround: Lighter может игнорировать clientOrderId при наличии stopLoss —
        # если coid не найден, пробуем fallback match по (baseSymbol, side, size).
        try:
            r_open = await client.get(
                "/exchange/open-orders",
                params={"exchange": limit_exchange},
            )
            if r_open.status_code == 200:
                open_orders = r_open.json() or []
                matched_by_coid = any(
                    o.get("clientOrderId") == limit_coid for o in open_orders
                )
                # Fallback: если clientOrderId не найден, матчинг по asset+side+size
                matched_by_fields = not matched_by_coid and any(
                    (o.get("baseSymbol") or o.get("asset") or "").upper() == limit_base.upper()
                    and o.get("side") == limit_side
                    and abs(Decimal(str(o.get("size", "0"))) - matched_size) < Decimal("0.0001")
                    for o in open_orders
                )
                still_open = matched_by_coid or matched_by_fields
                if matched_by_fields and not matched_by_coid:
                    log.emit(
                        "OPEN_LIMIT_POLL_COID_FALLBACK",
                        arb_id=arb_id,
                        hint="clientOrderId not found, matched by asset+side+size (Lighter+SL workaround)",
                    )
                if not still_open:
                    # Ордер исчез из open-orders — ждём секунду, чтобы history успела обновиться
                    await asyncio.sleep(2)
                    r_hist = await client.get(
                        "/exchange/orders",
                        params={"exchange": limit_exchange, "limit": 20},
                    )
                    if r_hist.status_code == 200:
                        raw = r_hist.json() or {}
                        hist = raw.get("items", raw) if isinstance(raw, dict) else raw
                        for o in hist:
                            # Матчинг по coid или по fields (тот же workaround)
                            coid_match = o.get("clientOrderId") == limit_coid
                            fields_match = (
                                (o.get("baseSymbol") or o.get("asset") or "").upper() == limit_base.upper()
                                and o.get("side") == limit_side
                                and abs(Decimal(str(o.get("size", "0"))) - matched_size) < Decimal("0.0001")
                            )
                            if coid_match or fields_match:
                                if o.get("status") == "executed":
                                    filled = True
                                break
                    break
        except httpx.HTTPError as e:
            log.emit("OPEN_LIMIT_POLL_ERROR", arb_id=arb_id, error=str(e))

        # Проверяем drift market-ноги
        try:
            r_mq = await client.get(
                "/exchange/quotes",
                params={
                    "exchanges": market_exchange,
                    "asset": market_base,
                    "side": market_side,
                    "quoteSize": str(leg_notional_usd),
                    "leverage": effective_lev,
                    "marginMode": "cross",
                },
            )
            if r_mq.status_code == 200:
                mq_body = r_mq.json()
                if mq_body and isinstance(mq_body, list) and isinstance(mq_body[0], dict):
                    new_mid_raw = mq_body[0].get("quote", {}).get("averageExecutionPrice")
                    if new_mid_raw:
                        new_mid = Decimal(str(new_mid_raw))
                        drift = abs(new_mid - initial_mid) / initial_mid * Decimal(10000)
                        if drift > Decimal(settings.limit_alo_drift_bps):
                            log.emit(
                                "OPEN_LIMIT_CANCELLED_DRIFT",
                                arb_id=arb_id,
                                drift_bps=str(drift),
                                cap_bps=settings.limit_alo_drift_bps,
                            )
                            break
        except httpx.HTTPError:
            pass

    # SSE wake cleanup — unregister any still-live waiter for this leg.
    await _cleanup_sse_wake(_sse_wake)

    if not filled:
        # Б3: DELETE /exchange/orders требует orderId (clientOrderId НЕ поддерживается API).
        # Если limit_order_id None — пробуем fetch'ить open-orders и сматчить по
        # (asset, side, size), чтобы получить orderId. Если не нашли — не POST'им
        # фейковый cancel (получим 400 "asset undefined"), а проверяем позицию.
        if not limit_order_id:
            try:
                r_oo = await client.get(
                    "/exchange/orders",
                    params={"exchange": limit_exchange, "limit": 50},
                )
                if r_oo.status_code == 200:
                    raw_oo = r_oo.json() or {}
                    open_list = raw_oo.get("items", raw_oo) if isinstance(raw_oo, dict) else raw_oo
                    for o in open_list:
                        same_asset = (o.get("baseSymbol") or o.get("asset") or "").upper() == limit_base.upper()
                        same_side = o.get("side") == limit_side
                        same_size = abs(Decimal(str(o.get("size", "0"))) - matched_size) < Decimal("0.0001")
                        coid_match = o.get("clientOrderId") == limit_coid
                        if (same_asset and same_side and same_size) or coid_match:
                            limit_order_id = str(o.get("orderId") or "") or None
                            if limit_order_id:
                                log.emit(
                                    "OPEN_LIMIT_CANCEL_COID_TO_ORDERID",
                                    arb_id=arb_id,
                                    order_id=limit_order_id,
                                    hint="resolved orderId via open-orders lookup",
                                )
                                break
            except httpx.HTTPError as e:
                log.emit("OPEN_LIMIT_CANCEL_LOOKUP_ERROR", arb_id=arb_id, error=str(e))

        if not limit_order_id:
            # Не смогли найти orderId — DELETE без него заведомо вернёт 400.
            # Сразу идём в reconcile-режим: проверяем позицию.
            log.emit(
                "OPEN_LIMIT_CANCEL_SKIP_NO_ORDER_ID",
                arb_id=arb_id,
                hint="no orderId available — skipping DELETE, verifying position instead",
            )
            try:
                r_pos_chk = await client.get(
                    "/exchange/positions", params={"exchange": limit_exchange}
                )
                if r_pos_chk.status_code == 200:
                    for pos in (r_pos_chk.json() or []):
                        ps = (pos.get("baseSymbol") or pos.get("asset") or "").upper()
                        ps_side = (pos.get("side") or "").lower()
                        ls_lower = limit_side.lower()
                        side_ok = (
                            ps_side == ls_lower
                            or (ls_lower == "sell" and ps_side == "short")
                            or (ls_lower == "buy" and ps_side == "long")
                        )
                        if ps == limit_base.upper() and side_ok:
                            log.emit(
                                "OPEN_LIMIT_FILLED_LATE",
                                arb_id=arb_id,
                                hint="no orderId found, but position exists — treating as filled",
                            )
                            filled = True
                            break
            except Exception:
                pass
            if not filled:
                log.emit("OPEN_LIMIT_CANCELLED", arb_id=arb_id)
                return None

    # DELETE отправляем только если у нас есть orderId И ордер ещё не filled.
    if not filled and limit_order_id:
        cancel_body: dict[str, Any] = {
            "exchange": limit_exchange,
            "asset": limit_base,
            "orderId": limit_order_id,
        }
        try:
            r_cancel = await client.delete("/exchange/orders", body=cancel_body)
            if r_cancel.status_code >= 400:
                cancel_text = r_cancel.text[:200]
                # Б8: HL race — "Order was never placed, already canceled, or filled"
                # означает что ордер уже в финальном состоянии. Проверяем позицию:
                # есть → filled (это нормально); нет → cancel-no-op (тоже нормально).
                # Это НЕ orphan-сценарий и НЕ требует MANUAL CLEANUP.
                already_final = (
                    "already canceled" in cancel_text
                    or "already cancelled" in cancel_text
                    or "never placed" in cancel_text
                    or "or filled" in cancel_text
                )
                try:
                    r_pos_chk = await client.get(
                        "/exchange/positions", params={"exchange": limit_exchange}
                    )
                    if r_pos_chk.status_code == 200:
                        for pos in (r_pos_chk.json() or []):
                            ps = (pos.get("baseSymbol") or pos.get("asset") or "").upper()
                            ps_side = (pos.get("side") or "").lower()
                            ls_lower = limit_side.lower()
                            side_ok = (
                                ps_side == ls_lower
                                or (ls_lower == "sell" and ps_side == "short")
                                or (ls_lower == "buy" and ps_side == "long")
                            )
                            if ps == limit_base.upper() and side_ok:
                                log.emit(
                                    "OPEN_LIMIT_FILLED_LATE",
                                    arb_id=arb_id,
                                    hint="cancel rejected but position exists — treating as filled",
                                )
                                filled = True
                                break
                except Exception:
                    pass
                if not filled:
                    if already_final:
                        # Ордер уже финальный (cancel/filled на бирже), позиции нет →
                        # значит реально canceled. Это benign race, не warning.
                        log.emit(
                            "OPEN_LIMIT_CANCEL_ALREADY_FINAL",
                            arb_id=arb_id,
                            status=r_cancel.status_code,
                            body=cancel_text,
                            hint="exchange-side cancel/finalize, no orphan",
                        )
                    else:
                        log.emit(
                            "OPEN_LIMIT_CANCEL_REJECTED",
                            arb_id=arb_id,
                            status=r_cancel.status_code,
                            body=cancel_text,
                            hint="order may still be open on exchange",
                        )
        except httpx.HTTPError as e:
            log.emit("OPEN_LIMIT_CANCEL_ERROR", arb_id=arb_id, error=str(e))

        # 2xx-cancel race: Lighter может вернуть успех на DELETE ордера, уже
        # исполнённого между последним poll'ом и cancel'ом. Reject-ветка выше
        # делает position-check для 4xx already_final — здесь страхуем 2xx путь.
        # Без этой проверки orphan-нога не детектится (см. WLD 2026-05-12).
        if not filled:
            try:
                r_pos_chk = await client.get(
                    "/exchange/positions", params={"exchange": limit_exchange}
                )
                if r_pos_chk.status_code == 200:
                    for pos in (r_pos_chk.json() or []):
                        ps = (pos.get("baseSymbol") or pos.get("asset") or "").upper()
                        ps_side = (pos.get("side") or "").lower()
                        ls_lower = limit_side.lower()
                        side_ok = (
                            ps_side == ls_lower
                            or (ls_lower == "sell" and ps_side == "short")
                            or (ls_lower == "buy" and ps_side == "long")
                        )
                        if ps == limit_base.upper() and side_ok:
                            log.emit(
                                "OPEN_LIMIT_FILLED_LATE",
                                arb_id=arb_id,
                                hint="cancel returned 2xx but position exists — treating as filled",
                            )
                            filled = True
                            break
            except Exception:
                pass

        if not filled:
            log.emit("OPEN_LIMIT_CANCELLED", arb_id=arb_id)
            return None

    log.emit("OPEN_LIMIT_FILLED", arb_id=arb_id)

    # Patch A3: market_sl/tp уже посчитаны выше (pre-compute для cross-leg TP).
    # Просто переименовываем для совместимости с дальнейшим кодом.
    market_sl_trigger = market_sl_trigger_pre
    market_tp_trigger = market_tp_trigger_pre

    # Patch E: snapshot ДО market ноги
    accounts_before = await _snapshot_accounts(client, settings)

    market_body: dict[str, Any] = {
        "exchange": market_exchange,
        "asset": market_base,
        "side": market_side,
        "size": str(matched_size),
        "reduceOnly": False,
        "clientOrderId": market_coid,
    }
    if market_sl_trigger is not None:
        market_body["stopLoss"] = {"triggerPrice": str(market_sl_trigger)}
    if market_tp_trigger is not None:
        market_body["takeProfit"] = {"triggerPrice": str(market_tp_trigger)}

    try:
        r_market = await client.post("/exchange/orders", body=market_body)
    except httpx.HTTPError as e:
        log.emit("OPEN_MARKET_LEG_ERROR", arb_id=arb_id, error=str(e), warning="EMERGENCY_UNWIND_NEEDED")
        await _emergency_unwind(client, log, settings, opp, arb_id,
                                limit_exchange, limit_base, limit_side, matched_size)
        return None

    log.emit("OPEN_MARKET_LEG_RESP", arb_id=arb_id, status=r_market.status_code, body=r_market.text[:200])

    if r_market.status_code >= 400:
        # If failure looks like bracket trigger price rejected, retry without stopLoss+takeProfit.
        if r_market.status_code in (400, 503) and ("stopLoss" in market_body or "takeProfit" in market_body):
            market_body_no_bracket = {
                k: v for k, v in market_body.items() if k not in ("stopLoss", "takeProfit")
            }
            log.emit("OPEN_MARKET_LEG_RETRY_NO_BRACKET", arb_id=arb_id, original_status=r_market.status_code)
            try:
                r_market = await client.post("/exchange/orders", body=market_body_no_bracket)
                log.emit("OPEN_MARKET_LEG_RESP", arb_id=arb_id, status=r_market.status_code,
                         body=r_market.text[:200])
                if r_market.status_code < 400:
                    market_sl_trigger = None  # bracket not submitted; OPEN_OK must reflect this
                    market_tp_trigger = None
            except httpx.HTTPError as e:
                log.emit("OPEN_MARKET_LEG_ERROR", arb_id=arb_id, error=str(e))
                await _emergency_unwind(client, log, settings, opp, arb_id,
                                        limit_exchange, limit_base, limit_side, matched_size)
                return None

        if r_market.status_code >= 400:
            log.emit("OPEN_MARKET_LEG_FAIL", arb_id=arb_id, status=r_market.status_code)
            await _emergency_unwind(client, log, settings, opp, arb_id,
                                    limit_exchange, limit_base, limit_side, matched_size)
            return None

    # Patch E: snapshot ПОСЛЕ
    await asyncio.sleep(2.0)
    accounts_after = await _snapshot_accounts(client, settings)
    _log_open_friction(log, arb_id, opp, accounts_before, accounts_after)

    # Если market_sl_trigger=None — HL cross-margin не возвращает liqPrice в quote.
    # Standalone stop-order через VOOI API не поддерживается (stopLoss.triggerPrice работает
    # только как bracket при открытии новой позиции). Защита — software stop-loss в cycle().
    if market_sl_trigger is None and settings.exchange_sl_enabled:
        log.emit(
            "OPEN_MARKET_SL_SKIPPED",
            arb_id=arb_id,
            exchange=market_exchange,
            reason="liqPrice_null_in_quote_cross_margin",
        )

    # Определяем long/short coids для OpenPosition
    long_sl = (limit_sl_trigger if not limit_is_short else market_sl_trigger)
    short_sl = (limit_sl_trigger if limit_is_short else market_sl_trigger)
    long_tp = (limit_tp_trigger if not limit_is_short else market_tp_trigger)
    short_tp = (limit_tp_trigger if limit_is_short else market_tp_trigger)

    pos = OpenPosition(
        arb_id=arb_id,
        asset=opp.asset,
        long_exchange=opp.long_exchange,
        short_exchange=opp.short_exchange,
        long_base_symbol=opp.long_base_symbol,
        short_base_symbol=opp.short_base_symbol,
        opened_at=datetime.now(UTC),
        open_net_apr=opp.net_apr,
        leg_collat_usd=settings.leg_collat_usd,
        leg_notional_usd=leg_notional_usd,
        effective_leverage=effective_lev,
        long_client_order_id=coid_long,
        short_client_order_id=coid_short,
        last_seen_net_apr=opp.net_apr,
        last_seen_at=datetime.now(UTC),
    )
    log.emit(
        "OPEN_OK",
        arb_id=arb_id,
        asset=opp.asset,
        open_mode="limit_then_market",
        long_sl_trigger=str(long_sl) if long_sl else None,
        short_sl_trigger=str(short_sl) if short_sl else None,
        long_tp_trigger=str(long_tp) if long_tp else None,
        short_tp_trigger=str(short_tp) if short_tp else None,
        long_liq_price=str(_decimal_or_none(long_quote.get("liquidationPrice"))),
        short_liq_price=str(_decimal_or_none(short_quote.get("liquidationPrice"))),
    )
    return pos


async def _emergency_unwind(
    client: VooiClient,
    log: NDJsonLog,
    settings: Settings,
    opp: Opportunity,
    arb_id: str,
    exchange: str,
    base: str,
    filled_side: str,
    size: Decimal,
) -> None:
    """Аварийное закрытие filled ноги рыночным reduce-only при неудаче второй ноги."""
    close_side = "buy" if filled_side == "sell" else "sell"
    unwind_body: dict[str, Any] = {
        "exchange": exchange,
        "asset": base,
        "side": close_side,
        "size": str(size),
        "reduceOnly": True,
        "clientOrderId": make_coid(settings.instance_uuid, arb_id, "unwind", exchange),
    }
    for attempt in range(1, 4):
        try:
            r = await client.post("/exchange/orders", body=unwind_body)
            log.emit("EMERGENCY_UNWIND", arb_id=arb_id, attempt=attempt,
                     status=r.status_code, body=r.text[:200])
            if r.status_code < 400:
                return
        except httpx.HTTPError as e:
            log.emit("EMERGENCY_UNWIND_ERROR", arb_id=arb_id, attempt=attempt, error=str(e))
        if attempt < 3:
            await asyncio.sleep(2 ** attempt)
    log.emit("EMERGENCY_UNWIND_GAVE_UP", arb_id=arb_id, warning="MANUAL CLEANUP REQUIRED")


# =============================================================================
# Patch E helpers — account snapshots + friction logging
# =============================================================================


async def _snapshot_accounts(
    client: VooiClient,
    settings: Settings,
) -> dict[str, dict[str, Decimal]]:
    """Снять snapshot totalBalance/availableMargin/marginInUse по каждой бирже."""
    result: dict[str, dict[str, Decimal]] = {}
    try:
        r = await client.get(
            "/exchange/accounts",
            params={"exchanges": list(settings.target_exchanges)},
        )
        if r.status_code == 200:
            for a in r.json():
                key = a["exchange"]
                tb = Decimal(str(a.get("totalBalance", "0")))
                am = Decimal(str(a.get("availableMargin", "0")))
                mu = Decimal(str(a.get("marginInUse", "0")))
                if key in result:
                    result[key]["totalBalance"] += tb
                    result[key]["availableMargin"] += am
                    result[key]["marginInUse"] += mu
                else:
                    result[key] = {
                        "totalBalance": tb,
                        "availableMargin": am,
                        "marginInUse": mu,
                    }
    except Exception:
        pass
    return result


def _log_open_friction(
    log: NDJsonLog,
    arb_id: str,
    opp: Opportunity,
    before: dict[str, dict[str, Decimal]],
    after: dict[str, dict[str, Decimal]],
) -> None:
    if not before or not after:
        return
    friction_per_ex: dict[str, str] = {}
    friction_total = Decimal(0)
    for ex in (opp.long_exchange, opp.short_exchange):
        tb_before = before.get(ex, {}).get("totalBalance", Decimal(0))
        tb_after = after.get(ex, {}).get("totalBalance", Decimal(0))
        diff = tb_after - tb_before
        friction_per_ex[ex] = str(diff)
        friction_total += diff
    log.emit(
        "OPEN_OK_FRICTION",
        arb_id=arb_id,
        friction_per_exchange=friction_per_ex,
        friction_total_usd=str(friction_total),
    )


# =============================================================================
# Closer
# =============================================================================


def _is_urgent_close_reason(reason: str) -> bool:
    """ALO close should be skipped for time-sensitive exits."""
    return reason.startswith("hard_stop_loss") or reason.startswith("max_hold")


async def _close_leg_alo_then_market(
    client: "VooiClient",
    log: "NDJsonLog",
    settings: "Settings",
    market_body: dict[str, Any],
    arb_id: str,
    leg_label: str,
    attempt: int,
) -> "httpx.Response":
    """Patch J: try a post-only ALO reduce-only close first, fall back to market.

    Caller passes the already-built market `body`. If anything in the ALO path fails
    (no quote, post-only rejected, cancel race, poll timeout) → fall through to market
    using the original body. Returns the httpx.Response of whichever order completed
    the leg (always 200 on ALO-filled path; whatever venue returns on market path).
    """
    exchange = market_body["exchange"]
    base = market_body["asset"]
    side = market_body["side"]
    size_str = market_body["size"]

    # 1) Get a mid via /exchange/quotes. We use a small notional just to read price.
    margin_mode = "cross"
    quote = await quote_leg(
        client, log, exchange, base, side,
        quote_size_usd=Decimal(settings.leg_collat_usd),
        leverage=int(settings.leverage_target),
        margin_mode=margin_mode,
    )
    if quote is None or quote.get("avgPrice") in (None, ""):
        log.emit("CLOSE_ALO_NO_QUOTE", arb_id=arb_id, leg=leg_label, exchange=exchange, base=base)
        return await client.post("/exchange/orders", body=market_body)

    try:
        mid = Decimal(str(quote["avgPrice"]))
    except (KeyError, TypeError, ValueError, ArithmeticError):
        log.emit("CLOSE_ALO_BAD_MID", arb_id=arb_id, leg=leg_label)
        return await client.post("/exchange/orders", body=market_body)

    meta = market_meta(exchange, base)
    price_decimals = meta.price_decimals if meta else 6
    max_sig_figs = meta.max_sig_figs if meta else None

    offset = Decimal(settings.limit_alo_close_offset_bps) / Decimal(10000)
    # Post-only ALO requires the price to NOT cross the spread.
    # close-sell  → place above mid (passive sell, above bid)
    # close-buy   → place below mid (passive buy, below ask)
    if side == "sell":
        alo_price = mid * (Decimal(1) + offset)
    else:
        alo_price = mid * (Decimal(1) - offset)
    alo_price = round_price(alo_price, price_decimals, side)
    if max_sig_figs is not None:
        alo_price = round_sig_figs(alo_price, max_sig_figs, side)

    alo_coid = make_coid(
        settings.instance_uuid, arb_id, f"close-alo-{leg_label}-a{attempt}", exchange
    )
    alo_body = dict(market_body)
    alo_body["price"] = str(alo_price)
    alo_body["timeInForce"] = "alo"
    alo_body["clientOrderId"] = alo_coid

    try:
        r_alo = await client.post("/exchange/orders", body=alo_body)
    except httpx.HTTPError as e:
        log.emit("CLOSE_ALO_POST_ERR", arb_id=arb_id, leg=leg_label, error=str(e))
        return await client.post("/exchange/orders", body=market_body)

    log.emit(
        "CLOSE_ALO_POSTED",
        arb_id=arb_id, leg=leg_label, exchange=exchange,
        status=r_alo.status_code, body=r_alo.text[:200],
        price=str(alo_price), size=size_str,
    )
    if r_alo.status_code >= 400:
        return await client.post("/exchange/orders", body=market_body)

    alo_order_id: str | None = None
    try:
        body_json = r_alo.json()
        alo_order_id = str(body_json.get("orderId") or body_json.get("id") or "") or None
    except (ValueError, json.JSONDecodeError):
        pass

    # 2) Poll positions for fill. Lighter sometimes hides clientOrderId when bracket
    # is present (BUG-3) — for reduce-only close there is no bracket, but we still
    # check the position size as the authoritative signal.
    target_entry_side = "buy" if side == "sell" else "sell"
    deadline = time.monotonic() + settings.limit_alo_close_timeout_sec
    started = time.monotonic()

    # SSE wake: register a position-waiter (close → size goes to 0) so we can
    # wake earlier than the 5s poll tick. REST below is still canonical.
    _stream = _sse_mod.get_active_stream()
    _close_sse_wake: asyncio.Event | None = None
    if _stream is not None and _stream.is_healthy():
        _close_sse_wake = await _stream.register_position_waiter(
            exchange=exchange, asset=base,
        )

    try:
        while time.monotonic() < deadline:
            woke_early = await _sse_mod.wait_first_or_timeout(
                [_close_sse_wake] if _close_sse_wake is not None else [],
                timeout=settings.limit_alo_poll_sec,
            )
            if woke_early and _close_sse_wake is not None and _close_sse_wake.is_set():
                log.emit("CLOSE_ALO_SSE_WAKE", arb_id=arb_id, leg=leg_label)
                # Re-arm — if our REST check shows still not flat, we keep waiting.
                if _stream is not None and _stream.is_healthy():
                    _close_sse_wake = await _stream.register_position_waiter(
                        exchange=exchange, asset=base,
                    )
                else:
                    _close_sse_wake = None

            positions_by_ex = await fetch_positions(client, (exchange,), settings)
            cur_list = positions_by_ex.get(exchange)
            if cur_list is None:
                # transient fetch failure — keep waiting until deadline
                continue
            cur = _find_real_pos(cur_list, base, target_entry_side)
            cur_size = _decimal_or_none(cur.get("size")) if cur else None
            cur_size_abs = abs(cur_size) if cur_size is not None else Decimal(0)
            if cur_size_abs < Decimal("0.0001"):
                elapsed = round(time.monotonic() - started, 2)
                log.emit(
                    "CLOSE_ALO_FILLED",
                    arb_id=arb_id, leg=leg_label, exchange=exchange,
                    elapsed_sec=elapsed, alo_price=str(alo_price),
                )
                return r_alo  # already 2xx — position is flat
    finally:
        if _stream is not None and _close_sse_wake is not None and not _close_sse_wake.is_set():
            await _stream.unregister(_close_sse_wake)

    # 3) Timeout — cancel ALO and fall back to market.
    cancel_body: dict[str, Any] = {"exchange": exchange, "asset": base}
    if alo_order_id:
        cancel_body["orderId"] = alo_order_id
    else:
        cancel_body["clientOrderId"] = alo_coid
    try:
        r_cancel = await client.delete("/exchange/orders", body=cancel_body)
        log.emit(
            "CLOSE_ALO_CANCELLED",
            arb_id=arb_id, leg=leg_label, exchange=exchange,
            status=r_cancel.status_code, hint="timeout — falling back to market",
        )
    except httpx.HTTPError as e:
        log.emit("CLOSE_ALO_CANCEL_ERR", arb_id=arb_id, leg=leg_label, error=str(e))

    # 4) Tiny settle pause so cancel propagates before the market POST.
    await asyncio.sleep(1.0)
    log.emit("CLOSE_ALO_FALLBACK_MARKET", arb_id=arb_id, leg=leg_label)
    return await client.post("/exchange/orders", body=market_body)


async def close_position(
    client: VooiClient,
    log: NDJsonLog,
    settings: Settings,
    pos: OpenPosition,
    reason: str,
    cooldown_tracker: dict[str, list[str]] | None = None,
) -> bool:
    log.emit("CLOSE_INTENT", arb_id=pos.arb_id, asset=pos.asset, reason=reason)
    if settings.dry_run:
        log.emit("CLOSE_DRY_RUN_SKIP", arb_id=pos.arb_id)
        pos.closed = True
        return True

    # Patch E: snapshot балансов ДО close
    accounts_before = await _snapshot_accounts(client, settings)

    # Берём актуальный размер позиции с биржи (а не re-quote по leg_notional_usd —
    # цена могла уйти, тогда reduceOnly закроет меньше, чем нужно, и оставит хвост).
    # Цикл с верификацией: если после первого reduce-only остались дробные хвосты
    # (precision-rounding на бирже), делаем ещё попытку.
    exchanges_to_query = tuple({pos.long_exchange, pos.short_exchange})
    max_attempts = 3
    last_long_size: Decimal | None = None
    last_short_size: Decimal | None = None
    any_order_sent = False

    for attempt in range(1, max_attempts + 1):
        positions_by_ex = await fetch_positions(client, exchanges_to_query, settings)
        # Б9: если fetch_positions вернул None для нужной биржи — НЕ маркировать
        # позицию закрытой. Это значит что fetch упал (401/timeout/5xx), и реальное
        # состояние позиции unknown. Без этой проверки бот ловит истёкший токен
        # и логирует false CLOSE_OK с close_source=exchange_sl_or_external,
        # хотя на бирже позиции реально открыты.
        long_fetch_ok = positions_by_ex.get(pos.long_exchange) is not None
        short_fetch_ok = positions_by_ex.get(pos.short_exchange) is not None
        if not long_fetch_ok or not short_fetch_ok:
            log.emit(
                "CLOSE_SKIP_FETCH_FAILED",
                arb_id=pos.arb_id,
                asset=pos.asset,
                attempt=attempt,
                long_exchange_ok=long_fetch_ok,
                short_exchange_ok=short_fetch_ok,
                hint="positions fetch failed (401/timeout/5xx) — cannot determine state, retry next cycle",
            )
            return False  # пробуем заново на следующем cycle
        real_long = _find_real_pos(
            positions_by_ex.get(pos.long_exchange) or [],
            pos.long_base_symbol,
            "buy",
        )
        real_short = _find_real_pos(
            positions_by_ex.get(pos.short_exchange) or [],
            pos.short_base_symbol,
            "sell",
        )
        long_size_dec = _decimal_or_none(real_long.get("size")) if real_long else None
        short_size_dec = _decimal_or_none(real_short.get("size")) if real_short else None
        # Lighter возвращает size со знаком (отрицательный для шортов), Hyperliquid — модуль.
        # Унифицируем: для market reduce-only ордера нужен модуль size (направление задаётся side).
        if long_size_dec is not None:
            long_size_dec = abs(long_size_dec)
        if short_size_dec is not None:
            short_size_dec = abs(short_size_dec)
        last_long_size = long_size_dec
        last_short_size = short_size_dec

        long_open = long_size_dec is not None and long_size_dec > 0
        short_open = short_size_dec is not None and short_size_dec > 0

        log.emit(
            "CLOSE_REAL_SIZES",
            arb_id=pos.arb_id,
            attempt=attempt,
            long_size=str(long_size_dec) if long_size_dec is not None else None,
            short_size=str(short_size_dec) if short_size_dec is not None else None,
        )

        if not long_open and not short_open:
            pos.closed = True

            # RISK-1: если на attempt=1 позиции уже нет и наши ордера не отправлялись —
            # вероятно биржевой SL уже закрыл позицию. Realized PnL snapshot в этом случае
            # ненадёжен (баланс уже изменился до accounts_before).
            close_source = "bot"
            if attempt == 1 and not any_order_sent:
                close_source = "exchange_sl_or_external"
                log.emit(
                    "CLOSE_EXCHANGE_SL_DETECTED",
                    arb_id=pos.arb_id,
                    hint="position already closed before our orders — likely exchange bracket SL",
                )

            # Patch E: snapshot балансов ПОСЛЕ close (ненадёжен при close_source=exchange_sl_or_external)
            await asyncio.sleep(2.0)
            accounts_after = await _snapshot_accounts(client, settings)
            realized_per_ex: dict[str, str] = {}
            realized_total = Decimal(0)
            for ex in (pos.long_exchange, pos.short_exchange):
                tb_before = accounts_before.get(ex, {}).get("totalBalance", Decimal(0))
                tb_after = accounts_after.get(ex, {}).get("totalBalance", Decimal(0))
                diff = tb_after - tb_before
                realized_per_ex[ex] = str(diff)
                realized_total += diff

            held_h = (datetime.now(UTC) - pos.opened_at).total_seconds() / 3600
            log.emit(
                "CLOSE_OK",
                arb_id=pos.arb_id,
                asset=pos.asset,
                attempts=attempt,
                sent_orders=any_order_sent,
                reason=reason,
                close_source=close_source,
                held_hours=str(held_h),
                open_net_apr=str(pos.open_net_apr),
                realized_per_exchange=realized_per_ex,
                realized_total_usd=str(realized_total),
                realized_pnl_reliable=(close_source == "bot"),
                mark_long_upnl=str(pos.long_upnl_usd) if pos.long_upnl_usd is not None else None,
                mark_short_upnl=str(pos.short_upnl_usd) if pos.short_upnl_usd is not None else None,
                mark_long_funding=str(pos.long_funding_usd) if pos.long_funding_usd is not None else None,
                mark_short_funding=str(pos.short_funding_usd) if pos.short_funding_usd is not None else None,
                mark_total=str(pos.arb_total_pnl_usd),
            )

            # Tuning 2026-05-13: per-asset losing-close cooldown.
            # Записываем убыточные закрытия по asset. При close_source="bot"
            # используем точный realized_total; при "exchange_sl_or_external"
            # (биржевой SL или внешнее закрытие) — это, как правило, убыток,
            # но measured balance ненадёжен → fallback на mark_total (uPnL+funding
            # с последнего тика).
            if cooldown_tracker is not None:
                if close_source == "bot":
                    loss_indicator = realized_total
                else:
                    loss_indicator = pos.arb_total_pnl_usd
                if loss_indicator < 0:
                    asset_key = pos.asset.upper()
                    cooldown_tracker.setdefault(asset_key, []).append(
                        datetime.now(UTC).isoformat()
                    )
                    log.emit(
                        "COOLDOWN_LOSS_RECORDED",
                        asset=asset_key,
                        arb_id=pos.arb_id,
                        close_source=close_source,
                        loss_indicator=str(loss_indicator),
                        losses_in_log=len(cooldown_tracker[asset_key]),
                    )
            return True

        legs: list[tuple[str, dict[str, Any]]] = []
        if long_open:
            long_close: dict[str, Any] = {
                "exchange": pos.long_exchange,
                "asset": pos.long_base_symbol,
                "side": "sell",
                "size": str(long_size_dec),
                "reduceOnly": True,
                "clientOrderId": make_coid(
                    settings.instance_uuid, pos.arb_id, f"close-long-{attempt}", pos.long_exchange,
                ),
            }
            legs.append(("long", long_close))

        if short_open:
            short_close: dict[str, Any] = {
                "exchange": pos.short_exchange,
                "asset": pos.short_base_symbol,
                "side": "buy",
                "size": str(short_size_dec),
                "reduceOnly": True,
                "clientOrderId": make_coid(
                    settings.instance_uuid, pos.arb_id, f"close-short-{attempt}", pos.short_exchange,
                ),
            }
            legs.append(("short", short_close))

        # Patch J: on attempt 1 of soft (non-urgent) exits, try ALO close first
        # (per leg, sequential — keeps risk surface small). Urgent reasons
        # (hard_stop_loss / max_hold) and retry attempts skip ALO.
        use_alo_close = (
            settings.limit_alo_close_enabled
            and attempt == 1
            and not _is_urgent_close_reason(reason)
        )
        for label, body in legs:
            try:
                if use_alo_close:
                    r = await _close_leg_alo_then_market(
                        client, log, settings, body, pos.arb_id, label, attempt,
                    )
                else:
                    r = await client.post("/exchange/orders", body=body)
                resp_text = r.text[:200]
                log.emit(
                    f"CLOSE_{label.upper()}_RESP",
                    arb_id=pos.arb_id,
                    attempt=attempt,
                    status=r.status_code,
                    body=resp_text,
                )
                any_order_sent = True
                # Б6: HL race "Reduce only order would increase position" означает что
                # позиция закрылась между нашим fetch_positions и POST (биржевой SL
                # или внешний actor). Не фейл — на следующей итерации позиция исчезнет.
                if r.status_code >= 400 and (
                    "Reduce only" in resp_text or "increase position" in resp_text
                ):
                    log.emit(
                        "CLOSE_POSITION_GONE_RACE",
                        arb_id=pos.arb_id,
                        leg=label,
                        attempt=attempt,
                        hint="position likely closed by exchange SL between fetch and POST",
                    )
            except httpx.HTTPError as e:
                log.emit(
                    f"CLOSE_{label.upper()}_ERROR",
                    arb_id=pos.arb_id,
                    attempt=attempt,
                    error=str(e),
                )

        # Дать бирже время на исполнение market reduce-only ордера до перезапроса позиций.
        await asyncio.sleep(1.5)

    log.emit(
        "CLOSE_PARTIAL_FAIL",
        arb_id=pos.arb_id,
        attempts=max_attempts,
        residual_long_size=str(last_long_size) if last_long_size is not None else None,
        residual_short_size=str(last_short_size) if last_short_size is not None else None,
        warning="manual cleanup may be needed",
    )
    return False


# =============================================================================
# Reporting
# =============================================================================


def _fmt_money(d: Decimal) -> str:
    return f"${d.quantize(Decimal('0.01'), rounding=ROUND_DOWN)}"


def _fmt_money_signed(d: Decimal | None, prec: int = 4) -> str:
    if d is None:
        return "    n/a "
    q = Decimal("0." + "0" * prec)
    sign = "+" if d >= 0 else ""
    return f"{sign}${d.quantize(q, rounding=ROUND_DOWN)}"


def _fmt_pct(d: Decimal) -> str:
    return f"{(d * 100).quantize(Decimal('0.01'), rounding=ROUND_DOWN)}%"


def render_report(report: CycleReport, settings: Settings) -> str:
    lines = []
    lines.append("=" * 80)
    is_trading = not any(a.startswith("MONITOR_ONLY") for a in report.actions)
    mode = "TRADING" if is_trading else "MONITOR "
    lines.append(
        f"# CYCLE #{report.cycle} [{mode}]  "
        f"started={report.started_at.strftime('%H:%M:%S')}Z  "
        f"elapsed={report.elapsed_sec:.2f}s  "
        f"DRY_RUN={settings.dry_run}",
    )
    if settings.loop_interval_sec >= 3600:
        lines.append(
            "# hourly mode: 1 cycle/hour at UTC hour boundary (scan + trading decisions)",
        )
    else:
        lines.append(
            f"# monitor every {settings.loop_interval_sec}s  "
            f"trading every {settings.trading_cycle_sec}s",
        )
    lines.append("=" * 80)

    lines.append("\n## Accounts (margin buckets)")
    if not report.accounts:
        lines.append("   (no accounts data)")
    else:
        w = max(len(k) for k in report.accounts)
        w = max(w, 12)
        for key in sorted(report.accounts.keys()):
            a = report.accounts[key]
            lines.append(
                f"   {key:{w}}  total={_fmt_money(a['total']):>10}  "
                f"available={_fmt_money(a['available']):>10}  "
                f"in_use={_fmt_money(a['in_use']):>10}",
            )

    lines.append("\n## Open positions (uPnL = mark-to-market, funding = накопленный funding fee)")
    if not report.positions:
        lines.append("   (none)")
    portfolio_upnl = Decimal(0)
    portfolio_funding = Decimal(0)
    portfolio_total = Decimal(0)
    portfolio_collat = Decimal(0)
    for p in report.positions:
        held_h = (datetime.now(UTC) - p.opened_at).total_seconds() / 3600
        portfolio_upnl += p.arb_upnl_usd
        portfolio_funding += p.arb_funding_usd
        portfolio_total += p.arb_total_pnl_usd
        portfolio_collat += p.leg_collat_usd * 2  # 2 ноги
        long_label = f"{p.long_exchange} ({p.long_base_symbol})"
        short_label = f"{p.short_exchange} ({p.short_base_symbol})"
        lines.append(
            f"   ▸ {p.asset:8}  LONG: {long_label:28}  SHORT: {short_label:28}  "
            f"collat={_fmt_money(p.leg_collat_usd * 2):>7}  "
            f"notional={_fmt_money(p.leg_notional_usd * 2):>7}  "
            f"lev={p.effective_leverage}x  "
            f"open_apr={_fmt_pct(p.open_net_apr):>9}  "
            f"now_apr={_fmt_pct(p.last_seen_net_apr or Decimal(0)):>9}  "
            f"held={held_h:.2f}h",
        )
        lines.append(
            f"        Σ uPnL    = {_fmt_money_signed(p.arb_upnl_usd):>10}     "
            f"Σ funding = {_fmt_money_signed(p.arb_funding_usd):>10}     "
            f"NET = {_fmt_money_signed(p.arb_total_pnl_usd):>10}",
        )
        lines.append(
            f"          ├─ {p.long_exchange:11} LONG  : uPnL={_fmt_money_signed(p.long_upnl_usd):>10}   funding={_fmt_money_signed(p.long_funding_usd):>10}",
        )
        lines.append(
            f"          └─ {p.short_exchange:11} SHORT : uPnL={_fmt_money_signed(p.short_upnl_usd):>10}   funding={_fmt_money_signed(p.short_funding_usd):>10}    arb_id={p.arb_id}",
        )

    if report.positions:
        roi_pct = (portfolio_total / portfolio_collat * 100) if portfolio_collat > 0 else Decimal(0)
        lines.append(
            f"\n   ┃ PORTFOLIO TOTAL ({len(report.positions)} strategies,  "
            f"capital={_fmt_money(portfolio_collat)})",
        )
        lines.append(
            f"   ┃   Σ uPnL    = {_fmt_money_signed(portfolio_upnl):>10}     "
            f"Σ funding = {_fmt_money_signed(portfolio_funding):>10}     "
            f"NET = {_fmt_money_signed(portfolio_total):>10}  ({_fmt_pct(roi_pct / 100):>7} ROI)",
        )

    if report.blocked_assets:
        lines.append(
            f"\n## Blocked assets ({len(report.blocked_assets)}) — opportunities по ним пропускаются",
        )
        for asset, reason in sorted(report.blocked_assets.items()):
            lines.append(f"   {asset:14}  {reason}")

    min_apr_str = _fmt_pct(settings.min_net_apr) if settings.min_net_apr > 0 else "any"
    max_apr_str = _fmt_pct(settings.max_net_apr) if settings.max_net_apr > 0 else "no cap"
    wl_str = f", whitelist={len(settings.asset_whitelist)} assets" if settings.asset_whitelist else ""
    lines.append(
        f"\n## Top opportunities (gross_h>0, apr1h+24h+7d>0, netApr {min_apr_str}..{max_apr_str}, "
        f"vol24h ≥ {_fmt_money(settings.min_volume_24h_usd)}, "
        f"target_lev={settings.leverage_target}x{wl_str})",
    )
    if not report.opportunities:
        lines.append("   (none pass filter)")
    for o in report.opportunities[:15]:
        eff = o.effective_leverage(settings.leverage_target)
        apr7d_s = _fmt_pct(o.apr7d) if o.apr7d is not None else "  n/a "
        lines.append(
            f"   {o.asset:14}  {o.long_exchange[:3]}:{o.long_base_symbol:10} long / "
            f"{o.short_exchange[:3]}:{o.short_base_symbol:10} short  "
            f"netAPR={_fmt_pct(o.net_apr):>10}  apr24h={_fmt_pct(o.apr24h):>9}  apr7d={apr7d_s:>9}  "
            f"vol24h={_fmt_money(o.volume_24h_usd):>14}  "
            f"lev=min({o.long_max_leverage},{o.short_max_leverage},{settings.leverage_target})={eff}x",
        )

    if report.actions:
        opens_closes = [a for a in report.actions if not a.startswith("SKIP ")]
        skips = [a for a in report.actions if a.startswith("SKIP ")]
        if opens_closes:
            lines.append("\n## Actions this cycle")
            for action in opens_closes:
                lines.append(f"   - {action}")
        if skips:
            lines.append(f"\n## Skipped this cycle ({len(skips)} opportunities)")
            for action in skips[:3]:
                lines.append(f"   - {action}")
            if len(skips) > 3:
                lines.append(f"   ... and {len(skips) - 3} more (all cap_per_exchange_reached / insufficient margin)")
    if report.errors:
        lines.append("\n## Errors this cycle")
        for err in report.errors:
            lines.append(f"   ! {err}")

    lines.append("=" * 80)
    return "\n".join(lines)


# =============================================================================
# Main loop
# =============================================================================


async def cycle(
    client: VooiClient,
    log: NDJsonLog,
    settings: Settings,
    state: dict[str, OpenPosition],
    cycle_no: int,
    *,
    is_trading_cycle: bool,
    cooldown: dict[str, list[str]] | None = None,
) -> CycleReport:
    """Один цикл бота.

    is_trading_cycle=True → выполняет closer + opener (часовой trading cycle).
    is_trading_cycle=False → только bootstrap+scan+render (5-min monitor cycle):
        обновляет uPnL/funding в позициях но не открывает/закрывает.
    """
    started = datetime.now(UTC)
    t0 = time.monotonic()
    report = CycleReport(cycle=cycle_no, started_at=started, elapsed_sec=0.0)
    if not is_trading_cycle:
        report.actions.append("MONITOR_ONLY (no open/close decisions; next trading cycle scheduled)")

    # 0. Patch C: обновляем markets cache (priceDecimals/baseDecimals) в начале каждого цикла.
    await refresh_markets_cache(client, log, settings)
    if not markets_cache:
        log.emit("MARKETS_CACHE_EMPTY_WARN", hint="trading blocked until cache populated")

    # 1. Bootstrap accounts + positions (REST primary, C3).
    accounts = await fetch_accounts(client, settings.target_exchanges, log)
    report.accounts = accounts

    real_positions = await fetch_positions(client, settings.target_exchanges, settings)

    # Обогащаем все наши OpenPosition реальными данными с биржи (uPnL, funding).
    for pos in state.values():
        if pos.closed:
            continue
        long_pos_list = real_positions.get(pos.long_exchange) or []
        short_pos_list = real_positions.get(pos.short_exchange) or []
        real_long = _find_real_pos(long_pos_list, pos.long_base_symbol, "buy")
        real_short = _find_real_pos(short_pos_list, pos.short_base_symbol, "sell")
        _enrich_position_with_real(pos, real_long, real_short)

    # BALANCE_SNAPSHOT — логируем equity каждый цикл для hourly P&L трекинга.
    # equity = settled balance + unrealized PnL (то что реально стоит портфель).
    _bal_snap: dict[str, Any] = {}
    _total_settled = Decimal(0)
    _total_unrealized = Decimal(0)
    _trading_account_keys = {k for k in accounts if k.startswith("hyperliquid:") or k == "lighter"}
    for _ex_key, _bal in accounts.items():
        _settled = _bal.get("total", Decimal(0))
        _bal_snap[f"{_ex_key}.settled"] = str(_settled)
        if _ex_key in _trading_account_keys:
            _total_settled += _settled
    for _ex in settings.target_exchanges:
        _upnl = sum(
            _decimal_or_none(p.get("unrealizedPnl")) or Decimal(0)
            for p in (real_positions.get(_ex) or [])
        )
        _total_unrealized += _upnl
        _bal_snap[f"{_ex}.unrealized"] = str(round(_upnl, 4))
    _total_equity = _total_settled + _total_unrealized
    log.emit(
        "BALANCE_SNAPSHOT",
        cycle=cycle_no,
        settled=str(round(_total_settled, 4)),
        unrealized=str(round(_total_unrealized, 4)),
        equity=str(round(_total_equity, 4)),
        per_exchange=_bal_snap,
        open_positions=sum(1 for p in state.values() if not p.closed),
    )

    # 2. Scanner.
    opps, scan_errs = await fetch_opportunities(client, settings)
    report.errors.extend(scan_errs)

    # 2b. Блокировка assets от двойного открытия.
    # Правило: если по asset уже есть позиция на ЛЮБОЙ из поддерживаемых бирж
    # (наша или открытая руками, на нашей target-бирже или нет) — стратегию
    # по этому asset НЕ открываем. Это защищает от:
    #   - дублирования коллатерала на одном asset,
    #   - "случайного нетто-закрытия" если новая стратегия даст обратное
    #     направление по уже открытой ноге (например YZY long hyp + новая
    #     YZY short hyp/long aster нетто-обнулит hyp-ногу).
    blocked_reasons: dict[str, str] = {}
    for pos in state.values():
        if pos.closed:
            continue
        blocked_reasons[pos.asset.upper()] = (
            f"own_position {pos.long_exchange[:3]}/{pos.short_exchange[:3]} arb_id={pos.arb_id}"
        )

    # Опрашиваем positions на ВСЕХ известных биржах (а не только target),
    # чтобы external поза на Aster тоже блокировала открытие YZY-стратегии
    # на hyper/lighter.
    all_real_positions = await fetch_positions(client, ALL_SUPPORTED_EXCHANGES, settings)

    # Ghost reconciliation: авто-закрываем осиротевшие ноги из snapshot позиций.
    # Запускаем на каждом цикле — дешевле чем руками чистить ghost-позиции.
    await _reconcile_orphaned_legs(client, log, settings, state, all_real_positions)
    # После reconcile snapshot изменился — сохраняем сразу
    save_snapshot(state, settings.snapshot_file)

    for ex, ex_pos_list in all_real_positions.items():
        for p in (ex_pos_list or []):
            sym = (p.get("baseSymbol") or "").upper()
            if not sym:
                continue
            if sym in blocked_reasons:
                continue  # уже заблокирован своей же позой
            blocked_reasons[sym] = f"external_position on {ex} side={p.get('side')}"

    # Tuning 2026-05-13: cooldown blocks — assets с N убытками за окно.
    # Prune старых записей и применяем cooldown — но НЕ перетираем own_position/
    # external_position причины (они приоритетнее, дают больше контекста).
    if cooldown is not None and settings.pair_cooldown_after_losses > 0:
        prune_cooldown(cooldown, settings.pair_cooldown_hours)
        cd_blocks = cooldown_blocked_assets(
            cooldown,
            settings.pair_cooldown_after_losses,
            settings.pair_cooldown_hours,
        )
        for asset_upper, reason in cd_blocks.items():
            if asset_upper not in blocked_reasons:
                blocked_reasons[asset_upper] = reason

    blocked_assets = set(blocked_reasons) | {a.lower() for a in blocked_reasons}
    if blocked_reasons:
        log.emit(
            "BLOCKED_ASSETS",
            cycle=cycle_no,
            count=len(blocked_reasons),
            reasons=blocked_reasons,
        )

    # Сохраняем real_positions из target_exchanges для enrich (используется
    # выше) — но fetched отдельно от all_real_positions чтобы избежать лишних
    # обращений в enrich-цикле.
    filtered = filter_opportunities(opps, settings, blocked_assets)
    report.opportunities = filtered
    report.blocked_assets = dict(blocked_reasons)

    # 3. Update last_seen_net_apr на каждом цикле (даже monitor-only),
    # чтобы now_apr в отчёте всегда был свежий.
    for pos in state.values():
        if pos.closed:
            continue
        for o in opps:
            if (
                o.asset == pos.asset
                and o.long_exchange == pos.long_exchange
                and o.short_exchange == pos.short_exchange
            ):
                pos.last_seen_net_apr = o.net_apr
                pos.last_seen_at = datetime.now(UTC)
                break

    # 3b. Closer — Patch F: smart-exit + hard stop-loss.
    #
    # A. HARD stop-loss — на КАЖДОМ цикле (не только trading).
    #    Threshold = stop_loss_pct × collat × 2 (обе ноги).
    for arb_id, pos in list(state.items()):
        if pos.closed:
            continue
        ups = [pos.long_upnl_usd, pos.short_upnl_usd]
        fds = [pos.long_funding_usd, pos.short_funding_usd]
        if any(v is None for v in (*ups, *fds)):
            continue
        net_usd = sum((Decimal(str(v)) for v in (*ups, *fds)), Decimal(0))
        threshold = -settings.stop_loss_pct * pos.leg_collat_usd * 2
        if net_usd < threshold:
            reason = (
                f"hard_stop_loss net={net_usd:.4f} threshold={threshold:.4f} "
                f"(pct={settings.stop_loss_pct} * collat*2={pos.leg_collat_usd * 2})"
            )
            await close_position(client, log, settings, pos, reason, cooldown_tracker=cooldown)
            report.actions.append(f"CLOSE arb_id={arb_id} reason={reason}")

    # B. Smart-exit + max hold — только в trading cycle.
    if is_trading_cycle:
        # Обновляем legacy streak counter (совместимость) и apr_history.
        for pos in state.values():
            if pos.closed:
                continue
            if pos.last_seen_net_apr is None:
                continue
            # Legacy streak (not used for close decision anymore, kept for metrics)
            if pos.last_seen_net_apr < 0:
                pos.negative_apr_trading_streak += 1
            else:
                pos.negative_apr_trading_streak = 0
            # Patch F: APR history (last 24 hourly snapshots)
            pos.apr_history.append(pos.last_seen_net_apr)
            if len(pos.apr_history) > 24:
                pos.apr_history = pos.apr_history[-24:]

            # Funding breakeven check
            funding_total = (pos.long_funding_usd or Decimal(0)) + (pos.short_funding_usd or Decimal(0))
            friction_est = (
                Decimal(settings.estimated_friction_bps) / Decimal(10000)
                * pos.leg_notional_usd * Decimal(2) * Decimal(2)
            )
            if not pos.funding_breakeven_achieved and funding_total > friction_est * Decimal("1.5"):
                pos.funding_breakeven_achieved = True
                log.emit(
                    "FUNDING_BREAKEVEN",
                    arb_id=pos.arb_id,
                    funding_total=str(funding_total),
                    friction_est=str(friction_est),
                )
            pos.peak_funding_cum = max(pos.peak_funding_cum, funding_total)

        # Smart-exit close decisions
        for arb_id, pos in list(state.items()):
            if pos.closed:
                continue
            held_h = (datetime.now(UTC) - pos.opened_at).total_seconds() / 3600
            close_reason: str | None = None

            if held_h > settings.max_hold_hours:
                close_reason = f"max_hold held={held_h:.1f}h cap={settings.max_hold_hours}"

            # Tuning 2026-05-13: soft-exits заблокированы в первые min_hold_hours.
            # hard_stop_loss (выше, в monitor-блоке) и max_hold (только что) — продолжают работать.
            soft_exit_allowed = held_h >= settings.min_hold_hours

            if not close_reason and soft_exit_allowed and pos.last_seen_net_apr is not None:
                hist = pos.apr_history
                current_apr = pos.last_seen_net_apr
                safe_floor = max(
                    pos.open_net_apr * settings.safe_floor_mult,
                    settings.min_net_apr,
                )
                # Пропускаем safe_floor если funding уже покрыл friction
                check_floor = not (
                    settings.funding_breakeven_skip_safe_floor
                    and pos.funding_breakeven_achieved
                )

                if not check_floor or current_apr < safe_floor:
                    # N negative consecutive (tuning 2026-05-13: каждое чтение
                    # должно быть < smart_neg_value_floor, а не любое <0 —
                    # шум ±1% APR раньше триггерил преждевременные exits)
                    if (
                        len(hist) >= settings.negative_window
                        and all(
                            x < settings.smart_neg_value_floor
                            for x in hist[-settings.negative_window:]
                        )
                    ):
                        close_reason = (
                            f"smart_neg_{settings.negative_window} "
                            f"hist={[float(x) for x in hist[-settings.negative_window:]]} "
                            f"value_floor={float(settings.smart_neg_value_floor):.4f} "
                            f"floor_check={'skipped' if not check_floor else f'<{safe_floor:.4f}'}"
                        )
                    # N declines подряд
                    elif (
                        len(hist) >= settings.decline_window + 1
                        and all(
                            hist[-1 - i] < hist[-2 - i]
                            for i in range(settings.decline_window)
                        )
                    ):
                        close_reason = (
                            f"smart_decl_{settings.decline_window} "
                            f"hist={[float(x) for x in hist[-settings.decline_window - 1:]]} "
                            f"floor_check={'skipped' if not check_floor else f'<{safe_floor:.4f}'}"
                        )

            # Patch H: Low-APR sustained exit — независимо от safe_floor,
            # но как soft-exit подчиняется min_hold_hours (tuning 2026-05-13).
            # tuning 2026-05-18: gate on funding_breakeven_achieved — если funding
            # уже отбил friction, не режем позицию на низком APR (она уже в плюсе).
            if (
                not close_reason
                and soft_exit_allowed
                and pos.last_seen_net_apr is not None
                and not pos.funding_breakeven_achieved
            ):
                hist = pos.apr_history
                if (
                    len(hist) >= settings.low_apr_window
                    and all(x < settings.low_apr_threshold for x in hist[-settings.low_apr_window:])
                ):
                    close_reason = (
                        f"low_apr_{settings.low_apr_window} "
                        f"hist={[float(x) for x in hist[-settings.low_apr_window:]]} "
                        f"threshold={float(settings.low_apr_threshold):.2f} "
                        f"fb_achieved=False"
                    )

            if close_reason:
                await close_position(client, log, settings, pos, close_reason, cooldown_tracker=cooldown)
                report.actions.append(f"CLOSE arb_id={arb_id} reason={close_reason}")

    # Patch I: если в этом цикле были закрытия — ждём settlement и перезапрашиваем балансы.
    closed_this_cycle = sum(1 for a in report.actions if a.startswith("CLOSE "))
    if is_trading_cycle and closed_this_cycle > 0:
        log.emit(
            "POST_CLOSE_SETTLE_WAIT",
            closed=closed_this_cycle,
            wait_sec=settings.post_close_settle_sec,
        )
        await asyncio.sleep(settings.post_close_settle_sec)
        accounts = await fetch_accounts(client, settings.target_exchanges, log)
        log.emit("POST_CLOSE_ACCOUNTS_REFRESHED", buckets=list(accounts.keys()))
        # Patch J: re-scan opportunities so opener sees current apr1h/net_apr
        opps, _ = await fetch_opportunities(client, settings)
        filtered = filter_opportunities(opps, settings, blocked_assets)
        log.emit("POST_CLOSE_OPPS_REFRESHED", count=len(filtered))

    # 4. Opener — только в trading cycle. Один цикл = ОДНА стратегия
    # (защита от каскадного открытия + проще оценить эффект на следующем
    # цикле).
    if is_trading_cycle:
        for opp in filtered:
            if opp.asset in blocked_assets or opp.asset.upper() in blocked_assets:
                continue
            long_key = margin_bucket_key(opp.long_exchange, opp.long_base_symbol, opp.long_quote_symbol)
            short_key = margin_bucket_key(opp.short_exchange, opp.short_base_symbol, opp.short_quote_symbol)
            long_acct = accounts.get(long_key)
            short_acct = accounts.get(short_key)
            if not long_acct or not short_acct:
                # Не молчим: рассинхрон между margin_bucket_key и fetch_accounts
                # ключами однажды уже маскировал полный простой опеннера — больше нет.
                report.actions.append(
                    f"SKIP {opp.asset} netAPR={_fmt_pct(opp.net_apr)} reason=account_bucket_not_found "
                    f"(long_key={long_key} {'OK' if long_acct else 'MISSING'} "
                    f"short_key={short_key} {'OK' if short_acct else 'MISSING'} "
                    f"have={sorted(accounts.keys())})",
                )
                log.emit(
                    "OPEN_SKIP_BUCKET_NOT_FOUND",
                    asset=opp.asset,
                    long_key=long_key,
                    short_key=short_key,
                    long_found=bool(long_acct),
                    short_found=bool(short_acct),
                    available_keys=sorted(accounts.keys()),
                )
                continue
            eff_lev = opp.effective_leverage(settings.leverage_target, settings.leverage_cap)
            leg_notional = settings.leg_collat_usd * eff_lev
            # Проверка max_notional_per_position_usd (2 ноги = leg_notional × 2)
            if (
                settings.max_notional_per_position_usd > 0
                and leg_notional * 2 > settings.max_notional_per_position_usd
            ):
                report.actions.append(
                    f"SKIP {opp.asset} netAPR={_fmt_pct(opp.net_apr)} reason=notional_exceeds_cap "
                    f"(notional={_fmt_money(leg_notional * 2)} cap={_fmt_money(settings.max_notional_per_position_usd)})",
                )
                continue
            # Cap проверяется по collat (margin), не по notional.
            if (
                long_acct["in_use"] + settings.leg_collat_usd > settings.max_margin_per_ex_usd
                or short_acct["in_use"] + settings.leg_collat_usd > settings.max_margin_per_ex_usd
            ):
                report.actions.append(
                    f"SKIP {opp.asset} netAPR={_fmt_pct(opp.net_apr)} reason=cap_per_exchange_reached "
                    f"(long_bucket={long_key} in_use={_fmt_money(long_acct['in_use'])}/cap="
                    f"{_fmt_money(settings.max_margin_per_ex_usd)} "
                    f"short_bucket={short_key} in_use={_fmt_money(short_acct['in_use'])}/cap="
                    f"{_fmt_money(settings.max_margin_per_ex_usd)})",
                )
                continue
            if (
                long_acct["available"] < settings.leg_collat_usd
                or short_acct["available"] < settings.leg_collat_usd
            ):
                report.actions.append(
                    f"SKIP {opp.asset} netAPR={_fmt_pct(opp.net_apr)} reason=insufficient_available_margin "
                    f"(long_bucket={long_key} avail={_fmt_money(long_acct['available'])} "
                    f"short_bucket={short_key} avail={_fmt_money(short_acct['available'])})",
                )
                continue

            if settings.dry_run:
                report.actions.append(
                    f"WOULD_OPEN {opp.asset} long={opp.long_exchange} short={opp.short_exchange} "
                    f"netAPR={_fmt_pct(opp.net_apr)} collat=${settings.leg_collat_usd} "
                    f"notional=${leg_notional} eff_lev={eff_lev}x "
                    f"(market_max long={opp.long_max_leverage}x short={opp.short_max_leverage}x)",
                )
                log.emit(
                    "OPEN_DRY_RUN_WOULD",
                    asset=opp.asset,
                    long_exchange=opp.long_exchange,
                    short_exchange=opp.short_exchange,
                    net_apr=str(opp.net_apr),
                    collat_usd=str(settings.leg_collat_usd),
                    notional_usd=str(leg_notional),
                    effective_leverage=eff_lev,
                    target_leverage=settings.leverage_target,
                    market_max_long=opp.long_max_leverage,
                    market_max_short=opp.short_max_leverage,
                )
                break

            new_pos = await open_strategy(client, log, settings, opp)
            if new_pos is not None:
                state[new_pos.arb_id] = new_pos
                blocked_assets.add(new_pos.asset)
                blocked_assets.add(new_pos.asset.upper())
                report.actions.append(
                    f"OPEN arb_id={new_pos.arb_id} asset={opp.asset} "
                    f"netAPR={_fmt_pct(opp.net_apr)} collat=${settings.leg_collat_usd} "
                    f"notional=${leg_notional} eff_lev={eff_lev}x",
                )
                break

    # 5. Refresh report.positions + persist snapshot.
    report.positions = [p for p in state.values() if not p.closed]
    save_snapshot(state, settings.snapshot_file)
    # Tuning 2026-05-13: persist cooldown-log (close_position уже мог дописать
    # новые убытки; prune'нутый dict уже без устаревших записей).
    if cooldown is not None:
        save_cooldown_state(cooldown, settings.cooldown_file)
    report.elapsed_sec = time.monotonic() - t0
    return report


async def _reconcile_orphaned_legs(
    client: VooiClient,
    log: NDJsonLog,
    settings: Settings,
    state: dict[str, "OpenPosition"],
    all_real_positions: dict[str, list[dict[str, Any]] | None],
) -> None:
    """
    Для каждой snapshot-позиции проверяет обе ноги на бирже.
    Если одна нога есть, а второй нет — это ghost: авто-закрываем осиротевшую ногу.
    Если обе ноги пропали — позиция закрыта снаружи (биржевой SL или user), чистим snapshot.
    """
    for arb_id, pos in list(state.items()):
        if pos.closed or pos.close_in_progress:
            continue

        long_pos_raw = all_real_positions.get(pos.long_exchange)
        short_pos_raw = all_real_positions.get(pos.short_exchange)

        # If either exchange fetch failed (None), we can't safely determine which
        # legs are present. Skip reconciliation to avoid false-positive orphan detection.
        if long_pos_raw is None or short_pos_raw is None:
            log.emit(
                "RECONCILE_SKIP_FETCH_FAILED",
                arb_id=arb_id,
                asset=pos.asset,
                long_exchange_ok=long_pos_raw is not None,
                short_exchange_ok=short_pos_raw is not None,
            )
            continue

        long_pos_list: list[dict[str, Any]] = long_pos_raw
        short_pos_list: list[dict[str, Any]] = short_pos_raw

        has_long = _find_real_pos(long_pos_list, pos.long_base_symbol, "buy") is not None
        has_short = _find_real_pos(short_pos_list, pos.short_base_symbol, "sell") is not None

        if has_long and has_short:
            continue  # нормальная хеджированная пара

        if not has_long and not has_short:
            # Обе ноги исчезли — закрыты снаружи (биржевой SL или руками)
            log.emit(
                "RECONCILE_BOTH_GONE",
                arb_id=arb_id,
                asset=pos.asset,
                hint="both legs missing externally, removing from snapshot",
            )
            pos.closed = True
            continue

        # Одна нога есть, второй нет — ghost pair
        if has_long and not has_short:
            orphan_exchange = pos.long_exchange
            orphan_symbol = pos.long_base_symbol
            orphan_side = "buy"
            close_side = "sell"
        else:
            orphan_exchange = pos.short_exchange
            orphan_symbol = pos.short_base_symbol
            orphan_side = "sell"
            close_side = "buy"

        # Б4: для legacy позиций long_base_symbol / short_base_symbol могут быть пустыми
        # ("" или None) — fallback на pos.asset. Иначе POST /orders уходит с asset=null
        # и VOOI отвечает 400 "Validation failed: asset undefined".
        if not orphan_symbol:
            log.emit(
                "RECONCILE_ORPHAN_SYMBOL_FALLBACK",
                arb_id=arb_id,
                orphan_exchange=orphan_exchange,
                fallback_symbol=pos.asset,
                hint=f"{'long' if has_long else 'short'}_base_symbol was empty, using pos.asset",
            )
            orphan_symbol = pos.asset

        log.emit(
            "RECONCILE_ORPHAN_DETECTED",
            arb_id=arb_id,
            asset=pos.asset,
            orphan_exchange=orphan_exchange,
            orphan_side=orphan_side,
            hint="one leg missing — closing orphaned leg",
        )

        if settings.dry_run:
            log.emit("RECONCILE_DRY_RUN_SKIP", arb_id=arb_id)
            continue

        real_orphan = _find_real_pos(
            all_real_positions.get(orphan_exchange) or [], orphan_symbol, orphan_side
        )
        if real_orphan is None:
            log.emit("RECONCILE_ORPHAN_ALREADY_GONE", arb_id=arb_id, exchange=orphan_exchange)
            pos.closed = True
            continue

        orphan_size = abs(_decimal_or_none(real_orphan.get("size")) or Decimal(0))
        if orphan_size == 0:
            pos.closed = True
            continue

        close_body: dict[str, Any] = {
            "exchange": orphan_exchange,
            "asset": orphan_symbol,
            "quoteSymbol": "USDC",
            "side": close_side,
            "type": "market",
            "size": str(orphan_size),
            "reduceOnly": True,
            "marginMode": "cross",
        }
        # Patch A3: mark in-flight to prevent double-close race с watcher.
        pos.close_in_progress = True
        try:
            r_close = await client.post("/exchange/orders", body=close_body)
            close_ok = 200 <= r_close.status_code < 300
            log.emit(
                "RECONCILE_ORPHAN_CLOSED",
                arb_id=arb_id,
                asset=pos.asset,
                exchange=orphan_exchange,
                side=close_side,
                size=str(orphan_size),
                status=r_close.status_code,
                body=r_close.text[:200],
            )
            if close_ok:
                pos.closed = True
        except httpx.HTTPError as e:
            log.emit("RECONCILE_ORPHAN_CLOSE_ERROR", arb_id=arb_id, error=str(e))
        finally:
            pos.close_in_progress = False


# =============================================================================
# Patch A3 — Survivor watcher (parallel 1s poll, closes naked leg ASAP)
# =============================================================================


async def survivor_watcher_loop(
    client: VooiClient,
    log: NDJsonLog,
    settings: Settings,
    state: dict[str, OpenPosition],
    stop: asyncio.Event,
) -> None:
    """Параллельный watcher: каждые survivor_watch_sec проверяет, не пропала ли
    одна нога у каждой открытой позиции (после срабатывания биржевого SL/TP).
    При detect — закрывает survivor leg market reduce-only.

    Idle backoff когда нет open positions. Использует тот же _reconcile_orphaned_legs
    что и main-cycle (с координацией через close_in_progress / closed flags).
    """
    log.emit(
        "SURVIVOR_WATCH_STARTED",
        enabled=settings.survivor_watch_enabled,
        watch_sec=settings.survivor_watch_sec,
        idle_sec=settings.survivor_watch_idle_sec,
    )
    if not settings.survivor_watch_enabled:
        return

    consecutive_fetch_errors = 0
    while not stop.is_set():
        # Backoff если нет активных позиций — нечего сторожить.
        active = [pos for pos in state.values() if not pos.closed]
        if not active:
            try:
                await asyncio.wait_for(stop.wait(), timeout=settings.survivor_watch_idle_sec)
            except asyncio.TimeoutError:
                pass
            continue

        # Собираем уникальные exchanges из активных позиций (обычно 2: HL + Lighter).
        exchanges = tuple({
            ex for pos in active for ex in (pos.long_exchange, pos.short_exchange)
        })
        try:
            all_real_positions = await fetch_positions(client, exchanges, settings)
        except (httpx.HTTPError, RuntimeError) as e:
            consecutive_fetch_errors += 1
            log.emit(
                "SURVIVOR_WATCH_FETCH_ERROR",
                error=str(e),
                consecutive=consecutive_fetch_errors,
            )
            # Экспоненциальный backoff при подряд-неудачах (5 fails → 30с пауза).
            backoff = min(30.0, settings.survivor_watch_sec * (2 ** min(consecutive_fetch_errors, 5)))
            try:
                await asyncio.wait_for(stop.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass
            continue
        consecutive_fetch_errors = 0

        # Reuse main reconcile path. Координация через pos.close_in_progress / pos.closed.
        await _reconcile_orphaned_legs(client, log, settings, state, all_real_positions)

        # Wait для следующего poll'а (или stop, или SSE position-event для одного из
        # активных активов — последнее даёт <1s detection при срабатывании SL/TP
        # вместо ожидания полного survivor_watch_sec). REST остаётся источником истины:
        # выше уже идёт fetch_positions + reconcile, и SSE wake лишь будит этот цикл.
        _stream = _sse_mod.get_active_stream()
        _sse_waiters: list[asyncio.Event] = []
        if _stream is not None and _stream.is_healthy():
            seen: set[tuple[str, str]] = set()
            for pos in active:
                for ex in (pos.long_exchange, pos.short_exchange):
                    asset_for_ex = (
                        pos.long_base_symbol if ex == pos.long_exchange else pos.short_base_symbol
                    ) or pos.asset
                    key = (ex, asset_for_ex.upper())
                    if key in seen:
                        continue
                    seen.add(key)
                    ev = await _stream.register_position_waiter(exchange=ex, asset=asset_for_ex)
                    _sse_waiters.append(ev)

        # Add stop event so a graceful shutdown wakes us immediately.
        woke_early = await _sse_mod.wait_first_or_timeout(
            _sse_waiters + [stop],
            timeout=settings.survivor_watch_sec,
        )
        # Best-effort unregister of un-fired waiters (avoids registry buildup).
        if _stream is not None:
            for ev in _sse_waiters:
                if not ev.is_set():
                    await _stream.unregister(ev)
        if woke_early and not stop.is_set():
            log.emit("SURVIVOR_WATCH_SSE_WAKE", waiters=len(_sse_waiters))

    log.emit("SURVIVOR_WATCH_STOPPED")


async def run() -> int:
    # Load .env (минимально).
    env_path = Path(".env")
    if env_path.exists():
        for raw in env_path.read_text(encoding="utf-8").splitlines():
            s = raw.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            k, v = k.strip(), v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v

    try:
        settings = Settings.from_env()
    except RuntimeError as e:
        sys.stderr.write(f"FATAL: {e}\n")
        return 2

    # PID lock — запрет запуска второго инстанса бота (основная причина ghost-позиций)
    _pid_file = Path(os.environ.get("BOT_PID_FILE", "/tmp/vooi-funding-arb-bot.pid"))
    try:
        if _pid_file.exists():
            _old_pid = int(_pid_file.read_text().strip())
            try:
                os.kill(_old_pid, 0)  # 0 = просто проверить существование процесса
                sys.stderr.write(
                    f"FATAL: Another instance already running (PID {_old_pid}). "
                    f"Kill it first or remove {_pid_file}.\n"
                )
                return 1
            except ProcessLookupError:
                pass  # old process is dead — перезаписываем pid файл
        _pid_file.write_text(str(os.getpid()))
    except Exception as _pid_err:
        sys.stderr.write(f"WARN: could not write pid file: {_pid_err}\n")

    import atexit as _atexit
    _atexit.register(lambda: _pid_file.unlink(missing_ok=True))

    log = NDJsonLog(settings.state_file, settings.instance_uuid)
    log.emit(
        "BOT_STARTED",
        instance_uuid=settings.instance_uuid,
        target_exchanges=list(settings.target_exchanges),
        leg_collat_usd=str(settings.leg_collat_usd),
        max_margin_per_ex_usd=str(settings.max_margin_per_ex_usd),
        min_net_apr=str(settings.min_net_apr),
        max_net_apr=str(settings.max_net_apr),
        leverage_target=settings.leverage_target,
        asset_whitelist=list(settings.asset_whitelist),
        asset_blacklist=list(settings.asset_blacklist),
        max_hold_hours=settings.max_hold_hours,
        stop_loss_pct=str(settings.stop_loss_pct),
        negative_apr_trading_streak_threshold=settings.negative_apr_trading_streak_threshold,
        max_slippage_bps=settings.max_slippage_bps,
        loop_interval_sec=settings.loop_interval_sec,
        dry_run=settings.dry_run,
        include_hl_non_crypto=settings.include_hl_non_crypto,
    )

    state: dict[str, OpenPosition] = load_snapshot(settings.snapshot_file)
    if state:
        log.emit(
            "STATE_RESTORED_FROM_SNAPSHOT",
            file=str(settings.snapshot_file),
            count=len(state),
            arb_ids=list(state.keys()),
        )
    # Tuning 2026-05-13: load cooldown-log + prune старых записей.
    cooldown: dict[str, list[str]] = load_cooldown_state(settings.cooldown_file)
    prune_cooldown(cooldown, settings.pair_cooldown_hours)
    if cooldown:
        log.emit(
            "COOLDOWN_RESTORED",
            file=str(settings.cooldown_file),
            assets={a: len(s) for a, s in cooldown.items()},
            after_losses=settings.pair_cooldown_after_losses,
            window_hours=settings.pair_cooldown_hours,
        )
    book_assets = {p.asset.upper() for p in state.values()}
    missing_managed = sorted(MANAGED_BOOK_EXTRA_ASSETS - book_assets)
    if missing_managed:
        log.emit(
            "BOOK_MANAGED_MISSING",
            assets=missing_managed,
            hint="Добавь записи в state-snapshot.json или убери лишнее из MANAGED_BOOK_EXTRA_ASSETS.",
        )
    else:
        log.emit(
            "BOOK_MANAGED_OK",
            extra_assets=sorted(MANAGED_BOOK_EXTRA_ASSETS),
            book_asset_count=len(book_assets),
        )
    stop = asyncio.Event()

    def _signal_handler(*_: Any) -> None:
        log.emit("BOT_STOP_REQUESTED")
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            signal.signal(sig, _signal_handler)

    def _next_tick(interval_sec: int) -> tuple[float, float]:
        """Возвращает (target_tick_ts, delay_sec) до следующего «стенно-часового» tick'а.

        Циклы привязаны к UTC wall-clock (а не к moment-of-bot-start).
        При interval_sec=300: TRADING в HH:00, MONITOR в HH:05, HH:10, …
        При interval_sec=3600: каждый tick = начало часа (только TRADING-цикл).

        Возврат target_tick_ts необходим, потому что asyncio.sleep может проснуться
        на несколько миллисекунд РАНЬШЕ boundary (наблюдалось 16:59:59.998 вместо
        17:00:00.000 → minute=59 вместо 0 → ошибочно MONITOR вместо TRADING).
        Используем target_tick для решения is_trading, а не actual wake time.
        """
        now_ts = time.time()
        next_tick = (int(now_ts) // interval_sec + 1) * interval_sec
        return float(next_tick), max(0.0, next_tick - now_ts)

    async def _run_one_cycle(client_: VooiClient, cycle_id: int, *, trading: bool) -> None:
        try:
            r = await cycle(
                client_, log, settings, state, cycle_id,
                is_trading_cycle=trading,
                cooldown=cooldown,
            )
            sys.stdout.write("\n" + render_report(r, settings) + "\n")
            sys.stdout.flush()
            log.emit(
                "CYCLE_DONE",
                cycle=cycle_id,
                elapsed_sec=r.elapsed_sec,
                is_trading_cycle=trading,
                actions=r.actions,
                errors=r.errors,
                open_positions=len(r.positions),
            )
        except (httpx.HTTPError, RuntimeError, ValueError) as e:
            log.emit("CYCLE_ERROR", cycle=cycle_id, error=str(e))
            sys.stderr.write(f"CYCLE {cycle_id} error: {e}\n")

    cycle_no = 0
    # hourly_mode: каждый тик = trading. Включается либо при длинном интервале (≥1ч),
    # либо когда trading_cycle_sec ≤ loop_interval_sec (тест с коротким циклом).
    hourly_mode = (
        settings.loop_interval_sec >= 3600
        or settings.trading_cycle_sec <= settings.loop_interval_sec
    )

    async with VooiClient(settings.base_url, settings.bearer_token) as client:
        # Patch A3: параллельный survivor-watcher (1s poll, закрывает наг ногу на bracket trigger).
        watcher_task = asyncio.create_task(
            survivor_watcher_loop(client, log, settings, state, stop)
        )

        # SSE event stream — Phase 1. When enabled, spawns a background task
        # that opens /exchange/updates and lets tight-loop pollers wake on
        # push events (still REST-confirms every decision). When disabled,
        # everything below behaves exactly as before.
        sse_task: asyncio.Task[None] | None = None
        if settings.sse_enabled:
            stream = _sse_mod.VooiEventStream(
                base_url=settings.base_url,
                bearer_token=settings.bearer_token,
                exchanges=tuple(settings.target_exchanges),
                log_emit=log.emit,
                heartbeat_timeout_sec=settings.sse_heartbeat_timeout_sec,
                reconnect_max_attempts=settings.sse_reconnect_max_attempts,
            )
            _sse_mod.set_active_stream(stream)
            sse_task = asyncio.create_task(stream.run(stop))
        else:
            _sse_mod.set_active_stream(None)
            log.emit("SSE_DISABLED", reason="BOT_SSE_ENABLED=false")

        try:
            if hourly_mode:
                # Почасовой режим: при старте — сразу полный TRADING (проверка + решения),
                # без ожидания HH:00; дальше в цикле — сон до следующей границы часа UTC.
                # Если есть открытые позы — перед этим один MONITOR (stop-loss на каждом
                # цикле; B/C только в TRADING).
                if any(not p.closed for p in state.values()):
                    cycle_no += 1
                    await _run_one_cycle(client, cycle_no, trading=False)
                cycle_no += 1
                await _run_one_cycle(client, cycle_no, trading=True)
            else:
                # 5-мин режим: MONITOR сразу при старте (uPnL/funding + отчёт), trading
                # только на tick'ах с minute==0.
                cycle_no = 1
                await _run_one_cycle(client, cycle_no, trading=False)

            while not stop.is_set():
                target_tick_ts, delay = _next_tick(settings.loop_interval_sec)
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                if stop.is_set():
                    break

                cycle_no += 1
                # TRADING — на основе TARGET tick (HH:00:00), а НЕ на actual wake time
                # (asyncio.sleep может проснуться на ms раньше boundary, давая minute=59
                # вместо 0). Target_tick_ts всегда лежит ровно на 5-мин границе.
                target_dt = datetime.fromtimestamp(target_tick_ts, UTC)
                is_trading = hourly_mode or target_dt.minute == settings.trading_minute
                await _run_one_cycle(client, cycle_no, trading=is_trading)
        finally:
            # Graceful shutdown watcher'а.
            stop.set()
            try:
                await asyncio.wait_for(watcher_task, timeout=5.0)
            except asyncio.TimeoutError:
                watcher_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await watcher_task
            # SSE stream graceful shutdown.
            if sse_task is not None:
                try:
                    await asyncio.wait_for(sse_task, timeout=5.0)
                except asyncio.TimeoutError:
                    sse_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await sse_task
                _sse_mod.set_active_stream(None)

    log.emit("BOT_STOPPED")
    return 0


def main() -> int:
    try:
        return asyncio.run(run())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
