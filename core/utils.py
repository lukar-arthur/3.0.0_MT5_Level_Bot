# ============================================================
#  MT5_Level_Bot — core/utils.py  v2.0.0
#  ИЗМЕНЕНИЯ v2.0.0:
#  - Добавлена calc_ema() — настоящая EMA (исправление П-1)
#  - Добавлена calc_rsi() — единая точка (ранее дублировалась)
#  - calc_atr/calc_adx — единый источник истины
# ============================================================

import logging
import math
import os
import time
import functools
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Optional, Callable, Any

LOGS_DIR         = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
LOG_MAX_BYTES    = 5 * 1024 * 1024
LOG_BACKUP_COUNT = 3


def get_logger(module_name: str) -> logging.Logger:
    os.makedirs(LOGS_DIR, exist_ok=True)
    logger = logging.getLogger(module_name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        fmt="%(asctime)s UTC | %(name)-12s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fmt.converter = time.gmtime
    fh = RotatingFileHandler(
        os.path.join(LOGS_DIR, f"{module_name}.log"),
        maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding="utf-8"
    )
    fh.setLevel(logging.DEBUG); fh.setFormatter(fmt)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO); ch.setFormatter(fmt)
    logger.addHandler(fh); logger.addHandler(ch)
    return logger


def safe_normalize(value: float, max_val: float,
                   default: float = 0.0, clamp: bool = True) -> float:
    if value is None or max_val is None or max_val <= 0:
        return default
    result = float(value) / float(max_val)
    if clamp:
        result = max(0.0, min(1.0, result))
    return round(result, 6)


def freshness_score(last_touch_utc: Optional[datetime], lam: float = 0.005) -> float:
    if last_touch_utc is None:
        return 0.0
    now = datetime.now(timezone.utc)
    if last_touch_utc.tzinfo is None:
        last_touch_utc = last_touch_utc.replace(tzinfo=timezone.utc)
    hours = (now - last_touch_utc).total_seconds() / 3600.0
    return round(math.exp(-lam * max(hours, 0.0)), 6)


def retry(max_attempts: int = 5, base_delay: float = 2.0,
          exceptions: tuple = (Exception,),
          logger: Optional[logging.Logger] = None) -> Callable:
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            delay = base_delay
            last_exc: Optional[Exception] = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if logger:
                        logger.warning(
                            f"[retry] {func.__name__} attempt {attempt}/{max_attempts} "
                            f"failed: {exc}. "
                            f"{'Retrying in {:.1f}s'.format(delay) if attempt < max_attempts else 'Giving up.'}"
                        )
                    if attempt < max_attempts:
                        time.sleep(delay)
                        delay *= 2
            raise last_exc
        return wrapper
    return decorator


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def to_utc(dt: datetime) -> datetime:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def hours_since(dt: Optional[datetime]) -> Optional[float]:
    if dt is None:
        return None
    return (utcnow() - to_utc(dt)).total_seconds() / 3600.0


_ZONE_STEPS = {"JPY": 0.050, "DEFAULT": 0.0005}


def price_to_zone(symbol: str, price: float) -> float:
    step = _ZONE_STEPS["JPY"] if symbol.upper().endswith("JPY") else _ZONE_STEPS["DEFAULT"]
    return round(round(price / step) * step, 5)


ATR_PERIOD_BY_TF: dict = {
    "D": 14, "H4": 14, "H1": 20, "M15": 20, "M5": 20, "M1": 20,
}
ATR_PERIOD_DEFAULT = 14


def calc_atr(rates: list, period: int) -> float:
    """ATR — Wilder method. Единственный источник истины в проекте."""
    available = len(rates) - 1
    if available < 1:
        return rates[0]["high"] - rates[0]["low"] if rates else 0.0001
    use_period = min(period, available)
    true_ranges = []
    for i in range(1, use_period + 1):
        curr = rates[-i]; prev = rates[-(i + 1)]
        tr = max(curr["high"] - curr["low"],
                 abs(curr["high"] - prev["close"]),
                 abs(curr["low"]  - prev["close"]))
        true_ranges.append(tr)
    return sum(true_ranges) / len(true_ranges) if true_ranges else 0.0001


def calc_ema(rates: list, period: int) -> float:
    """
    ИСПРАВЛЕНИЕ П-1: Настоящая EMA (Exponential Moving Average).
    Ранее signal_engine.py использовал SMA под названием EMA.
    Теперь — единственная реализация EMA для всего проекта.

    Алгоритм:
      SMA первых period баров → seed
      EMA(t) = close(t) * k + EMA(t-1) * (1-k), k = 2/(period+1)
    """
    if not rates or len(rates) < period:
        return 0.0
    closes = [float(r["close"]) for r in rates]
    k   = 2.0 / (period + 1)
    ema = sum(closes[:period]) / period  # SMA seed
    for c in closes[period:]:
        ema = c * k + ema * (1 - k)
    return ema


def calc_adx(rates: list, period: int = 14) -> float:
    """ADX — Wilder method. Единственный источник истины в проекте."""
    need = period * 2 + 1
    if len(rates) < need:
        return 0.0
    bars = rates[-need:]
    dm_plus_list = []; dm_minus_list = []; tr_list = []
    for i in range(1, len(bars)):
        curr = bars[i]; prev = bars[i - 1]
        tr = max(curr["high"] - curr["low"],
                 abs(curr["high"] - prev["close"]),
                 abs(curr["low"]  - prev["close"]))
        tr_list.append(tr)
        up   = curr["high"] - prev["high"]
        down = prev["low"]  - curr["low"]
        dm_plus_list.append(up   if (up > down and up > 0)   else 0.0)
        dm_minus_list.append(down if (down > up and down > 0) else 0.0)
    if len(tr_list) < period:
        return 0.0

    def _dx(dmp, dmm, atr):
        if atr == 0: return 0.0
        di_p = 100 * dmp / atr; di_m = 100 * dmm / atr
        s = di_p + di_m
        return 100 * abs(di_p - di_m) / s if s else 0.0

    atr_s = sum(tr_list[:period])
    dmp_s = sum(dm_plus_list[:period])
    dmm_s = sum(dm_minus_list[:period])
    dx_list = [_dx(dmp_s, dmm_s, atr_s)]
    for i in range(period, len(tr_list)):
        atr_s = atr_s - atr_s / period + tr_list[i]
        dmp_s = dmp_s - dmp_s / period + dm_plus_list[i]
        dmm_s = dmm_s - dmm_s / period + dm_minus_list[i]
        dx_list.append(_dx(dmp_s, dmm_s, atr_s))
    if len(dx_list) < period:
        return 0.0
    return round(sum(dx_list[-period:]) / period, 2)


def calc_rsi(rates: list, period: int = 14) -> float:
    """RSI — Wilder method. Единственный источник истины в проекте."""
    if not rates or len(rates) < period + 1:
        return 50.0
    closes = [float(b["close"]) for b in rates]
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains  = [max(d, 0) for d in deltas]
    losses = [abs(min(d, 0)) for d in deltas]
    avg_g  = sum(gains[:period])  / period
    avg_l  = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + gains[i])  / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    return round(100.0 - 100.0 / (1.0 + avg_g / avg_l), 1)
