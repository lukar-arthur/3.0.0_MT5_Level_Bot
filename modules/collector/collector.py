# ============================================================
#  MT5_Level_Bot — modules/collector/collector.py
#  Version : 2.0.0
#
#  ИЗМЕНЕНИЯ v2.0.0 (после полного аудита):
#
#  C-1  Retry при неполных барах (история ещё качается)
#  C-2  Нормализация времени D-баров до полуночи UTC
#  C-3  Retry + graceful skip при обрыве соединения внутри цикла
#  C-4  Batch-транзакция: 30 UPSERT на пару/ТФ = 1 транзакция
#  C-5  Правильные пипсы для JPY пар (×100 вместо ×10000)
#  V-1  Валидация OHLC целостности каждого бара
#  V-2  Детекция временны́х разрывов в истории
#  V-3  Минимальный порог баров дифференцирован по ТФ
#  V-4  ATR_PERIOD дифференцирован по ТФ
#  V-5  Уровни с B=0 отфильтровываются до записи в БД
#  V-6  Предупреждение о несинхронности ТФ внутри одной пары
#  V-7  _count_bounces и _find_last_touch_time объединены
#  U-1  Валидация символа (суффиксы брокера EURUSDm и т.п.)
#  U-2  Таймаут на весь цикл сбора
#  U-3  Мониторинг дрейфа ATR
#  U-4  Корректный fallback ATR при малом числе баров
#  U-5  Расширенная статистика в лог
# ============================================================

import configparser
import os
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from core.base_module    import BaseModule
from core.db_connection  import get_db
from core.mt5_bridge     import get_mt5_bridge
from core.utils          import (get_logger, price_to_zone, utcnow,
                                  calc_atr, calc_adx, calc_rsi,
                                  calc_ema, ATR_PERIOD_BY_TF)
from core.config_loader  import load_module_config

logger = get_logger("collector")

# Plug-and-Play: collector использует config/collector.ini
# При отсутствии ключа — откат к config/config.ini
_MODULE_CFG = load_module_config("collector")

# ── Таймфреймы ───────────────────────────────────────────────
TIMEFRAMES = ["D", "H4", "H1"]

# ── Параметры алгоритма ──────────────────────────────────────
EXTREMUM_LEFT  = 5
EXTREMUM_RIGHT = 2

# V-4: ATR период дифференцирован по ТФ
ATR_PERIOD_BY_TF: Dict[str, int] = {
    "D":   14,   # 14 дней  — классический Wilder
    "H4":  14,   # 14 × H4 = ~2.5 дня
    "H1":  20,   # 20 × H1 = 20 часов — сглаживаем шум
    "M15": 20,
    "M5":  20,
    "M1":  20,
}
ATR_PERIOD_DEFAULT = 14

# V-3: минимальный порог баров по ТФ
MIN_BARS_BY_TF: Dict[str, int] = {
    "D":   200,   # ~0.8 года
    "H4":  300,   # ~50 дней
    "H1":  400,   # ~17 дней
    "M15": 200,
    "M5":  200,
    "M1":  200,
}
MIN_BARS_DEFAULT = 100

# ── Параметры попыток получения баров (C-1) ──────────────────
_RATES_RETRY_COUNT  = 3
_RATES_RETRY_DELAY  = 2.5    # секунды между попытками

# ── Параметры соединения (C-3) ───────────────────────────────
_CONN_RETRY_COUNT = 2
_CONN_RETRY_DELAY = 5.0

# ── Таймаут цикла (U-2) ──────────────────────────────────────
_CYCLE_MAX_FRACTION = 0.80   # не более 80% от interval_sec

# ── Минимальный bounce для записи в БД (V-5) ─────────────────
MIN_BOUNCE_TO_RECORD = 1

# ── ATR мультипликаторы зоны по ТФ ───────────────────────────
ATR_ZONE_MULT: Dict[str, float] = {
    "D":   0.10,
    "H4":  0.15,
    "H1":  0.25,
    "M15": 0.35,
    "M5":  0.50,
    "M1":  0.50,
}
ATR_ZONE_MULT_DEFAULT = 0.20

# ── Ожидаемый шаг между барами в секундах (V-2) ──────────────
_TF_STEP_SEC: Dict[str, int] = {
    "D":   86400,
    "H4":  14400,
    "H1":  3600,
    "M15": 900,
    "M5":  300,
    "M1":  60,
}

# MAX уровней на ТФ
MAX_LEVELS_PER_TF = 30


# ------------------------------------------------------------------
# Конфиг
# ------------------------------------------------------------------

# Разрешённые таймфреймы (фиксированный список)
_ALLOWED_TF = {"D", "H4", "H1", "H2", "M30", "M15"}

def _load_config() -> dict:
    """Plug-and-Play: читает из config/collector.ini (fallback: config.ini)."""
    cfg = _MODULE_CFG
    cfg.reload()  # горячая перезагрузка при каждом цикле
    symbols_raw = cfg.get("COLLECTOR", "symbols",
                           fallback="EURUSD,GBPUSD,USDJPY,USDCHF,AUDUSD,EURGBP")
    tf_raw      = cfg.get("COLLECTOR", "timeframes", fallback="D,H4,H1")
    timeframes  = [t.strip().upper() for t in tf_raw.split(",")
                   if t.strip().upper() in _ALLOWED_TF]
    if not timeframes:
        timeframes = ["D", "H4", "H1"]
    return {
        "symbols":       [s.strip() for s in symbols_raw.split(",") if s.strip()],
        "timeframes":    timeframes,
        "bars_to_fetch": cfg.getint("COLLECTOR", "bars_to_fetch", fallback=700),
        "interval_sec":  cfg.getint("COLLECTOR", "interval_sec",  fallback=1800),
    }


# ------------------------------------------------------------------
# C-5: пипс-мультипликатор по символу
# ------------------------------------------------------------------

def _pip_mult(symbol: str) -> int:
    """
    Возвращает множитель для перевода ценовой разницы в пипсы.
    JPY пары: 1 пипс = 0.01  → множитель 100
    Остальные: 1 пипс = 0.0001 → множитель 10000
    """
    return 100 if symbol.upper().endswith("JPY") else 10000


# ------------------------------------------------------------------
# V-1: валидация OHLC бара
# ------------------------------------------------------------------

def _is_valid_bar(bar: dict) -> bool:
    """
    Проверяет логическую целостность OHLC бара.
    Отбрасывает: инвертированные, нулевые, выходящие за диапазон.
    """
    try:
        o, h, l, c = bar["open"], bar["high"], bar["low"], bar["close"]
        return (
            h > 0 and l > 0 and
            h >= l and
            l <= o <= h and
            l <= c <= h
        )
    except (KeyError, TypeError):
        return False


# ------------------------------------------------------------------
# V-2: детекция временных разрывов
# ------------------------------------------------------------------

def _detect_gaps(rates: list, timeframe: str) -> List[Tuple[datetime, datetime]]:
    """
    Ищет временны́е разрывы в истории баров.
    Нормальные паузы НЕ считаются разрывом:
      - H1/H4: пятница 22:00 → воскресенье/понедельник (≤ 54 часа)
      - D:     пятница → понедельник (≤ 4 дня включая праздники)
      - Праздники: Рождество (24-26 дек), Новый год (31 дек - 2 янв)
    Возвращает список (gap_start, gap_end) — только реальные разрывы.
    """
    tf_upper     = timeframe.upper()
    expected_sec = _TF_STEP_SEC.get(tf_upper, 3600)
    gaps         = []

    for i in range(1, len(rates)):
        t_prev = rates[i - 1]["time"]
        t_curr = rates[i]["time"]
        delta  = (t_curr - t_prev).total_seconds()

        if delta <= 0:
            continue

        # Праздничные дни — биржа не работает 1-2 дня
        is_holiday = (
            # Рождество: 24-26 декабря
            (t_prev.month == 12 and t_prev.day in (24, 25, 26)) or
            (t_curr.month == 12 and t_curr.day in (24, 25, 26)) or
            # Новый год: 31 декабря - 2 января
            (t_prev.month == 12 and t_prev.day == 31) or
            (t_curr.month ==  1 and t_curr.day in (1, 2))
        )

        # Нормальные выходные — не разрыв
        if tf_upper == "D":
            # D бары: пятница→понедельник = 3 дня = 259200с
            # Допускаем до 4 дней (+ праздники)
            is_normal_pause = (
                (t_prev.weekday() in (4, 5) and
                 t_curr.weekday() in (0, 6) and
                 delta <= 86400 * 4) or
                is_holiday
            )
        else:
            # H1/H4: пятница вечер → воскресенье/понедельник ≤ 54 часа
            is_normal_pause = (
                (t_prev.weekday() == 4 and
                 t_curr.weekday() in (6, 0) and
                 delta <= 3600 * 54) or
                (is_holiday and delta <= 3600 * 54)
            )

        if not is_normal_pause and delta > expected_sec * 2:
            gaps.append((t_prev, t_curr))

    return gaps


# ------------------------------------------------------------------
# U-3: мониторинг дрейфа ATR (хранилище предыдущих значений)
# ------------------------------------------------------------------
_prev_atr: Dict[str, float] = {}   # ключ: "EURUSD/H1"


def _check_atr_drift(symbol: str, tf: str, atr: float) -> None:
    key = f"{symbol}/{tf}"
    prev = _prev_atr.get(key)
    if prev and prev > 0:
        ratio = atr / prev
        if ratio > 3.0:
            logger.warning(
                f"ATR аномалия {key}: {prev:.5f} → {atr:.5f} "
                f"(×{ratio:.1f}) — возможны аномальные бары"
            )
        elif ratio < 0.2:
            logger.warning(
                f"ATR резко упал {key}: {prev:.5f} → {atr:.5f} "
                f"(×{ratio:.2f}) — проверь историю"
            )
    _prev_atr[key] = atr


# ------------------------------------------------------------------
# Математика
# ------------------------------------------------------------------

def _calc_atr(rates: list, period: int) -> float:
    """
    ATR (Average True Range).
    U-4: корректный fallback при малом числе баров.
    """
    available = len(rates) - 1   # нужен prev_close → нужна пара баров
    if available < 1:
        return rates[0]["high"] - rates[0]["low"] if rates else 0.0001

    # Используем сколько есть, но не больше period
    use_period = min(period, available)

    true_ranges = []
    for i in range(1, use_period + 1):
        curr = rates[-i]
        prev = rates[-(i + 1)]
        tr = max(
            curr["high"] - curr["low"],
            abs(curr["high"] - prev["close"]),
            abs(curr["low"]  - prev["close"]),
        )
        true_ranges.append(tr)

    return sum(true_ranges) / len(true_ranges) if true_ranges else 0.0001


def _calc_adx(rates: list, period: int = 14) -> float:
    """
    ADX (Average Directional Index) — метод Wilder.
    Сила тренда (не направление):
      < 20  — боковик  (уровни работают отлично)
      20-40 — умеренный тренд
      > 40  — сильный тренд (уровни чаще пробиваются)
    Возвращает 0.0 если данных недостаточно.
    """
    need = period * 2 + 1
    if len(rates) < need:
        return 0.0

    bars = rates[-need:]
    dm_plus_list  = []
    dm_minus_list = []
    tr_list       = []

    for i in range(1, len(bars)):
        curr = bars[i]
        prev = bars[i - 1]
        tr = max(
            curr["high"] - curr["low"],
            abs(curr["high"] - prev["close"]),
            abs(curr["low"]  - prev["close"]),
        )
        tr_list.append(tr)
        up   = curr["high"] - prev["high"]
        down = prev["low"]  - curr["low"]
        dm_plus_list.append(up   if (up > down and up > 0)   else 0.0)
        dm_minus_list.append(down if (down > up and down > 0) else 0.0)

    if len(tr_list) < period:
        return 0.0

    # Wilder smoothing
    atr_s = sum(tr_list[:period])
    dmp_s = sum(dm_plus_list[:period])
    dmm_s = sum(dm_minus_list[:period])

    def _dx(dmp, dmm, atr):
        if atr == 0: return 0.0
        di_p = 100 * dmp / atr
        di_m = 100 * dmm / atr
        s    = di_p + di_m
        return 100 * abs(di_p - di_m) / s if s else 0.0

    dx_list = [_dx(dmp_s, dmm_s, atr_s)]
    for i in range(period, len(tr_list)):
        atr_s = atr_s - atr_s / period + tr_list[i]
        dmp_s = dmp_s - dmp_s / period + dm_plus_list[i]
        dmm_s = dmm_s - dmm_s / period + dm_minus_list[i]
        dx_list.append(_dx(dmp_s, dmm_s, atr_s))

    if len(dx_list) < period:
        return 0.0
    return round(sum(dx_list[-period:]) / period, 2)


def _detect_role_reversal(rates: list, price: float,
                           direction: str,
                           atr_zone: float) -> int:
    """
    Определяет смену роли уровня (Support ↔ Resistance).

    Support → был Resistance:
      В первой половине истории цена торговалась ВЫШЕ уровня.
      В последней четверти — торгуется НИЖЕ и отскакивает вверх.

    Resistance → был Support:
      В первой половине истории цена торговалась НИЖЕ уровня.
      В последней четверти — торгуется ВЫШЕ и отскакивает вниз.

    Критерий: ≥5 баров по "старую" сторону + ≥5 баров по "новую" сторону
    + реальное пересечение уровня в середине истории.
    Возвращает 1 если смена роли обнаружена, иначе 0.
    """
    if len(rates) < 30:
        return 0

    zone_top    = price + atr_zone
    zone_bottom = price - atr_zone
    n           = len(rates)
    half        = n // 2
    quarter     = n * 3 // 4

    if direction == "Support":
        # Раньше был Resistance: цена выше уровня (≥5 баров)
        above_old = sum(1 for b in rates[:half] if b["close"] > zone_top)
        # Теперь Support: цена ниже уровня (≥5 баров)
        below_new = sum(1 for b in rates[quarter:] if b["close"] < zone_top)
        # Обязательно: бар пересёк уровень снизу вверх в середине истории
        crossed = any(
            rates[i]["close"] < zone_bottom and rates[i+1]["close"] > zone_top
            for i in range(half - 1, quarter)
            if i + 1 < len(rates)
        )
        return 1 if (above_old >= 5 and below_new >= 5 and crossed) else 0
    else:
        # Раньше был Support: цена ниже уровня (≥5 баров)
        below_old = sum(1 for b in rates[:half] if b["close"] < zone_bottom)
        # Теперь Resistance: цена выше уровня (≥5 баров)
        above_new = sum(1 for b in rates[quarter:] if b["close"] > zone_bottom)
        # Обязательно: бар пересёк уровень сверху вниз в середине истории
        crossed = any(
            rates[i]["close"] > zone_top and rates[i+1]["close"] < zone_bottom
            for i in range(half - 1, quarter)
            if i + 1 < len(rates)
        )
        return 1 if (below_old >= 5 and above_new >= 5 and crossed) else 0


def _is_local_high(rates: list, idx: int,
                    left: int, right: int) -> bool:
    if idx < left or idx > len(rates) - 1 - right:
        return False
    high = rates[idx]["high"]
    for j in range(idx - left, idx + right + 1):
        if j == idx:
            continue
        if rates[j]["high"] >= high:
            return False
    return True


def _is_local_low(rates: list, idx: int,
                   left: int, right: int) -> bool:
    if idx < left or idx > len(rates) - 1 - right:
        return False
    low = rates[idx]["low"]
    for j in range(idx - left, idx + right + 1):
        if j == idx:
            continue
        if rates[j]["low"] <= low:
            return False
    return True



def _calc_ema(rates: list, period: int) -> float:
    """
    FIX-7: EMA (Exponential Moving Average) на закрытиях.
    Возвращает последнее значение EMA.
    """
    if not rates or len(rates) < period:
        return 0.0
    closes = [r["close"] for r in rates]
    k = 2.0 / (period + 1)
    ema = sum(closes[:period]) / period  # SMA as seed
    for c in closes[period:]:
        ema = c * k + ema * (1 - k)
    return ema


def _calc_ema_score(rates: list) -> Tuple[bool, bool, float]:
    """
    FIX-7: Рассчитывает EMA50, EMA200 и ema_score.
    Возвращает (matches_ema50, matches_ema200, ema_score).
    ema_score: 0.0–1.0 — насколько цена близка к динамическим уровням.
    """
    if not rates or len(rates) < 10:
        return False, False, 0.0

    last_close = rates[-1]["close"]
    last_high  = rates[-1]["high"]
    last_low   = rates[-1]["low"]

    ema50  = _calc_ema(rates, 50)  if len(rates) >= 50  else 0.0
    ema200 = _calc_ema(rates, 200) if len(rates) >= 200 else 0.0
    atr    = _calc_atr(rates, min(14, len(rates)-1))

    tolerance = atr * 0.3  # ±30% ATR от EMA = "совпадение"

    matches_50  = (ema50  > 0 and abs(last_close - ema50)  <= tolerance)
    matches_200 = (ema200 > 0 and abs(last_close - ema200) <= tolerance)

    # ema_score: 1.0 если цена у EMA50 И EMA200, 0.5 если у одной, 0.0 иначе
    score = 0.0
    if matches_50 and matches_200:
        score = 1.0
    elif matches_50 or matches_200:
        score = 0.5

    return matches_50, matches_200, score

def _calc_rsi(rates: list, period: int = 14) -> float:
    """
    RSI-14 (Relative Strength Index, Wilder) по барам OHLCV.
    Возвращает значение 0..100, или 50.0 при нехватке баров.
    Хранится в raw_levels.rsi_value для последующего использования
    Analyzer-ом и Signal Engine.
    """
    if not rates or len(rates) < period + 1:
        return 50.0
    closes = [float(b["close"]) for b in rates]
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains  = [max(d, 0)   for d in deltas]
    losses = [abs(min(d, 0)) for d in deltas]
    avg_g  = sum(gains[:period])  / period
    avg_l  = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + gains[i])  / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    return round(100.0 - 100.0 / (1.0 + avg_g / avg_l), 2)


def _avg_volume(rates: list) -> int:
    if not rates:
        return 0
    return int(sum(r["volume"] for r in rates) / len(rates))


def _analyze_touches(rates: list, price: float,
                      atr_zone: float,
                      symbol: str) -> Tuple[int, int, float, Optional[datetime]]:
    """
    V-7: объединённая функция — считает касания И находит время последнего.
    C-5: использует _pip_mult для корректных пипсов.

    Возвращает: (bounce_count, last_touch_volume, avg_bounce_pips, last_touch_time)
    """
    zone_top    = price + atr_zone
    zone_bottom = price - atr_zone
    mult        = _pip_mult(symbol)

    touches     = []
    last_touch_time: Optional[datetime] = None

    for i, bar in enumerate(rates):
        low   = bar["low"]
        high  = bar["high"]
        close = bar["close"]
        open_ = bar["open"]

        touched_from_below = (low  <= zone_top    and low  >= zone_bottom)
        touched_from_above = (high >= zone_bottom and high <= zone_top)
        pierced_through    = (low  <= zone_bottom and high >= zone_top)

        is_touch = False

        if touched_from_below:
            if close > zone_bottom:
                pips = abs(close - open_) * mult
                touches.append({"volume": bar["volume"], "pips": pips})
                is_touch = True

        elif touched_from_above:
            if close < zone_top:
                pips = abs(close - open_) * mult
                touches.append({"volume": bar["volume"], "pips": pips})
                is_touch = True

        elif pierced_through:
            # П-4 ИСПРАВЛЕНО: проверяем следующий бар (подтверждение отскока)
            if i + 1 < len(rates):
                next_bar   = rates[i + 1]
                next_close = next_bar["close"]
                # Бар пронзил зону — смотрим куда закрылся СЛЕДУЮЩИЙ бар
                if next_close > zone_top:
                    # Пробой вверх подтверждён → был Support → касание снизу
                    pips = abs(next_close - next_bar["open"]) * mult
                    touches.append({"volume": next_bar["volume"], "pips": pips})
                    is_touch = True
                elif next_close < zone_bottom:
                    # Пробой вниз подтверждён → был Resistance → касание сверху
                    pips = abs(next_close - next_bar["open"]) * mult
                    touches.append({"volume": next_bar["volume"], "pips": pips})
                    is_touch = True

        # Обновляем last_touch_time при любом касании зоны (шире чем bounce)
        if (bar["low"] <= zone_top and bar["high"] >= zone_bottom):
            last_touch_time = bar["time"]

    if not touches:
        return 0, 0, 0.0, last_touch_time

    last     = touches[-1]
    avg_pips = sum(t["pips"] for t in touches) / len(touches)
    return len(touches), last["volume"], round(avg_pips, 1), last_touch_time


# ------------------------------------------------------------------
# C-2: нормализация времени D-баров
# ------------------------------------------------------------------

def _normalize_bar_time(bar_time: datetime, timeframe: str) -> datetime:
    """
    C-2: для D таймфрейма нормализуем время до полуночи UTC.
    Некоторые брокеры отдают D-бары с timestamp 21:00 или 22:00 UTC.
    """
    if timeframe.upper() == "D":
        return bar_time.replace(hour=0, minute=0, second=0, microsecond=0)
    return bar_time


# ------------------------------------------------------------------
# Основной алгоритм определения уровней
# ------------------------------------------------------------------

def find_levels(rates: list, symbol: str, timeframe: str) -> dict:
    """
    Найти уровни Support и Resistance из OHLCV баров.

    Возвращает dict с ключами:
        "levels" — list of dicts для UPSERT
        "stats"  — статистика для лога (V-5, U-5)
    """
    tf_upper  = timeframe.upper()
    atr_period = ATR_PERIOD_BY_TF.get(tf_upper, ATR_PERIOD_DEFAULT)
    min_bars   = MIN_BARS_BY_TF.get(tf_upper, MIN_BARS_DEFAULT)

    stats = {
        "total_bars":     len(rates),
        "invalid_bars":   0,
        "gaps":           0,
        "extrema_found":  0,
        "skip_b0":        0,
        "levels_built":   0,
    }

    # V-1: фильтруем невалидные бары
    valid_rates = []
    for bar in rates:
        if _is_valid_bar(bar):
            valid_rates.append(bar)
        else:
            stats["invalid_bars"] += 1

    if stats["invalid_bars"] > 0:
        logger.warning(
            f"{symbol}/{timeframe}: отфильтровано "
            f"{stats['invalid_bars']} невалидных баров"
        )

    # V-3: проверка минимального порога
    if len(valid_rates) < min_bars:
        logger.warning(
            f"{symbol}/{timeframe}: мало баров "
            f"{len(valid_rates)} < {min_bars} — пропускаем"
        )
        return {"levels": [], "stats": stats}

    # Дополнительная проверка — нужны соседи для экстремумов
    if len(valid_rates) < atr_period + EXTREMUM_LEFT + EXTREMUM_RIGHT + 5:
        logger.warning(
            f"Недостаточно баров для алгоритма: "
            f"{symbol}/{timeframe}: {len(valid_rates)}"
        )
        return {"levels": [], "stats": stats}

    # V-2: детекция временных разрывов
    gaps = _detect_gaps(valid_rates, tf_upper)
    stats["gaps"] = len(gaps)
    if gaps:
        gap_str = ", ".join(
            f"{g[0].strftime('%m-%d %H:%M')}→{g[1].strftime('%m-%d %H:%M')}"
            for g in gaps[:3]
        )
        logger.warning(
            f"{symbol}/{timeframe}: "
            f"{len(gaps)} временных разрывов: {gap_str}"
        )

    # Вычисляем ATR и ADX один раз для всего набора баров
    atr      = calc_atr(valid_rates, atr_period)
    mult_z   = ATR_ZONE_MULT.get(tf_upper, ATR_ZONE_MULT_DEFAULT)
    atr_zone = atr * mult_z
    avg_vol  = _avg_volume(valid_rates)
    adx_val  = calc_adx(valid_rates, period=14)   # Этап 6: реальный ADX

    # U-3: мониторинг дрейфа ATR
    _check_atr_drift(symbol, tf_upper, atr)

    # БАГ-2 ИСПРАВЛЕНО: EMA/RSI вычисляются для каждого уровня отдельно
    # (не один раз для всего набора — уровни в разных ценовых зонах!)
    # Глобальные вычисления только для данных не зависящих от цены уровня:
    _ema50_val  = calc_ema(valid_rates, 50)  if len(valid_rates) >= 50  else 0.0
    _ema200_val = calc_ema(valid_rates, 200) if len(valid_rates) >= 200 else 0.0
    _rsi_val    = calc_rsi(valid_rates, period=14)  # RSI глобальный (одно значение на бар)
    _atr_tol    = atr * 0.3  # допуск ±30% ATR для совпадения с EMA

    seen_zones = set()
    levels     = []

    start = EXTREMUM_LEFT
    end   = len(valid_rates) - EXTREMUM_RIGHT - 1

    for idx in range(start, end):
        bar = valid_rates[idx]

        # ── Resistance ──────────────────────────────────────
        if _is_local_high(valid_rates, idx, EXTREMUM_LEFT, EXTREMUM_RIGHT):
            stats["extrema_found"] += 1
            price = bar["high"]
            zone  = price_to_zone(symbol, price)
            key   = (zone, "Resistance")

            if key not in seen_zones:
                seen_zones.add(key)
                b_count, last_vol, avg_pips, last_touch = _analyze_touches(
                    valid_rates, price, atr_zone, symbol)

                # V-5: пропускаем уровни без подтверждённых отскоков
                if b_count < MIN_BOUNCE_TO_RECORD:
                    stats["skip_b0"] += 1
                    continue

                # C-2: нормализуем время
                touch_time = _normalize_bar_time(
                    last_touch or bar["time"], tf_upper)

                # БАГ-2: EMA совпадение по ЦЕНЕ КОНКРЕТНОГО УРОВНЯ
                _m50_r  = _ema50_val  > 0 and abs(zone - _ema50_val)  <= _atr_tol
                _m200_r = _ema200_val > 0 and abs(zone - _ema200_val) <= _atr_tol
                _ema_sc = (1.0 if (_m50_r and _m200_r) else
                           0.5 if (_m50_r or  _m200_r) else 0.0)
                levels.append({
                    "symbol":            symbol,
                    "timeframe":         timeframe,
                    "price_level":       round(price, 6),
                    "price_zone":        zone,
                    "direction":         "Resistance",
                    "bounce_count":      b_count,
                    "last_touch_time":   touch_time,
                    "last_touch_volume": last_vol or bar["volume"],
                    "avg_volume":        avg_vol,
                    "confluence_count":  0,
                    "tf_confirmed_count": 1,
                    "is_role_reversal":  _detect_role_reversal(
                        valid_rates, price, "Resistance", atr_zone),
                    "adx_value":         adx_val,
                    "matches_ema50":     1 if _m50_r  else 0,
                    "matches_ema200":    1 if _m200_r else 0,
                    "ema_score":         _ema_sc,
                    "rsi_value":         _rsi_val,
                    "avg_bounce_pips":   avg_pips,
                })

        # ── Support ─────────────────────────────────────────
        if _is_local_low(valid_rates, idx, EXTREMUM_LEFT, EXTREMUM_RIGHT):
            stats["extrema_found"] += 1
            price = bar["low"]
            zone  = price_to_zone(symbol, price)
            key   = (zone, "Support")

            if key not in seen_zones:
                seen_zones.add(key)
                b_count, last_vol, avg_pips, last_touch = _analyze_touches(
                    valid_rates, price, atr_zone, symbol)

                if b_count < MIN_BOUNCE_TO_RECORD:
                    stats["skip_b0"] += 1
                    continue

                touch_time = _normalize_bar_time(
                    last_touch or bar["time"], tf_upper)

                # БАГ-2: EMA совпадение по ЦЕНЕ КОНКРЕТНОГО УРОВНЯ
                _m50_s  = _ema50_val  > 0 and abs(zone - _ema50_val)  <= _atr_tol
                _m200_s = _ema200_val > 0 and abs(zone - _ema200_val) <= _atr_tol
                _ema_ss = (1.0 if (_m50_s and _m200_s) else
                           0.5 if (_m50_s or  _m200_s) else 0.0)
                levels.append({
                    "symbol":            symbol,
                    "timeframe":         timeframe,
                    "price_level":       round(price, 6),
                    "price_zone":        zone,
                    "direction":         "Support",
                    "bounce_count":      b_count,
                    "last_touch_time":   touch_time,
                    "last_touch_volume": last_vol or bar["volume"],
                    "avg_volume":        avg_vol,
                    "confluence_count":  0,
                    "tf_confirmed_count": 1,
                    "is_role_reversal":  _detect_role_reversal(
                        valid_rates, price, "Support", atr_zone),
                    "adx_value":         adx_val,
                    "matches_ema50":     1 if _m50_s  else 0,
                    "matches_ema200":    1 if _m200_s else 0,
                    "ema_score":         _ema_ss,
                    "rsi_value":         _rsi_val,
                    "avg_bounce_pips":   avg_pips,
                })

    # Сортируем по свежести, берём топ MAX_LEVELS_PER_TF
    _epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    levels.sort(
        key=lambda x: x["last_touch_time"] if x["last_touch_time"] else _epoch,
        reverse=True
    )
    levels = levels[:MAX_LEVELS_PER_TF]
    stats["levels_built"] = len(levels)

    return {"levels": levels, "stats": stats}


# ------------------------------------------------------------------
# CollectorModule
# ------------------------------------------------------------------

class CollectorModule(BaseModule):

    def __init__(self):
        super().__init__("collector")
        self._thread:      Optional[threading.Thread] = None
        self._stop_event   = threading.Event()
        self._cfg          = _load_config()
        self._db           = get_db()
        self._bridge       = get_mt5_bridge()
        # U-1: кеш проверенных символов брокера
        self._symbol_map:  Dict[str, Optional[str]] = {}
        # V-6: время последнего бара по пара/ТФ
        self._last_bar:    Dict[str, datetime] = {}

    # ----------------------------------------------------------
    # BaseModule interface
    # ----------------------------------------------------------

    def start(self) -> None:
        if self._running:
            logger.warning("Collector уже запущен")
            return
        self._cfg = _load_config()
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="collector-thread",
            daemon=True
        )
        self._set_running(True)
        self._thread.start()
        logger.info(
            f"Collector v2.0.0 запущен "
            f"(интервал: {self._cfg['interval_sec']}с, "
            f"пары: {self._cfg['symbols']}, ТФ: {self._cfg['timeframes']})"
        )

    def stop(self) -> None:
        if not self._running:
            return
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=30)
        self._set_running(False)
        logger.info("Collector остановлен")

    def run_once(self) -> bool:
        self._mark_run_start()
        logger.info("Collector: запуск цикла сбора...")
        try:
            result = self._collect()
            if result:
                self._mark_success()
            else:
                self._mark_error("_collect() вернул False")
            return result
        except Exception as e:
            self._mark_error(str(e))
            logger.error(f"Collector run_once ошибка: {e}", exc_info=True)
            return False

    def get_config(self) -> dict:
        return self._cfg

    # ----------------------------------------------------------
    # Internal loop
    # ----------------------------------------------------------

    def _run_loop(self) -> None:
        logger.info("Collector: фоновый поток запущен")
        while not self._stop_event.is_set():
            self.run_once()
            self._stop_event.wait(timeout=self._cfg["interval_sec"])
        logger.info("Collector: фоновый поток завершён")

    # ----------------------------------------------------------
    # Основная логика сбора
    # ----------------------------------------------------------

    def _collect(self) -> bool:
        cfg   = _load_config()
        self._cfg = cfg
        symbols   = cfg["symbols"]
        bars      = cfg["bars_to_fetch"]
        interval  = cfg["interval_sec"]

        total_saved  = 0
        total_errors = 0
        cycle_start  = time.time()
        # U-2: дедлайн цикла
        cycle_deadline = cycle_start + interval * _CYCLE_MAX_FRACTION

        if not self._bridge.monitor.is_connected:
            logger.warning("Collector: MT5 недоступен — пропускаем цикл")
            return False

        for symbol in symbols:
            for tf in cfg["timeframes"]:
                if self._stop_event.is_set():
                    logger.info("Collector: штатная остановка")
                    return True

                # U-2: проверка таймаута цикла
                if time.time() > cycle_deadline:
                    logger.warning(
                        f"Collector: превышен таймаут цикла "
                        f"({int(interval * _CYCLE_MAX_FRACTION)}с) — "
                        f"прерываем на {symbol}/{tf}"
                    )
                    break

                saved, errors = self._process_symbol_tf(symbol, tf, bars)
                total_saved  += saved
                total_errors += errors

        # V-6: проверка синхронности ТФ
        self._check_tf_sync(symbols)

        elapsed = int(time.time() - cycle_start)
        logger.info(
            f"Collector цикл завершён за {elapsed}с: "
            f"сохранено {total_saved} уровней, ошибок {total_errors}"
        )
        self._db.log_to_db(
            "collector", "INFO",
            f"Цикл {elapsed}с: +{total_saved} уровней, ошибок: {total_errors}"
        )
        return total_errors == 0

    def _process_symbol_tf(self, symbol: str,
                            tf: str, bars: int) -> Tuple[int, int]:
        """
        C-3: Обработать пару/ТФ с retry при обрыве соединения.
        C-4: Batch-транзакция для записи уровней.
        """
        # U-1: проверяем символ брокера
        real_symbol = self._resolve_symbol(symbol)
        if real_symbol is None:
            # Символ не найден — это проблема конфигурации, не ошибка сбора.
            # Не увеличиваем total_errors чтобы не блокировать цикл.
            logger.error(
                f"Символ {symbol} недоступен в MT5 — пропускаем. "
                f"Проверь название в MarketWatch и config.ini [COLLECTOR] symbols."
            )
            return 0, 0

        # C-3: retry при ошибке соединения
        for attempt in range(1, _CONN_RETRY_COUNT + 2):
            try:
                return self._do_process(real_symbol, symbol, tf, bars)
            except ConnectionError as e:
                if attempt <= _CONN_RETRY_COUNT:
                    logger.warning(
                        f"Соединение MT5 потеряно {real_symbol}/{tf} "
                        f"(попытка {attempt}/{_CONN_RETRY_COUNT}): {e}. "
                        f"Ждём {_CONN_RETRY_DELAY}с..."
                    )
                    time.sleep(_CONN_RETRY_DELAY)
                else:
                    logger.error(
                        f"Не удалось получить бары {real_symbol}/{tf} "
                        f"после {_CONN_RETRY_COUNT} попыток — пропускаем"
                    )
                    return 0, 0   # пропуск, не ошибка
            except Exception as e:
                logger.error(
                    f"_process_symbol_tf {real_symbol}/{tf}: {e}",
                    exc_info=True
                )
                return 0, 1

        return 0, 0

    def _do_process(self, real_symbol: str, config_symbol: str,
                     tf: str, bars: int) -> Tuple[int, int]:
        """
        Внутренний метод: получить бары → найти уровни → batch UPSERT.
        """
        # C-1: retry при неполных барах
        min_required = MIN_BARS_BY_TF.get(tf.upper(), MIN_BARS_DEFAULT)
        rates = None

        for attempt in range(1, _RATES_RETRY_COUNT + 1):
            with self._bridge.session() as mt5:
                rates = self._bridge.get_rates(mt5, real_symbol, tf, bars=bars)

            if rates and len(rates) >= min_required:
                break

            got = len(rates) if rates else 0
            logger.warning(
                f"C-1: {real_symbol}/{tf}: баров {got} < {min_required} "
                f"(попытка {attempt}/{_RATES_RETRY_COUNT}) — "
                f"ждём {_RATES_RETRY_DELAY}с..."
            )
            if attempt < _RATES_RETRY_COUNT:
                time.sleep(_RATES_RETRY_DELAY)

        if not rates:
            logger.warning(f"Нет баров для {real_symbol}/{tf}")
            return 0, 1

        if len(rates) < min_required:
            logger.warning(
                f"{real_symbol}/{tf}: после {_RATES_RETRY_COUNT} попыток "
                f"баров {len(rates)} < {min_required} — строим уровни на том что есть"
            )

        # Запоминаем время последнего бара (V-6)
        self._last_bar[f"{config_symbol}/{tf}"] = rates[-1]["time"]

        # Нормализуем символ обратно для записи в БД
        result = find_levels(rates, config_symbol, tf)
        levels = result["levels"]
        stats  = result["stats"]

        # U-5: расширенный лог
        atr_period = ATR_PERIOD_BY_TF.get(tf.upper(), ATR_PERIOD_DEFAULT)
        atr_val    = calc_atr(rates, atr_period)
        logger.info(
            f"  {config_symbol:8} {tf:4} | "
            f"баров: {len(rates):4} | "
            f"ATR: {atr_val:.5f} | "
            f"ADX: {calc_adx(rates):4.1f} | "
            f"уровней: {len(levels):3} | "
            f"skip_B0: {stats['skip_b0']:2} | "
            f"gaps: {stats['gaps']:1} | "
            f"bad_bars: {stats['invalid_bars']:1}"
        )

        if not levels:
            return 0, 0

        # C-4: batch-транзакция — все уровни пары/ТФ = одна транзакция
        saved  = 0
        errors = 0
        try:
            with self._db.transaction() as tx:
                for lvl in levels:
                    tx.upsert(
                        table="raw_levels",
                        insert_data=lvl,
                        update_data={
                            "bounce_count":      lvl["bounce_count"],
                            "last_touch_time":   lvl["last_touch_time"],
                            "last_touch_volume": lvl["last_touch_volume"],
                            "avg_volume":        lvl["avg_volume"],
                            "avg_bounce_pips":   lvl["avg_bounce_pips"],
                            "price_level":       lvl["price_level"],
                            "adx_value":         lvl["adx_value"],
                            "is_role_reversal":  lvl["is_role_reversal"],
                        }
                    )
                    saved += 1
        except Exception as e:
            logger.warning(
                f"Batch UPSERT {config_symbol}/{tf} откат: {e} "
                f"— пробуем по одному"
            )
            # Fallback: сохраняем по одному без транзакции
            saved = errors = 0
            for lvl in levels:
                try:
                    self._db.upsert(
                        table="raw_levels",
                        insert_data=lvl,
                        update_data={
                            "bounce_count":      lvl["bounce_count"],
                            "last_touch_time":   lvl["last_touch_time"],
                            "last_touch_volume": lvl["last_touch_volume"],
                            "avg_volume":        lvl["avg_volume"],
                            "avg_bounce_pips":   lvl["avg_bounce_pips"],
                            "price_level":       lvl["price_level"],
                            "adx_value":         lvl["adx_value"],
                            "is_role_reversal":  lvl["is_role_reversal"],
                        }
                    )
                    saved += 1
                except Exception:
                    errors += 1

        return saved, errors

    # ----------------------------------------------------------
    # U-1: валидация символа брокера
    # ----------------------------------------------------------

    def _resolve_symbol(self, symbol: str) -> Optional[str]:
        """
        Проверяет доступность символа в MT5.
        Пробует варианты суффиксов брокера.
        Результат кешируется на всё время жизни модуля.
        """
        if symbol in self._symbol_map:
            return self._symbol_map[symbol]

        try:
            with self._bridge.session() as mt5:
                # 1. Точное совпадение
                info = mt5.symbol_info(symbol)
                if info:
                    # Символ найден — убеждаемся что он виден в MarketWatch
                    if not info.visible:
                        mt5.symbol_select(symbol, True)
                    self._symbol_map[symbol] = symbol
                    return symbol

                # 2. Пробуем типичные суффиксы брокеров
                suffixes = ["m", ".m", "i", ".i", "pro", ".pro",
                            "_", "c", ".c", "n", ".n"]
                for suffix in suffixes:
                    candidate = symbol + suffix
                    info = mt5.symbol_info(candidate)
                    if info:
                        if not info.visible:
                            mt5.symbol_select(candidate, True)
                        logger.info(
                            f"Символ {symbol} → {candidate} "
                            f"(суффикс брокера '{suffix}')"
                        )
                        self._symbol_map[symbol] = candidate
                        return candidate

                # 3. Ищем по частичному совпадению в списке символов
                all_symbols = mt5.symbols_get()
                if all_symbols:
                    sym_upper = symbol.upper()
                    matches = [
                        s.name for s in all_symbols
                        if s.name.upper().startswith(sym_upper)
                        and len(s.name) <= len(symbol) + 3
                    ]
                    if matches:
                        candidate = matches[0]
                        logger.info(
                            f"Символ {symbol} → {candidate} "
                            f"(поиск по списку брокера)"
                        )
                        self._symbol_map[symbol] = candidate
                        return candidate

                logger.error(
                    f"Символ {symbol} не найден в MT5. "
                    f"Проверь название в MarketWatch терминала."
                )
                self._symbol_map[symbol] = None
                return None

        except Exception as e:
            logger.error(f"_resolve_symbol {symbol}: {e}")
            # При ошибке проверки — пробуем оригинал
            return symbol

    # ----------------------------------------------------------
    # V-6: синхронность таймфреймов
    # ----------------------------------------------------------

    def _check_tf_sync(self, symbols: list) -> None:
        """
        Проверяет что данные по ТФ для каждой пары синхронны.

        Пороги:
          H4 vs H1 → допускаем до 8 часов (жёсткий)
          D  vs H1 → допускаем до 240 часов (мягкий)

        Почему 240 часов для D:
          Брокеры (особенно для JPY и AUD пар) открывают D-бар
          в 22:00 UTC предыдущего дня (сессия Азия/Токио).
          MT5 возвращает этот timestamp как время бара.
          При нормализации до полуночи разница между D-баром
          и H1-баром по timestamp может достигать 200+ часов —
          это норма, не ошибка. Реальная проблема — если
          расхождение > 10 дней (240 часов).
        """
        timeframes = self._cfg.get("timeframes", ["D", "H4", "H1"])
        for symbol in symbols:
            times = {}
            for tf in timeframes:
                key = f"{symbol}/{tf}"
                if key in self._last_bar:
                    t = self._last_bar[key]
                    # Нормализуем D-бар до полуночи для корректного сравнения
                    if tf == "D":
                        t = t.replace(hour=0, minute=0, second=0, microsecond=0)
                    times[tf] = t

            if len(times) < 2:
                continue

            # Проверяем H4 vs H1 (жёсткий порог — 8 часов)
            if "H4" in times and "H1" in times:
                delta_h = abs(
                    (times["H4"] - times["H1"]).total_seconds()
                ) / 3600
                if delta_h > 8:
                    logger.warning(
                        f"V-6 TF sync {symbol}: "
                        f"H4/H1 расхождение {delta_h:.1f}ч "
                        f"(H4:{times['H4'].strftime('%m-%d %H:%M')} "
                        f"H1:{times['H1'].strftime('%m-%d %H:%M')})"
                    )

            # Проверяем D vs H1
            # Порог 240 часов (10 дней) — учитывает особенности брокеров
            # с JPY/AUD парами где D-бар открывается в 22:00 UTC
            if "D" in times and "H1" in times:
                delta_d = abs(
                    (times["D"] - times["H1"]).total_seconds()
                ) / 3600
                if delta_d > 240:
                    logger.warning(
                        f"V-6 TF sync {symbol}: "
                        f"D/H1 расхождение {delta_d:.1f}ч (>{240}ч) — "
                        f"история D возможно устарела более чем на 10 дней"
                    )
                elif delta_d > 48:
                    # Информационный уровень — норма для брокеров с азиатской сессией
                    logger.debug(
                        f"V-6 TF sync {symbol}: "
                        f"D/H1 расхождение {delta_d:.1f}ч — "
                        f"в пределах нормы для данного брокера"
                    )


# ------------------------------------------------------------------
# Фабричная функция
# ------------------------------------------------------------------
def get_module() -> CollectorModule:
    return CollectorModule()
