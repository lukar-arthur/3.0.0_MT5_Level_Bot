# ============================================================
#  MT5_Level_Bot — modules/signal/signal_engine.py
#  Version : 2.0.0
#
#  ДОБАВЛЕНО v2.0.0 — M5 Monitor:
#
#  Два независимых цикла в одном модуле:
#
#  1. SCAN (каждые 5 мин) — как раньше:
#     Читает analyzed_levels → вычисляет T → создаёт сигнал
#     в signal_queue со статусом 'pending'
#
#  2. M5 MONITOR (каждые 30 сек) — новый:
#     Берёт 'pending' сигналы → получает 3 бара M5 из MT5 →
#     проверяет подтверждение свечой M5 →
#     если подтверждено → статус 'confirmed' + уведомление
#
#  Логика подтверждения M5:
#    Support:    последняя M5 свеча бычья И закрылась выше зоны
#    Resistance: последняя M5 свеча медвежья И закрылась ниже зоны
#    + объём M5 последних 2 баров > среднего
#    + цена всё ещё в зоне ±8 пипсов
#
#  Статусы сигнала:
#    pending   → создан Signal Engine, ждёт подтверждения M5
#    confirmed → подтверждён M5 Monitor, готов к открытию
#    opened    → трейдер нажал "Открыть сделку"
#    closed    → сделка закрыта (заполняется вручную)
#    expired   → истёк TTL (30 мин) без подтверждения
#    cancelled → трейдер нажал "Пропустить"
# ============================================================

import configparser
import os
import threading
from datetime import datetime, timezone, timedelta
from typing   import Dict, List, Optional, Tuple

from core.base_module    import BaseModule
from core.db_connection  import get_db
from core.mt5_bridge     import get_mt5_bridge
from core.utils          import (get_logger, safe_normalize,
                                  calc_atr, calc_adx, calc_ema,
                                  ATR_PERIOD_BY_TF)
from core.config_loader  import load_module_config

# Plug-and-Play: scalping использует config/scalping.ini
_MODULE_CFG = load_module_config("scalping")

logger = get_logger("signal")

# __file__ = .../modules/strategies/scalping/signal_engine.py
# 4 x dirname → project root
# Plug-and-Play: путь конфига — через config_loader

# ── Параметры ────────────────────────────────────────────────
_MIN_S_SCORE        = 7.5   # минимальный S для рассмотрения
_MIN_T_SCORE        = 0.70  # минимальный T для создания сигнала
_PROXIMITY_PIPS     = 10    # зона ±10 пипсов (fallback)
_PROXIMITY_M5_PIPS  = 8     # зона ±8 пипсов (M5 Monitor — строже)
# Адаптивный proximity: ATR × множитель по ТФ
# H1: ATR×0.5 ≈ 8-10п | H4: ATR×0.3 ≈ 13п | D: ATR×0.2 ≈ 20п
_PROX_ATR_MULT: dict = {"H1": 0.5, "H4": 0.3, "D": 0.2}
_PROX_ATR_DEFAULT    = 0.4

# ── Корреляционные конфликты ──────────────────────────────────
# Если открыта сделка (пара1, направление1), то блокируем
# создание сигнала (пара2, направление2).
# Логика: одна валюта не может двигаться в двух направлениях.
#
# GBP риск:  GBPUSD Buy  ↔  EURGBP Sell  (оба: GBP растёт)
#            GBPUSD Sell ↔  EURGBP Buy   (оба: GBP падает)
# USD риск:  EURUSD Buy  +  GBPUSD Buy  = двойная ставка на USD падение
#            EURUSD Sell +  USDCHF Buy  = двойная ставка на USD рост
# EUR риск:  EURUSD Buy  ↔  EURGBP Buy  (оба: EUR растёт)
#
# Формат: (пара_открытая, направление_открытое) → {(пара_блок, направление_блок)}
# Валютный риск: не открывать две сделки ставящие на одну валюту.
#
# EUR риск:
#   EURUSD Support + EURGBP Support = обе: EUR растёт → двойная ставка
#   EURUSD Resistance + EURGBP Resistance = обе: EUR падает
#
# GBP риск:
#   GBPUSD Support + EURGBP Resistance = обе: GBP растёт
#   GBPUSD Resistance + EURGBP Support = обе: GBP падает
#
# USD риск:
#   EURUSD Support + GBPUSD Support = USD падает у обеих
#   EURUSD Support + USDCHF Resistance = USD падает
#
_CORR_CONFLICTS: dict = {
    ("GBPUSD", "Support"):    {("EURGBP", "Resistance"), ("EURUSD", "Support")},
    ("GBPUSD", "Resistance"): {("EURGBP", "Support"),    ("EURUSD", "Resistance")},
    # EUR риск: EURGBP Support блокирует EURUSD Support (обе = EUR вверх)
    ("EURGBP", "Support"):    {("GBPUSD", "Resistance"), ("EURUSD", "Support")},
    ("EURGBP", "Resistance"): {("GBPUSD", "Support"),    ("EURUSD", "Resistance")},
    # USD риск + EUR риск
    ("EURUSD", "Support"):    {("GBPUSD", "Support"),    ("EURGBP", "Support"),
                               ("USDCHF", "Resistance")},
    ("EURUSD", "Resistance"): {("GBPUSD", "Resistance"), ("EURGBP", "Resistance"),
                               ("USDCHF", "Support")},
    ("USDCHF", "Support"):    {("EURUSD", "Resistance"), ("AUDUSD", "Resistance")},
    ("USDCHF", "Resistance"): {("EURUSD", "Support"),    ("AUDUSD", "Support")},
    ("AUDUSD", "Support"):    {("USDCHF", "Resistance")},
    ("AUDUSD", "Resistance"): {("USDCHF", "Support")},
}
_ADX_MAX_SIDEWAYS   = 25.0
_SIGNAL_TTL_MIN     = 30    # сигнал истекает через 30 минут
_SESSION_START_UTC  = 7     # 11:00 Ереван = 07:00 UTC
_SESSION_END_UTC    = 16    # 20:00 Ереван = 16:00 UTC (включает EU/US overlap)
_SL_ATR_MULT        = 1.0
_TP_ATR_MULT        = 1.5
_VOL_SPIKE_MIN      = 1.2
_M5_MONITOR_SEC     = 30    # интервал M5 Monitor


def _load_config() -> dict:
    """Plug-and-Play: читает из config/scalping.ini (fallback: config.ini)."""
    cfg = _MODULE_CFG
    cfg.reload()
    return {
        "interval_sec":   cfg.getint  ("SIGNAL", "interval_sec",   fallback=300),
        "min_s_score":    cfg.getfloat("SIGNAL", "min_s_score",    fallback=_MIN_S_SCORE),
        "min_t_score":    cfg.getfloat("SIGNAL", "min_t_score",    fallback=_MIN_T_SCORE),
        "proximity_pips": cfg.getint  ("SIGNAL", "proximity_pips", fallback=_PROXIMITY_PIPS),
        "m5_monitor_sec": cfg.getint  ("SIGNAL", "m5_monitor_sec", fallback=_M5_MONITOR_SEC),
        "sl_atr_mult":    cfg.getfloat("SIGNAL", "sl_atr_mult",    fallback=_SL_ATR_MULT),
        "tp_atr_mult":    cfg.getfloat("SIGNAL", "tp_atr_mult",    fallback=_TP_ATR_MULT),
        "session_start_utc": cfg.getint("SIGNAL", "session_start_utc", fallback=_SESSION_START_UTC),
        "session_end_utc":   cfg.getint("SIGNAL", "session_end_utc",   fallback=_SESSION_END_UTC),
        "adx_max_sideways":  cfg.getfloat("SIGNAL", "adx_max_sideways", fallback=_ADX_MAX_SIDEWAYS),
        "signal_ttl_min":    cfg.getint("SIGNAL", "signal_ttl_min",    fallback=_SIGNAL_TTL_MIN),
    }


def _pip_size(symbol: str) -> float:
    return 0.01 if symbol.upper().endswith("JPY") else 0.0001


def _pips_to_price(symbol: str, pips: int) -> float:
    return pips * _pip_size(symbol)


def _adaptive_proximity(timeframe: str, atr: float,
                         fallback_pips: int,
                         symbol: str) -> float:
    """
    FIX-5: Возвращает proximity в единицах цены.
    Адаптируется к ATR и таймфрейму:
      H1: ATR×0.5  H4: ATR×0.3  D: ATR×0.2
    Не менее fallback_pips и не более 3×fallback.
    """
    pip = _pip_size(symbol)
    if atr and atr > 0:
        mult  = _PROX_ATR_MULT.get(timeframe.upper(), _PROX_ATR_DEFAULT)
        price = atr * mult
        min_p = fallback_pips * pip * 0.5
        max_p = fallback_pips * pip * 4.0
        return max(min_p, min(price, max_p))
    return fallback_pips * _pip_size(symbol)


def _calc_rsi(bars: list, period: int = 14) -> float:
    """
    RSI (Relative Strength Index) по Уайлдеру.
    Возвращает RSI [0..100] или 50.0 если недостаточно баров.

    Интерпретация:
      RSI > 70 → перекупленность (цена перегрета — не покупать)
      RSI < 30 → перепроданность (цена упала — не продавать)
      30..70   → нейтральная зона
    """
    if not bars or len(bars) < period + 1:
        return 50.0

    closes = [float(b["close"]) for b in bars]
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]

    gains  = [max(d, 0) for d in deltas]
    losses = [abs(min(d, 0)) for d in deltas]

    # Первый период — простое среднее (seed)
    avg_gain = sum(gains[:period])  / period
    avg_loss = sum(losses[:period]) / period

    # Уайлдер: EMA с α = 1/period
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i])  / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs  = avg_gain / avg_loss
    return round(100.0 - 100.0 / (1.0 + rs), 1)


# ------------------------------------------------------------------
# Trade Readiness Score (T)
# ------------------------------------------------------------------

def calc_trade_readiness(
    level: dict,
    current_price: float,
    h1_bars: list,
    cfg: dict
) -> Tuple[float, dict]:
    """
    Вычисляет Trade Readiness Score T ∈ [0, 1].

    FIX-2: Добавлен фильтр направления тренда:
      Support в нисходящем тренде (цена < уровня) → штраф
      Resistance в восходящем тренде (цена > уровня) → штраф

    FIX-5: Proximity адаптивный (ATR × множитель по ТФ).

    Компоненты:
      proximity    — цена рядом с уровнем (ATR-адаптивный)
      trend_align  — направление сделки совпадает с трендом (FIX-2)
      adx_context  — боковик выгоднее для уровней
      vol_spike    — объём на касании выше среднего
      freshness    — уровень свежий (недавнее касание)
      candle       — H1 свеча подтверждает направление
    """
    from core.utils import calc_adx, calc_atr, ATR_PERIOD_BY_TF
    zone      = float(level["price_zone"])
    direction = level["direction"]
    symbol    = level["symbol"]
    timeframe = level.get("timeframe", "H1")
    pip       = _pip_size(symbol)
    factors   = {}

    # ── FIX-5: Адаптивный proximity ──────────────────────────
    atr_h1 = 0.0
    if h1_bars and len(h1_bars) >= 15:
        atr_h1 = calc_atr(h1_bars, ATR_PERIOD_BY_TF.get("H1", 20))

    prox_price = _adaptive_proximity(
        timeframe, atr_h1,
        cfg.get("proximity_pips", _PROXIMITY_PIPS),
        symbol
    )

    # 1. Proximity
    dist = abs(current_price - zone)
    factors["proximity"] = max(0.0, 1.0 - dist / prox_price) \
        if dist <= prox_price else 0.0

    # ── П-1 ИСПРАВЛЕНО: настоящая EMA вместо SMA ─────────────
    # Используем calc_ema() из core/utils.py (Wilder EMA)
    # Определяем тренд по EMA20 vs EMA50 на H1
    trend_factor = 1.0
    if h1_bars and len(h1_bars) >= 50:
        ema20      = calc_ema(h1_bars, 20)   # ← настоящая EMA, не SMA
        ema50      = calc_ema(h1_bars, 50)   # ← настоящая EMA, не SMA
        last_close = h1_bars[-1]["close"]

        downtrend = (ema20 < ema50 and last_close < ema50)
        uptrend   = (ema20 > ema50 and last_close > ema50)

        if direction == "Support" and downtrend:
            # Buy против нисходящего тренда → сильный штраф
            trend_factor = 0.3
        elif direction == "Resistance" and uptrend:
            # Sell против восходящего тренда → сильный штраф
            trend_factor = 0.3
        elif (direction == "Support" and uptrend) or \
             (direction == "Resistance" and downtrend):
            # С трендом → бонус
            trend_factor = 1.0
        else:
            # Боковик или неопределённость
            trend_factor = 0.7

    factors["trend_align"] = trend_factor

    # 2. ADX контекст
    adx = 0.0
    if h1_bars and len(h1_bars) >= 29:
        adx = calc_adx(h1_bars, period=14)

    if adx <= 0:
        factors["adx"] = 0.5
    elif adx <= _ADX_MAX_SIDEWAYS:
        factors["adx"] = 1.0
    elif adx <= 40:
        factors["adx"] = 1.0 - (adx - _ADX_MAX_SIDEWAYS) / 15 * 0.7
    else:
        factors["adx"] = 0.2

    # 3. Volume spike
    if h1_bars and len(h1_bars) >= 10:
        avg_vol   = sum(b["volume"] for b in h1_bars) / len(h1_bars)
        last3_vol = sum(b["volume"] for b in h1_bars[-3:]) / 3
        ratio = last3_vol / avg_vol if avg_vol > 0 else 1.0
        factors["volume"] = min(ratio / _VOL_SPIKE_MIN, 1.0)
    else:
        factors["volume"] = 0.5

    # 4. Свежесть — последнее касание уровня
    touch = level.get("last_touch_time")
    if touch:
        if hasattr(touch, "tzinfo") and touch.tzinfo is None:
            touch = touch.replace(tzinfo=timezone.utc)
        hours_ago = (datetime.now(tz=timezone.utc) - touch
                     ).total_seconds() / 3600
        factors["freshness"] = max(0.0, 1.0 - hours_ago / 24)
    else:
        factors["freshness"] = 0.3

    # 5. Направление последней H1 свечи
    if h1_bars and len(h1_bars) >= 2:
        last    = h1_bars[-1]
        bullish = last["close"] > last["open"]
        if direction == "Support" and bullish:
            factors["candle"] = 1.0
        elif direction == "Resistance" and not bullish:
            factors["candle"] = 1.0
        else:
            factors["candle"] = 0.35
    else:
        factors["candle"] = 0.5

    # 6. RSI фильтр — берём из level["rsi_value"] (сохранён Collector-ом
    #    в raw_levels → Analyzer пересчитал в analyzed_levels → здесь читаем)
    #    Это фундаментально: RSI хранится в БД, не вычисляется повторно.
    rsi = float(level.get("rsi_value") or 50.0)
    factors["rsi_value"] = rsi  # для логирования

    if direction == "Support":     # Buy
        if rsi > 70:   factors["rsi"] = 0.1  # перекупленность — опасно
        elif rsi > 60: factors["rsi"] = 0.4
        elif rsi < 30: factors["rsi"] = 1.0  # перепроданность — отлично
        elif rsi < 40: factors["rsi"] = 0.9
        else:          factors["rsi"] = 0.65  # нейтраль
    else:                          # Sell (Resistance)
        if rsi < 30:   factors["rsi"] = 0.1  # перепроданность — опасно
        elif rsi < 40: factors["rsi"] = 0.4
        elif rsi > 70: factors["rsi"] = 1.0  # перекупленность — отлично
        elif rsi > 60: factors["rsi"] = 0.9
        else:          factors["rsi"] = 0.65  # нейтраль

    # T = взвешенное среднее
    # RSI получает вес 0.15 — важный фильтр
    weights = {
        "proximity":   0.25,
        "trend_align": 0.20,
        "rsi":         0.15,
        "adx":         0.15,
        "volume":      0.10,
        "freshness":   0.10,
        "candle":      0.05,
    }
    T = sum(factors[k] * weights[k] for k in weights)
    T = round(min(T, 1.0), 4)

    return T, factors


# ------------------------------------------------------------------
# Расчёт SL / TP
# ------------------------------------------------------------------

def calc_sl_tp(level: dict, current_price: float,
               h1_bars: list,
               sl_mult: float = _SL_ATR_MULT,
               tp_mult: float = _TP_ATR_MULT) -> Tuple[float, float, float]:
    """
    FIX-3: SL ставится ЗА структурным уровнем (зоной), а не на ATR от входа.

    Логика:
      Support (Buy):
        SL = нижняя граница зоны − ATR×sl_mult (буфер за структурой)
        TP = entry + ATR×tp_mult

      Resistance (Sell):
        SL = верхняя граница зоны + ATR×sl_mult (буфер за структурой)
        TP = entry − ATR×tp_mult

    Это гарантирует что SL стоит ЗА уровнем, а не внутри зоны.
    """
    from core.utils import calc_atr, ATR_PERIOD_BY_TF
    symbol    = level["symbol"]
    direction = level["direction"]
    pip       = _pip_size(symbol)
    zone      = float(level["price_zone"])

    if h1_bars and len(h1_bars) >= 15:
        atr = calc_atr(h1_bars, ATR_PERIOD_BY_TF.get("H1", 20))
    else:
        atr = 10 * pip   # fallback: 10 пипсов

    entry = current_price

    # ATR-зона уровня (половина от зоны коллектора)
    atr_zone = atr * 0.3

    if direction == "Support":
        # SL ниже нижней границы зоны + буфер sl_mult×ATR
        zone_bottom = zone - atr_zone
        sl = zone_bottom - atr * sl_mult
        tp = entry + atr * tp_mult
    else:
        # SL выше верхней границы зоны + буфер sl_mult×ATR
        zone_top = zone + atr_zone
        sl = zone_top + atr * sl_mult
        tp = entry - atr * tp_mult

    sl_pips = round(abs(entry - sl) / pip)
    tp_pips = round(abs(entry - tp) / pip)

    # Минимальный SL = 8п, TP = 10п (по ТЗ)
    if sl_pips < 8:
        sl_pips = 8
        sl = (entry - sl_pips * pip) if direction == "Support" \
             else (entry + sl_pips * pip)
    if tp_pips < 10:
        tp_pips = 10
        tp = (entry + tp_pips * pip) if direction == "Support" \
             else (entry - tp_pips * pip)

    # Проверка R:R ≥ 1:1.3 (по ТЗ)
    if sl_pips > 0 and tp_pips / sl_pips < 1.3:
        tp_pips = round(sl_pips * 1.3) + 1
        tp = (entry + tp_pips * pip) if direction == "Support" \
             else (entry - tp_pips * pip)

    return round(entry, 5), round(sl, 5), round(tp, 5)


# ------------------------------------------------------------------
# M5 подтверждение
# ------------------------------------------------------------------

def check_m5_confirmation(signal: dict,
                           current_price: float,
                           m5_bars: list) -> Tuple[bool, str]:
    """
    Проверяет подтверждение сигнала на таймфрейме M5.

    Условия для Support (Buy):
      1. Цена в зоне ±_PROXIMITY_M5_PIPS от уровня
      2. Последняя M5 свеча БЫЧЬЯ (close > open)
      3. Свеча закрылась ВЫШЕ зоны уровня
      4. Объём последних 2 баров M5 > среднего

    Условия для Resistance (Sell):
      1. Цена в зоне ±_PROXIMITY_M5_PIPS от уровня
      2. Последняя M5 свеча МЕДВЕЖЬЯ (close < open)
      3. Свеча закрылась НИЖЕ зоны уровня
      4. Объём последних 2 баров M5 > среднего

    Возвращает (подтверждено, причина).
    """
    if not m5_bars or len(m5_bars) < 3:
        return False, "мало M5 баров"

    symbol    = signal["symbol"]
    direction = signal["direction"]
    zone      = float(signal["price_zone"])
    pip       = _pip_size(symbol)
    prox      = _PROXIMITY_M5_PIPS * pip

    # 1. Проверка proximity
    dist = abs(current_price - zone)
    if dist > prox:
        return False, f"цена далеко от зоны ({dist/pip:.1f} пипсов > {_PROXIMITY_M5_PIPS})"

    last   = m5_bars[-1]   # последний закрытый бар
    prev   = m5_bars[-2]   # предпоследний

    # 2. Направление свечи
    is_bullish = last["close"] > last["open"]
    is_bearish = last["close"] < last["open"]

    # 3. Закрытие относительно зоны
    if direction == "Support":
        if not is_bullish:
            return False, "M5 свеча медвежья (нет отскока от Support)"
        if last["close"] <= zone:
            return False, f"M5 закрылась ниже зоны {zone:.5f}"
    else:  # Resistance
        if not is_bearish:
            return False, "M5 свеча бычья (нет отскока от Resistance)"
        if last["close"] >= zone:
            return False, f"M5 закрылась выше зоны {zone:.5f}"

    # 4. Объём
    avg_vol = sum(b["volume"] for b in m5_bars) / len(m5_bars)
    recent_vol = (last["volume"] + prev["volume"]) / 2
    if avg_vol > 0 and recent_vol < avg_vol * 0.8:
        return False, f"объём M5 слабый ({recent_vol:.0f} < {avg_vol*0.8:.0f})"

    reason = (
        f"M5 {'▲бычья' if is_bullish else '▼медвежья'} | "
        f"close={last['close']:.5f} | "
        f"vol={recent_vol:.0f}"
    )
    return True, reason

class SignalEngineModule(BaseModule):

    def __init__(self):
        super().__init__("signal")
        self._thread:    Optional[threading.Thread] = None
        self._m5_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._db         = get_db()
        self._bridge     = get_mt5_bridge()
        self._cfg        = _load_config()

    def start(self) -> None:
        if self._running:
            logger.warning("SignalEngine уже запущен")
            return
        self._stop_event.clear()
        # Поток 1: SCAN (каждые 5 мин)
        self._thread = threading.Thread(
            target=self._run_loop,
            name="signal-scan-thread",
            daemon=True
        )
        # Поток 2: M5 Monitor (каждые 30 сек)
        self._m5_thread = threading.Thread(
            target=self._run_m5_loop,
            name="signal-m5-thread",
            daemon=True
        )
        self._set_running(True)
        self._thread.start()
        self._m5_thread.start()
        logger.info(
            f"SignalEngine запущен "
            f"(SCAN: {self._cfg['interval_sec']}с, "
            f"M5: {self._cfg['m5_monitor_sec']}с, "
            f"min_S={self._cfg['min_s_score']}, "
            f"min_T={self._cfg['min_t_score']})"
        )

    def stop(self) -> None:
        if not self._running:
            return
        self._stop_event.set()
        for t in [self._thread, self._m5_thread]:
            if t and t.is_alive():
                t.join(timeout=10)
        self._set_running(False)
        logger.info("SignalEngine остановлен")

    def run_once(self) -> bool:
        self._mark_run_start()
        logger.info("SignalEngine: запуск цикла...")
        try:
            result = self._scan()
            if result:
                self._mark_success()
            else:
                self._mark_error("_scan() вернул False")
            return result
        except Exception as e:
            self._mark_error(str(e))
            logger.error(f"SignalEngine run_once: {e}", exc_info=True)
            return False

    def get_config(self) -> dict:
        return self._cfg

    def _run_loop(self) -> None:
        logger.info("SignalEngine SCAN: поток запущен")
        while not self._stop_event.is_set():
            self.run_once()
            self._stop_event.wait(timeout=self._cfg["interval_sec"])
        logger.info("SignalEngine SCAN: поток завершён")

    def _run_m5_loop(self) -> None:
        """M5 Monitor — отдельный поток, каждые 30 секунд."""
        logger.info("SignalEngine M5 Monitor: поток запущен")
        # Первый запуск через 10 сек после старта
        self._stop_event.wait(timeout=10)
        while not self._stop_event.is_set():
            try:
                self._monitor_m5()
            except Exception as e:
                logger.debug(f"M5 Monitor ошибка: {e}")
            self._stop_event.wait(
                timeout=self._cfg.get("m5_monitor_sec",
                                      _M5_MONITOR_SEC))
        logger.info("SignalEngine M5 Monitor: поток завершён")

    def _monitor_m5(self) -> None:
        """
        Берёт все pending сигналы и проверяет подтверждение на M5.
        Если подтверждено → статус 'confirmed'.
        """
        if not self._bridge.monitor.is_connected:
            return

        pending = self._fetch_pending_signals()
        if not pending:
            return

        confirmed_count = 0
        for sig in pending:
            symbol = sig["symbol"]
            try:
                with self._bridge.session() as mt5:
                    price_info = self._bridge.get_current_price(
                        mt5, symbol)
                    m5_bars = self._bridge.get_rates(
                        mt5, symbol, "M5", bars=10)
            except Exception:
                continue

            if not price_info or not m5_bars:
                continue

            current_price = (price_info["bid"] +
                             price_info["ask"]) / 2

            ok, reason = check_m5_confirmation(
                sig, current_price, m5_bars)

            if ok:
                self._confirm_signal(sig["id"], reason,
                                     current_price)
                confirmed_count += 1
                logger.info(
                    f"M5 ✓ ПОДТВЕРЖДЁН: {symbol} "
                    f"{sig['direction']} "
                    f"зона={float(sig['price_zone']):.5f} "
                    f"| {reason}"
                )
            else:
                logger.debug(
                    f"M5 ✗ {symbol} {sig['direction']}: {reason}")

        if confirmed_count:
            logger.info(
                f"M5 Monitor: подтверждено {confirmed_count} сигналов")

    def _fetch_pending_signals(self) -> List[dict]:
        """Загружает все pending сигналы из signal_queue."""
        try:
            with self._db.cursor() as cur:
                cur.execute("""
                    SELECT id, symbol, timeframe, direction,
                           price_zone, entry_price,
                           sl_price, tp_price,
                           sl_pips, tp_pips, rr_ratio,
                           s_score, t_score
                    FROM signal_queue
                    WHERE status = 'pending'
                      AND expires_at > %s
                """, (datetime.now(tz=timezone.utc),))
                return cur.fetchall()
        except Exception as e:
            logger.debug(f"_fetch_pending_signals: {e}")
            return []

    def _confirm_signal(self, signal_id: int,
                         reason: str,
                         current_price: float) -> None:
        """Переводит сигнал в статус confirmed."""
        try:
            with self._db.cursor(commit=True) as cur:
                cur.execute("""
                    UPDATE signal_queue
                    SET status = 'confirmed',
                        entry_price = %s,
                        close_reason = %s
                    WHERE id = %s
                      AND status = 'pending'
                """, (round(current_price, 5), reason, signal_id))
        except Exception as e:
            logger.error(f"_confirm_signal: {e}")

    # ----------------------------------------------------------
    # Основная логика
    # ----------------------------------------------------------

    def _scan(self) -> bool:
        """
        Один цикл сканирования:
        1. Отменяем истёкшие сигналы
        2. Загружаем топ уровни (S >= min_s_score)
        3. Для каждого уровня: получаем текущую цену + H1 бары
        4. Вычисляем T, если T >= min_t_score → создаём сигнал
        """
        cfg = _load_config()
        self._cfg = cfg

        # 0. Проверяем торговую сессию (11:00-20:00 Ереван = 07:00-16:00 UTC)
        now_utc = datetime.now(tz=timezone.utc)
        session_start = cfg.get("session_start_utc", _SESSION_START_UTC)
        session_end   = cfg.get("session_end_utc",   _SESSION_END_UTC)
        if not (session_start <= now_utc.hour < session_end):
            logger.debug(
                f"SignalEngine: вне торговой сессии "
                f"(UTC {now_utc.hour:02d}:xx, окно 07-16 UTC)"
            )
            return True

        # 1. Отменяем истёкшие сигналы
        self._expire_old_signals()

        # 2. Загружаем топ уровни
        candidates = self._fetch_candidates(cfg["min_s_score"])
        if not candidates:
            logger.debug("SignalEngine: нет кандидатов (S >= %.1f)",
                         cfg["min_s_score"])
            return True

        # 3. Получаем активные символы + корреляционные блоки
        self._blocked_pairs = set()  # инициализация до вызова
        active_symbols = self._get_active_symbols()

        signals_created = 0
        if not self._bridge.monitor.is_connected:
            logger.warning("SignalEngine: MT5 недоступен — пропускаем")
            return True

        # Пр-3 ИСПРАВЛЕНО: группируем кандидатов по символу →
        # одна MT5-сессия на символ вместо одной на каждый уровень
        from itertools import groupby
        # Сортируем по символу для groupby
        candidates_sorted = sorted(candidates, key=lambda l: l["symbol"])

        for symbol, symbol_levels in groupby(candidates_sorted,
                                             key=lambda l: l["symbol"]):
            symbol_levels = list(symbol_levels)

            if symbol in active_symbols:
                continue

            try:
                with self._bridge.session() as mt5:
                    price_info = self._bridge.get_current_price(mt5, symbol)
                    h1_bars    = self._bridge.get_rates(mt5, symbol, "H1", bars=100)
            except Exception as e:
                logger.debug(f"SignalEngine: не удалось получить данные {symbol}: {e}")
                continue

            if not price_info:
                continue

            for level in symbol_levels:
              direction = level["direction"]
              blocked_pairs = getattr(self, "_blocked_pairs", set())
              if (symbol, direction) in blocked_pairs:
                  logger.debug(
                      f"КОРРЕЛЯЦИЯ: {symbol} {direction} заблокирован "
                      f"(конфликт с открытой сделкой)")
                  continue

              current_price = (price_info["bid"] + price_info["ask"]) / 2

              # 4. Trade Readiness
            T, factors = calc_trade_readiness(
                level, current_price, h1_bars or [], cfg)

            logger.debug(
                f"  {symbol:8} {level['timeframe']:4} "
                f"{level['direction']:12} "
                f"zone={float(level['price_zone']):.5f} "
                f"S={float(level['strength_score']):.2f} "
                f"T={T:.3f} "
                f"[prox={factors.get('proximity',0):.2f} "
                f"trend={factors.get('trend_align',0):.2f} "
                f"rsi={factors.get('rsi_value',50):.0f}({factors.get('rsi',0):.2f}) "
                f"adx={factors.get('adx',0):.2f} "
                f"vol={factors.get('volume',0):.2f} "
                f"f={factors.get('freshness',0):.2f}]"
            )

            if T >= cfg["min_t_score"]:
                entry, sl, tp = calc_sl_tp(
                    level, current_price, h1_bars or [],
                    sl_mult=cfg.get("sl_atr_mult", _SL_ATR_MULT),
                    tp_mult=cfg.get("tp_atr_mult", _TP_ATR_MULT))
                sl_pips = round(abs(entry - sl) / _pip_size(symbol))
                tp_pips = round(abs(entry - tp) / _pip_size(symbol))
                rr      = round(tp_pips / sl_pips, 2) if sl_pips else 0

                self._create_signal(
                    level=level,
                    entry=entry,
                    sl=sl,
                    tp=tp,
                    sl_pips=sl_pips,
                    tp_pips=tp_pips,
                    rr=rr,
                    t_score=T,
                    t_factors=factors,
                )
                active_symbols.add(symbol)
                signals_created += 1

                logger.info(
                    f"СИГНАЛ: {symbol} {level['direction']} "
                    f"Вход={entry:.5f} SL={sl:.5f}(-{sl_pips}п) "
                    f"TP={tp:.5f}(+{tp_pips}п) "
                    f"R:R=1:{rr} S={float(level['strength_score']):.2f} "
                    f"T={T:.3f}"
                )

        if signals_created:
            logger.info(f"SignalEngine: создано {signals_created} сигналов")
            self._db.log_to_db("signal", "INFO",
                               f"Создано {signals_created} сигналов")
        else:
            logger.info("SignalEngine: нет новых сигналов в этом цикле")

        return True

    # ----------------------------------------------------------
    # DB helpers
    # ----------------------------------------------------------

    def _fetch_candidates(self, min_s: float) -> List[dict]:
        """Топ уровни из analyzed_levels с S >= min_s, только H1/H4."""
        with self._db.cursor() as cur:
            cur.execute("""
                SELECT
                    a.symbol, a.timeframe, a.direction,
                    a.price_zone, a.strength_score, a.classification,
                    a.f_dynamics, a.last_touch_time,
                    COALESCE(a.rsi_value, 50.0) AS rsi_value,
                    COALESCE(a.f_rsi, 0.5)      AS f_rsi,
                    r.adx_value, r.avg_volume
                FROM analyzed_levels a
                LEFT JOIN raw_levels r
                    ON r.symbol    = a.symbol
                    AND r.timeframe = a.timeframe
                    AND r.price_zone = a.price_zone
                    AND r.direction  = a.direction
                WHERE a.strength_score >= %s
                  AND a.timeframe IN ('H1', 'H4', 'D')
                ORDER BY a.strength_score DESC
                LIMIT 50
            """, (min_s,))
            return cur.fetchall()

    def _get_active_symbols(self) -> set:
        """
        Возвращает множество ЗАБЛОКИРОВАННЫХ пар (symbol, direction).
        Блокирует:
          1. pending/confirmed сигналы (ещё не открыты)
          2. реальные открытые сделки (уже в рынке)
          3. корреляционные конфликты (нельзя ставить на одну валюту дважды)

        Возвращает set строк-символов (для обратной совместимости)
        и дополнительно self._blocked_pairs: set кортежей (symbol, direction).
        """
        blocked_symbols  = set()
        # (symbol, direction) → заблокированные пары+направления
        self._blocked_pairs = set()

        try:
            with self._db.cursor() as cur:
                # 1. Сигналы ожидающие открытия
                cur.execute("""
                    SELECT DISTINCT symbol FROM signal_queue
                    WHERE status IN ('pending', 'confirmed')
                """)
                for r in cur.fetchall():
                    blocked_symbols.add(r["symbol"])

                # 2. Реальные открытые сделки + корреляционные конфликты
                cur.execute("""
                    SELECT symbol, direction
                    FROM trades
                    WHERE status = 'open'
                """)
                open_trades = cur.fetchall()

            for t in open_trades:
                sym = t["symbol"]
                # "Buy"→"Support", "Sell"→"Resistance"
                sig_dir = "Support" if t["direction"] == "Buy" else "Resistance"
                blocked_symbols.add(sym)

                # Добавляем корреляционные конфликты
                conflicts = _CORR_CONFLICTS.get((sym, sig_dir), set())
                for c_sym, c_dir in conflicts:
                    self._blocked_pairs.add((c_sym, c_dir))
                    logger.debug(
                        f"Корреляционный блок: {c_sym} {c_dir} "
                        f"(открыта {sym} {sig_dir})"
                    )

        except Exception as e:
            logger.debug(f"_get_active_symbols: {e}")

        return blocked_symbols

    def _expire_old_signals(self) -> None:
        """
        БАГ-5 ИСПРАВЛЕНО: используем expires_at вместо created_at.
        expires_at уже установлен при создании сигнала.
        Это гарантирует правильную работу при изменении TTL в конфиге.
        """
        try:
            with self._db.cursor(commit=True) as cur:
                cur.execute("""
                    UPDATE signal_queue
                    SET status = 'expired'
                    WHERE status IN ('pending', 'confirmed')
                      AND expires_at < %s
                """, (datetime.now(tz=timezone.utc),))
                if cur.rowcount:
                    logger.info(
                        f"SignalEngine: отменено {cur.rowcount} "
                        f"истёкших сигналов"
                    )
        except Exception as e:
            logger.error(f"_expire_old_signals: {e}")

    def _create_signal(self, level: dict, entry: float,
                        sl: float, tp: float,
                        sl_pips: int, tp_pips: int,
                        rr: float, t_score: float,
                        t_factors: dict) -> None:
        """Записывает новый сигнал в signal_queue."""
        import json
        expires = (datetime.now(tz=timezone.utc)
                   + timedelta(minutes=_SIGNAL_TTL_MIN))
        try:
            with self._db.cursor(commit=True) as cur:
                cur.execute("""
                    INSERT INTO signal_queue
                    (symbol, timeframe, direction, price_zone,
                     entry_price, sl_price, tp_price,
                     sl_pips, tp_pips, rr_ratio,
                     s_score, t_score, t_factors,
                     strategy, status,
                     rsi_at_signal,
                     created_at, expires_at)
                    VALUES
                    (%s, %s, %s, %s,
                     %s, %s, %s,
                     %s, %s, %s,
                     %s, %s, %s,
                     %s, 'pending',
                     %s,
                     %s, %s)
                """, (
                    level["symbol"],
                    level["timeframe"],
                    level["direction"],
                    float(level["price_zone"]),
                    entry, sl, tp,
                    sl_pips, tp_pips, rr,
                    float(level["strength_score"]),
                    t_score,
                    json.dumps(t_factors),
                    "scalping",
                    round(float(level.get("rsi_value") or 50.0), 1),
                    datetime.now(tz=timezone.utc),
                    expires,
                ))
        except Exception as e:
            logger.error(f"_create_signal: {e}")


# ------------------------------------------------------------------
# ScalpingModule — единая точка входа для ModuleManager
# Запускает и SignalEngine, и Trader как единый Plug-and-Play модуль
# ------------------------------------------------------------------

class ScalpingModule(BaseModule):
    """
    Объединяет SignalEngineModule + TraderModule + SignalEvaluator.
    ModuleManager грузит только этот класс через get_module().
    """

    def __init__(self):
        super().__init__("scalping")
        self._signal = SignalEngineModule()
        from modules.strategies.scalping.trader    import TraderModule
        from modules.strategies.scalping.evaluator import SignalEvaluator
        self._trader    = TraderModule()
        self._evaluator = SignalEvaluator()

    def start(self) -> None:
        if self._running:
            return
        self._signal.start()
        self._trader.start()
        self._evaluator.start()
        self._set_running(True)
        logger.info("Scalping модуль запущен (Signal + Trader + Evaluator)")

    def stop(self) -> None:
        if not self._running:
            return
        self._signal.stop()
        self._trader.stop()
        self._evaluator.stop()
        self._set_running(False)
        logger.info("Scalping модуль остановлен")

    def run_once(self) -> bool:
        ok1 = self._signal.run_once()
        ok2 = self._trader.run_once()
        return ok1 and ok2

    def status(self) -> dict:
        s = self._signal.status() or {}
        t = self._trader.status()  or {}
        return {
            "running":      self._running,
            "signal":       s,
            "trader":       t,
            "last_run":     s.get("last_run"),
            "error_count":  s.get("error_count", 0)
                            + t.get("error_count", 0),
        }

    def get_config(self) -> dict:
        return {
            "signal": self._signal.get_config(),
            "trader": self._trader.get_config(),
        }


# Переопределяем get_module — возвращает ScalpingModule
def get_module() -> ScalpingModule:
    return ScalpingModule()
