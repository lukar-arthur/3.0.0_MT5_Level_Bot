# ============================================================
#  MT5_Level_Bot — modules/analyzer/analyzer.py
#  Version : 2.0.0
#
#  ИЗМЕНЕНИЯ v2.0.0 (Этап 5 — факторы M и C):
#
#  M (мультитаймфрейм):
#    - Адаптивный порог proximity по символу (JPY ≠ остальные)
#    - Учёт направления: Support vs Resistance раздельно
#    - Взвешенный score: D=3, H4=2, H1=1 (уже было, оставляем)
#    - Добавлен бонус за совпадение на D таймфрейме
#
#  C (конфлюенс):
#    - Полный расчёт Pivot Points (Classic PP) из raw_levels D
#    - Числа Фибоначчи от последнего D swing (38.2%, 50%, 61.8%)
#    - Round numbers (уже был, улучшен — учитываем 00 и 50 уровни)
#    - Совпадение с уровнем другого таймфрейма (пересечение)
#    - Итог: confluence_count записывается в raw_levels для отчётности
#
#  Дополнительно:
#    - stat_pips_max адаптирован по символу (JPY × 10)
#    - _filter_qualified: убран жёсткий max_touch_age_days для D уровней
#    - Логирование факторов в топ-10 (было топ-5)
#    - Batch UPSERT через db.transaction() как в Collector
# ============================================================

import configparser
import math
import os
import threading
from datetime import datetime, timezone, timedelta
from typing   import Dict, List, Optional, Tuple

from core.base_module    import BaseModule
from core.db_connection  import get_db
from core.utils          import get_logger, safe_normalize, freshness_score
from core.config_loader  import load_module_config

# Plug-and-Play: analyzer использует config/analyzer.ini
_MODULE_CFG = load_module_config("analyzer")

logger = get_logger("analyzer")

_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "config", "config.ini"
)

# ── Порог близости уровней по символу (для M и C) ────────────
# Насколько близко два уровня считаются "одним и тем же"
_PROXIMITY_BY_SYMBOL: Dict[str, float] = {
    "JPY": 0.060,      # USDJPY: ±6 пипсов = 0.06
    "DEFAULT": 0.0006, # остальные: ±6 пипсов = 0.0006
}

# ── Round number шаги ────────────────────────────────────────
_ROUND_STEPS = {
    "JPY": 0.50,      # 50 пипсов для JPY
    "DEFAULT": 0.0050, # 50 пипсов для остальных
}

# ── Весовые коэффициенты ТФ для M фактора ────────────────────
_TF_WEIGHT = {"D": 3, "H4": 2, "H1": 1}
_TF_WEIGHT_MAX = sum(_TF_WEIGHT.values())   # 6

# ── Fibonacci уровни ─────────────────────────────────────────
_FIBO_LEVELS = [0.236, 0.382, 0.500, 0.618, 0.786]


# ------------------------------------------------------------------
# Конфиг
# ------------------------------------------------------------------

def _load_config() -> dict:
    """Plug-and-Play: читает из config/analyzer.ini (fallback: config.ini)."""
    cfg = _MODULE_CFG
    cfg.reload()
    return {
        "min_bounce_count":   cfg.getint("ANALYZER",  "min_bounce_count",        fallback=2),  # noqa
        "max_touch_age_days": cfg.getint("ANALYZER",  "max_touch_age_days",       fallback=14),
        "adx_sideways":       cfg.getfloat("ANALYZER","adx_sideways_threshold",   fallback=20),
        "adx_trend":          cfg.getfloat("ANALYZER","adx_trend_threshold",      fallback=40),
        # Веса
        "w_bounce":     cfg.getfloat("WEIGHTS","w_bounce",     fallback=0.25),
        "w_freshness":  cfg.getfloat("WEIGHTS","w_freshness",  fallback=0.20),
        "w_confluence": cfg.getfloat("WEIGHTS","w_confluence", fallback=0.15),
        "w_volume":     cfg.getfloat("WEIGHTS","w_volume",     fallback=0.15),
        "w_multitf":    cfg.getfloat("WEIGHTS","w_multitf",    fallback=0.10),
        "w_reversal":   cfg.getfloat("WEIGHTS","w_reversal",   fallback=0.03),
        "w_dynamics":   cfg.getfloat("WEIGHTS","w_dynamics",   fallback=0.04),
        "w_stat":       cfg.getfloat("WEIGHTS","w_stat",       fallback=0.01),
        "w_rsi":        cfg.getfloat("WEIGHTS","w_rsi",        fallback=0.07),
        # Нормализация
        "bounce_max":       cfg.getint  ("NORMALISATION","bounce_max",       fallback=10),
        "confluence_max":   cfg.getint  ("NORMALISATION","confluence_max",   fallback=5),
        "volume_ratio_max": cfg.getfloat("NORMALISATION","volume_ratio_max", fallback=3.0),
        "stat_pips_max":    cfg.getfloat("NORMALISATION","stat_pips_max",    fallback=50.0),
        "freshness_lambda": cfg.getfloat("NORMALISATION","freshness_lambda", fallback=0.005),
        # Классификация
        "very_strong": cfg.getfloat("CLASSIFICATION","very_strong", fallback=7.5),
        "strong":      cfg.getfloat("CLASSIFICATION","strong",      fallback=5.5),
        "medium":      cfg.getfloat("CLASSIFICATION","medium",      fallback=3.5),
        "weak":        cfg.getfloat("CLASSIFICATION","weak",        fallback=2.0),
    }


# ------------------------------------------------------------------
# Вспомогательные функции
# ------------------------------------------------------------------

def _proximity(symbol: str) -> float:
    """Порог близости двух уровней для данного символа."""
    is_jpy = symbol.upper().endswith("JPY")
    return _PROXIMITY_BY_SYMBOL["JPY"] if is_jpy else _PROXIMITY_BY_SYMBOL["DEFAULT"]


def _pip_size(symbol: str) -> float:
    """Размер одного пипса для символа."""
    return 0.01 if symbol.upper().endswith("JPY") else 0.0001


def _stat_pips_max(symbol: str, base: float) -> float:
    """
    Адаптированный максимум пипсов для нормализации Stat.
    JPY пары имеют большие абсолютные значения пипсов.
    """
    return base * 10 if symbol.upper().endswith("JPY") else base


def _classify(s_score: float, cfg: dict) -> str:
    if s_score >= cfg["very_strong"]: return "Very Strong"
    if s_score >= cfg["strong"]:      return "Strong"
    if s_score >= cfg["medium"]:      return "Medium"
    if s_score >= cfg["weak"]:        return "Weak"
    return "Ignore"


def _is_round_number(price: float, symbol: str) -> Tuple[bool, int]:
    """
    Проверяет является ли цена круглым числом.
    Возвращает (is_round, bonus):
      - ×00 уровень (1.0800, 150.00): bonus=2
      - ×50 уровень (1.0850, 150.50): bonus=1
    """
    is_jpy = symbol.upper().endswith("JPY")
    step   = _ROUND_STEPS["JPY"] if is_jpy else _ROUND_STEPS["DEFAULT"]
    tol    = step * 0.10

    remainder = price % step
    is_50 = remainder < tol or remainder > (step - tol)

    if not is_50:
        return False, 0

    # Проверяем ×00 — ещё более круглое число
    step_big = step * 2
    rem_big  = price % step_big
    is_00    = rem_big < tol or rem_big > (step_big - tol)

    return True, (2 if is_00 else 1)


# ------------------------------------------------------------------
# Расчёт Pivot Points (Classic)
# ------------------------------------------------------------------

def _calc_pivot_points(d_levels: List[dict],
                        symbol: str,
                        ohlc_data: Optional[Dict] = None) -> List[float]:
    """
    БАГ-3 ИСПРАВЛЕНО: Classic Pivot Points теперь используют
    реальные OHLC дневной свечи из ohlc_data (если переданы).

    Если ohlc_data не передан → используем приближение из D-уровней
    (оба Support и Resistance) как оценку дневного диапазона.
    Это лучше предыдущего варианта, где смешивались типы уровней.

    ohlc_data: {"high": float, "low": float, "close": float}
    """
    # Вариант 1: есть реальные OHLC (передаются из AnalyzerModule._analyze)
    if ohlc_data and all(k in ohlc_data for k in ("high", "low", "close")):
        H = float(ohlc_data["high"])
        L = float(ohlc_data["low"])
        C = float(ohlc_data["close"])
    else:
        # Вариант 2: приближение из D-уровней
        # Resistance = хаи, Support = лои — разделяем корректно
        d_sym = [l for l in d_levels
                 if l["symbol"] == symbol and l["timeframe"] == "D"]
        if len(d_sym) < 3:
            return []
        res_prices = [float(l["price_zone"]) for l in d_sym
                      if l["direction"] == "Resistance"]
        sup_prices = [float(l["price_zone"]) for l in d_sym
                      if l["direction"] == "Support"]
        if not res_prices or not sup_prices:
            return []
        H = max(res_prices)
        L = min(sup_prices)
        C = res_prices[0] if res_prices else (H + L) / 2

    PP = (H + L + C) / 3
    R1 = 2 * PP - L;   S1 = 2 * PP - H
    R2 = PP + (H - L); S2 = PP - (H - L)
    R3 = H + 2 * (PP - L); S3 = L - 2 * (H - PP)
    return [PP, R1, S1, R2, S2, R3, S3]


# ------------------------------------------------------------------
# Расчёт Fibonacci уровней
# ------------------------------------------------------------------

def _calc_fibonacci_levels(d_levels: List[dict],
                             symbol: str) -> List[float]:
    """
    Вычисляет Fibonacci retracement уровни из последнего D swing.
    Swing = разница между max и min D уровней за последние 20 баров.
    Возвращает список Fibo уровней (38.2%, 50%, 61.8% и др.)
    """
    d_sym = [l for l in d_levels
             if l["symbol"] == symbol and l["timeframe"] == "D"]
    if len(d_sym) < 5:
        return []

    prices = [float(l["price_zone"]) for l in d_sym[:20]]
    if not prices:
        return []

    swing_high = max(prices)
    swing_low  = min(prices)
    swing_diff = swing_high - swing_low

    if swing_diff <= 0:
        return []

    fibo_levels = []
    for ratio in _FIBO_LEVELS:
        # Retracement от high
        fibo_levels.append(swing_high - swing_diff * ratio)
        # Retracement от low
        fibo_levels.append(swing_low  + swing_diff * ratio)

    return fibo_levels


# ------------------------------------------------------------------
# ФАКТОР B
# ------------------------------------------------------------------

def factor_B(bounce_count: int, bounce_max: int) -> float:
    """Количество отскоков. min(bounce_count / bounce_max, 1.0)"""
    return safe_normalize(bounce_count, bounce_max)


# ------------------------------------------------------------------
# ФАКТОР F
# ------------------------------------------------------------------

def factor_F(last_touch: Optional[datetime],
              lam: float = 0.005) -> float:
    """
    Свежесть касания (экспоненциальный распад по часам).
    f = exp(-λ × hours_since_touch)
    """
    if last_touch is None:
        return 0.0
    return freshness_score(last_touch, lam)


# ------------------------------------------------------------------
# ФАКТОР C v2.0 — полный конфлюенс
# ------------------------------------------------------------------

def factor_C(price: float, symbol: str,
              confluence_count: int,
              confluence_max: int,
              all_levels: List[dict]) -> Tuple[float, int]:
    """
    C v2.0 — конфлюенс: совпадение нескольких независимых факторов.

    Компоненты (каждый даёт +N к счётчику):
      +2/+1  Round number (×00 = +2, ×50 = +1)
      +2     Совпадение с Pivot Point (PP, R1-R3, S1-S3) ± proximity
      +1     Совпадение с Fibonacci 38.2/50/61.8% ± proximity
      +1     Уже подтверждён на другом ТФ (из raw_levels, без текущего)

    Итог нормализуется к confluence_max (обычно 5).
    Возвращает (f_score, total_count).
    """
    prox  = _proximity(symbol)
    total = confluence_count   # базовый счёт из raw_levels (обычно 0)
    detail = []

    # 1. Round numbers
    is_round, round_bonus = _is_round_number(price, symbol)
    if is_round:
        total += round_bonus
        detail.append(f"round+{round_bonus}")

    # 2. Pivot Points
    d_levels = [l for l in all_levels
                if l["symbol"] == symbol and l["timeframe"] == "D"]
    pp_levels = _calc_pivot_points(d_levels, symbol)
    for pp in pp_levels:
        if abs(price - pp) <= prox:
            total += 2
            detail.append("PP")
            break   # один бонус за PP, не суммируем несколько

    # 3. Fibonacci
    fibo_levels = _calc_fibonacci_levels(d_levels, symbol)
    for fib in fibo_levels:
        if abs(price - fib) <= prox:
            total += 1
            detail.append("Fibo")
            break   # один бонус за Fibo

    # 4. Подтверждение другим таймфреймом
    # БАГ-4 ИСПРАВЛЕНО: фильтруем чужой ТФ явно
    # Передаём timeframe через замыкание (caller должен передать его)
    # Для factor_C используем упрощённую проверку — детальная в factor_M
    other_tf_levels = [l for l in all_levels
                       if (l["symbol"] == symbol
                           and abs(float(l["price_zone"]) - price) <= prox)]
    # Считаем уникальные таймфреймы — если больше 1, есть MTF-подтверждение
    unique_tfs = {l.get("timeframe", "") for l in other_tf_levels if l.get("timeframe")}
    if len(unique_tfs) >= 2:
        total += 1
        detail.append("MTF")

    score = safe_normalize(total, confluence_max)
    return score, total


# ------------------------------------------------------------------
# ФАКТОР V
# ------------------------------------------------------------------

def factor_V(last_touch_volume: int, avg_volume: int) -> float:
    """Объём на касании vs средний. ratio / volume_ratio_max."""
    if avg_volume <= 0:
        return 0.0
    return safe_normalize(last_touch_volume / avg_volume, 3.0)


# ------------------------------------------------------------------
# ФАКТОР M v2.0 — улучшенный мультитаймфрейм
# ------------------------------------------------------------------

def factor_M(symbol: str, price_zone: float,
              direction: str,
              timeframe: str,
              all_levels: List[dict]) -> Tuple[float, int, List[str]]:
    """
    M v2.0 — мультитаймфрейм подтверждение.

    Улучшения vs v1.0:
    - Адаптивный proximity по символу (JPY ≠ остальные)
    - Раздельный поиск по направлению (Support/Resistance)
    - Бонус ×1.5 если среди подтверждающих есть D таймфрейм
    - Исключаем текущий ТФ из подсчёта

    Возвращает (f_score, tf_count, confirmed_tf_list).
    """
    prox           = _proximity(symbol)
    confirmed_tfs  = set()

    for lvl in all_levels:
        if lvl["symbol"]    != symbol:                         continue
        if lvl["timeframe"] == timeframe:                      continue  # не считаем себя
        if lvl["direction"] != direction:                      continue
        if abs(float(lvl["price_zone"]) - price_zone) > prox: continue
        confirmed_tfs.add(lvl["timeframe"])

    if not confirmed_tfs:
        # Нет подтверждения — базовый score 1/6 (только свой ТФ)
        own_weight = _TF_WEIGHT.get(timeframe, 1)
        return safe_normalize(own_weight, _TF_WEIGHT_MAX), 1, [timeframe]

    # Взвешенный счёт включая текущий ТФ
    all_confirmed = confirmed_tfs | {timeframe}
    score = sum(_TF_WEIGHT.get(tf, 1) for tf in all_confirmed)

    # Бонус за подтверждение на D — самый важный ТФ
    if "D" in confirmed_tfs and timeframe != "D":
        score = min(score * 1.3, _TF_WEIGHT_MAX)

    f_score = safe_normalize(score, _TF_WEIGHT_MAX)
    return f_score, len(all_confirmed), sorted(all_confirmed)


# ------------------------------------------------------------------
# ФАКТОР R
# ------------------------------------------------------------------

def factor_R(is_role_reversal: int) -> float:
    """Смена роли Support ↔ Resistance. Бинарный: 0 или 1."""
    return 1.0 if is_role_reversal else 0.0


# ------------------------------------------------------------------
# ФАКТОР D
# ------------------------------------------------------------------

def factor_D(adx_value: float, adx_sideways: float,
              adx_trend: float) -> float:
    """
    Динамика рынка через ADX.
    Боковик (ADX<20) → уровни работают лучше → 1.0
    Тренд   (ADX>40) → уровни пробиваются  → 0.2
    """
    if adx_value <= 0:    return 0.5
    if adx_value <= adx_sideways: return 1.0
    if adx_value >= adx_trend:    return 0.2
    ratio = (adx_value - adx_sideways) / (adx_trend - adx_sideways)
    return 1.0 - ratio * 0.8


# ------------------------------------------------------------------
# ФАКТОР Stat
# ------------------------------------------------------------------

def factor_Stat(avg_bounce_pips: float, symbol: str,
                 stat_pips_max: float) -> float:
    """
    Статистика отскоков (средний размер в пипсах).
    Адаптирован для JPY: max × 10.
    """
    adapted_max = _stat_pips_max(symbol, stat_pips_max)
    return safe_normalize(avg_bounce_pips, adapted_max)


# ------------------------------------------------------------------
# ФАКТОР RSI
# ------------------------------------------------------------------

def factor_RSI(rsi_value: float, direction: str) -> float:
    """
    RSI-фактор для S-score уровня.

    Логика как у профессионального трейдера:
      Support (Buy): уровень сильнее когда RSI низкий (перепроданность)
        RSI < 30 → f=1.0  (рынок перепродан — отскок от Support вероятен)
        RSI < 40 → f=0.8
        RSI 40-60 → f=0.6 (нейтраль)
        RSI > 60 → f=0.3  (перекупленность — Support может не удержать)
        RSI > 70 → f=0.1  (очень перекупленность — опасно)

      Resistance (Sell): зеркально
        RSI > 70 → f=1.0  (рынок перекуплен — разворот от Resistance вероятен)
        RSI > 60 → f=0.8
        RSI 40-60 → f=0.6
        RSI < 40 → f=0.3
        RSI < 30 → f=0.1
    """
    if direction == "Support":
        if   rsi_value < 30: return 1.0
        elif rsi_value < 40: return 0.8
        elif rsi_value < 60: return 0.6
        elif rsi_value < 70: return 0.3
        else:                return 0.1
    else:  # Resistance
        if   rsi_value > 70: return 1.0
        elif rsi_value > 60: return 0.8
        elif rsi_value > 40: return 0.6
        elif rsi_value > 30: return 0.3
        else:                return 0.1


# ------------------------------------------------------------------
# Главная функция расчёта S
# ------------------------------------------------------------------

def calculate_strength(row: dict, cfg: dict,
                         all_levels: List[dict]) -> dict:
    """
    Вычислить силу уровня S и все факторы.
    Входные данные: строка из raw_levels.
    """
    symbol    = row["symbol"]
    price     = float(row["price_level"])
    price_zone = float(row["price_zone"])
    direction  = row["direction"]
    timeframe  = row["timeframe"]

    # ── 8 факторов ───────────────────────────────────────────
    f_B = factor_B(row["bounce_count"], cfg["bounce_max"])

    f_F = factor_F(row["last_touch_time"], cfg["freshness_lambda"])

    f_C, c_count = factor_C(
        price, symbol,
        row["confluence_count"],
        cfg["confluence_max"],
        all_levels
    )

    f_V = factor_V(row["last_touch_volume"], row["avg_volume"])

    f_M, tf_count, tf_list = factor_M(
        symbol, price_zone, direction, timeframe, all_levels
    )

    f_R = factor_R(row["is_role_reversal"])

    f_D = factor_D(
        float(row["adx_value"]),
        cfg["adx_sideways"],
        cfg["adx_trend"]
    )

    f_Stat = factor_Stat(
        float(row["avg_bounce_pips"]),
        symbol,
        cfg["stat_pips_max"]
    )

    # RSI-фактор: берём из raw_levels (собирается Collector-ом)
    f_RSI = factor_RSI(
        float(row.get("rsi_value") or 50.0),
        direction
    )

    # ── S = 10 × Σ(wᵢ · fᵢ) ─────────────────────────────────
    # П-9 ИСПРАВЛЕНО: веса синхронизированы с config/analyzer.ini
    # w_bounce=0.25 w_freshness=0.20 w_confluence=0.15 w_volume=0.15
    # w_multitf=0.10 w_reversal=0.03 w_dynamics=0.04 w_stat=0.01 w_rsi=0.07
    # Сумма = 1.0
    S = 10.0 * (
        cfg["w_bounce"]     * f_B    +
        cfg["w_freshness"]  * f_F    +
        cfg["w_confluence"] * f_C    +
        cfg["w_volume"]     * f_V    +
        cfg["w_multitf"]    * f_M    +
        cfg["w_reversal"]   * f_R    +
        cfg["w_dynamics"]   * f_D    +
        cfg["w_stat"]       * f_Stat +
        cfg["w_rsi"]        * f_RSI
    )
    S = round(min(S, 10.0), 2)

    return {
        "symbol":             symbol,
        "timeframe":          timeframe,
        "price_zone":         price_zone,
        "direction":          direction,
        "strength_score":     S,
        "classification":     _classify(S, cfg),
        "f_bounce":           round(f_B,    4),
        "f_freshness":        round(f_F,    4),
        "f_confluence":       round(f_C,    4),
        "f_volume":           round(f_V,    4),
        "f_multitf":          round(f_M,    4),
        "f_reversal":         round(f_R,    4),
        "f_dynamics":         round(f_D,    4),
        "f_stat":             round(f_Stat, 4),
        "f_rsi":              round(f_RSI,  4),
        "rsi_value":          float(row.get("rsi_value") or 50.0),
        "tf_confirmed_count": tf_count,
        "tf_confirmed_list":  ",".join(tf_list),
        "confluence_count":   c_count,
        "last_touch_time":    row["last_touch_time"],
        "raw_level_id":       row.get("id"),
    }


# ------------------------------------------------------------------
# AnalyzerModule
# ------------------------------------------------------------------

class AnalyzerModule(BaseModule):

    def __init__(self):
        super().__init__("analyzer")
        self._thread:    Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._db         = get_db()

    def start(self) -> None:
        if self._running:
            logger.warning("Analyzer уже запущен")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name="analyzer-thread", daemon=True)
        self._set_running(True)
        self._thread.start()
        logger.info("Analyzer v2.0.0 запущен (интервал: 3600с)")

    def stop(self) -> None:
        if not self._running:
            return
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=30)
        self._set_running(False)
        logger.info("Analyzer остановлен")

    def run_once(self) -> bool:
        self._mark_run_start()
        logger.info("Analyzer: запуск цикла анализа...")
        try:
            result = self._analyze()
            if result:
                self._mark_success()
            else:
                self._mark_error("_analyze() вернул False")
            return result
        except Exception as e:
            self._mark_error(str(e))
            logger.error(f"Analyzer run_once ошибка: {e}", exc_info=True)
            return False

    def get_config(self) -> dict:
        return _load_config()

    def _run_loop(self) -> None:
        logger.info("Analyzer: фоновый поток запущен")
        while not self._stop_event.is_set():
            self.run_once()
            self._stop_event.wait(timeout=3600)
        logger.info("Analyzer: фоновый поток завершён")

    def _analyze(self) -> bool:
        cfg        = _load_config()
        all_levels = self._fetch_raw_levels()

        if not all_levels:
            logger.warning("Analyzer: raw_levels пуста")
            return True

        logger.info(f"Analyzer: загружено {len(all_levels)} уровней")

        qualified = self._filter_qualified(all_levels, cfg)
        logger.info(
            f"Analyzer: прошли фильтр {len(qualified)}/{len(all_levels)} "
            f"(min_bounce={cfg['min_bounce_count']}, "
            f"max_age={cfg['max_touch_age_days']}д)"
        )

        # Вычисляем S
        results = []
        for row in qualified:
            try:
                results.append(calculate_strength(row, cfg, all_levels))
            except Exception as e:
                logger.error(
                    f"calculate_strength {row.get('symbol')}/"
                    f"{row.get('timeframe')}: {e}"
                )

        # Batch UPSERT через транзакцию
        saved = errors = 0
        try:
            with self._db.transaction() as tx:
                for r in results:
                    tx.upsert(
                        table="analyzed_levels",
                        insert_data={
                            "symbol":             r["symbol"],
                            "timeframe":          r["timeframe"],
                            "price_zone":         r["price_zone"],
                            "direction":          r["direction"],
                            "strength_score":     r["strength_score"],
                            "classification":     r["classification"],
                            "f_bounce":           r["f_bounce"],
                            "f_freshness":        r["f_freshness"],
                            "f_confluence":       r["f_confluence"],
                            "f_volume":           r["f_volume"],
                            "f_multitf":          r["f_multitf"],
                            "f_reversal":         r["f_reversal"],
                            "f_dynamics":         r["f_dynamics"],
                            "f_stat":             r["f_stat"],
                            "f_rsi":              r["f_rsi"],
                            "rsi_value":          r["rsi_value"],
                            "tf_confirmed_count": r["tf_confirmed_count"],
                            "last_touch_time":    r["last_touch_time"],
                            "raw_level_id":       r["raw_level_id"],
                        },
                        update_data={
                            "strength_score":     r["strength_score"],
                            "classification":     r["classification"],
                            "f_bounce":           r["f_bounce"],
                            "f_freshness":        r["f_freshness"],
                            "f_confluence":       r["f_confluence"],
                            "f_volume":           r["f_volume"],
                            "f_multitf":          r["f_multitf"],
                            "f_reversal":         r["f_reversal"],
                            "f_dynamics":         r["f_dynamics"],
                            "f_stat":             r["f_stat"],
                            "f_rsi":              r["f_rsi"],
                            "rsi_value":          r["rsi_value"],
                            "tf_confirmed_count": r["tf_confirmed_count"],
                            "last_touch_time":    r["last_touch_time"],
                        }
                    )
                    saved += 1
        except Exception as e:
            logger.error(f"Analyzer batch UPSERT откат: {e}")
            # Пробуем сохранить по одному без транзакции
            saved = errors = 0
            for r in results:
                try:
                    self._db.upsert(
                        table="analyzed_levels",
                        insert_data={
                            "symbol": r["symbol"], "timeframe": r["timeframe"],
                            "price_zone": r["price_zone"], "direction": r["direction"],
                            "strength_score": r["strength_score"],
                            "classification": r["classification"],
                            "f_bounce": r["f_bounce"], "f_freshness": r["f_freshness"],
                            "f_confluence": r["f_confluence"], "f_volume": r["f_volume"],
                            "f_multitf": r["f_multitf"], "f_reversal": r["f_reversal"],
                            "f_dynamics": r["f_dynamics"], "f_stat": r["f_stat"],
                            "f_rsi": r["f_rsi"], "rsi_value": r["rsi_value"],
                            "tf_confirmed_count": r["tf_confirmed_count"],
                            "last_touch_time": r["last_touch_time"],
                            "raw_level_id": r["raw_level_id"],
                        },
                        update_data={
                            "strength_score": r["strength_score"],
                            "classification": r["classification"],
                            "f_bounce": r["f_bounce"], "f_freshness": r["f_freshness"],
                            "f_confluence": r["f_confluence"], "f_multitf": r["f_multitf"],
                            "f_dynamics": r["f_dynamics"], "f_stat": r["f_stat"],
                            "f_rsi": r["f_rsi"], "rsi_value": r["rsi_value"],
                            "tf_confirmed_count": r["tf_confirmed_count"],
                            "last_touch_time": r["last_touch_time"],
                        }
                    )
                    saved += 1
                except Exception:
                    errors += 1

        # Топ-10
        top10 = sorted(results, key=lambda x: x["strength_score"],
                        reverse=True)[:10]
        logger.info(f"Analyzer завершён: сохранено {saved}, ошибок {errors}")
        logger.info("  Топ-10 уровней:")
        logger.info(
            f"  {'Символ':<8} {'ТФ':<5} {'Напр.':<13} "
            f"{'Зона':<10} {'S':>5}  {'Класс':<12} "
            f"{'B':>5} {'F':>5} {'C':>5} {'M':>5} "
            f"{'ТФ подтв.'}"
        )
        for r in top10:
            logger.info(
                f"  {r['symbol']:<8} {r['timeframe']:<5} "
                f"{r['direction']:<13} "
                f"{r['price_zone']:<10.5f} "
                f"{r['strength_score']:>5.2f}  "
                f"{r['classification']:<12} "
                f"{r['f_bounce']:>5.2f} "
                f"{r['f_freshness']:>5.2f} "
                f"{r['f_confluence']:>5.2f} "
                f"RSI={r.get('rsi_value',50):.0f}({r.get('f_rsi',0.5):.2f}) "
                f"{r['f_multitf']:>5.2f} "
                f"{r.get('tf_confirmed_list','')}"
            )

        self._db.log_to_db(
            "analyzer", "INFO",
            f"v2.0: {saved} уровней, "
            f"топ S={top10[0]['strength_score'] if top10 else 0:.2f}"
        )
        return errors == 0

    def _fetch_raw_levels(self) -> List[dict]:
        with self._db.cursor() as cur:
            cur.execute("""
                SELECT id, symbol, timeframe, price_level, price_zone,
                       direction, bounce_count, last_touch_time,
                       last_touch_volume, avg_volume, confluence_count,
                       tf_confirmed_count, is_role_reversal,
                       adx_value, ema_score, avg_bounce_pips,
                       COALESCE(rsi_value, 50.0) AS rsi_value
                FROM raw_levels
                ORDER BY symbol, timeframe, price_zone
            """)
            return cur.fetchall()

    def _filter_qualified(self, levels: List[dict],
                            cfg: dict) -> List[dict]:
        """
        Фильтр допуска к расчёту S.
        max_touch_age_days дифференцирован по таймфрейму:
          H1 = base (3 дня)
          H4 = base × 1.7 (~5 дней)
          D  = base × 4.7 (~14 дней)
        """
        min_b    = cfg["min_bounce_count"]
        base_age = cfg["max_touch_age_days"]
        now      = datetime.now(tz=timezone.utc)

        _tf_age_mult = {"H1": 1.0, "H4": 1.7, "D": 4.7}

        qualified = []
        for lvl in levels:
            if lvl["bounce_count"] < min_b:
                continue
            touch = lvl["last_touch_time"]
            if touch is None:
                continue
            if touch.tzinfo is None:
                touch = touch.replace(tzinfo=timezone.utc)
            mult   = _tf_age_mult.get(lvl["timeframe"], 1.0)
            cutoff = now - timedelta(days=base_age * mult)
            if touch < cutoff:
                continue
            qualified.append(lvl)
        return qualified


# ------------------------------------------------------------------
# Фабричная функция
# ------------------------------------------------------------------
def get_module() -> AnalyzerModule:
    return AnalyzerModule()
