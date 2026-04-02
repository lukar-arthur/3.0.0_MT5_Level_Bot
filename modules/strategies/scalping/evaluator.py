# ============================================================
#  MT5_Level_Bot — modules/strategies/scalping/evaluator.py
#  Version : 1.0.0
#
#  SignalEvaluator — фоновый оценщик виртуальных результатов.
#
#  ЗАДАЧА: для каждого сигнала (confirmed/expired/cancelled)
#  проверить постфактум куда пошла цена — TP или SL —
#  и записать виртуальный результат в signal_queue.
#
#  Это позволяет анализировать стратегию без реального
#  открытия сделок. Запускается каждые 5 минут.
#
#  Алгоритм оценки:
#    1. Берём сигналы старше 30 мин (TTL истёк) без оценки
#    2. Запрашиваем бары M5 из MT5 за период жизни сигнала
#    3. Ищем: какой уровень (TP или SL) был достигнут первым
#    4. Если ни один не достигнут за 2 часа → expired_neutral
# ============================================================

import threading
from datetime import datetime, timezone, timedelta
from typing import Optional, List

from core.db_connection  import get_db
from core.mt5_bridge     import get_mt5_bridge
from core.utils          import get_logger
from core.config_loader  import load_module_config

# Plug-and-Play: scalping использует config/scalping.ini
_MODULE_CFG = load_module_config("scalping")

logger = get_logger("evaluator")

# Параметры из config/scalping.ini [EVALUATOR]
def _get_cfg():
    _MODULE_CFG.reload()
    return {
        "interval_sec":   _MODULE_CFG.getint("EVALUATOR", "interval_sec",   fallback=300),
        "eval_delay_min": _MODULE_CFG.getint("EVALUATOR", "eval_delay_min", fallback=30),
        "eval_window_min":_MODULE_CFG.getint("EVALUATOR", "eval_window_min",fallback=120),
        "batch_size":     _MODULE_CFG.getint("EVALUATOR", "batch_size",     fallback=20),
    }

_EVAL_INTERVAL_SEC  = 300
_EVAL_DELAY_MIN     = 30
_EVAL_WINDOW_MIN    = 120
_BATCH_SIZE         = 20


def _pip_size(symbol: str) -> float:
    return 0.01 if symbol.upper().endswith("JPY") else 0.0001


class SignalEvaluator:
    """
    Фоновый оценщик результатов сигналов.
    Запускается как часть ScalpingModule.
    """

    def __init__(self):
        self._db     = get_db()
        self._bridge = get_mt5_bridge()
        self._thread: Optional[threading.Thread] = None
        self._stop   = threading.Event()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name="signal-evaluator",
            daemon=True
        )
        self._thread.start()
        logger.info("SignalEvaluator запущен")

    def stop(self) -> None:
        self._stop.set()
        logger.info("SignalEvaluator остановлен")

    # ----------------------------------------------------------
    # Основной цикл
    # ----------------------------------------------------------

    def _loop(self) -> None:
        # Первый запуск через 60 сек после старта бота
        self._stop.wait(timeout=60)
        while not self._stop.is_set():
            try:
                evaluated = self._run_batch()
                if evaluated:
                    logger.info(
                        f"SignalEvaluator: оценено {evaluated} сигналов")
            except Exception as e:
                logger.debug(f"SignalEvaluator ошибка: {e}")
            self._stop.wait(timeout=_EVAL_INTERVAL_SEC)

    def _run_batch(self) -> int:
        """Оценивает пачку необработанных сигналов."""
        signals = self._fetch_unevaluated()
        if not signals:
            return 0

        if not self._bridge.monitor.is_connected:
            logger.debug("SignalEvaluator: MT5 недоступен")
            return 0

        evaluated = 0
        for sig in signals:
            try:
                outcome, profit_pips, close_price = \
                    self._evaluate_signal(sig)
                self._save_outcome(
                    sig["id"], outcome,
                    profit_pips, close_price)
                evaluated += 1
            except Exception as e:
                logger.debug(
                    f"Ошибка оценки сигнала {sig['id']}: {e}")

        return evaluated

    # ----------------------------------------------------------
    # Загрузка сигналов для оценки
    # ----------------------------------------------------------

    def _fetch_unevaluated(self) -> List[dict]:
        """
        Берём сигналы которые:
        - старше _EVAL_DELAY_MIN минут (TTL истёк)
        - ещё не оценены (evaluated_at IS NULL)
        - статус: confirmed, expired, cancelled, opened
          (pending пропускаем — ещё активны)
        """
        cutoff = (datetime.now(tz=timezone.utc)
                  - timedelta(minutes=_EVAL_DELAY_MIN))
        try:
            with self._db.cursor() as cur:
                cur.execute("""
                    SELECT id, symbol, direction,
                           price_zone, entry_price,
                           sl_price, tp_price,
                           sl_pips, tp_pips,
                           s_score, t_score,
                           status, created_at
                    FROM signal_queue
                    WHERE evaluated_at IS NULL
                      AND status IN (
                          'confirmed','expired','cancelled')
                      AND created_at < %s
                      -- 'opened' и 'closed' не оцениваем виртуально:
                      -- по ним есть РЕАЛЬНЫЕ данные в таблице trades
                    ORDER BY created_at ASC
                    LIMIT %s
                """, (cutoff, _BATCH_SIZE))
                return cur.fetchall()
        except Exception as e:
            logger.debug(f"_fetch_unevaluated: {e}")
            return []

    # ----------------------------------------------------------
    # Оценка одного сигнала
    # ----------------------------------------------------------

    def _evaluate_signal(
        self, sig: dict
    ) -> tuple:
        """
        Оценивает сигнал по историческим барам M5.

        Returns:
            (outcome, profit_pips, close_price)
            outcome: 'tp_hit' | 'sl_hit' | 'expired_neutral'
        """
        symbol    = sig["symbol"]
        direction = sig["direction"]
        entry     = float(sig["entry_price"])
        sl        = float(sig["sl_price"])
        tp        = float(sig["tp_price"])
        pip       = _pip_size(symbol)
        created   = sig["created_at"]

        # Делаем created timezone-aware если нет
        if hasattr(created, "tzinfo") and created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)

        # П-7 ИСПРАВЛЕНО: используем bridge.get_rates_from() —
        # стандартный метод проекта, возвращает list[dict]
        cfg = _get_cfg()
        eval_window = cfg["eval_window_min"]
        bars_needed = eval_window // 5 + 5
        try:
            with self._bridge.session() as mt5:
                rates = self._bridge.get_rates_from(
                    mt5, symbol, "M5", created, bars_needed)
        except Exception as e:
            logger.debug(f"Не удалось получить бары {symbol}: {e}")
            return "unknown", None, None

        if not rates:
            return "unknown", None, None

        # Ищем какой уровень (TP или SL) достигнут первым
        for bar in rates:
            high = float(bar["high"])
            low  = float(bar["low"])

            if direction == "Support":  # Buy
                if low <= sl:
                    profit_pips = -round(abs(entry - sl) / pip)
                    return "sl_hit", profit_pips, round(sl, 5)
                if high >= tp:
                    profit_pips = round(abs(tp - entry) / pip)
                    return "tp_hit", profit_pips, round(tp, 5)
            else:  # Resistance / Sell
                if high >= sl:
                    profit_pips = -round(abs(sl - entry) / pip)
                    return "sl_hit", profit_pips, round(sl, 5)
                if low <= tp:
                    profit_pips = round(abs(entry - tp) / pip)
                    return "tp_hit", profit_pips, round(tp, 5)

        # Ни TP ни SL не были достигнуты за 2 часа
        # Берём цену закрытия последнего бара как "нейтральную"
        last_close = float(rates[-1]["close"])
        if direction == "Support":
            neutral_pips = round((last_close - entry) / pip)
        else:
            neutral_pips = round((entry - last_close) / pip)

        return "expired_neutral", neutral_pips, round(last_close, 5)

    # ----------------------------------------------------------
    # Сохранение результата
    # ----------------------------------------------------------

    def _save_outcome(self,
                      signal_id: int,
                      outcome: str,
                      profit_pips: Optional[float],
                      close_price: Optional[float]) -> None:
        try:
            with self._db.cursor(commit=True) as cur:
                cur.execute("""
                    UPDATE signal_queue
                    SET virtual_outcome      = %s,
                        virtual_profit_pips  = %s,
                        virtual_close_price  = %s,
                        evaluated_at         = %s
                    WHERE id = %s
                      AND evaluated_at IS NULL
                """, (
                    outcome,
                    profit_pips,
                    close_price,
                    datetime.now(tz=timezone.utc),
                    signal_id
                ))
        except Exception as e:
            logger.error(f"_save_outcome id={signal_id}: {e}")
