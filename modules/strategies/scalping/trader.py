# ============================================================
#  MT5_Level_Bot — modules/strategies/scalping/trader.py
#  Version : 3.0.0
#
#  НОВОЕ v3.0.0 — Профессиональная система управления позицией:
#
#  1. ФИКСИРОВАННЫЙ SL = 8 пипсов ($0.80 при лоте 0.01)
#     Это 1% депозита $83 — профессиональная норма.
#     Не ATR-зависимый — предсказуемый риск.
#
#  2. BREAKEVEN при P&L ≥ +5п
#     SL переносится на уровень входа.
#     Сделка становится "бесплатной".
#
#  3. ЧАСТИЧНОЕ ЗАКРЫТИЕ при P&L ≥ +8п
#     Закрываем 50% позиции → гарантируем $0.40.
#     Оставшиеся 50% едут с SL на безубытке.
#
#  4. ТАЙМ-СТОП через 20 минут
#     Если после 20 мин P&L в диапазоне -2..+4п
#     (не идёт никуда) → закрываем в ноль.
#     Избегаем "зависших" сделок.
#
#  5. ФИЛЬТР ДУБЛЕЙ — блокирует новые сигналы
#     пока по этой паре+направлению открыта реальная сделка.
#
#  Мониторинг каждые 10 секунд (было 30).
# ============================================================

import configparser
import os
import threading
from datetime import datetime, timezone, timedelta
from typing   import Optional, List

from core.base_module    import BaseModule
from core.db_connection  import get_db
from core.mt5_bridge     import get_mt5_bridge
from core.utils          import get_logger
from core.config_loader  import load_module_config

# Plug-and-Play: scalping использует config/scalping.ini
_MODULE_CFG = load_module_config("scalping")

logger = get_logger("trader")

# Plug-and-Play: путь конфига — через config_loader

# ── Параметры управления позицией ────────────────────────────
_MONITOR_SEC       = 10    # проверяем каждые 10 сек (было 30)
_LOT_SIZE          = 0.01
_MAGIC_NUMBER      = 20250318

# Уровни управления позицией (в пипсах)
_SL_FIXED_PIPS     = 8     # фиксированный SL = 8п = $0.80
_BREAKEVEN_PIPS    = 5     # при +5п → SL в безубыток
_PARTIAL_CLOSE_PIPS= 8     # при +8п → закрыть 50%
_TIME_STOP_MIN     = 20    # закрыть если болтается 20 мин без движения
_TIME_STOP_MIN_PIPS= -2    # нижняя граница "болтания"
_TIME_STOP_MAX_PIPS= 4     # верхняя граница "болтания"


def _load_config() -> dict:
    """Plug-and-Play: читает из config/scalping.ini (fallback: config.ini)."""
    cfg = _MODULE_CFG
    cfg.reload()
    return {
        "monitor_sec":        cfg.getint    ("TRADER", "monitor_sec",        fallback=_MONITOR_SEC),
        "lot_size":           cfg.getfloat  ("TRADER", "lot_size",           fallback=_LOT_SIZE),
        "allow_real":         cfg.getboolean("TRADER", "allow_real",         fallback=False),
        "magic":              cfg.getint    ("TRADER", "magic",              fallback=_MAGIC_NUMBER),
        "sl_fixed_pips":      cfg.getint    ("TRADER", "sl_fixed_pips",      fallback=_SL_FIXED_PIPS),
        "breakeven_pips":     cfg.getint    ("TRADER", "breakeven_pips",     fallback=_BREAKEVEN_PIPS),
        "partial_close_pips": cfg.getint    ("TRADER", "partial_close_pips", fallback=_PARTIAL_CLOSE_PIPS),
        "time_stop_min":      cfg.getint    ("TRADER", "time_stop_min",      fallback=_TIME_STOP_MIN),
        "time_stop_min_pips": cfg.getint    ("TRADER", "time_stop_min_pips", fallback=_TIME_STOP_MIN_PIPS),
        "time_stop_max_pips": cfg.getint    ("TRADER", "time_stop_max_pips", fallback=_TIME_STOP_MAX_PIPS),
        "trailing_stop_pips": cfg.getint    ("TRADER", "trailing_stop_pips", fallback=8),
        "trailing_start_pips":cfg.getint    ("TRADER", "trailing_start_pips",fallback=5),
    }


def _pip_size(symbol: str) -> float:
    return 0.01 if symbol.upper().endswith("JPY") else 0.0001


# ------------------------------------------------------------------
# Открытие ордера через bridge
# ------------------------------------------------------------------

def _open_trade_via_bridge(signal: dict, lot: float, magic: int,
                            sl_fixed_pips: int) -> dict:
    """
    Открывает рыночный ордер.
    SL фиксированный = sl_fixed_pips от MID-цены (с учётом спреда).
    TP динамический = sl_fixed_pips * 2.5 (R:R = 1:2.5).

    ИСПРАВЛЕНИЕ: SL теперь считается от MID-цены, а не от ASK/BID.

    Проблема старого кода:
      Buy:  price = ASK,  SL = ASK - 8п
      Но MT5 срабатывает SL по BID. Если спред = 1п,
      то BID = ASK - 1п, значит реальное расстояние до SL = 7п.
      Спред «съедал» 1 пип буфера сразу при открытии.

    Новая логика:
      mid   = (ASK + BID) / 2
      Buy:  entry = ASK,  SL = mid - sl_fixed_pips * pip
      Sell: entry = BID,  SL = mid + sl_fixed_pips * pip

      Теперь SL всегда ровно sl_fixed_pips от средней цены,
      независимо от размера спреда.
    """
    bridge = get_mt5_bridge()
    try:
        with bridge.session() as mt5:
            symbol    = signal["symbol"]
            direction = signal["direction"]
            pip       = _pip_size(symbol)

            price_info = bridge.get_current_price(mt5, symbol)
            if not price_info:
                return {"success": False, "ticket": None,
                        "price": None, "error": "нет тика"}

            ask = price_info["ask"]
            bid = price_info["bid"]
            mid = (ask + bid) / 2   # средняя цена — нейтральная точка отсчёта

            # Цена исполнения: ASK для Buy, BID для Sell
            price = ask if direction == "Support" else bid

            # SL от MID — одинаковое реальное расстояние для Buy и Sell
            sl_price = (mid - sl_fixed_pips * pip) \
                if direction == "Support" \
                else (mid + sl_fixed_pips * pip)

            # TP = SL × 2.5 от цены исполнения, минимум 20п (15п для JPY)
            # R:R = 1:2.5 → математика работает при WR > 28%
            min_tp = 15 if symbol.upper().endswith("JPY") else 20
            tp_pips  = max(round(sl_fixed_pips * 2.5), min_tp)
            tp_price = (price + tp_pips * pip) \
                if direction == "Support" \
                else (price - tp_pips * pip)

            sl_price = round(sl_price, 5)
            tp_price = round(tp_price, 5)

            # Реальное расстояние до SL от цены входа (для лога)
            real_sl_pips = round(abs(price - sl_price) / pip)

            sym_info = bridge.get_symbol_info(mt5, symbol)
            if not sym_info:
                mt5.symbol_select(symbol, True)

            order_type = mt5.ORDER_TYPE_BUY \
                if direction == "Support" else mt5.ORDER_TYPE_SELL

            request = {
                "action":       mt5.TRADE_ACTION_DEAL,
                "symbol":       symbol,
                "volume":       lot,
                "type":         order_type,
                "price":        price,
                "sl":           sl_price,
                "tp":           tp_price,
                "deviation":    20,
                "magic":        magic,
                "comment":      f"LvlBot S={signal.get('s_score',0):.2f}",
                "type_time":    mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }

            result = mt5.order_send(request)

            if result is None:
                return {"success": False, "ticket": None,
                        "price": price,
                        "error": f"order_send=None: {mt5.last_error()}"}

            if result.retcode == mt5.TRADE_RETCODE_DONE:
                spread_pips = round((ask - bid) / pip, 1)
                logger.info(
                    f"Ордер открыт: {symbol} {direction} "
                    f"ticket={result.order} price={result.price:.5f} "
                    f"mid={mid:.5f} спред={spread_pips}п "
                    f"SL={sl_price:.5f}(-{real_sl_pips}п от входа, -{sl_fixed_pips}п от mid) "
                    f"TP={tp_price:.5f}(+{tp_pips}п) lot={lot}"
                )
                return {
                    "success":   True,
                    "ticket":    result.order,
                    "price":     result.price,
                    "sl_price":  sl_price,
                    "tp_price":  tp_price,
                    "sl_pips":   real_sl_pips,   # реальное расстояние от входа
                    "tp_pips":   tp_pips,
                    "error":     None,
                }
            else:
                err = f"retcode={result.retcode} {result.comment}"
                logger.error(f"Ошибка открытия {symbol}: {err}")
                return {"success": False, "ticket": None,
                        "price": price, "error": err}

    except Exception as e:
        logger.error(f"_open_trade_via_bridge: {e}", exc_info=True)
        return {"success": False, "ticket": None,
                "price": None, "error": str(e)}


# ------------------------------------------------------------------
# Управление SL через bridge (modify position)
# ------------------------------------------------------------------

def _modify_sl_via_bridge(ticket: int, symbol: str,
                           new_sl: float, tp: float) -> bool:
    """Переносит SL позиции (для безубытка или trailing)."""
    bridge = get_mt5_bridge()
    try:
        with bridge.session() as mt5:
            request = {
                "action":   mt5.TRADE_ACTION_SLTP,
                "position": ticket,
                "symbol":   symbol,
                "sl":       round(new_sl, 5),
                "tp":       round(tp, 5),
            }
            result = mt5.order_send(request)
            if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                return True
            err = result.comment if result else "None"
            logger.warning(
                f"Modify SL ticket={ticket}: retcode={result.retcode if result else '?'} {err}")
            return False
    except Exception as e:
        logger.error(f"_modify_sl_via_bridge: {e}")
        return False


# П-8 ИСПРАВЛЕНО: _partial_close_via_bridge удалена — мёртвый код.
# При лоте 0.01 частичное закрытие (0.005) технически невозможно.
# v3.0 использует агрессивный trailing вместо частичного закрытия.


def _close_position_via_bridge(ticket: int, symbol: str,
                                lot: float, direction: str) -> bool:
    """Полностью закрывает позицию."""
    bridge = get_mt5_bridge()
    try:
        with bridge.session() as mt5:
            price_info = bridge.get_current_price(mt5, symbol)
            if not price_info:
                return False
            price = price_info["bid"] \
                if direction == "Buy" else price_info["ask"]
            close_type = mt5.ORDER_TYPE_SELL \
                if direction == "Buy" else mt5.ORDER_TYPE_BUY
            request = {
                "action":       mt5.TRADE_ACTION_DEAL,
                "symbol":       symbol,
                "volume":       lot,
                "type":         close_type,
                "price":        price,
                "position":     ticket,
                "deviation":    20,
                "comment":      "time_stop",
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            result = mt5.order_send(request)
            return bool(result and
                        result.retcode == mt5.TRADE_RETCODE_DONE)
    except Exception as e:
        logger.error(f"_close_position_via_bridge: {e}")
        return False


# ------------------------------------------------------------------
# TraderModule
# ------------------------------------------------------------------

class TraderModule(BaseModule):

    def __init__(self):
        super().__init__("trader")
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._db         = get_db()
        self._bridge     = get_mt5_bridge()
        self._cfg        = _load_config()
        # П-2 ИСПРАВЛЕНО: _position_state читается/пишется из двух потоков
        # (GUI → open_trade, Monitor → _manage_open_position) — нужен Lock
        self._state_lock  = threading.Lock()
        self._position_state: dict = {}

    def start(self) -> None:
        if self._running:
            return
        self._stop_event.clear()
        self._monitor_thread = threading.Thread(
            target=self._run_monitor,
            name="trader-monitor",
            daemon=True
        )
        self._set_running(True)
        self._monitor_thread.start()
        logger.info(
            f"Trader v3.0 запущен — SL={self._cfg['sl_fixed_pips']}п "
            f"breakeven=+{self._cfg['breakeven_pips']}п "
            f"partial=+{self._cfg['partial_close_pips']}п "
            f"time_stop={self._cfg['time_stop_min']}мин"
        )

    def stop(self) -> None:
        if not self._running:
            return
        self._stop_event.set()
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=15)
        self._set_running(False)
        logger.info("Trader остановлен")

    def run_once(self) -> bool:
        self._mark_run_start()
        try:
            self._monitor_open_trades()
            self._mark_success()
            return True
        except Exception as e:
            self._mark_error(str(e))
            logger.error(f"Trader run_once: {e}", exc_info=True)
            return False

    def get_config(self) -> dict:
        return self._cfg

    # ----------------------------------------------------------
    # Открытие сделки
    # ----------------------------------------------------------

    def open_trade(self, signal: dict) -> dict:
        self._cfg = _load_config()

        if not self._cfg["allow_real"]:
            acct_type = self._get_account_type()
            if acct_type == "real":
                return {
                    "success": False,
                    "error": "Реальный счёт запрещён! "
                             "Включи allow_real=true в config.ini"
                }

        if not self._bridge.monitor.is_connected:
            return {"success": False, "error": "MT5 не подключён"}

        result = _open_trade_via_bridge(
            signal,
            lot=self._cfg["lot_size"],
            magic=self._cfg["magic"],
            sl_fixed_pips=self._cfg["sl_fixed_pips"]
        )

        if result["success"]:
            # Обновляем SL/TP в сигнале реальными значениями из MT5
            signal["sl_price"] = result["sl_price"]
            signal["tp_price"] = result["tp_price"]
            signal["sl_pips"]  = result["sl_pips"]
            signal["tp_pips"]  = result["tp_pips"]

            # БАГ№5: обновляем signal_queue реальными SL/TP пипсами
            if signal.get("id"):
                try:
                    with self._db.cursor(commit=True) as cur:
                        cur.execute("""
                            UPDATE signal_queue
                            SET sl_price=%s, tp_price=%s,
                                sl_pips=%s,  tp_pips=%s
                            WHERE id=%s
                        """, (
                            result["sl_price"], result["tp_price"],
                            result["sl_pips"],  result["tp_pips"],
                            signal["id"]
                        ))
                except Exception as e:
                    logger.debug(f"Обновление sl/tp в signal_queue: {e}")

            trade_id = self._save_trade(signal, result)
            result["trade_id"] = trade_id

            # Инициализируем состояние позиции
            ticket = result["ticket"]
            with self._state_lock:   # П-2: thread-safe запись
                self._position_state[ticket] = {
                    "breakeven_set": False,
                    "partial_done":  False,
                    "open_time":     datetime.now(tz=timezone.utc),
                    "remaining_lot": self._cfg["lot_size"],
                    "peak_pips":     0,
                    "trailing_on":   False,
                }
            logger.info(
                f"Сделка ID={trade_id} ticket={ticket} "
                f"— управление позицией активировано"
            )

        return result

    # ----------------------------------------------------------
    # Мониторинг позиций
    # ----------------------------------------------------------

    def _run_monitor(self) -> None:
        logger.info("Trader Monitor: поток запущен")
        while not self._stop_event.is_set():
            try:
                self._monitor_open_trades()
            except Exception as e:
                logger.debug(f"Monitor ошибка: {e}")
            self._stop_event.wait(
                timeout=self._cfg.get("monitor_sec", _MONITOR_SEC))
        logger.info("Trader Monitor: поток завершён")

    def _monitor_open_trades(self) -> None:
        if not self._bridge.monitor.is_connected:
            return
        open_trades = self._fetch_open_trades()
        if not open_trades:
            return

        cfg = _load_config()
        self._cfg = cfg

        try:
            with self._bridge.session() as mt5:
                DEAL_ENTRY_OUT = mt5.DEAL_ENTRY_OUT

                for trade in open_trades:
                    ticket = trade.get("mt5_ticket")
                    if not ticket:
                        continue

                    positions = mt5.positions_get(ticket=ticket)

                    if positions:
                        pos = positions[0]
                        self._manage_open_position(
                            trade, pos, cfg, mt5)
                    else:
                        history = mt5.history_deals_get(
                            position=ticket)
                        self._process_closed_trade(
                            trade, history, DEAL_ENTRY_OUT)

        except Exception as e:
            logger.error(f"_monitor_open_trades: {e}")

    # ----------------------------------------------------------
    # Управление открытой позицией
    # ----------------------------------------------------------

    def _manage_open_position(self, trade: dict, pos,
                               cfg: dict, mt5) -> None:
        """
        Профессиональная система управления позицией:
        1. Безубыток при +breakeven_pips
        2. Частичное закрытие при +partial_close_pips
        3. Тайм-стоп при болтании > time_stop_min
        """
        ticket    = trade["mt5_ticket"]
        symbol    = trade["symbol"]
        direction = trade["direction"]   # "Buy" или "Sell"
        entry     = float(trade["entry_price"])
        tp        = float(trade["tp_price"])
        sl        = float(pos.sl)        # текущий SL из MT5
        pip       = _pip_size(symbol)

        # Текущий P&L в пипсах
        price = pos.price_current
        if direction == "Buy":
            pnl_pips = round((price - entry) / pip)
        else:
            pnl_pips = round((entry - price) / pip)

        logger.debug(
            f"Позиция {ticket} открыта: {symbol} P&L={pnl_pips}п "
            f"[entry={entry:.5f} cur={price:.5f} sl={sl:.5f}]"
        )

        # Инициализируем состояние если нет (перезапуск бота)
        with self._state_lock:   # П-2: thread-safe чтение/инициализация
            if ticket not in self._position_state:
                self._position_state[ticket] = {
                    "breakeven_set": False,
                    "partial_done":  False,
                    "open_time":     datetime.now(tz=timezone.utc),
                    "remaining_lot": float(pos.volume),
                    "peak_pips":     0,
                    "trailing_on":   False,
                }
            state = self._position_state[ticket]

        # ── Шаг 1: BREAKEVEN при +breakeven_pips ─────────────
        if (not state["breakeven_set"] and
                pnl_pips >= cfg["breakeven_pips"]):

            # SL на уровне входа + 1 пип (чтобы покрыть спред)
            if direction == "Buy":
                new_sl = entry + pip
            else:
                new_sl = entry - pip

            # Только если новый SL лучше текущего
            current_sl = float(pos.sl)
            move_sl = (direction == "Buy" and new_sl > current_sl) or \
                      (direction == "Sell" and new_sl < current_sl)

            if move_sl and _modify_sl_via_bridge(ticket, symbol,
                                                  new_sl, tp):
                state["breakeven_set"] = True
                # Обновляем SL в БД
                self._update_sl_in_db(trade["id"], new_sl)
                logger.info(
                    f"✅ БЕЗУБЫТОК: {symbol} ticket={ticket} "
                    f"P&L=+{pnl_pips}п → SL перенесён на вход "
                    f"{entry:.5f} (риск = $0)"
                )

        # ── Шаг 2: АГРЕССИВНЫЙ TRAILING при +partial_close_pips ─
        # При лоте 0.01 частичное закрытие (0.005) невозможно.
        # Вместо этого: при достижении +partial_close_pips
        # активируем trailing с шагом 3п (агрессивный режим).
        if (not state["partial_done"] and
                pnl_pips >= cfg["partial_close_pips"]):
            state["partial_done"] = True
            state["trailing_on"]  = True
            logger.info(
                f"🎯 АГРЕССИВНЫЙ TRAILING: {symbol} ticket={ticket} "
                f"P&L=+{pnl_pips}п ≥ +{cfg['partial_close_pips']}п "
                f"→ trailing активирован (шаг 3п)"
            )


        # ── Шаг 3: TRAILING STOP ─────────────────────────────────
        # Обновляем пик P&L за всё время сделки
        if pnl_pips > state["peak_pips"]:
            state["peak_pips"] = pnl_pips

        trail_pips  = cfg.get("trailing_stop_pips",  8)
        trail_start = cfg.get("trailing_start_pips", 5)

        # Trailing активируется когда пик достиг trail_start пипсов
        if state["peak_pips"] >= trail_start:
            state["trailing_on"] = True

        if state["trailing_on"]:
            # Новый SL = пик - trail_distance пипсов
            # В агрессивном режиме (после +partial_close_pips) шаг = 3п
            effective_trail = 3 if state["partial_done"] else trail_pips
            trail_sl_offset = (state["peak_pips"] - effective_trail) * pip
            if direction == "Buy":
                new_trail_sl = entry + trail_sl_offset
                # Никогда не хуже безубытка
                new_trail_sl = max(new_trail_sl, entry + pip)
            else:
                new_trail_sl = entry - trail_sl_offset
                # Никогда не хуже безубытка
                new_trail_sl = min(new_trail_sl, entry - pip)

            current_sl_mt5 = float(pos.sl)
            # Двигаем SL только вперёд (в сторону прибыли), НИКОГДА назад
            should_trail = (
                (direction == "Buy"  and
                 new_trail_sl > current_sl_mt5 + pip) or
                (direction == "Sell" and
                 new_trail_sl < current_sl_mt5 - pip)
            )
            if should_trail:
                if _modify_sl_via_bridge(ticket, symbol,
                                          new_trail_sl, tp):
                    self._update_sl_in_db(trade["id"], new_trail_sl)
                    locked = state["peak_pips"] - trail_pips
                    logger.info(
                        f"📈 TRAILING: {symbol} ticket={ticket} "
                        f"P&L=+{pnl_pips}п пик=+{state['peak_pips']}п "
                        f"SL→{new_trail_sl:.5f} "
                        f"(зафиксировано +{locked}п)"
                    )

        # ── Шаг 4: ТАЙМ-СТОП ─────────────────────────────────
        elapsed = (datetime.now(tz=timezone.utc) -
                   state["open_time"]).total_seconds() / 60

        if (elapsed >= cfg["time_stop_min"] and
                cfg.get("time_stop_min_pips", _TIME_STOP_MIN_PIPS) <= pnl_pips
                <= cfg.get("time_stop_max_pips", _TIME_STOP_MAX_PIPS) and
                not state["partial_done"]):

            # Цена болтается у входа — закрываем
            if _close_position_via_bridge(
                    ticket, symbol,
                    state["remaining_lot"], direction):

                logger.info(
                    f"⏱ ТАЙМ-СТОП: {symbol} ticket={ticket} "
                    f"P&L={pnl_pips}п после {elapsed:.0f} мин "
                    f"— позиция закрыта (болтание)"
                )
                # Процесс закрытия подхватит _process_closed_trade
                # на следующем цикле монитора

    # ----------------------------------------------------------
    # Обработка закрытой позиции
    # ----------------------------------------------------------

    def _process_closed_trade(self, trade: dict,
                               history,
                               DEAL_ENTRY_OUT) -> None:
        ticket = trade["mt5_ticket"]
        if not history:
            self._update_trade_status(
                trade["id"], "error",
                close_reason="not_found")
            self._position_state.pop(ticket, None)
            return

        close_deal = None
        for deal in history:
            if deal.entry == DEAL_ENTRY_OUT:
                close_deal = deal
                break

        if not close_deal:
            return

        pip         = _pip_size(trade["symbol"])
        entry       = float(trade["entry_price"])
        close_price = close_deal.price
        profit_usd  = close_deal.profit

        if trade["direction"] == "Buy":
            profit_pips = round((close_price - entry) / pip)
        else:
            profit_pips = round((entry - close_price) / pip)

        tp = float(trade["tp_price"])
        sl = float(trade["sl_price"])
        # Сначала проверяем comment сделки (надёжнее чем proximity к SL/TP)
        comment = getattr(close_deal, "comment", "") or ""
        if "time_stop" in comment:
            reason = "time_stop"
        elif "partial" in comment:
            reason = "partial_close"
        elif abs(close_price - tp) < pip * 3:
            reason = "tp_hit"
        elif abs(close_price - sl) < pip * 3:
            reason = "sl_hit"
        else:
            reason = "manual"

        logger.info(
            f"Сделка закрыта: {trade['symbol']} "
            f"ticket={ticket} "
            f"profit={profit_pips}п (${profit_usd:.2f}) "
            f"причина={reason}"
        )

        self._update_trade_closed(
            trade_id=trade["id"],
            close_price=close_price,
            profit_pips=profit_pips,
            profit_usd=profit_usd,
            close_reason=reason,
            close_time=datetime.now(tz=timezone.utc)
        )

        if trade.get("signal_id"):
            try:
                with self._db.cursor(commit=True) as cur:
                    cur.execute("""
                        UPDATE signal_queue
                        SET status = 'closed'
                        WHERE id = %s
                    """, (trade["signal_id"],))
            except Exception:
                pass

        # Чистим кеш состояния позиции (thread-safe)
        with self._state_lock:
            self._position_state.pop(ticket, None)

    # ----------------------------------------------------------
    # Метод для Signal Engine — получить занятые пары
    # ----------------------------------------------------------

    def get_open_symbols_directions(self) -> set:
        """
        Возвращает set кортежей (symbol, direction).
        Signal Engine использует для блокировки дублей.
        Например: {('EURUSD', 'Support'), ('USDCHF', 'Sell')}
        """
        try:
            with self._db.cursor() as cur:
                cur.execute("""
                    SELECT symbol, direction
                    FROM trades
                    WHERE status = 'open'
                """)
                rows = cur.fetchall()
            result = set()
            for r in rows:
                d = r["direction"]
                # Нормализуем к формату signal_queue
                sig_dir = "Support" if d == "Buy" else "Resistance"
                result.add((r["symbol"], sig_dir))
            return result
        except Exception:
            return set()

    # ----------------------------------------------------------
    # Тип счёта
    # ----------------------------------------------------------

    def _get_account_type(self) -> str:
        try:
            with self._bridge.session() as mt5:
                info = mt5.account_info()
                if info:
                    return "demo" if info.trade_mode == 0 else "real"
        except Exception:
            pass
        return "demo"

    # ----------------------------------------------------------
    # DB helpers
    # ----------------------------------------------------------

    def _save_trade(self, signal: dict, result: dict) -> int:
        direction_trade = "Buy" \
            if signal["direction"] == "Support" else "Sell"
        now = datetime.now(tz=timezone.utc)
        with self._db.cursor(commit=True) as cur:
            cur.execute("""
                INSERT INTO trades
                (signal_id, symbol, direction, timeframe,
                 lot_size, mt5_ticket,
                 entry_price, sl_price, tp_price,
                 s_score, t_score,
                 status, mode, open_time, created_at)
                VALUES (%s,%s,%s,%s, %s,%s, %s,%s,%s,
                        %s,%s, 'open',%s,%s,%s)
            """, (
                signal.get("id"),
                signal["symbol"],
                direction_trade,
                signal.get("timeframe", "H4"),
                self._cfg["lot_size"],
                result["ticket"],
                result["price"],
                float(signal["sl_price"]),
                float(signal["tp_price"]),
                float(signal.get("s_score", 0)),
                float(signal.get("t_score", 0)),
                "demo" if not self._cfg["allow_real"] else "real",
                now, now
            ))
            return cur.lastrowid

    def _fetch_open_trades(self) -> list:
        try:
            with self._db.cursor() as cur:
                cur.execute("""
                    SELECT id, signal_id, symbol, direction,
                           entry_price, sl_price, tp_price,
                           mt5_ticket, lot_size
                    FROM trades
                    WHERE status = 'open'
                      AND mt5_ticket IS NOT NULL
                """)
                return cur.fetchall()
        except Exception as e:
            logger.debug(f"_fetch_open_trades: {e}")
            return []

    def _update_sl_in_db(self, trade_id: int,
                          new_sl: float) -> None:
        try:
            with self._db.cursor(commit=True) as cur:
                cur.execute("""
                    UPDATE trades SET sl_price=%s
                    WHERE id=%s
                """, (new_sl, trade_id))
        except Exception as e:
            logger.debug(f"_update_sl_in_db: {e}")

    def _update_trade_status(self, trade_id: int,
                              status: str,
                              close_reason: str = None) -> None:
        try:
            with self._db.cursor(commit=True) as cur:
                cur.execute("""
                    UPDATE trades SET status=%s, close_reason=%s
                    WHERE id=%s
                """, (status, close_reason, trade_id))
        except Exception as e:
            logger.error(f"_update_trade_status: {e}")

    def _update_trade_closed(self, trade_id: int,
                              close_price: float,
                              profit_pips: int,
                              profit_usd: float,
                              close_reason: str,
                              close_time: datetime) -> None:
        try:
            with self._db.cursor(commit=True) as cur:
                cur.execute("""
                    UPDATE trades
                    SET status='closed',
                        close_price=%s,
                        profit_pips=%s,
                        profit_usd=%s,
                        close_reason=%s,
                        close_time=%s
                    WHERE id=%s
                """, (close_price, profit_pips, profit_usd,
                      close_reason, close_time, trade_id))
        except Exception as e:
            logger.error(f"_update_trade_closed: {e}")


def get_module() -> TraderModule:
    return TraderModule()
