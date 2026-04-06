# ============================================================
#  MT5_Level_Bot — core/mt5_bridge.py  v2.2.0
#
#  ИСПРАВЛЕНИЯ v2.2.0:
#  П-3: Race condition в _reconnect_count / _disconnected_at —
#       все изменения теперь внутри self._lock
#  БАГ-1: конфиг через config_loader (нет хардкода пути)
#  Plug-and-Play: load_mt5_config() из config_loader
# ============================================================

import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Dict, List, Optional

from core.config_loader import load_mt5_config
from core.utils import get_logger, retry

logger = get_logger("mt5_bridge")

TF_MAP_NAMES = {
    "M1":  "TIMEFRAME_M1",
    "M5":  "TIMEFRAME_M5",
    "M15": "TIMEFRAME_M15",
    "H1":  "TIMEFRAME_H1",
    "H4":  "TIMEFRAME_H4",
    "D":   "TIMEFRAME_D1",
    "W":   "TIMEFRAME_W1",
}

_REQUEST_TIMEOUT_SEC  = 7
_PING_INTERVAL_SEC    = 30
_ALERT_AFTER_SEC      = 300
_INIT_HARD_TIMEOUT_SEC = 10


def _safe_initialize(mt5, kwargs: dict,
                     timeout_sec: float = _INIT_HARD_TIMEOUT_SEC) -> bool:
    result = {"ok": False, "done": False}
    def _run():
        try:
            result["ok"] = mt5.initialize(**kwargs)
        except Exception as e:
            logger.debug(f"_safe_initialize thread exception: {e}")
            result["ok"] = False
        finally:
            result["done"] = True
    t = threading.Thread(target=_run, daemon=True, name="MT5-Init")
    t.start(); t.join(timeout=timeout_sec)
    if not result["done"]:
        logger.warning(
            f"mt5.initialize() не ответил за {timeout_sec}с — принудительно прерываем")
        try: mt5.shutdown()
        except Exception: pass
        return False
    return result["ok"]


class ConnectionState(Enum):
    CONNECTED    = "connected"
    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"


class ConnectionMonitor:
    """
    Фоновый поток мониторинга MT5.
    ИСПРАВЛЕНИЕ П-3: _reconnect_count и все поля состояния
    изменяются исключительно внутри self._lock.
    """

    def __init__(self, bridge: "MT5Bridge"):
        self._bridge   = bridge
        self._state    = ConnectionState.DISCONNECTED
        self._lock     = threading.Lock()
        self._callbacks: List[Callable] = []
        self._running  = False
        self._thread:  Optional[threading.Thread] = None

        self._disconnected_at:   Optional[float] = None
        self._reconnect_count    = 0
        self._last_connected_at: Optional[float] = None
        self._alert_sent         = False

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="MT5-Monitor")
        self._thread.start()
        logger.info("ConnectionMonitor запущен")

    def stop(self):
        self._running = False
        logger.info("ConnectionMonitor остановлен")

    def subscribe(self, callback: Callable[[ConnectionState, str], None]):
        with self._lock:
            self._callbacks.append(callback)

    def unsubscribe(self, callback: Callable):
        with self._lock:
            self._callbacks = [c for c in self._callbacks if c != callback]

    @property
    def state(self) -> ConnectionState:
        with self._lock:
            return self._state

    @property
    def is_connected(self) -> bool:
        with self._lock:
            return self._state == ConnectionState.CONNECTED

    def get_status_dict(self) -> Dict:
        # ИСПРАВЛЕНИЕ П-3: читаем все поля внутри одного lock
        with self._lock:
            disconnected_sec = None
            if self._disconnected_at:
                disconnected_sec = int(time.time() - self._disconnected_at)
            return {
                "state":             self._state.value,
                "is_connected":      self._state == ConnectionState.CONNECTED,
                "reconnect_count":   self._reconnect_count,
                "disconnected_sec":  disconnected_sec,
                "last_connected_at": self._last_connected_at,
            }

    def _loop(self):
        self._do_ping()
        while self._running:
            time.sleep(_PING_INTERVAL_SEC)
            if self._running:
                self._do_ping()

    def _do_ping(self):
        try:
            ok = self._bridge.ping()
        except Exception:
            ok = False
        if ok:
            self._on_connected()
        else:
            self._on_disconnected()

    def _on_connected(self):
        with self._lock:
            prev_state = self._state
            self._state             = ConnectionState.CONNECTED
            self._disconnected_at   = None
            self._alert_sent        = False
            self._last_connected_at = time.time()
            # ИСПРАВЛЕНИЕ П-3: счётчик внутри lock
            if prev_state == ConnectionState.RECONNECTING:
                self._reconnect_count += 1
                msg = (f"MT5 соединение восстановлено "
                       f"(попытка #{self._reconnect_count})")
            elif prev_state != ConnectionState.CONNECTED:
                msg = "MT5 подключён"
            else:
                return   # не изменилось

        logger.info(msg)
        self._notify(ConnectionState.CONNECTED, msg)

    def _on_disconnected(self):
        with self._lock:
            if self._disconnected_at is None:
                self._disconnected_at = time.time()
            prev_state   = self._state
            self._state  = ConnectionState.RECONNECTING
            disconnected_sec = int(time.time() - self._disconnected_at)
            alert_needed = (disconnected_sec >= _ALERT_AFTER_SEC
                            and not self._alert_sent)
            if alert_needed:
                self._alert_sent = True

        if prev_state == ConnectionState.CONNECTED:
            msg = "MT5 соединение потеряно — начинаем переподключение..."
            logger.warning(msg)
            self._notify(ConnectionState.DISCONNECTED, msg)

        logger.info(f"MT5 переподключение... (без связи {disconnected_sec}с)")
        self._notify(ConnectionState.RECONNECTING,
                     f"Переподключение к MT5... (без связи {disconnected_sec}с)")

        if alert_needed:
            msg = (f"⚠ MT5 недоступен уже {disconnected_sec // 60} мин! "
                   f"Проверь терминал и интернет!")
            logger.critical(msg)
            self._notify(ConnectionState.DISCONNECTED, msg)
            self._play_alert_sound()

    def _notify(self, state: ConnectionState, message: str):
        with self._lock:
            callbacks = list(self._callbacks)
        for cb in callbacks:
            try: cb(state, message)
            except Exception as e:
                logger.warning(f"Callback error в ConnectionMonitor: {e}")

    @staticmethod
    def _play_alert_sound():
        try:
            import winsound
            for _ in range(3):
                winsound.Beep(880, 400); time.sleep(0.2)
        except Exception:
            pass


class MT5Bridge:

    def __init__(self):
        self._lock               = threading.Lock()
        self._consecutive_errors = 0
        self._max_consecutive_errors = 3
        self.monitor             = ConnectionMonitor(self)

    @contextmanager
    def session(self):
        mt5 = self._connect()
        try:
            yield mt5
            self._consecutive_errors = 0
        except Exception as e:
            self._consecutive_errors += 1
            logger.error(
                f"MT5 session error "
                f"({self._consecutive_errors}/{self._max_consecutive_errors}): {e}"
            )
            if self._consecutive_errors >= self._max_consecutive_errors:
                logger.critical(
                    f"MT5: {self._consecutive_errors} сбоёв подряд — проверь терминал")
            raise
        finally:
            self._disconnect(mt5)

    def get_rates(self, mt5, symbol: str, timeframe_str: str,
                  bars: int = 500) -> Optional[list]:
        tf_const = self._resolve_timeframe(mt5, timeframe_str)
        if tf_const is None:
            return None
        t_start = time.time()
        rates   = mt5.copy_rates_from_pos(symbol, tf_const, 0, bars + 1)
        elapsed = time.time() - t_start
        if elapsed > _REQUEST_TIMEOUT_SEC:
            logger.warning(f"get_rates медленно: {symbol} {timeframe_str} заняло {elapsed:.1f}с")
        if rates is None or len(rates) == 0:
            err = mt5.last_error()
            logger.error(f"copy_rates_from_pos failed {symbol}/{timeframe_str}: {err}")
            return None
        closed = rates[1:]
        return [
            {
                "time":   datetime.fromtimestamp(r["time"], tz=timezone.utc),
                "open":   float(r["open"]),
                "high":   float(r["high"]),
                "low":    float(r["low"]),
                "close":  float(r["close"]),
                "volume": int(r["tick_volume"]),
            }
            for r in closed
        ]

    def get_rates_from(self, mt5, symbol: str, timeframe_str: str,
                       from_dt: datetime, count: int) -> Optional[list]:
        """
        ИСПРАВЛЕНИЕ П-7: метод для загрузки баров с конкретной даты.
        Используется в evaluator.py (ранее вызывал mt5.copy_rates_from напрямую).
        """
        tf_const = self._resolve_timeframe(mt5, timeframe_str)
        if tf_const is None:
            return None
        rates = mt5.copy_rates_from(symbol, tf_const, from_dt, count)
        if rates is None or len(rates) == 0:
            return None
        return [
            {
                "time":   datetime.fromtimestamp(r["time"], tz=timezone.utc),
                "open":   float(r["open"]),
                "high":   float(r["high"]),
                "low":    float(r["low"]),
                "close":  float(r["close"]),
                "volume": int(r["tick_volume"]),
            }
            for r in rates
        ]

    def get_current_price(self, mt5, symbol: str) -> Optional[Dict]:
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            logger.error(f"symbol_info_tick failed: {symbol}")
            return None
        return {
            "symbol": symbol,
            "bid":    tick.bid,
            "ask":    tick.ask,
            "spread": round((tick.ask - tick.bid) * 10000, 1),
            "time":   datetime.fromtimestamp(tick.time, tz=timezone.utc),
        }

    def get_symbol_info(self, mt5, symbol: str) -> Optional[Dict]:
        info = mt5.symbol_info(symbol)
        if info is None:
            logger.warning(f"symbol_info not found: {symbol}")
            return None
        return {
            "symbol":      symbol,
            "digits":      info.digits,
            "point":       info.point,
            "spread":      info.spread,
            "trade_mode":  info.trade_mode,
            "volume_min":  info.volume_min,
            "volume_max":  info.volume_max,
            "volume_step": info.volume_step,
        }

    def get_account_info(self, mt5) -> Optional[Dict]:
        info = mt5.account_info()
        if info is None:
            logger.error("account_info() вернул None")
            return None
        return {
            "login":       info.login,
            "balance":     info.balance,
            "equity":      info.equity,
            "margin":      info.margin,
            "margin_free": info.margin_free,
            "currency":    info.currency,
            "leverage":    info.leverage,
            "server":      info.server,
        }

    def get_available_symbols(self, mt5) -> List[str]:
        symbols = mt5.symbols_get()
        if not symbols:
            return []
        return [s.name for s in symbols if s.visible]

    def ping(self) -> bool:
        try:
            import MetaTrader5 as mt5
            cfg    = load_mt5_config()
            # БАГ-1: пароль из env если не задан
            if not cfg["password"]:
                cfg["password"] = os.environ.get("MT5_PASSWORD", "")
            kwargs = {"path": cfg["terminal_path"], "timeout": cfg["timeout"]}
            if cfg["login"]:
                kwargs.update({"login": cfg["login"],
                               "password": cfg["password"],
                               "server": cfg["server"]})
            ok = _safe_initialize(mt5, kwargs)
            if ok:
                info = mt5.terminal_info()
                mt5.shutdown()
                return info is not None
            try: mt5.shutdown()
            except Exception: pass
            return False
        except Exception:
            return False

    def _connect(self):
        try:
            import MetaTrader5 as mt5
        except ImportError:
            raise RuntimeError("MetaTrader5 не установлен. pip install MetaTrader5")

        cfg = load_mt5_config()
        if not cfg["password"]:
            cfg["password"] = os.environ.get("MT5_PASSWORD", "")
        kwargs = {"path": cfg["terminal_path"], "timeout": cfg["timeout"]}
        if cfg["login"]:
            kwargs.update({"login": cfg["login"],
                           "password": cfg["password"],
                           "server": cfg["server"]})

        with self._lock:
            ok = _safe_initialize(mt5, kwargs)
            if not ok:
                err = mt5.last_error()
                try: mt5.shutdown()
                except Exception: pass
                raise ConnectionError(f"mt5.initialize() failed: {err}")

        info = mt5.terminal_info()
        logger.debug(f"MT5 подключён: build={info.build if info else '?'}")
        return mt5

    def _disconnect(self, mt5) -> None:
        try:
            mt5.shutdown()
            logger.debug("MT5 отключён")
        except Exception as e:
            logger.warning(f"MT5 shutdown error: {e}")

    @staticmethod
    def _resolve_timeframe(mt5, tf_str: str):
        const_name = TF_MAP_NAMES.get(tf_str.upper())
        if not const_name:
            logger.error(f"Неизвестный таймфрейм: {tf_str}")
            return None
        tf_const = getattr(mt5, const_name, None)
        if tf_const is None:
            logger.error(f"mt5 не имеет атрибута {const_name}")
        return tf_const


_bridge_instance = MT5Bridge()


def get_mt5_bridge() -> MT5Bridge:
    return _bridge_instance
