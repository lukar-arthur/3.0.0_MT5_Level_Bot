# ============================================================
#  MT5_Level_Bot — core/db_connection.py  v3.0.0
#
#  ИСПРАВЛЕНИЯ v3.0.0:
#  П-5: ping() — AttributeError при сбое init() (pool=None)
#  БАГ-1: пароль берётся из env MT5_PASSWORD / DB_PASSWORD
#  Plug-and-Play: использует core.config_loader вместо
#    жёстко прописанного _CONFIG_PATH
# ============================================================

import os
import threading
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple

import mysql.connector
from mysql.connector import pooling, Error as MySQLError

from core.config_loader import load_db_config
from core.utils import get_logger, utcnow

logger = get_logger("db_connection")


class DBConnection:
    """Thread-safe MySQL connection pool — singleton."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._pool        = None
                cls._instance._initialized = False
                cls._instance._init_lock   = threading.Lock()
            return cls._instance

    def init(self, pool_size: int = 5) -> None:
        with self._init_lock:
            if self._initialized:
                return
            db_cfg = load_db_config()
            # П-5/БАГ-1: пароль из env переменной если не задан в config
            if not db_cfg["password"]:
                db_cfg["password"] = os.environ.get("DB_PASSWORD", "")
            try:
                self._pool = pooling.MySQLConnectionPool(
                    pool_name="mt5_pool",
                    pool_size=pool_size,
                    pool_reset_session=True,
                    **db_cfg
                )
                self._initialized = True
                logger.info(
                    f"MySQL pool initialised: "
                    f"{db_cfg['user']}@{db_cfg['host']}:{db_cfg['port']}"
                    f"/{db_cfg['database']} (pool_size={pool_size})"
                )
            except MySQLError as e:
                logger.warning(f"MySQL недоступен: {e}")
                raise

    def ping(self) -> bool:
        """
        ИСПРАВЛЕНИЕ П-5: AttributeError при pool=None.
        Если init() упал — pool остаётся None.
        Явная проверка предотвращает AttributeError.
        """
        try:
            if not self._initialized or self._pool is None:
                self.init()
            # После init() pool может быть None если init() бросил исключение
            if self._pool is None:
                return False
            conn = self._pool.get_connection()
            conn.ping(reconnect=True, attempts=2, delay=1)
            conn.close()
            return True
        except Exception:
            self._initialized = False
            return False

    @contextmanager
    def cursor(self, dictionary: bool = True, commit: bool = False):
        conn = None; cur = None
        try:
            conn = self._pool.get_connection()
            try:
                conn.ping(reconnect=True, attempts=3, delay=1)
            except Exception:
                conn.close(); conn = self._pool.get_connection()

            conn.autocommit = not commit
            cur = conn.cursor(dictionary=dictionary)
            yield cur
            if commit:
                conn.commit()
        except MySQLError as e:
            if conn and commit:
                try: conn.rollback()
                except Exception: pass
            logger.error(f"Transaction rolled back: {e}")
            raise
        finally:
            if cur:
                try: cur.close()
                except Exception: pass
            if conn:
                try: conn.close()
                except Exception: pass

    def upsert(self, table: str,
               insert_data: Dict[str, Any],
               update_data: Dict[str, Any]) -> Optional[int]:
        sql, values = _build_upsert_sql(table, insert_data, update_data)
        try:
            with self.cursor(commit=True) as cur:
                cur.execute(sql, values)
                return cur.lastrowid
        except MySQLError as e:
            logger.error(f"UPSERT failed on {table}: {e}")
            return None

    @contextmanager
    def transaction(self):
        conn = None
        try:
            conn = self._pool.get_connection()
            try:
                conn.ping(reconnect=True, attempts=3, delay=1)
            except Exception:
                conn.close(); conn = self._pool.get_connection()
            conn.autocommit = False
            yield _TransactionContext(conn)
            conn.commit()
        except Exception as e:
            if conn:
                try: conn.rollback()
                except Exception: pass
            logger.error(f"Transaction rolled back: {e}")
            raise
        finally:
            if conn:
                try: conn.close()
                except Exception: pass

    def execute_many(self, sql: str, params_list: List[Tuple]) -> bool:
        try:
            with self.cursor(commit=True) as cur:
                cur.executemany(sql, params_list)
            return True
        except MySQLError as e:
            logger.error(f"execute_many failed: {e}")
            return False

    def log_to_db(self, module_name: str, level: str, message: str) -> None:
        sql = ("INSERT INTO bot_logs "
               "(module_name, log_level, message, created_at) "
               "VALUES (%s, %s, %s, %s)")
        try:
            with self.cursor(commit=True) as cur:
                cur.execute(sql, (module_name, level.upper(), message, utcnow()))
        except MySQLError:
            pass

    def update_module_status(self, module_name: str, status: str) -> None:
        sql = (
            "UPDATE module_registry SET status = %s, "
            "last_started = IF(%s = 'running', %s, last_started), "
            "last_stopped = IF(%s = 'stopped', %s, last_stopped) "
            "WHERE module_name = %s"
        )
        now = utcnow()
        try:
            with self.cursor(commit=True) as cur:
                cur.execute(sql, (status, status, now, status, now, module_name))
        except MySQLError as e:
            logger.warning(f"update_module_status failed: {e}")


class _TransactionContext:
    def __init__(self, conn):
        self._conn = conn

    def upsert(self, table: str,
               insert_data: Dict[str, Any],
               update_data: Dict[str, Any]) -> None:
        sql, values = _build_upsert_sql(table, insert_data, update_data)
        cur = self._conn.cursor(dictionary=True)
        try:
            cur.execute(sql, values)
        finally:
            cur.close()

    def execute(self, sql: str, params=None) -> None:
        cur = self._conn.cursor(dictionary=True)
        try:
            cur.execute(sql, params or ())
        finally:
            cur.close()


def _build_upsert_sql(table: str,
                       insert_data: Dict[str, Any],
                       update_data: Dict[str, Any]) -> tuple:
    cols          = list(insert_data.keys())
    placeholders  = ", ".join(["%s"] * len(cols))
    col_names     = ", ".join(cols)
    update_clause = ", ".join([f"{k} = %s" for k in update_data.keys()])
    sql = (
        f"INSERT INTO {table} ({col_names}) "
        f"VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_clause}"
    )
    values = list(insert_data.values()) + list(update_data.values())
    return sql, values


_db_instance = DBConnection()


def get_db() -> DBConnection:
    return _db_instance
