# ============================================================
#  Plug-and-Play: использует core.config_loader вместо
#    жёстко прописанного _CONFIG_PATH
# ============================================================
# ============================================================
#  MT5_Level_Bot — core/db_connection.py  v3.0.2
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
    """Потокобезопасный пул соединений MySQL (Синглтон)."""

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
        """Инициализация пула соединений."""
        with self._init_lock:
            if self._initialized:
                return
            
            db_cfg = load_db_config()
            if not db_cfg.get("password"):
                db_cfg["password"] = os.environ.get("DB_PASSWORD", "")
                
            try:
                db_params = {
                    "host": db_cfg["host"],
                    "port": db_cfg["port"],
                    "database": db_cfg["database"],
                    "user": db_cfg["user"],
                    "password": db_cfg["password"],
                    "charset": "utf8mb4",
                    "connect_timeout": 10,
                    "get_warnings": True,
                    "raise_on_warnings": False,
                    "use_unicode": True
                }
                
                self._pool = pooling.MySQLConnectionPool(
                    pool_name="mt5_pool",
                    pool_size=pool_size,
                    pool_reset_session=True,
                    **db_params
                )
                self._initialized = True
                logger.info(f"MySQL пул запущен: {db_cfg['user']}@{db_cfg['host']}/{db_cfg['database']}")
            except MySQLError as e:
                # Тихая ошибка при старте (DEBUG вместо ERROR)
                logger.debug(f"MySQL пока недоступен: {e}")
                raise

    def ping(self) -> bool:
        try:
            if not self._initialized or self._pool is None:
                self.init()
            if self._pool is None: return False
            conn = self._pool.get_connection()
            conn.ping(reconnect=True, attempts=2, delay=1)
            conn.close()
            return True
        except Exception:
            self._initialized = False
            return False

    @contextmanager
    def cursor(self, dictionary: bool = True, commit: bool = False):
        conn = None
        cur = None
        try:
            if not self._initialized: self.init()
            conn = self._pool.get_connection()
            conn.autocommit = not commit
            cur = conn.cursor(dictionary=dictionary)
            yield cur
            if commit: conn.commit()
        except MySQLError as e:
            if conn and commit: conn.rollback()
            if self._initialized: logger.error(f"Ошибка БД: {e}")
            raise
        finally:
            if cur: cur.close()
            if conn: conn.close()

    # --- ВОССТАНОВЛЕННЫЕ МЕТОДЫ (необходимы для работы модулей) ---

    def upsert(self, table: str, insert_data: Dict[str, Any], update_data: Dict[str, Any]) -> Optional[int]:
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
            if not self._initialized: self.init()
            conn = self._pool.get_connection()
            conn.autocommit = False
            ctx = _TransactionContext(conn)
            yield ctx
            conn.commit()
        except Exception as e:
            if conn and conn.is_connected(): conn.rollback()
            logger.error(f"Transaction rolled back: {e}")
            raise
        finally:
            if conn: conn.close()

    def log_to_db(self, module_name: str, level: str, message: str) -> None:
        sql = "INSERT INTO bot_logs (module_name, log_level, message, created_at) VALUES (%s, %s, %s, %s)"
        try:
            with self.cursor(commit=True) as cur:
                cur.execute(sql, (module_name, level.upper(), message, utcnow()))
        except MySQLError: pass

    def update_module_status(self, module_name: str, status: str) -> None:
        sql = "UPDATE module_registry SET status = %s WHERE module_name = %s"
        try:
            with self.cursor(commit=True) as cur:
                cur.execute(sql, (status, module_name))
        except MySQLError as e:
            logger.warning(f"update_module_status failed: {e}")

class _TransactionContext:
    def __init__(self, conn):
        self._conn = conn
    def execute(self, sql: str, params=None) -> None:
        cur = self._conn.cursor(dictionary=True)
        try: cur.execute(sql, params or ())
        finally: cur.close()
    def upsert(self, table: str, insert_data: Dict[str, Any], update_data: Dict[str, Any]) -> Optional[int]:
        sql, values = _build_upsert_sql(table, insert_data, update_data)
        cur = self._conn.cursor(dictionary=True)
        try:
            cur.execute(sql, values)
            return cur.lastrowid
        finally:
            cur.close()

def _build_upsert_sql(table: str, insert_data: Dict[str, Any], update_data: Dict[str, Any]) -> tuple:
    cols = list(insert_data.keys())
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join(cols)
    update_clause = ", ".join([f"{k} = %s" for k in update_data.keys()])
    sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"
    values = list(insert_data.values()) + list(update_data.values())
    return sql, values

_db_instance = DBConnection()
def get_db() -> DBConnection:
    return _db_instance