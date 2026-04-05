# ============================================================
#  MT5_Level_Bot — core/db_connection.py  v3.0.0
#
# 
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
            if self._initialized: return
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
                    "use_unicode": True
                }
                self._pool = pooling.MySQLConnectionPool(
                    pool_name="mt5_pool", pool_size=pool_size,
                    pool_reset_session=True, **db_params
                )
                self._initialized = True
                logger.info(f"MySQL пул запущен: {db_cfg['user']}@{db_cfg['host']}/{db_cfg['database']}")
            except MySQLError as e:
                # Используем DEBUG вместо ERROR для первой попытки, чтобы не пугать пользователя
                logger.debug(f"MySQL пока недоступен (это нормально при старте): {e}")
                raise

    def ping(self) -> bool:
        try:
            if not self._initialized or self._pool is None:
                self.init()
            conn = self._pool.get_connection()
            conn.ping(reconnect=True, attempts=1, delay=1)
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
            # Логируем ошибку только если база реально «отвалилась» в процессе работы
            if self._initialized:
                logger.error(f"Ошибка в транзакции БД: {e}")
            raise
        finally:
            if cur: cur.close()
            if conn: conn.close()

_db_instance = DBConnection()
def get_db() -> DBConnection:
    return _db_instance