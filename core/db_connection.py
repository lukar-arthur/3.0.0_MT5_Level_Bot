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

# ПРАВИЛЬНЫЙ ИМПОРТ: используем функцию загрузки конфига БД
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
            
            # Загружаем настройки через наш config_loader
            db_cfg = load_db_config()
            
            # Если пароль не указан в .ini, пробуем взять из переменной окружения
            if not db_cfg.get("password"):
                db_cfg["password"] = os.environ.get("DB_PASSWORD", "")
                
            try:
                # Добавляем необходимые параметры для mysql-connector
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
                    pool_name="mt5_pool",
                    pool_size=pool_size,
                    pool_reset_session=True,
                    **db_params
                )
                self._initialized = True
                logger.info(f"MySQL пул запущен: {db_cfg['user']}@{db_cfg['host']}/{db_cfg['database']}")
            except MySQLError as e:
                logger.error(f"Критическая ошибка БД: {e}")
                raise

    def ping(self) -> bool:
        """Проверка живое ли соединение."""
        try:
            if not self._initialized or self._pool is None:
                self.init()
            conn = self._pool.get_connection()
            conn.ping(reconnect=True, attempts=2, delay=1)
            conn.close()
            return True
        except Exception:
            self._initialized = False
            return False

    @contextmanager
    def cursor(self, dictionary: bool = True, commit: bool = False):
        """Контекстный менеджер для работы с курсором."""
        conn = None
        cur = None
        try:
            if not self._initialized:
                self.init()
            conn = self._pool.get_connection()
            conn.autocommit = not commit
            cur = conn.cursor(dictionary=dictionary)
            yield cur
            if commit:
                conn.commit()
        except MySQLError as e:
            if conn and commit:
                conn.rollback()
            logger.error(f"Ошибка в транзакции БД: {e}")
            raise
        finally:
            if cur: cur.close()
            if conn: conn.close()

# Глобальный объект для доступа
_db_instance = DBConnection()

def get_db() -> DBConnection:
    return _db_instance