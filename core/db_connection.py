# ============================================================ХХ
#  MT5_Level_Bot — core/db_connection.py  v3.0.0
#
# 
#  Plug-and-Play: использует core.config_loader вместо
#    жёстко прописанного _CONFIG_PATH
# ============================================================
import pymysql
import threading
import logging
import json
import os

# Настройка логгера
logger = logging.getLogger(__name__)

class DatabaseConnection:
    """
    Потокобезопасный Singleton для подключения к MySQL.
    Использует threading.local() для создания отдельного соединения 
    внутри каждого потока.
    """
    
    _instance = None
    _lock = threading.Lock()
    _thread_local = threading.local()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(DatabaseConnection, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        # Инициализация происходит только один раз
        if hasattr(self, 'initialized') and self.initialized:
            return

        # --- Загрузка конфигурации напрямую (исправление ошибки Import) ---
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
        config_data = {}
        
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
            else:
                logger.warning("config.json not found, using defaults.")
        except Exception as e:
            logger.error(f"Error loading config.json: {e}")

        self.config = {
            'host': config_data.get('db_host', 'localhost'),
            'user': config_data.get('db_user', 'root'),
            'password': config_data.get('db_password', ''),
            'database': config_data.get('db_name', 'mt5_level_engine'),
            'port': int(config_data.get('db_port', 3306)),
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor,
            'connect_timeout': 10,
            'read_timeout': 30,
            'write_timeout': 30,
            'autocommit': False
        }
        self.initialized = True
        logger.info("DatabaseConnection initialized.")

    def _get_connection(self):
        """Возвращает соединение для текущего потока."""
        conn = getattr(self._thread_local, 'connection', None)
        
        if conn:
            try:
                conn.ping(reconnect=True)
                return conn
            except Exception:
                logger.warning("Connection lost, reconnecting...")
                self._thread_local.connection = None

        try:
            self._thread_local.connection = pymysql.connect(**self.config)
            return self._thread_local.connection
        except Exception as e:
            logger.critical(f"DB Connection Failed: {e}")
            raise

    def execute(self, query, params=None, fetch=False):
        """Выполнение запроса с автоматическим commit/rollback."""
        conn = self._get_connection()
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            if fetch:
                result = cursor.fetchall()
            else:
                result = cursor.lastrowid
            conn.commit()
            return result
        except Exception as e:
            conn.rollback()
            logger.error(f"SQL Error: {e}")
            raise e
        finally:
            if cursor:
                cursor.close()

    def disconnect(self):
        """Закрытие соединения."""
        conn = getattr(self._thread_local, 'connection', None)
        if conn:
            try: conn.close()
            except: pass
            self._thread_local.connection = None

# Функция для совместимости с module_manager.py
def get_db():
    return DatabaseConnection()