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
import time
from core.config_loader import ConfigLoader

# Настройка логгера
logger = logging.getLogger(__name__)

class DatabaseConnection:
    """
    Потокобезопасный Singleton для подключения к MySQL.
    Использует threading.local() для создания отдельного соединения 
    внутри каждого потока (Thread-Local Storage).
    """
    
    _instance = None
    _lock = threading.Lock()
    _thread_local = threading.local()

    def __new__(cls, *args, **kwargs):
        # Двойная проверка для потокобезопасного Singleton
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(DatabaseConnection, cls).__new__(cls)
                    logger.info("DatabaseConnection Singleton instance created.")
        return cls._instance

    def __init__(self, host=None, user=None, password=None, db=None, port=3306):
        # Инициализация конфига происходит только один раз
        if hasattr(self, 'initialized') and self.initialized:
            return

        # Загрузка конфигурации
        try:
            config = ConfigLoader().get_config()
        except Exception as e:
            logger.critical(f"ConfigLoader error: {e}")
            config = {}

        self.config = {
            'host': host or config.get('db_host', 'localhost'),
            'user': user or config.get('db_user', 'root'),
            'password': password or config.get('db_password', ''),
            'database': db or config.get('db_name', 'mt5_level_engine'),
            'port': int(port or config.get('db_port', 3306)),
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor,
            # КРИТИЧЕСКИ ВАЖНЫЕ ПАРАМЕТРЫ
            'connect_timeout': 10,  # Таймаут на подключение (сек)
            'read_timeout': 30,     # Таймаут на чтение (сек)
            'write_timeout': 30,    # Таймаут на запись (сек)
            'autocommit': False     # Явное управление транзакциями
        }
        self.initialized = True
        logger.debug(f"Database config initialized for {self.config['host']}")

    def _get_connection(self):
        """
        Возвращает соединение для ТЕКУЩЕГО потока. 
        Если соединения нет или оно разорвано - создает новое.
        """
        # Проверяем, есть ли соединение в текущем потоке
        conn = getattr(self._thread_local, 'connection', None)
        
        if conn:
            # Проверяем живое ли соединение
            try:
                conn.ping(reconnect=True)
                return conn
            except Exception:
                logger.warning(f"Connection lost in thread {threading.current_thread().name}. Reconnecting...")
                self._thread_local.connection = None

        # Создаем новое соединение для текущего потока
        try:
            self._thread_local.connection = pymysql.connect(**self.config)
            logger.debug(f"New DB connection established for thread: {threading.current_thread().name}")
            return self._thread_local.connection
        except Exception as e:
            logger.critical(f"Failed to connect to database: {e}")
            raise ConnectionError(f"Database connection failed: {e}")

    def execute(self, query, params=None, fetch=False):
        """
        Универсальный метод выполнения запроса.
        Автоматически делает commit при успехе и rollback при ошибке.
        """
        conn = self._get_connection()
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            
            if fetch:
                result = cursor.fetchall()
            else:
                result = cursor.lastrowid # Возвращает ID вставленной строки
            
            conn.commit() # Фиксируем изменения
            return result
            
        except Exception as e:
            if conn:
                conn.rollback() # Откат при ошибке
            logger.error(f"SQL Error: {e} | Query: {query}")
            raise e
        finally:
            if cursor:
                cursor.close()

    def disconnect(self):
        """Закрывает соединение в текущем потоке"""
        conn = getattr(self._thread_local, 'connection', None)
        if conn:
            try:
                conn.close()
                logger.info(f"DB connection closed for thread: {threading.current_thread().name}")
            except:
                pass
            self._thread_local.connection = None

# -----------------------------------------------------------------------------
# ФУНКЦИЯ-ОБОЛОЧКА (Необходима для совместимости с module_manager.py)
# -----------------------------------------------------------------------------
def get_db():
    """
    Возвращает экземпляр подключения (Singleton).
    Используется устаревшим кодом: from core.db_connection import get_db
    """
    return DatabaseConnection()