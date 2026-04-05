# ============================================================ХХ
#  MT5_Level_Bot — main.py  v3.0.0 
#
#  Принцип: GUI запускается МГНОВЕННО.
#  Все проверки соединений — через GUI, не в main.py.
#  main.py только инициализирует минимум и открывает окно.
# ============================================================

import sys
import logging
import time
from core.module_manager import ModuleManager
from core.db_connection import DatabaseConnection
from core.config_loader import ConfigLoader

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot_log.log")
    ]
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting MT5 Level Bot...")

    # Инициализация менеджера модулей
    manager = ModuleManager()
    
    try:
        # 1. Загрузка конфигурации
        config = ConfigLoader().get_config()
        if not config:
            logger.critical("Config not found! Exiting.")
            return

        # 2. Проверка подключения к БД (Singleton инициируется здесь)
        try:
            db = DatabaseConnection()
            # Простой тестовый запрос для проверки связи
            # (предполагается, что execute есть, или используем _get_connection)
            conn = db._get_connection()
            logger.info(f"Database connection successful: {conn.get_host_info()}")
        except Exception as e:
            logger.critical(f"Failed to connect to Database: {e}")
            return

        # 3. Запуск всех модулей через менеджер
        manager.load_all()
        manager.start_all()

        logger.info("Bot started successfully. Press Ctrl+C to stop.")

        # Главный цикл (чтобы процесс не завершался)
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Shutdown signal received (Ctrl+C).")
    
    except Exception as e:
        logger.critical(f"Critical error in main loop: {e}", exc_info=True)
    
    finally:
        # ГАРАНТИРОВАННОЕ ЗАВЕРШЕНИЕ
        logger.info("Stopping modules...")
        manager.stop_all()
        
        # Закрытие соединения с БД
        logger.info("Closing database connection...")
        try:
            DatabaseConnection().disconnect()
        except:
            pass
        
        logger.info("Bot stopped.")

if __name__ == "__main__":
    main()