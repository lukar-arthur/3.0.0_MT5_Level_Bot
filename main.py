# ============================================================XX
#  MT5_Level_Bot — main.py  v3.0.0  (ИСПРАВЛЕНО П-6)
#
#  Принцип: GUI запускается МГНОВЕННО.
#  Все проверки соединений — через GUI, не в main.py.
#  main.py только инициализирует минимум и открывает окно.
# ============================================================

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from core.utils          import get_logger
from core.module_manager import get_module_manager

logger = get_logger("main")

# ИСПРАВЛЕНИЕ П-6: версия синхронизирована
_VERSION = "3.0.0"


def main():
    logger.info("=" * 55)
    logger.info(f"  MT5_Level_Bot v{_VERSION} запускается...")
    logger.info("=" * 55)

    mm = get_module_manager()

    from gui.app import run_app
    logger.info("Запуск GUI...")
    try:
        run_app(mm)
    except Exception as e:
        logger.critical(f"GUI упал: {e}", exc_info=True)
    finally:
        _shutdown(mm)


def _shutdown(mm):
    logger.info("Завершение работы...")
    try:
        mm.stop_all()
    except Exception:
        pass
    try:
        from core.mt5_bridge import get_mt5_bridge
        get_mt5_bridge().monitor.stop()
    except Exception:
        pass
    logger.info(f"MT5_Level_Bot v{_VERSION} завершил работу")


if __name__ == "__main__":
    main()
