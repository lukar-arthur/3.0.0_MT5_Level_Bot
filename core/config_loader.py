# ============================================================
#  MT5_Level_Bot — core/config_loader.py
#  Version : 1.0.0
#
#  Единый загрузчик конфигурации — Plug-and-Play архитектура.
#
#  Принцип:
#    Каждый модуль имеет СВОЙ конфигурационный файл:
#      config/collector.ini, config/analyzer.ini, etc.
#    Если секция/ключ не найдены в модульном файле →
#    автоматически ищем в глобальном config/config.ini.
#
#    Это позволяет:
#    - Добавить новый модуль без изменения глобального конфига
#    - Настраивать каждый модуль независимо
#    - Переносить модуль в другой проект — берёт только свой .ini
# ============================================================

import configparser
import os
from typing import Any, Optional

# Корень проекта — одна директория вверх от core/
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CONFIG_DIR   = os.path.join(_PROJECT_ROOT, "config")
_GLOBAL_CFG   = os.path.join(_CONFIG_DIR, "config.ini")


def get_config_path(module_name: Optional[str] = None) -> str:
    """
    Возвращает путь к конфиг-файлу.
    module_name=None → глобальный config.ini
    module_name="collector" → config/collector.ini
    """
    if module_name is None:
        return _GLOBAL_CFG
    return os.path.join(_CONFIG_DIR, f"{module_name}.ini")


class ModuleConfig:
    """
    Умный конфигуратор: сначала ищет в модульном .ini,
    при отсутствии — в глобальном config.ini.

    Использование:
        cfg = ModuleConfig("collector")
        symbols = cfg.get("COLLECTOR", "symbols", fallback="EURUSD")
        interval = cfg.getint("COLLECTOR", "interval_sec", fallback=1800)
    """

    def __init__(self, module_name: Optional[str] = None):
        self._module_name = module_name
        self._module_cfg  = configparser.ConfigParser()
        self._global_cfg  = configparser.ConfigParser()

        # Загружаем глобальный конфиг
        self._global_cfg.read(_GLOBAL_CFG, encoding="utf-8")

        # Загружаем модульный конфиг (если есть)
        if module_name:
            module_path = get_config_path(module_name)
            if os.path.exists(module_path):
                self._module_cfg.read(module_path, encoding="utf-8")

    def _resolve(self, section: str, key: str) -> tuple[bool, str]:
        """Возвращает (найдено, значение) — сначала модульный, потом глобальный."""
        # 1. Модульный конфиг
        if self._module_cfg.has_option(section, key):
            return True, self._module_cfg.get(section, key)
        # 2. Глобальный конфиг
        if self._global_cfg.has_option(section, key):
            return True, self._global_cfg.get(section, key)
        return False, ""

    def get(self, section: str, key: str, fallback: str = "") -> str:
        found, value = self._resolve(section, key)
        return value if found else fallback

    def getint(self, section: str, key: str, fallback: int = 0) -> int:
        found, value = self._resolve(section, key)
        return int(value) if found else fallback

    def getfloat(self, section: str, key: str, fallback: float = 0.0) -> float:
        found, value = self._resolve(section, key)
        return float(value) if found else fallback

    def getboolean(self, section: str, key: str, fallback: bool = False) -> bool:
        found, value = self._resolve(section, key)
        if not found:
            return fallback
        return value.strip().lower() in ("true", "1", "yes", "on")

    def sections(self) -> list:
        """Все секции из обоих конфигов (объединение)."""
        s = set(self._module_cfg.sections())
        s.update(self._global_cfg.sections())
        return list(s)

    def reload(self) -> None:
        """Перечитать конфиги с диска (для горячей перезагрузки)."""
        self.__init__(self._module_name)


def load_module_config(module_name: str) -> ModuleConfig:
    """
    Фабричная функция — создаёт ModuleConfig для указанного модуля.
    Рекомендованный способ использования во всех модулях:

        from core.config_loader import load_module_config
        _cfg = load_module_config("collector")
        symbols = _cfg.get("COLLECTOR", "symbols", fallback="EURUSD")
    """
    return ModuleConfig(module_name)


def load_db_config() -> dict:
    """Загружает конфигурацию БД из глобального config.ini."""
    cfg = ModuleConfig()
    return {
        "host":               cfg.get("DATABASE", "host",     fallback="localhost"),
        "port":               cfg.getint("DATABASE", "port",  fallback=3306),
        "database":           cfg.get("DATABASE", "db_name",  fallback="mt5_level_engine"),
        "user":               cfg.get("DATABASE", "user",     fallback="root"),
        "password":           cfg.get("DATABASE", "password", fallback=""),
        "charset":            "utf8mb4",
        "use_unicode":        True,
        "time_zone":          "+00:00",
        "connection_timeout": 10,
        "autocommit":         False,
    }


def load_mt5_config() -> dict:
    """Загружает конфигурацию MT5 из глобального config.ini."""
    cfg = ModuleConfig()
    return {
        "terminal_path": cfg.get("MT5", "terminal_path",
                                  fallback=r"C:\MetaTrader5\terminal64.exe"),
        "login":    cfg.getint("MT5", "login",    fallback=0),
        "password": cfg.get("MT5", "password",    fallback=""),
        "server":   cfg.get("MT5", "server",      fallback=""),
        "timeout":  cfg.getint("MT5", "timeout",  fallback=10000),
    }
