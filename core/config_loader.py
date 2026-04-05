# ============================================================
#  MT5_Level_Bot — core/config_loader.py
#  Version : 3.0.0
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

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CONFIG_DIR   = os.path.join(_PROJECT_ROOT, "config")
_GLOBAL_CFG   = os.path.join(_CONFIG_DIR, "config.ini")

class ModuleConfig:
    def __init__(self, module_name: Optional[str] = None):
        self._module_name = module_name
        self._module_cfg  = configparser.ConfigParser()
        self._global_cfg  = configparser.ConfigParser()
        
        if os.path.exists(_GLOBAL_CFG):
            self._global_cfg.read(_GLOBAL_CFG, encoding="utf-8")
            
        if module_name:
            path = os.path.join(_CONFIG_DIR, f"{module_name}.ini")
            if os.path.exists(path):
                self._module_cfg.read(path, encoding="utf-8")

    def _resolve(self, section: str, key: str) -> tuple[bool, str]:
        if self._module_cfg.has_option(section, key):
            return True, self._module_cfg.get(section, key)
        if self._global_cfg.has_option(section, key):
            return True, self._global_cfg.get(section, key)
        return False, ""

    def get(self, section: str, key: str, fallback: str = "") -> str:
        found, val = self._resolve(section, key)
        return val if found else fallback

    def getint(self, section: str, key: str, fallback: int = 0) -> int:
        found, val = self._resolve(section, key)
        try: return int(val) if found else fallback
        except: return fallback

    def getfloat(self, section: str, key: str, fallback: float = 0.0) -> float:
        found, val = self._resolve(section, key)
        try: return float(val) if found else fallback
        except: return fallback

    def getboolean(self, section: str, key: str, fallback: bool = False) -> bool:
        found, val = self._resolve(section, key)
        if not found: return fallback
        return val.strip().lower() in ("true", "1", "yes", "on")

    def reload(self) -> None:
        self.__init__(self._module_name)

def load_module_config(module_name: str) -> ModuleConfig:
    return ModuleConfig(module_name)

def load_db_config() -> dict:
    cfg = ModuleConfig()
    return {
        "host": cfg.get("DATABASE", "host", fallback="localhost"),
        "port": cfg.getint("DATABASE", "port", fallback=3306),
        "database": cfg.get("DATABASE", "db_name", fallback="mt5_level_engine"),
        "user": cfg.get("DATABASE", "user", fallback="root"),
        "password": cfg.get("DATABASE", "password", fallback=""),
    }

def load_mt5_config() -> dict:
    cfg = ModuleConfig()
    return {
        "terminal_path": cfg.get("MT5", "terminal_path", fallback=""),
        "login": cfg.getint("MT5", "login", fallback=0),
        "password": cfg.get("MT5", "password", fallback=""),
        "server": cfg.get("MT5", "server", fallback=""),
        "timeout": cfg.getint("MT5", "timeout", fallback=10000),
    }