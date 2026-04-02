# ============================================================
#  MT5_Level_Bot — core/module_manager.py
#  Version : 1.0.0
#  Desc    : Dynamic module loader — no hardcoded module references.
#            - Loads modules from module_registry table
#            - Manages start / stop / status of each module
#            - GUI calls ONLY ModuleManager — never modules directly
#            - New module = new file + DB entry, zero changes here
# ============================================================

import importlib.util
import os
import threading
from typing import Dict, Optional

from core.base_module import BaseModule
from core.db_connection import get_db
from core.utils import get_logger

logger = get_logger("module_manager")


class ModuleManager:
    """
    Discovers, loads, and manages all pluggable modules.

    Lifecycle:
        mm = ModuleManager()
        mm.load_all()            # reads module_registry from DB
        mm.start("collector")    # starts collector
        mm.status_all()          # returns dict of all statuses
        mm.stop("collector")     # graceful stop
    """

    def __init__(self):
        self._modules: Dict[str, BaseModule] = {}
        self._lock = threading.Lock()
        self._db = get_db()
        # Base path for resolving module_path from DB
        self._base_path = os.path.dirname(os.path.dirname(__file__))

    # ----------------------------------------------------------
    # Load
    # ----------------------------------------------------------

    def load_all(self) -> None:
        """
        Read all enabled modules from module_registry
        and dynamically import them.
        """
        try:
            with self._db.cursor() as cur:
                cur.execute(
                    "SELECT module_name, module_path "
                    "FROM module_registry WHERE is_enabled = 1"
                )
                rows = cur.fetchall()
        except Exception as e:
            logger.error(f"Failed to read module_registry: {e}")
            return

        for row in rows:
            name = row["module_name"]
            path = os.path.join(self._base_path, row["module_path"])
            self._load_module(name, path)

        logger.info(f"ModuleManager loaded: {list(self._modules.keys())}")

    def _load_module(self, name: str, path: str) -> None:
        """Dynamically import a module file and register its main class."""
        if not os.path.exists(path):
            logger.error(f"Module file not found: {path} (name={name})")
            return
        try:
            spec = importlib.util.spec_from_file_location(name, path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)

            # Convention: each module file exposes get_module() → BaseModule
            if not hasattr(mod, "get_module"):
                logger.error(
                    f"Module '{name}' has no get_module() factory — skipped. "
                    f"Add: def get_module(): return MyModule()"
                )
                return

            instance: BaseModule = mod.get_module()

            if not isinstance(instance, BaseModule):
                logger.error(
                    f"Module '{name}' get_module() did not return a BaseModule subclass"
                )
                return

            with self._lock:
                self._modules[name] = instance

            logger.info(f"Module loaded: '{name}' from {path}")

        except Exception as e:
            logger.error(f"Failed to load module '{name}': {e}")

    # ----------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------

    def start(self, module_name: str) -> bool:
        """Start a module by name. Returns True on success."""
        module = self._get(module_name)
        if module is None:
            return False
        if module.is_running():
            logger.warning(f"Module '{module_name}' is already running")
            return False
        try:
            module.start()
            self._db.update_module_status(module_name, "running")
            self._db.log_to_db(module_name, "INFO", f"Module '{module_name}' started")
            logger.info(f"Module started: '{module_name}'")
            return True
        except Exception as e:
            logger.error(f"Failed to start '{module_name}': {e}")
            self._db.update_module_status(module_name, "error")
            return False

    def stop(self, module_name: str) -> bool:
        """Gracefully stop a module by name. Returns True on success."""
        module = self._get(module_name)
        if module is None:
            return False
        if not module.is_running():
            logger.warning(f"Module '{module_name}' is not running")
            return False
        try:
            module.stop()
            self._db.update_module_status(module_name, "stopped")
            self._db.log_to_db(module_name, "INFO", f"Module '{module_name}' stopped")
            logger.info(f"Module stopped: '{module_name}'")
            return True
        except Exception as e:
            logger.error(f"Failed to stop '{module_name}': {e}")
            return False

    def run_once(self, module_name: str) -> bool:
        """
        Execute one cycle of the module (GUI 'One-time run' button).
        Safe to call while module scheduler is also running.
        """
        module = self._get(module_name)
        if module is None:
            return False
        try:
            logger.info(f"run_once: '{module_name}'")
            return module.run_once()
        except Exception as e:
            logger.error(f"run_once failed for '{module_name}': {e}")
            return False

    def start_all(self) -> None:
        """Start all loaded modules."""
        for name in list(self._modules.keys()):
            self.start(name)

    def stop_all(self) -> None:
        """Stop all running modules (called on app shutdown)."""
        for name in list(self._modules.keys()):
            if self._modules[name].is_running():
                self.stop(name)

    # ----------------------------------------------------------
    # Status / Info
    # ----------------------------------------------------------

    def status(self, module_name: str) -> Optional[dict]:
        """Return status dict for one module."""
        module = self._get(module_name)
        return module.status() if module else None

    def status_all(self) -> Dict[str, dict]:
        """Return status dict for all loaded modules."""
        return {name: mod.status() for name, mod in self._modules.items()}

    def list_modules(self) -> list:
        """Return list of loaded module names."""
        return list(self._modules.keys())

    def get_config(self, module_name: str) -> Optional[dict]:
        """Return config dict for one module (for GUI settings panel)."""
        module = self._get(module_name)
        return module.get_config() if module else None

    def get_module(self, module_name: str) -> Optional[BaseModule]:
        """Return module instance directly (for Trader etc.)."""
        return self._modules.get(module_name)

    # ----------------------------------------------------------
    # Private
    # ----------------------------------------------------------

    def _get(self, name: str) -> Optional[BaseModule]:
        with self._lock:
            module = self._modules.get(name)
        if module is None:
            logger.error(f"Module not found: '{name}'. Loaded: {list(self._modules.keys())}")
        return module


# ------------------------------------------------------------------
# Module-level singleton accessor
# ------------------------------------------------------------------
_manager_instance = ModuleManager()


def get_module_manager() -> ModuleManager:
    """
    Return the global ModuleManager singleton.
    GUI and main.py call this — never instantiate ModuleManager directly.
    """
    return _manager_instance
