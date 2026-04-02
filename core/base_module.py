# ============================================================
#  MT5_Level_Bot — core/base_module.py
#  Version : 1.0.0
#  Desc    : Abstract base class for all pluggable modules.
#            Every module (collector, analyzer, future bots)
#            MUST inherit BaseModule and implement all methods.
#            ModuleManager works exclusively through this interface.
# ============================================================

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional


class BaseModule(ABC):
    """
    Standard interface contract for every MT5_Level_Bot module.

    Rules:
    - No module may call another module directly.
    - All inter-module communication goes through MySQL.
    - GUI / ModuleManager interact only via this interface.
    """

    def __init__(self, module_name: str):
        self._module_name: str = module_name
        self._running: bool = False
        self._error_count: int = 0
        self._last_run: Optional[datetime] = None
        self._last_error: Optional[str] = None

    # ----------------------------------------------------------
    # Required interface — every module must implement these
    # ----------------------------------------------------------

    @abstractmethod
    def start(self) -> None:
        """Start the module (non-blocking — runs in its own thread)."""

    @abstractmethod
    def stop(self) -> None:
        """Gracefully stop the module."""

    @abstractmethod
    def run_once(self) -> bool:
        """
        Execute one full cycle of the module logic.
        Returns True on success, False on failure.
        Called both by the scheduler and by GUI 'One-time run' button.
        """

    @abstractmethod
    def get_config(self) -> dict:
        """
        Return current module configuration as a dict.
        Used by GUI Settings panel to display and edit config.
        """

    # ----------------------------------------------------------
    # Concrete methods — shared by all modules (do not override)
    # ----------------------------------------------------------

    def get_name(self) -> str:
        """Return the unique module identifier (lowercase, snake_case)."""
        return self._module_name

    def is_running(self) -> bool:
        return self._running

    def status(self) -> dict:
        """
        Return module health snapshot.
        GUI reads this to display status indicators.
        """
        return {
            "module":      self._module_name,
            "running":     self._running,
            "last_run":    self._last_run.isoformat() if self._last_run else None,
            "error_count": self._error_count,
            "last_error":  self._last_error,
        }

    # ----------------------------------------------------------
    # Protected helpers — for use inside subclasses only
    # ----------------------------------------------------------

    def _mark_run_start(self) -> None:
        self._last_run = datetime.now(timezone.utc)

    def _mark_success(self) -> None:
        self._error_count = 0
        self._last_error = None

    def _mark_error(self, message: str) -> None:
        self._error_count += 1
        self._last_error = message

    def _set_running(self, state: bool) -> None:
        self._running = state
