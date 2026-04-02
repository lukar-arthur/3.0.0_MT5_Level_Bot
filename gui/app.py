# ============================================================
#  MT5_Level_Bot — gui/app.py  v5.0.0
#
#  ПРАВИЛО: Никаких операций с Tkinter вне главного потока.
#  Все callbacks от MT5/MySQL передаются через after(0).
#
#  ВКЛАДКИ: Настройки соединения → Главная → Уровни
#  ВАЖНО: MT5 терминал НЕ запускается без команды пользователя.
# ============================================================

import logging
from datetime import datetime, timezone

import customtkinter as ctk

from gui.theme import (
    WINDOW_WIDTH, WINDOW_HEIGHT, WINDOW_MIN_W, WINDOW_MIN_H,
    FONT_LABEL_SM, COLOR_TEXT_OK, COLOR_TEXT_ERR, COLOR_WARN
)
from gui.main_panel       import MainPanel
from gui.connection_panel import ConnectionPanel
from gui.levels_panel     import LevelsPanel


class GUILogHandler(logging.Handler):
    """Перехватывает все логи и отправляет в консоль MainPanel."""

    def __init__(self, main_panel: MainPanel):
        super().__init__()
        self._main = main_panel

    def emit(self, record):
        try:
            ts = datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S")
            self._main.append_log(
                ts, record.levelname,
                record.name, record.getMessage())
        except Exception:
            pass


class MT5LevelBotApp(ctk.CTk):

    def __init__(self, module_manager):
        super().__init__()
        self._mm     = module_manager
        self._db_ok  = False
        self._mt5_ok = False

        self._setup_window()
        self._build_ui()
        self._attach_log_handler()
        self._on_startup()

    # ----------------------------------------------------------
    # Окно
    # ----------------------------------------------------------
    def _setup_window(self):
        self.title("MT5 Level Bot  v2.0.0  |  Инициализация...")
        self.geometry(f"{WINDOW_WIDTH}x{WINDOW_HEIGHT}")
        self.minsize(WINDOW_MIN_W, WINDOW_MIN_H)
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.update_idletasks()
        x = (self.winfo_screenwidth()  - WINDOW_WIDTH)  // 2
        y = (self.winfo_screenheight() - WINDOW_HEIGHT) // 2
        self.geometry(f"+{x}+{y}")

    # ----------------------------------------------------------
    # UI — только 3 вкладки
    # ----------------------------------------------------------
    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self._tabs = ctk.CTkTabview(self, anchor="nw")
        self._tabs.grid(row=0, column=0, sticky="nsew",
                        padx=8, pady=(4, 4))

        # Порядок: Настройки первой (стартовая вкладка)
        for name in ["Настройки соединения", "Главная", "Уровни"]:
            self._tabs.add(name)

        # ── Настройки соединения ──────────────────────────────
        self._connection_panel = ConnectionPanel(
            self._tabs.tab("Настройки соединения"),
            on_db_ready=self._on_db_ready,
            on_mt5_ready=self._on_mt5_ready,
        )
        self._connection_panel.pack(fill="both", expand=True)

        # ── Главная ───────────────────────────────────────────
        self._main_panel = MainPanel(
            self._tabs.tab("Главная"),
            module_manager=self._mm,
        )
        self._main_panel.pack(fill="both", expand=True)

        # ── Уровни ────────────────────────────────────────────
        self._levels_panel = LevelsPanel(
            self._tabs.tab("Уровни"), db_ok=False)
        self._levels_panel.pack(fill="both", expand=True)

        # ── Footer ────────────────────────────────────────────
        self._footer = ctk.CTkLabel(
            self,
            text="⚠  Выполни шаги в 'Настройки соединения'",
            font=FONT_LABEL_SM, anchor="w",
            text_color=COLOR_WARN
        )
        self._footer.grid(row=1, column=0,
                          sticky="ew", padx=12, pady=(0, 4))

        # Стартовая вкладка — Настройки
        self._tabs.set("Настройки соединения")

    # ----------------------------------------------------------
    # Callbacks (вызываются из ConnectionPanel через after(0))
    # ----------------------------------------------------------
    def _on_db_ready(self):
        """MySQL доступен — загружаем модули."""
        self._db_ok = True
        # Загружаем модули только после готовности БД
        if not self._mm.list_modules():
            self._mm.load_all()
        # Разблокируем панель уровней
        try:
            self._levels_panel._db_ok = True
            self._levels_panel._refresh()
        except Exception:
            pass
        self._update_status()

    def _on_mt5_ready(self):
        """
        MT5 подключён (пользователь нажал кнопку проверки).
        Запускаем ConnectionMonitor — только здесь, по команде пользователя.
        Monitor делает bridge.monitor.is_connected = True,
        что нужно main_panel для отображения данных счёта.
        """
        self._mt5_ok = True
        # Стартуем монитор — единственное место запуска!
        try:
            from core.mt5_bridge import get_mt5_bridge
            bridge = get_mt5_bridge()
            if not bridge.monitor._running:
                bridge.monitor.start()
        except Exception:
            pass
        self._update_status()
        if self._db_ok and self._mt5_ok:
            self._tabs.set("Главная")

    def _on_mt5_disconnected(self):
        """MT5 отключился."""
        self._mt5_ok = False
        self._update_status()

    # ----------------------------------------------------------
    # Статус (заголовок + footer)
    # ----------------------------------------------------------
    def _update_status(self):
        db  = "MySQL: ✓" if self._db_ok  else "MySQL: ✗"
        mt5 = "MT5: ✓"  if self._mt5_ok else "MT5: ✗"
        self.title(f"MT5 Level Bot v2.0.0  |  {db}  |  {mt5}")

        if self._db_ok and self._mt5_ok:
            self._footer.configure(
                text="✓ Готов к работе",
                text_color=COLOR_TEXT_OK)
        elif self._db_ok:
            self._footer.configure(
                text="MySQL ✓  |  MT5 ✗ — запусти терминал",
                text_color=COLOR_WARN)
        elif self._mt5_ok:
            self._footer.configure(
                text="MT5 ✓  |  MySQL ✗ — запусти XAMPP",
                text_color=COLOR_WARN)
        else:
            self._footer.configure(
                text="⚠  Выполни шаги в 'Настройки соединения'",
                text_color=COLOR_WARN)

    # ----------------------------------------------------------
    # Лог-хендлер
    # ----------------------------------------------------------
    def _attach_log_handler(self):
        handler = GUILogHandler(self._main_panel)
        handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(handler)

    # ----------------------------------------------------------
    # Startup / Close
    # ----------------------------------------------------------
    def _on_startup(self):
        from core.utils import get_logger
        lg = get_logger("gui")
        lg.info("GUI запущен. Выполни шаги в 'Настройки соединения'")

    def _on_close(self):
        try:
            self._mm.stop_all()
        except Exception:
            pass
        self.destroy()


def run_app(module_manager) -> None:
    app = MT5LevelBotApp(module_manager)
    app.mainloop()
