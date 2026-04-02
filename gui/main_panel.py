# ============================================================
#  MT5_Level_Bot — gui/main_panel.py  v4.0.0
#
#  ИСПРАВЛЕНО v4:
#  - "signal" → "scalping" (кнопка теперь запускает правильный модуль)
#  - MT5 аккаунт: подписка на ConnectionMonitor вместо session() каждые 3с
#  - ConnectionMonitorWidget встроен в строку времени
#  - _tick() / _poll() / _run_bg_refresh() — корректный stop при destroy
#  - _bg_collect_data: убрана тяжёлая bridge.session() из 3-сек цикла
#
#  ПРАВИЛО ПОТОКОБЕЗОПАСНОСТИ:
#  Главный поток — ТОЛЬКО виджеты.
#  Фоновый поток — БД, MT5. Передача через queue + after(200).
# ============================================================

import queue
import threading
import tkinter as tk
from datetime import datetime, timezone, timedelta
from typing import Optional

import customtkinter as ctk

from gui.theme import (
    FONT_TITLE, FONT_LABEL, FONT_LABEL_SM, FONT_BUTTON,
    FONT_CONSOLE, FONT_STATUS,
    COLOR_OK, COLOR_WARN, COLOR_ERROR, COLOR_STOP,
    LOG_COLORS, PAD_X, PAD_Y, PAD_SM
)

YEREVAN_OFFSET = timedelta(hours=4)
SESSIONS = [
    ("Азиатская",    0,  9),
    ("Европейская",  7, 16),
    ("Американская", 13, 22),
]

# ВАЖНО: ключи должны совпадать с именами в module_registry БД
_MODULE_LABELS = {
    "collector": "Сбор данных",
    "analyzer":  "Анализ",
    "scalping":  "Сигналы",   # ← было "signal" — исправлено
}


class MainPanel(ctk.CTkFrame):

    def __init__(self, parent, module_manager,
                 monitor=None, **kwargs):
        super().__init__(parent,
                         fg_color="transparent", **kwargs)
        self._mm      = module_manager
        self._running = True

        # Единая очередь для всех данных из фоновых потоков
        self._q: queue.Queue = queue.Queue()

        # Кеш сигнала
        self._current_signal_id = None

        # MT5 монитор — подписываемся для мгновенных обновлений счёта
        self._mt5_monitor = None

        self._build_ui()
        self._start_clock()
        self._start_poller()
        self._schedule_bg_refresh()
        self._subscribe_mt5_monitor()

    # ----------------------------------------------------------
    # MT5 Monitor подписка
    # ----------------------------------------------------------
    def _subscribe_mt5_monitor(self):
        """
        Подписывается на ConnectionMonitor.
        Вызывается один раз при инициализации.
        Если монитор ещё не запущен — повторяет попытку через 2 сек.
        """
        try:
            from core.mt5_bridge import get_mt5_bridge, ConnectionState
            bridge = get_mt5_bridge()
            self._mt5_monitor = bridge.monitor

            def _on_mt5_state(state, message):
                """Callback из фонового потока монитора."""
                if state == ConnectionState.CONNECTED:
                    # Запрашиваем данные аккаунта в отдельном потоке
                    threading.Thread(
                        target=self._bg_fetch_acct,
                        daemon=True).start()
                else:
                    # Отключились — сразу показываем в очереди
                    self._q.put(("acct_info", {"connected": False}))

            bridge.monitor.subscribe(_on_mt5_state)

            # Проверяем текущее состояние сразу
            if bridge.monitor.is_connected:
                threading.Thread(
                    target=self._bg_fetch_acct,
                    daemon=True).start()
        except Exception:
            # Монитор ещё не стартовал — попробуем снова через 2 сек
            if self._running:
                self.after(2000, self._subscribe_mt5_monitor)

    # ----------------------------------------------------------
    # Построение UI
    # ----------------------------------------------------------
    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(4, weight=1)

        # ── Строка времени ────────────────────────────────────
        time_row = ctk.CTkFrame(self, fg_color="transparent")
        time_row.grid(row=0, column=0, sticky="ew",
                      padx=PAD_X, pady=(PAD_SM, 0))
        time_row.grid_columnconfigure(0, weight=1)

        self._session_dot = ctk.CTkLabel(
            time_row, text="●", font=FONT_LABEL,
            text_color=COLOR_STOP)
        self._session_dot.pack(side="left", padx=(0, 4))

        self._session_lbl = ctk.CTkLabel(
            time_row,
            text="Сессия: —  │  Ереван: --:--  │  UTC: --:--",
            font=FONT_LABEL, anchor="w")
        self._session_lbl.pack(side="left")

        # Встроенный монитор MT5 — справа в строке времени
        self._mt5_dot = ctk.CTkLabel(
            time_row, text="●",
            font=(FONT_LABEL_SM[0], 12),
            text_color=COLOR_STOP)
        self._mt5_dot.pack(side="right", padx=(0, 4))

        self._mt5_lbl = ctk.CTkLabel(
            time_row, text="MT5: —",
            font=FONT_LABEL_SM,
            text_color=("gray50", "gray55"))
        self._mt5_lbl.pack(side="right", padx=(0, PAD_SM))

        # ── Кнопки модулей ────────────────────────────────────
        btn_outer = ctk.CTkFrame(self)
        btn_outer.grid(row=1, column=0, sticky="ew",
                       padx=PAD_X, pady=(PAD_Y, PAD_SM))
        btn_outer.grid_columnconfigure((0, 1, 2), weight=1)
        btn_outer.grid_columnconfigure(3, weight=0, minsize=180)

        ctk.CTkLabel(
            btn_outer,
            text="Запуск по шагам — нажми нужный этап:",
            font=FONT_LABEL_SM,
            text_color=("gray50", "gray55")
        ).grid(row=0, column=0, columnspan=4,
               sticky="w", padx=PAD_X, pady=(PAD_SM, 2))

        self._module_btns = {}
        self._module_dots = {}
        self._module_info = {}

        specs = [
            ("collector", "1. Сбор данных",
             "Собирает уровни H1/H4/D из MT5 (30 мин)"),
            ("analyzer",  "2. Анализ",
             "Оценивает силу уровней S=0..10 (30 мин)"),
            ("scalping",  "3. Сигналы",    # ← исправлено
             "Ищет точки входа T≥0.70 (5 мин)"),
        ]

        for col, (name, label, hint) in enumerate(specs):
            card = ctk.CTkFrame(btn_outer)
            card.grid(row=1, column=col, sticky="ew",
                      padx=PAD_SM, pady=PAD_SM)
            card.grid_columnconfigure(0, weight=1)

            btn = ctk.CTkButton(
                card, text=f"▶  {label}",
                font=FONT_BUTTON, height=44,
                fg_color=COLOR_STOP,
                hover_color="#27AE60",
                command=lambda n=name: self._toggle_module(n))
            btn.grid(row=0, column=0, sticky="ew",
                     padx=PAD_SM, pady=(PAD_SM, 2))

            sr = ctk.CTkFrame(card, fg_color="transparent")
            sr.grid(row=1, column=0, sticky="ew", padx=PAD_SM)

            dot = ctk.CTkLabel(sr, text="●",
                               font=("Segoe UI", 12),
                               text_color=COLOR_STOP)
            dot.pack(side="left", padx=(0, 4))

            info = ctk.CTkLabel(sr, text="Остановлен",
                                font=FONT_LABEL_SM,
                                text_color=COLOR_STOP,
                                anchor="w")
            info.pack(side="left")

            ctk.CTkLabel(
                card, text=hint,
                font=(FONT_LABEL_SM[0], FONT_LABEL_SM[1]-1),
                text_color=("gray50", "gray55"),
                wraplength=200, justify="left"
            ).grid(row=2, column=0, sticky="w",
                   padx=PAD_SM, pady=(2, PAD_SM))

            self._module_btns[name] = btn
            self._module_dots[name] = dot
            self._module_info[name] = info

        # ── Кнопка аналитики + Карточка аккаунта MT5 ────────
        # FIX-1: кнопка открытия панели аналитики Scalping
        analytics_btn = ctk.CTkButton(
            btn_outer,
            text="📊  Аналитика",
            font=FONT_LABEL_SM, height=28, width=130,
            fg_color="#1A4A6B", hover_color="#2A7AB5",
            command=self._open_analytics_panel)
        analytics_btn.grid(row=0, column=3, sticky="e",
                           padx=PAD_X, pady=(PAD_SM, 2))

        acct = ctk.CTkFrame(
            btn_outer,
            fg_color=("gray85", "gray20"),
            corner_radius=6)
        acct.grid(row=1, column=3, sticky="nsew",
                  padx=PAD_SM, pady=PAD_SM)
        acct.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(acct, text="Торговый счёт",
                     font=FONT_LABEL_SM,
                     text_color=("gray50", "gray55")
                     ).grid(row=0, column=0, padx=PAD_SM,
                            pady=(PAD_SM, 2), sticky="w")

        self._acct_dot = ctk.CTkLabel(
            acct, text="●", font=("Segoe UI", 14),
            text_color=COLOR_STOP)
        self._acct_dot.grid(row=1, column=0,
                             padx=PAD_SM, pady=2, sticky="w")

        self._acct_name = ctk.CTkLabel(
            acct, text="Не подключён",
            font=(FONT_LABEL_SM[0], FONT_LABEL_SM[1], "bold"),
            text_color=COLOR_STOP, anchor="w")
        self._acct_name.grid(row=2, column=0,
                              padx=PAD_SM, pady=(0, 2), sticky="w")

        self._acct_balance = ctk.CTkLabel(
            acct, text="", font=FONT_LABEL_SM,
            text_color=("gray50", "gray55"), anchor="w")
        self._acct_balance.grid(row=3, column=0,
                                 padx=PAD_SM, pady=(0, 2), sticky="w")

        self._acct_broker = ctk.CTkLabel(
            acct, text="",
            font=(FONT_LABEL_SM[0], FONT_LABEL_SM[1]-1),
            text_color=("gray50", "gray55"),
            anchor="w", wraplength=170)
        self._acct_broker.grid(row=4, column=0,
                                padx=PAD_SM,
                                pady=(0, PAD_SM), sticky="w")

        # ── Карточка сигнала (полная) ─────────────────────────
        self._signal_card = ctk.CTkFrame(
            self, fg_color=("gray88", "gray18"),
            corner_radius=8, border_width=1,
            border_color=("gray70", "gray35"))
        self._signal_card.grid(row=2, column=0, sticky="ew",
                                padx=PAD_X, pady=(0, PAD_SM))
        self._signal_card.grid_columnconfigure(0, weight=1)
        self._signal_card.grid_columnconfigure(1, weight=0)

        # Строка 0: заголовок сигнала
        self._signal_lbl = ctk.CTkLabel(
            self._signal_card,
            text="🔔  Сигналов нет — запусти модули",
            font=FONT_LABEL, anchor="w")
        self._signal_lbl.grid(row=0, column=0, columnspan=2,
                               sticky="ew",
                               padx=PAD_X, pady=(PAD_SM, 2))

        # Строка 1: SL / TP в ценах + риск/потенциал $
        self._signal_prices = ctk.CTkLabel(
            self._signal_card, text="",
            font=FONT_LABEL_SM, anchor="w")
        self._signal_prices.grid(row=1, column=0, columnspan=2,
                                  sticky="ew",
                                  padx=PAD_X, pady=(0, 2))

        # Строка 2: прогресс-бар S-score
        self._signal_bar_frame = ctk.CTkFrame(
            self._signal_card, fg_color="transparent")
        self._signal_bar_frame.grid(row=2, column=0,
                                     sticky="w",
                                     padx=PAD_X, pady=(0, PAD_SM))

        self._signal_bar_lbl = ctk.CTkLabel(
            self._signal_bar_frame,
            text="Качество:  ",
            font=FONT_LABEL_SM,
            text_color=("gray50", "gray55"))
        self._signal_bar_lbl.pack(side="left")

        self._signal_bar = ctk.CTkLabel(
            self._signal_bar_frame,
            text="",
            font=(FONT_CONSOLE[0], FONT_CONSOLE[1] + 1))
        self._signal_bar.pack(side="left")

        self._signal_score_lbl = ctk.CTkLabel(
            self._signal_bar_frame,
            text="",
            font=(FONT_LABEL_SM[0], FONT_LABEL_SM[1], "bold"))
        self._signal_score_lbl.pack(side="left", padx=(4, 0))

        # Строка 2 правая: кнопки
        btn_frame = ctk.CTkFrame(
            self._signal_card, fg_color="transparent")
        btn_frame.grid(row=2, column=1,
                       sticky="e", padx=PAD_X,
                       pady=(0, PAD_SM))

        self._open_btn = ctk.CTkButton(
            btn_frame,
            text="✅  Открыть сделку",
            font=FONT_BUTTON, width=180, height=36,
            fg_color="#1A5A2A", hover_color="#27AE60",
            command=self._on_open_trade)
        self._open_btn.pack(side="left", padx=(0, PAD_SM))

        self._skip_btn = ctk.CTkButton(
            btn_frame,
            text="✖  Пропустить",
            font=FONT_LABEL_SM, width=120, height=36,
            fg_color=("gray60", "gray40"),
            hover_color=("gray50", "gray30"),
            command=self._on_skip_signal)
        self._skip_btn.pack(side="left")

        # ── Консоль логов ─────────────────────────────────────
        log_frame = ctk.CTkFrame(self)
        log_frame.grid(row=4, column=0, sticky="nsew",
                       padx=PAD_X, pady=(0, PAD_SM))
        log_frame.grid_rowconfigure(1, weight=1)
        log_frame.grid_columnconfigure(0, weight=1)

        log_hdr = ctk.CTkFrame(log_frame, fg_color="transparent")
        log_hdr.grid(row=0, column=0, columnspan=2,
                     sticky="ew", padx=PAD_SM, pady=(PAD_SM, 0))

        ctk.CTkLabel(log_hdr, text="Консоль логов",
                     font=FONT_LABEL_SM).pack(side="left")
        ctk.CTkButton(
            log_hdr, text="Очистить",
            width=80, font=FONT_LABEL_SM, height=24,
            command=self._clear_console
        ).pack(side="right")

        self._console = tk.Text(
            log_frame, font=FONT_CONSOLE,
            bg="#0D0D0D", fg="#FFFFFF",
            insertbackground="white",
            state="disabled", wrap="none", height=8)
        self._console.grid(row=1, column=0, sticky="nsew",
                           padx=PAD_SM, pady=(0, PAD_SM))
        for level, color in LOG_COLORS.items():
            self._console.tag_config(level, foreground=color)

        sb = ctk.CTkScrollbar(log_frame,
                               command=self._console.yview)
        sb.grid(row=1, column=1, sticky="ns",
                pady=(0, PAD_SM))
        self._console.configure(yscrollcommand=sb.set)

    # ----------------------------------------------------------
    # FIX-1: Открытие панели аналитики Scalping
    # ----------------------------------------------------------
    def _open_analytics_panel(self):
        """Открывает ModulePanel из modules/strategies/scalping/panel.py."""
        try:
            import importlib.util, os
            # __file__ = .../gui/main_panel.py
            # dirname(1) = .../gui/
            # dirname(2) = project root ✓
            panel_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "modules", "strategies", "scalping", "panel.py"
            )
            if not os.path.isfile(panel_path):
                self._q.put(("log", (
                    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "ERROR", "gui",
                    f"Файл панели не найден: {panel_path}"
                )))
                return
            spec = importlib.util.spec_from_file_location(
                "scalping_panel", panel_path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            if not hasattr(mod, "ModulePanel"):
                return
            # Если окно уже открыто — поднять его
            existing = getattr(self, "_analytics_window", None)
            if existing is not None:
                try:
                    existing.lift()
                    existing.focus()
                    return
                except Exception:
                    pass
            window = mod.ModulePanel(
                self.winfo_toplevel(),
                module_manager=self._mm)
            self._analytics_window = window
            window.protocol("WM_DELETE_WINDOW",
                            self._on_analytics_close)
        except Exception as e:
            self._q.put(("log", (
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "ERROR", "gui", f"Ошибка открытия аналитики: {e}"
            )))

    def _on_analytics_close(self):
        try:
            self._analytics_window.destroy()
        except Exception:
            pass
        self._analytics_window = None

    # ----------------------------------------------------------
    # Управление модулями
    # ----------------------------------------------------------
    def _toggle_module(self, name: str):
        loaded = self._mm.list_modules()
        if name not in loaded:
            # Модуль не загружен — показываем подсказку в логе
            self._q.put(("log", (
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "WARNING", "gui",
                f"Модуль '{name}' не загружен. "
                f"Проверь подключение MySQL и таблицу module_registry."
            )))
            return
        status  = self._mm.status(name) or {}
        running = status.get("running", False)

        if running:
            self._module_btns[name].configure(
                text="◼  Остановка...",
                fg_color=COLOR_WARN)
            threading.Thread(
                target=lambda: self._do_stop(name),
                daemon=True).start()
        else:
            self._module_btns[name].configure(
                text="◐  Запуск...",
                fg_color=COLOR_WARN)
            threading.Thread(
                target=lambda: self._do_start(name),
                daemon=True).start()

    def _do_start(self, name: str):
        self._mm.start(name)
        self._q.put(("refresh_modules", None))

    def _do_stop(self, name: str):
        self._mm.stop(name)
        self._q.put(("refresh_modules", None))

    # ----------------------------------------------------------
    # Фоновые обновления
    # ----------------------------------------------------------
    def _schedule_bg_refresh(self):
        self._run_bg_refresh()

    def _run_bg_refresh(self):
        if not self._running:
            return
        threading.Thread(
            target=self._bg_collect_data,
            daemon=True).start()
        self.after(5000, self._run_bg_refresh)

    def _bg_collect_data(self):
        """
        Фоновый поток: только статусы модулей + активный сигнал.
        MT5 аккаунт теперь обновляется через подписку на monitor — 
        не нужен тяжёлый bridge.session() в каждом цикле.
        """
        if not self._running:
            return

        # 1. Статусы модулей
        try:
            loaded   = self._mm.list_modules()
            statuses = {}
            for name in _MODULE_LABELS:
                if name in loaded:
                    statuses[name] = self._mm.status(name) or {}
            self._q.put(("module_statuses", statuses))
        except Exception:
            pass

        # 2. Активный сигнал из БД
        try:
            from core.db_connection import get_db
            db = get_db()
            with db.cursor() as cur:
                cur.execute("""
                    SELECT id, symbol, timeframe, direction,
                           entry_price, sl_price, tp_price,
                           sl_pips, tp_pips,
                           rr_ratio, s_score, t_score, status
                    FROM signal_queue
                    WHERE status IN ('confirmed','pending')
                    ORDER BY
                        CASE status
                            WHEN 'confirmed' THEN 0
                            ELSE 1 END,
                        t_score DESC
                    LIMIT 1
                """)
                sig = cur.fetchone()
            self._q.put(("signal", sig))
        except Exception:
            pass

    def _bg_fetch_acct(self):
        """
        Запрашивает данные MT5 аккаунта.
        Вызывается только при событии CONNECTED от монитора.
        """
        if not self._running:
            return
        try:
            from core.mt5_bridge import get_mt5_bridge
            bridge = get_mt5_bridge()
            with bridge.session() as mt5:
                info = bridge.get_account_info(mt5)
            if info:
                self._q.put(("acct_info", {
                    "connected": True,
                    "name":      f"#{info.get('login', '—')}",
                    "company":   info.get("server", "—"),
                    "balance":   info.get("balance", 0.0),
                    "equity":    info.get("equity", 0.0),
                }))
            else:
                self._q.put(("acct_info",
                              {"connected": True, "name": "подключён"}))
        except Exception:
            self._q.put(("acct_info", {"connected": False}))

    # ----------------------------------------------------------
    # Поллер очереди (главный поток, каждые 200 мс)
    # ----------------------------------------------------------
    def _start_poller(self):
        self._poll()

    def _poll(self):
        if not self._running:
            return
        try:
            while not self._q.empty():
                msg_type, data = self._q.get_nowait()
                if msg_type == "module_statuses":
                    self._apply_module_statuses(data)
                elif msg_type == "acct_info":
                    self._apply_acct_info(data)
                elif msg_type == "signal":
                    self._apply_signal(data)
                elif msg_type == "restore_open_btn":
                    try:
                        self._open_btn.configure(
                            state="normal",
                            text="✅  Открыть сделку")
                    except Exception:
                        pass
                elif msg_type == "log":
                    ts, level, module, msg = data
                    self._write_console_line(ts, level, module, msg)
        except queue.Empty:
            pass
        self.after(200, self._poll)

    # ----------------------------------------------------------
    # Применение данных к виджетам (только главный поток)
    # ----------------------------------------------------------
    def _apply_module_statuses(self, statuses: dict):
        loaded = self._mm.list_modules()
        for name, btn in self._module_btns.items():
            dot  = self._module_dots[name]
            info = self._module_info[name]
            lbl  = _MODULE_LABELS.get(name, name)

            if name not in loaded:
                btn.configure(
                    text=f"▶  {lbl}  (нет в БД)",
                    fg_color=COLOR_STOP)
                dot.configure(text_color=("gray50", "gray50"))
                info.configure(
                    text="Нет в module_registry",
                    text_color=("gray50", "gray50"))
                continue

            s       = statuses.get(name, {})
            running = s.get("running", False)
            errors  = s.get("error_count", 0)
            last    = s.get("last_run")

            if errors > 0:
                color    = COLOR_ERROR
                s_text   = f"Ошибок: {errors}"
                btn_text = f"▶  {lbl}  ⚠"
                btn_clr  = COLOR_ERROR
            elif running:
                color    = COLOR_OK
                s_text   = "Работает"
                btn_text = f"◼  {lbl}  (стоп)"
                btn_clr  = "#1A6B3A"
            else:
                color    = COLOR_STOP
                s_text   = "Остановлен"
                btn_text = f"▶  {lbl}"
                btn_clr  = COLOR_STOP

            btn.configure(
                text=btn_text,
                fg_color=btn_clr,
                hover_color="#27AE60"
                    if not running else "#8B0000")
            dot.configure(text_color=color)

            if last:
                try:
                    dt  = datetime.fromisoformat(last)
                    yer = (dt.astimezone(timezone.utc)
                           + YEREVAN_OFFSET)
                    info.configure(
                        text=f"{s_text}  │  "
                             f"{yer.strftime('%H:%M:%S')}",
                        text_color=color)
                except Exception:
                    info.configure(text=s_text,
                                   text_color=color)
            else:
                info.configure(text=s_text, text_color=color)

    def _apply_acct_info(self, data: dict):
        connected = data.get("connected", False)
        if connected and "name" in data:
            self._acct_dot.configure(text_color=COLOR_OK)
            self._acct_name.configure(
                text=data["name"], text_color=COLOR_OK)
            if "balance" in data:
                self._acct_balance.configure(
                    text=f"Баланс: ${data['balance']:.2f}  "
                         f"Equity: ${data['equity']:.2f}",
                    text_color=COLOR_OK)
            self._acct_broker.configure(
                text=data.get("company", ""),
                text_color=("gray60", "gray50"))
            self._mt5_lbl.configure(
                text="MT5: ✓", text_color=COLOR_OK)
            self._mt5_dot.configure(text_color=COLOR_OK)
        else:
            self._acct_dot.configure(text_color=COLOR_STOP)
            self._acct_name.configure(
                text="Не подключён", text_color=COLOR_STOP)
            self._acct_balance.configure(text="")
            self._acct_broker.configure(text="")
            self._mt5_lbl.configure(
                text="MT5: ✗", text_color=COLOR_ERROR)
            self._mt5_dot.configure(text_color=COLOR_ERROR)

    # ----------------------------------------------------------
    # Вспомогательные методы карточки
    # ----------------------------------------------------------

    @staticmethod
    def _make_progress_bar(score: float) -> tuple:
        """
        Возвращает (bar_str, color, label) для S-score 0..10.
        ██████████ — заполненные блоки (10 позиций)
        ░░░░░░░░░░ — пустые блоки
        """
        filled  = round(score)          # 0-10
        bar     = "█" * filled + "░" * (10 - filled)
        if score >= 8.0:
            color = COLOR_OK             # Зелёный
            label = "Очень сильный"
        elif score >= 6.5:
            color = COLOR_WARN           # Оранжевый
            label = "Сильный"
        else:
            color = COLOR_ERROR          # Красный
            label = "Слабый"
        return bar, color, label

    @staticmethod
    def _calc_risk_reward(sl_pips: int, tp_pips: int,
                          symbol: str = "") -> tuple:
        """
        Считает риск/потенциал в $ для лота 0.01.
        Значение пипса зависит от пары:
          Не-JPY: 1 пипс = $0.10 при лоте 0.01
          JPY:    1 пипс ≈ $0.065 при лоте 0.01 (USD/JPY ~150)
        """
        if symbol.upper().endswith("JPY"):
            pip_value = 0.065
        else:
            pip_value = 0.10
        risk_usd   = round(sl_pips * pip_value, 2)
        reward_usd = round(tp_pips * pip_value, 2)
        return risk_usd, reward_usd

    def _apply_signal(self, sig):
        if sig:
            self._current_signal_id = sig["id"]
            status    = sig["status"]
            direction = sig["direction"]
            emoji     = "▲" if direction == "Support" else "▼"
            s_score   = float(sig["s_score"])
            t_score   = float(sig["t_score"])
            sl_pips   = int(sig["sl_pips"])
            tp_pips   = int(sig["tp_pips"])
            entry     = float(sig["entry_price"])
            sl_price  = float(sig.get("sl_price") or 0)
            tp_price  = float(sig.get("tp_price") or 0)
            rr        = float(sig["rr_ratio"])

            # ── Строка 0: заголовок ───────────────────────────
            if status == "confirmed":
                badge = "✅ M5 ПОДТВЕРЖДЁН"
                hdr_color = COLOR_OK
            else:
                badge = "⏳ ожидает M5"
                hdr_color = COLOR_WARN

            direction_ru = "BUY" if direction == "Support"                            else "SELL"
            hdr_text = (
                f"🔔  {badge}  —  "
                f"{sig['symbol']}  {sig['timeframe']}  "
                f"{emoji} {direction_ru}  │  "
                f"T={t_score:.3f}"
            )
            self._signal_lbl.configure(
                text=hdr_text, text_color=hdr_color)

            # ── Строка 1: цены SL/TP + Риск/Потенциал $ ──────
            risk_usd, reward_usd = self._calc_risk_reward(
                sl_pips, tp_pips, sig["symbol"])
            sl_str = f"{sl_price:.5f}" if sl_price else f"-{sl_pips}п"
            tp_str = f"{tp_price:.5f}" if tp_price else f"+{tp_pips}п"
            prices_text = (
                f"Вход: {entry:.5f}   "
                f"SL: {sl_str} (-{sl_pips}п)   "
                f"TP: {tp_str} (+{tp_pips}п)   "
                f"R:R = 1:{rr:.1f}   │   "
                f"Риск: ${risk_usd:.2f}   "
                f"Потенциал: ${reward_usd:.2f}"
            )
            self._signal_prices.configure(
                text=prices_text,
                text_color=("gray40", "gray70"))

            # ── Строка 2: прогресс-бар S-score ───────────────
            bar, bar_color, bar_label =                 self._make_progress_bar(s_score)
            self._signal_bar.configure(
                text=bar, text_color=bar_color)
            self._signal_score_lbl.configure(
                text=f"  {s_score:.1f}/10  {bar_label}",
                text_color=bar_color)

            # ── Кнопки ────────────────────────────────────────
            if status == "confirmed":
                self._open_btn.pack(side="left",
                                    padx=(0, PAD_SM))
            else:
                self._open_btn.pack_forget()
            self._skip_btn.pack(side="left")

            # Показываем доп. строки
            self._signal_prices.grid()
            self._signal_bar_frame.grid()

        else:
            self._current_signal_id = None
            self._signal_lbl.configure(
                text="🔔  Сигналов нет — запусти модули",
                text_color=("gray50", "gray55"))
            self._signal_prices.configure(text="")
            self._signal_bar.configure(text="")
            self._signal_score_lbl.configure(text="")
            self._open_btn.pack_forget()
            self._skip_btn.pack_forget()

    # ----------------------------------------------------------
    # Действия с сигналом
    # ----------------------------------------------------------
    def _on_open_trade(self):
        if not self._current_signal_id:
            return
        # BUG-7: защита от двойного нажатия — блокируем кнопку сразу
        self._open_btn.configure(state="disabled",
                                  text="⏳  Открываю...")
        self._current_signal_id_copy = self._current_signal_id
        self._current_signal_id = None  # сброс — повторное нажатие не пройдёт
        threading.Thread(
            target=self._do_open_trade,
            daemon=True).start()

    def _do_open_trade(self):
        """
        1. Загружает полные данные сигнала из БД
        2. Вызывает TraderModule.open_trade() → реальный ордер в MT5
        3. При успехе — обновляет статус сигнала в БД
        """
        signal_id = getattr(self, "_current_signal_id_copy", None)
        if not signal_id:
            return

        # Шаг 1: загружаем полные данные сигнала
        try:
            from core.db_connection import get_db
            db = get_db()
            with db.cursor() as cur:
                cur.execute("""
                    SELECT id, symbol, timeframe, direction,
                           price_zone, entry_price,
                           sl_price, tp_price,
                           sl_pips, tp_pips, rr_ratio,
                           s_score, t_score
                    FROM signal_queue
                    WHERE id = %s
                """, (signal_id,))
                signal = cur.fetchone()
        except Exception as e:
            self._q.put(("log", (
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "ERROR", "gui", f"Ошибка загрузки сигнала: {e}"
            )))
            return

        if not signal:
            return

        # Шаг 2: вызываем TraderModule.open_trade()
        try:
            scalping = self._mm.get_module("scalping")
            if scalping is None:
                self._q.put(("log", (
                    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "ERROR", "gui",
                    "Модуль scalping не загружен — невозможно открыть сделку"
                )))
                return

            trader = getattr(scalping, "_trader", None)
            if trader is None:
                self._q.put(("log", (
                    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "ERROR", "gui", "TraderModule не найден в scalping"
                )))
                return

            result = trader.open_trade(signal)

        except Exception as e:
            self._q.put(("log", (
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "ERROR", "gui", f"Ошибка открытия сделки: {e}"
            )))
            return

        # Восстанавливаем кнопку в любом случае (через очередь в главный поток)
        self._q.put(("restore_open_btn", None))

        # Шаг 3: логируем результат и обновляем статус сигнала
        if result.get("success"):
            # Обновляем статус в signal_queue → карточка исчезает с GUI
            try:
                from core.db_connection import get_db
                db = get_db()
                with db.cursor(commit=True) as cur:
                    cur.execute("""
                        UPDATE signal_queue
                        SET status='opened', opened_at=%s
                        WHERE id=%s
                    """, (datetime.now(tz=timezone.utc), signal_id))
            except Exception as e:
                self._q.put(("log", (
                    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "WARNING", "gui",
                    f"Статус сигнала не обновлён: {e}"
                )))

            price = result.get("price") or 0
            msg = (f"✅ Сделка открыта: {signal['symbol']} "
                   f"ticket={result.get('ticket')} "
                   f"цена={price:.5f}")
            self._q.put(("log", (
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "INFO", "gui", msg
            )))
        else:
            err = result.get("error", "неизвестная ошибка")
            self._q.put(("log", (
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "ERROR", "gui", f"❌ Сделка НЕ открыта: {err}"
            )))

    def _on_skip_signal(self):
        if not self._current_signal_id:
            return
        threading.Thread(
            target=self._do_skip_signal,
            daemon=True).start()

    def _do_skip_signal(self):
        try:
            from core.db_connection import get_db
            db = get_db()
            with db.cursor(commit=True) as cur:
                cur.execute("""
                    UPDATE signal_queue
                    SET status='cancelled'
                    WHERE id=%s
                """, (self._current_signal_id,))
        except Exception:
            pass

    # ----------------------------------------------------------
    # Часы (Ереван + UTC + Сессия)
    # ----------------------------------------------------------
    def _start_clock(self):
        self._tick()

    def _tick(self):
        if not self._running:
            return
        now_utc = datetime.now(timezone.utc)
        now_yer = now_utc + YEREVAN_OFFSET
        hour    = now_utc.hour
        active  = [s[0] for s in SESSIONS
                   if s[1] <= hour < s[2]]
        session = ", ".join(active) if active else "Закрыто"
        self._session_lbl.configure(
            text=(f"Сессия: {session}  │  "
                  f"Ереван: {now_yer.strftime('%H:%M:%S')}  │  "
                  f"UTC: {now_utc.strftime('%H:%M:%S')}"))
        self._session_dot.configure(
            text_color=COLOR_OK if active else COLOR_STOP)
        self.after(1000, self._tick)

    # ----------------------------------------------------------
    # Логи (вызывается из GUILogHandler)
    # ----------------------------------------------------------
    def append_log(self, ts, level, module, message):
        """Потокобезопасно — кладёт в очередь."""
        self._q.put(("log", (ts, level, module, message)))

    def _write_console_line(self, ts, level, module, msg):
        tag  = level.upper() \
            if level.upper() in LOG_COLORS else "INFO"
        line = f"{ts}  [{module:<12}]  {level:<8}  {msg}\n"
        self._console.configure(state="normal")
        self._console.insert("end", line, tag)
        self._console.see("end")
        self._console.configure(state="disabled")

    def _clear_console(self):
        self._console.configure(state="normal")
        self._console.delete("1.0", "end")
        self._console.configure(state="disabled")

    # ----------------------------------------------------------
    # Жизненный цикл
    # ----------------------------------------------------------
    def destroy(self):
        self._running = False
        # Отписываемся от монитора
        if self._mt5_monitor is not None:
            try:
                self._mt5_monitor.unsubscribe(
                    self._subscribe_mt5_monitor)
            except Exception:
                pass
        super().destroy()
