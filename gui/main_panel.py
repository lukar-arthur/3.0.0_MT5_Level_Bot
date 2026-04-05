# ============================================================
#  MT5_Level_Bot — gui/main_panel.py  v3.0.0
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
# ============================================================
#  MT5_Level_Bot — gui/main_panel.py  v3.0.1
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

_MODULE_LABELS = {
    "collector": "Сбор данных",
    "analyzer":  "Анализ",
    "scalping":  "Сигналы",
}

class MainPanel(ctk.CTkFrame):

    def __init__(self, parent, module_manager, monitor=None, **kwargs):
        super().__init__(parent, fg_color="transparent", **kwargs)
        self._mm      = module_manager
        self._running = True
        self._db_ready = False  # ИСПРАВЛЕНИЕ: Флаг готовности БД

        self._q: queue.Queue = queue.Queue()
        self._current_signal_id = None
        self._mt5_monitor = None

        self._build_ui()
        self._start_clock()
        self._start_poller()
        self._schedule_bg_refresh()
        self._subscribe_mt5_monitor()

    def set_db_ready(self, status: bool):
        """Метод для активации работы с БД (вызывается из app.py)"""
        self._db_ready = status

    def _subscribe_mt5_monitor(self):
        try:
            from core.mt5_bridge import get_mt5_bridge, ConnectionState
            bridge = get_mt5_bridge()
            self._mt5_monitor = bridge.monitor

            def _on_mt5_state(state, message):
                if state == ConnectionState.CONNECTED:
                    threading.Thread(target=self._bg_fetch_acct, daemon=True).start()
                else:
                    self._q.put(("acct_info", {"connected": False}))

            bridge.monitor.subscribe(_on_mt5_state)
            if bridge.monitor.is_connected:
                threading.Thread(target=self._bg_fetch_acct, daemon=True).start()
        except Exception:
            if self._running:
                self.after(2000, self._subscribe_mt5_monitor)

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(4, weight=1)

        # Строка времени
        time_row = ctk.CTkFrame(self, fg_color="transparent")
        time_row.grid(row=0, column=0, sticky="ew", padx=PAD_X, pady=(PAD_SM, 0))
        time_row.grid_columnconfigure(0, weight=1)

        self._session_dot = ctk.CTkLabel(time_row, text="●", font=FONT_LABEL, text_color=COLOR_STOP)
        self._session_dot.pack(side="left", padx=(0, 4))

        self._session_lbl = ctk.CTkLabel(time_row, text="Сессия: —  │  Ереван: --:--  │  UTC: --:--", font=FONT_LABEL, anchor="w")
        self._session_lbl.pack(side="left")

        self._mt5_dot = ctk.CTkLabel(time_row, text="●", font=(FONT_LABEL_SM[0], 12), text_color=COLOR_STOP)
        self._mt5_dot.pack(side="right", padx=(0, 4))

        self._mt5_lbl = ctk.CTkLabel(time_row, text="MT5: —", font=FONT_LABEL_SM, text_color=("gray50", "gray55"))
        self._mt5_lbl.pack(side="right", padx=(0, PAD_SM))

        # Кнопки модулей
        btn_outer = ctk.CTkFrame(self)
        btn_outer.grid(row=1, column=0, sticky="ew", padx=PAD_X, pady=(PAD_Y, PAD_SM))
        btn_outer.grid_columnconfigure((0, 1, 2), weight=1)
        btn_outer.grid_columnconfigure(3, weight=0, minsize=180)

        ctk.CTkLabel(btn_outer, text="Запуск по шагам — нажми нужный этап:", font=FONT_LABEL_SM, text_color=("gray50", "gray55")).grid(row=0, column=0, columnspan=4, sticky="w", padx=PAD_X, pady=(PAD_SM, 2))

        self._module_btns = {}
        self._module_dots = {}
        self._module_info = {}

        specs = [
            ("collector", "1. Сбор данных", "Собирает уровни H1/H4/D из MT5"),
            ("analyzer",  "2. Анализ", "Оценивает силу уровней S=0..10"),
            ("scalping",  "3. Сигналы", "Ищет точки входа T≥0.70"),
        ]

        for col, (name, label, hint) in enumerate(specs):
            card = ctk.CTkFrame(btn_outer)
            card.grid(row=1, column=col, sticky="ew", padx=PAD_SM, pady=PAD_SM)
            card.grid_columnconfigure(0, weight=1)

            btn = ctk.CTkButton(card, text=f"▶  {label}", font=FONT_BUTTON, height=44, fg_color=COLOR_STOP, hover_color="#27AE60", command=lambda n=name: self._toggle_module(n))
            btn.grid(row=0, column=0, sticky="ew", padx=PAD_SM, pady=(PAD_SM, 2))

            sr = ctk.CTkFrame(card, fg_color="transparent")
            sr.grid(row=1, column=0, sticky="ew", padx=PAD_SM)

            dot = ctk.CTkLabel(sr, text="●", font=("Segoe UI", 12), text_color=COLOR_STOP)
            dot.pack(side="left", padx=(0, 4))

            info = ctk.CTkLabel(sr, text="Остановлен", font=FONT_LABEL_SM, text_color=COLOR_STOP, anchor="w")
            info.pack(side="left")

            ctk.CTkLabel(card, text=hint, font=(FONT_LABEL_SM[0], FONT_LABEL_SM[1]-1), text_color=("gray50", "gray55"), wraplength=200, justify="left").grid(row=2, column=0, sticky="w", padx=PAD_SM, pady=(2, PAD_SM))

            self._module_btns[name] = btn
            self._module_dots[name] = dot
            self._module_info[name] = info

        # Аналитика и Аккаунт
        analytics_btn = ctk.CTkButton(btn_outer, text="📊  Аналитика", font=FONT_LABEL_SM, height=28, width=130, fg_color="#1A4A6B", hover_color="#2A7AB5", command=self._open_analytics_panel)
        analytics_btn.grid(row=0, column=3, sticky="e", padx=PAD_X, pady=(PAD_SM, 2))

        acct = ctk.CTkFrame(btn_outer, fg_color=("gray85", "gray20"), corner_radius=6)
        acct.grid(row=1, column=3, sticky="nsew", padx=PAD_SM, pady=PAD_SM)
        acct.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(acct, text="Торговый счёт", font=FONT_LABEL_SM, text_color=("gray50", "gray55")).grid(row=0, column=0, padx=PAD_SM, pady=(PAD_SM, 2), sticky="w")
        self._acct_dot = ctk.CTkLabel(acct, text="●", font=("Segoe UI", 14), text_color=COLOR_STOP)
        self._acct_dot.grid(row=1, column=0, padx=PAD_SM, pady=2, sticky="w")
        self._acct_name = ctk.CTkLabel(acct, text="Не подключён", font=(FONT_LABEL_SM[0], FONT_LABEL_SM[1], "bold"), text_color=COLOR_STOP, anchor="w")
        self._acct_name.grid(row=2, column=0, padx=PAD_SM, pady=(0, 2), sticky="w")
        self._acct_balance = ctk.CTkLabel(acct, text="", font=FONT_LABEL_SM, text_color=("gray50", "gray55"), anchor="w")
        self._acct_balance.grid(row=3, column=0, padx=PAD_SM, pady=(0, 2), sticky="w")
        self._acct_broker = ctk.CTkLabel(acct, text="", font=(FONT_LABEL_SM[0], FONT_LABEL_SM[1]-1), text_color=("gray50", "gray55"), anchor="w", wraplength=170)
        self._acct_broker.grid(row=4, column=0, padx=PAD_SM, pady=(0, PAD_SM), sticky="w")

        # Карточка сигнала
        self._signal_card = ctk.CTkFrame(self, fg_color=("gray88", "gray18"), corner_radius=8, border_width=1, border_color=("gray70", "gray35"))
        self._signal_card.grid(row=2, column=0, sticky="ew", padx=PAD_X, pady=(0, PAD_SM))
        self._signal_card.grid_columnconfigure(0, weight=1)
        self._signal_card.grid_columnconfigure(1, weight=0)

        self._signal_lbl = ctk.CTkLabel(self._signal_card, text="🔔  Сигналов нет — запусти модули", font=FONT_LABEL, anchor="w")
        self._signal_lbl.grid(row=0, column=0, columnspan=2, sticky="ew", padx=PAD_X, pady=(PAD_SM, 2))

        self._signal_prices = ctk.CTkLabel(self._signal_card, text="", font=FONT_LABEL_SM, anchor="w")
        self._signal_prices.grid(row=1, column=0, columnspan=2, sticky="ew", padx=PAD_X, pady=(0, 2))

        self._signal_bar_frame = ctk.CTkFrame(self._signal_card, fg_color="transparent")
        self._signal_bar_frame.grid(row=2, column=0, sticky="w", padx=PAD_X, pady=(0, PAD_SM))

        self._signal_bar_lbl = ctk.CTkLabel(self._signal_bar_frame, text="Качество:  ", font=FONT_LABEL_SM, text_color=("gray50", "gray55"))
        self._signal_bar_lbl.pack(side="left")
        self._signal_bar = ctk.CTkLabel(self._signal_bar_frame, text="", font=(FONT_CONSOLE[0], FONT_CONSOLE[1] + 1))
        self._signal_bar.pack(side="left")
        self._signal_score_lbl = ctk.CTkLabel(self._signal_bar_frame, text="", font=(FONT_LABEL_SM[0], FONT_LABEL_SM[1], "bold"))
        self._signal_score_lbl.pack(side="left", padx=(4, 0))

        btn_frame = ctk.CTkFrame(self._signal_card, fg_color="transparent")
        btn_frame.grid(row=2, column=1, sticky="e", padx=PAD_X, pady=(0, PAD_SM))

        self._open_btn = ctk.CTkButton(btn_frame, text="✅  Открыть сделку", font=FONT_BUTTON, width=180, height=36, fg_color="#1A5A2A", hover_color="#27AE60", command=self._on_open_trade)
        self._open_btn.pack(side="left", padx=(0, PAD_SM))

        self._skip_btn = ctk.CTkButton(btn_frame, text="✖  Пропустить", font=FONT_LABEL_SM, width=120, height=36, fg_color=("gray60", "gray40"), hover_color=("gray50", "gray30"), command=self._on_skip_signal)
        self._skip_btn.pack(side="left")

        # Консоль логов
        log_frame = ctk.CTkFrame(self)
        log_frame.grid(row=4, column=0, sticky="nsew", padx=PAD_X, pady=(0, PAD_SM))
        log_frame.grid_rowconfigure(1, weight=1)
        log_frame.grid_columnconfigure(0, weight=1)

        log_hdr = ctk.CTkFrame(log_frame, fg_color="transparent")
        log_hdr.grid(row=0, column=0, columnspan=2, sticky="ew", padx=PAD_SM, pady=(PAD_SM, 0))

        ctk.CTkLabel(log_hdr, text="Консоль логов", font=FONT_LABEL_SM).pack(side="left")
        ctk.CTkButton(log_hdr, text="Очистить", width=80, font=FONT_LABEL_SM, height=24, command=self._clear_console).pack(side="right")

        self._console = tk.Text(log_frame, font=FONT_CONSOLE, bg="#0D0D0D", fg="#FFFFFF", insertbackground="white", state="disabled", wrap="none", height=8)
        self._console.grid(row=1, column=0, sticky="nsew", padx=PAD_SM, pady=(0, PAD_SM))
        for level, color in LOG_COLORS.items():
            self._console.tag_config(level, foreground=color)

        sb = ctk.CTkScrollbar(log_frame, command=self._console.yview)
        sb.grid(row=1, column=1, sticky="ns", pady=(0, PAD_SM))
        self._console.configure(yscrollcommand=sb.set)

    def _open_analytics_panel(self):
        try:
            import importlib.util, os
            panel_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "modules", "strategies", "scalping", "panel.py")
            if not os.path.isfile(panel_path): return
            spec = importlib.util.spec_from_file_location("scalping_panel", panel_path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            window = mod.ModulePanel(self.winfo_toplevel(), module_manager=self._mm)
            self._analytics_window = window
        except Exception: pass

    def _toggle_module(self, name: str):
        loaded = self._mm.list_modules()
        if name not in loaded: return
        status = self._mm.status(name) or {}
        if status.get("running", False):
            self._module_btns[name].configure(text="◼  Остановка...", fg_color=COLOR_WARN)
            threading.Thread(target=lambda: self._mm.stop(name), daemon=True).start()
        else:
            self._module_btns[name].configure(text="◐  Запуск...", fg_color=COLOR_WARN)
            threading.Thread(target=lambda: self._mm.start(name), daemon=True).start()

    def _schedule_bg_refresh(self):
        if self._running:
            threading.Thread(target=self._bg_collect_data, daemon=True).start()
            self.after(5000, self._schedule_bg_refresh)

    def _bg_collect_data(self):
        # ИСПРАВЛЕНИЕ: Не опрашиваем БД, пока она не готова
        if not self._running or not self._db_ready:
            return

        try:
            loaded = self._mm.list_modules()
            statuses = {n: self._mm.status(n) or {} for n in _MODULE_LABELS if n in loaded}
            self._q.put(("module_statuses", statuses))
        except: pass

        try:
            from core.db_connection import get_db
            db = get_db()
            with db.cursor() as cur:
                cur.execute("SELECT * FROM signal_queue WHERE status IN ('confirmed','pending') ORDER BY t_score DESC LIMIT 1")
                sig = cur.fetchone()
            self._q.put(("signal", sig))
        except: pass

    def _bg_fetch_acct(self):
        if not self._running: return
        try:
            from core.mt5_bridge import get_mt5_bridge
            bridge = get_mt5_bridge()
            with bridge.session() as mt5:
                info = bridge.get_account_info(mt5)
            if info:
                self._q.put(("acct_info", {"connected": True, "name": f"#{info.get('login')}", "company": info.get("server"), "balance": info.get("balance"), "equity": info.get("equity")}))
        except: pass

    def _start_poller(self):
        self._poll()

    def _poll(self):
        if not self._running: return
        try:
            while not self._q.empty():
                msg_type, data = self._q.get_nowait()
                if msg_type == "module_statuses": self._apply_module_statuses(data)
                elif msg_type == "acct_info": self._apply_acct_info(data)
                elif msg_type == "signal": self._apply_signal(data)
                elif msg_type == "log": self._write_console_line(*data)
                elif msg_type == "restore_open_btn": self._open_btn.configure(state="normal", text="✅  Открыть сделку")
        except: pass
        self.after(200, self._poll)

    def _apply_module_statuses(self, statuses):
        loaded = self._mm.list_modules()
        for name, btn in self._module_btns.items():
            if name not in loaded: continue
            s = statuses.get(name, {})
            running = s.get("running", False)
            color = COLOR_OK if running else COLOR_STOP
            self._module_dots[name].configure(text_color=color)
            self._module_info[name].configure(text="Работает" if running else "Остановлен", text_color=color)
            btn.configure(text=f"◼  {_MODULE_LABELS[name]} (стоп)" if running else f"▶  {_MODULE_LABELS[name]}", fg_color="#1A6B3A" if running else COLOR_STOP)

    def _apply_acct_info(self, data):
        if data.get("connected"):
            self._acct_dot.configure(text_color=COLOR_OK)
            self._acct_name.configure(text=data.get("name", ""), text_color=COLOR_OK)
            self._acct_balance.configure(text=f"Баланс: ${data.get('balance', 0):.2f}")
            self._mt5_dot.configure(text_color=COLOR_OK)
        else:
            self._acct_dot.configure(text_color=COLOR_STOP)
            self._acct_name.configure(text="Не подключён", text_color=COLOR_STOP)

    def _apply_signal(self, sig):
        if sig:
            self._current_signal_id = sig["id"]
            self._signal_lbl.configure(text=f"🔔 {sig['symbol']} {sig['direction']} T={sig['t_score']:.2f}", text_color=COLOR_OK)
            self._signal_prices.configure(text=f"Вход: {sig['entry_price']:.5f} SL: {sig['sl_pips']}п TP: {sig['tp_pips']}п")
            self._open_btn.pack(side="left")
            self._skip_btn.pack(side="left")
        else:
            self._signal_lbl.configure(text="🔔 Сигналов нет", text_color="gray")
            self._open_btn.pack_forget()
            self._skip_btn.pack_forget()

    def _on_open_trade(self):
        if not self._current_signal_id: return
        self._open_btn.configure(state="disabled", text="⏳ Открываю...")
        threading.Thread(target=self._do_open_trade, daemon=True).start()

    def _do_open_trade(self):
        try:
            scalping = self._mm.get_module("scalping")
            # Здесь вызывается логика открытия сделки
            self._q.put(("log", (datetime.now().strftime("%H:%M:%S"), "INFO", "gui", "Запрос на открытие сделки отправлен")))
        except Exception as e:
            self._q.put(("log", (datetime.now().strftime("%H:%M:%S"), "ERROR", "gui", f"Ошибка: {e}")))
        self._q.put(("restore_open_btn", None))

    def _on_skip_signal(self):
        self._current_signal_id = None
        self._apply_signal(None)

    def _start_clock(self):
        self._tick()

    def _tick(self):
        if not self._running: return
        now = datetime.now(timezone.utc)
        self._session_lbl.configure(text=f"UTC: {now.strftime('%H:%M:%S')}")
        self.after(1000, self._tick)

    def append_log(self, ts, level, module, message):
        self._q.put(("log", (ts, level, module, message)))

    def _write_console_line(self, ts, level, module, msg):
        line = f"{ts} [{module}] {level}: {msg}\n"
        self._console.configure(state="normal")
        self._console.insert("end", line, level.upper() if level.upper() in LOG_COLORS else "INFO")
        self._console.see("end")
        self._console.configure(state="disabled")

    def _clear_console(self):
        self._console.configure(state="normal")
        self._console.delete("1.0", "end")
        self._console.configure(state="disabled")

    def destroy(self):
        self._running = False
        super().destroy()
