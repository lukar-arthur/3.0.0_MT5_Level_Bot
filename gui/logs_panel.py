# ============================================================
#  MT5_Level_Bot — gui/logs_panel.py
#  Version : 1.0.0
#  Desc    : Вкладка "Логи"
#            - Полная консоль всех логов
#            - Фильтр по модулю и уровню
#            - Автопрокрутка, очистка, экспорт в файл
# ============================================================

import tkinter as tk
from datetime import datetime

import customtkinter as ctk

from gui.theme import (
    FONT_LABEL, FONT_LABEL_SM, FONT_BUTTON, FONT_CONSOLE,
    COLOR_OK, COLOR_ERROR, COLOR_WARN, COLOR_STOP,
    LOG_COLORS, PAD_X, PAD_Y, PAD_SM
)


class LogsPanel(ctk.CTkFrame):
    """Вкладка 'Логи' — полная консоль с фильтрами."""

    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self._all_lines = []          # хранит все строки для фильтрации
        self._auto_scroll = True
        self._build_ui()

    # ----------------------------------------------------------
    # UI
    # ----------------------------------------------------------

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        # ── Панель фильтров и кнопок ──────────────────────────
        top = ctk.CTkFrame(self)
        top.grid(row=0, column=0, sticky="ew", padx=PAD_X, pady=PAD_Y)

        ctk.CTkLabel(top, text="Модуль:",
                     font=FONT_LABEL_SM).pack(side="left", padx=(PAD_SM, 2))
        self._filter_module = ctk.CTkComboBox(
            top,
            values=["Все", "collector", "analyzer",
                    "gui", "db_connection", "mt5_bridge",
                    "module_manager", "main"],
            width=130, font=FONT_LABEL_SM,
            command=self._apply_filter
        )
        self._filter_module.set("Все")
        self._filter_module.pack(side="left", padx=(0, PAD_X))

        ctk.CTkLabel(top, text="Уровень:",
                     font=FONT_LABEL_SM).pack(side="left", padx=(0, 2))
        self._filter_level = ctk.CTkComboBox(
            top,
            values=["Все", "DEBUG", "INFO",
                    "WARNING", "ERROR", "CRITICAL"],
            width=110, font=FONT_LABEL_SM,
            command=self._apply_filter
        )
        self._filter_level.set("Все")
        self._filter_level.pack(side="left", padx=(0, PAD_X))

        # Автопрокрутка
        self._autoscroll_var = ctk.BooleanVar(value=True)
        ctk.CTkCheckBox(
            top, text="Авто-прокрутка",
            variable=self._autoscroll_var,
            font=FONT_LABEL_SM,
            command=lambda: setattr(
                self, "_auto_scroll", self._autoscroll_var.get())
        ).pack(side="left", padx=(0, PAD_X))

        # Кнопки справа
        ctk.CTkButton(
            top, text="Экспорт",
            font=FONT_LABEL_SM, width=80, height=28,
            command=self._export_logs
        ).pack(side="right", padx=PAD_SM)

        ctk.CTkButton(
            top, text="Очистить",
            font=FONT_LABEL_SM, width=80, height=28,
            fg_color=COLOR_ERROR, hover_color="#C0392B",
            command=self._clear
        ).pack(side="right", padx=(0, PAD_SM))

        # Счётчик строк
        self._count_label = ctk.CTkLabel(
            top, text="Строк: 0", font=FONT_LABEL_SM,
            text_color=COLOR_STOP
        )
        self._count_label.pack(side="right", padx=PAD_X)

        # ── Консоль ───────────────────────────────────────────
        console_frame = ctk.CTkFrame(self)
        console_frame.grid(row=1, column=0, sticky="nsew",
                           padx=PAD_X, pady=(0, PAD_Y))
        console_frame.grid_rowconfigure(0, weight=1)
        console_frame.grid_columnconfigure(0, weight=1)

        self._console = tk.Text(
            console_frame,
            font=FONT_CONSOLE,
            bg="#0A0A0A", fg="#FFFFFF",
            state="disabled",
            wrap="none",
            cursor="arrow"
        )
        self._console.grid(row=0, column=0, sticky="nsew")

        # Цветовые теги
        for level, color in LOG_COLORS.items():
            self._console.tag_config(level, foreground=color)
        # Подсветка строк с ошибками (фон)
        self._console.tag_config(
            "ERROR_BG", background="#3D0000")
        self._console.tag_config(
            "CRITICAL_BG", background="#5D0000")

        # Скроллбары
        vsb = ctk.CTkScrollbar(console_frame,
                                command=self._console.yview)
        vsb.grid(row=0, column=1, sticky="ns")
        hsb = ctk.CTkScrollbar(console_frame, orientation="horizontal",
                                command=self._console.xview)
        hsb.grid(row=1, column=0, sticky="ew")
        self._console.configure(yscrollcommand=vsb.set,
                                 xscrollcommand=hsb.set)

    # ----------------------------------------------------------
    # Public API — вызывается из app.py / log handler
    # ----------------------------------------------------------

    def append(self, timestamp: str, level: str,
               module: str, message: str):
        """
        Добавить строку лога. Потокобезопасно через after().
        """
        entry = (timestamp, level.upper(), module, message)
        self._all_lines.append(entry)

        # Проверяем фильтры
        if self._passes_filter(level.upper(), module):
            self.after(0, lambda e=entry: self._write_line(*e))

    def _passes_filter(self, level: str, module: str) -> bool:
        sel_module = self._filter_module.get()
        sel_level  = self._filter_level.get()
        if sel_module != "Все" and module != sel_module:
            return False
        if sel_level != "Все" and level != sel_level:
            return False
        return True

    def _apply_filter(self, _=None):
        """Перерисовать консоль с текущими фильтрами."""
        self._console.configure(state="normal")
        self._console.delete("1.0", "end")
        for entry in self._all_lines:
            ts, level, module, message = entry
            if self._passes_filter(level, module):
                self._write_line(ts, level, module, message,
                                  update_count=False)
        self._console.configure(state="disabled")
        self._update_count()

    def _write_line(self, ts: str, level: str,
                     module: str, message: str,
                     update_count: bool = True):
        tag = level if level in LOG_COLORS else "INFO"
        line = f"{ts}  [{module:<14}]  {level:<8}  {message}\n"

        self._console.configure(state="normal")
        start = self._console.index("end-1c")
        self._console.insert("end", line)

        # Цвет текста
        end = self._console.index("end-1c")
        self._console.tag_add(tag, start, end)

        # Фон для ошибок
        if level == "ERROR":
            self._console.tag_add("ERROR_BG", start, end)
        elif level == "CRITICAL":
            self._console.tag_add("CRITICAL_BG", start, end)

        if self._auto_scroll:
            self._console.see("end")
        self._console.configure(state="disabled")

        if update_count:
            self._update_count()

    def _update_count(self):
        visible = sum(
            1 for ts, lv, mod, msg in self._all_lines
            if self._passes_filter(lv, mod)
        )
        self._count_label.configure(
            text=f"Строк: {visible}",
            text_color=COLOR_OK if visible > 0 else COLOR_STOP
        )

    # ----------------------------------------------------------
    # Actions
    # ----------------------------------------------------------

    def _clear(self):
        self._all_lines.clear()
        self._console.configure(state="normal")
        self._console.delete("1.0", "end")
        self._console.configure(state="disabled")
        self._update_count()

    def _export_logs(self):
        from tkinter import filedialog
        path = filedialog.asksaveasfilename(
            title="Сохранить логи",
            defaultextension=".log",
            filetypes=[("Log files", "*.log"), ("Text files", "*.txt")]
        )
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as f:
                for ts, level, module, message in self._all_lines:
                    f.write(f"{ts}  [{module:<14}]  {level:<8}  {message}\n")
            self._count_label.configure(
                text=f"Экспортировано: {path.split('/')[-1]}",
                text_color=COLOR_OK
            )
        except Exception as e:
            self._count_label.configure(
                text=f"Ошибка: {e}", text_color=COLOR_ERROR
            )
