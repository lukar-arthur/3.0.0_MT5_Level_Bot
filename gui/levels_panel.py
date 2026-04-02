# ============================================================
#  MT5_Level_Bot — gui/levels_panel.py
#  Version : 1.0.0
#  Desc    : Вкладка "Уровни" — главный рабочий экран.
#
#  Показывает топ уровней из analyzed_levels с фильтрами:
#    - Символ (EURUSD, GBPUSD, ...)
#    - Таймфрейм (Все / D / H4 / H1)
#    - Класс (Все / Very Strong / Strong / Medium)
#    - Направление (Все / Support / Resistance)
#
#  Колонки таблицы:
#    Символ | ТФ | Напр. | Зона | S | Класс | B | F | C | M | ADX | R
#
#  Автообновление каждые 60 секунд.
#  Клик на строку → показывает детали уровня.
# ============================================================

import os
import threading
from datetime import datetime, timezone
from typing import List, Optional

import customtkinter as ctk

from gui.theme import (
    FONT_LABEL, FONT_LABEL_SM, FONT_TITLE, FONT_BUTTON,
    COLOR_OK, COLOR_ERROR, COLOR_WARN,
    COLOR_TEXT_OK, COLOR_TEXT_ERR, COLOR_TEXT_WARN,
    PAD_X, PAD_Y, PAD_SM
)

# Цвета классификации
_CLASS_COLORS = {
    "Very Strong": ("#1A6B3A", "#22A35A"),   # тёмно-зелёный
    "Strong":      ("#1A4A6B", "#2A7AB5"),   # синий
    "Medium":      ("#5A4A1A", "#B58A2A"),   # жёлто-коричневый
    "Weak":        ("#4A2A1A", "#8A4A2A"),   # коричневый
    "Ignore":      ("#3A3A3A", "#6A6A6A"),   # серый
}

_DIRECTION_EMOJI = {"Support": "▲", "Resistance": "▼"}


class LevelsPanel(ctk.CTkFrame):
    """Вкладка топ уровней с фильтрами и автообновлением."""

    def __init__(self, parent, db_ok: bool = True, **kwargs):
        super().__init__(parent, fg_color="transparent", **kwargs)
        self._db_ok        = db_ok
        self._rows: List   = []
        self._selected_row = None
        self._auto_refresh = True
        self._last_refresh = None
        self._after_id     = None   # для отмены при destroy
        self._build_ui()
        if db_ok:
            self._refresh()
            self._schedule_refresh()

    # ----------------------------------------------------------
    # Построение UI
    # ----------------------------------------------------------

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1)

        # ── Строка фильтров ───────────────────────────────────
        filter_frame = ctk.CTkFrame(self, fg_color="transparent")
        filter_frame.grid(row=0, column=0, sticky="ew",
                          padx=PAD_X, pady=(PAD_SM, 0))

        ctk.CTkLabel(filter_frame, text="Символ:",
                     font=FONT_LABEL).pack(side="left", padx=(0, 4))
        self._sym_var = ctk.StringVar(value="Все")
        self._sym_menu = ctk.CTkOptionMenu(
            filter_frame, variable=self._sym_var,
            values=["Все", "EURUSD", "GBPUSD", "USDJPY",
                    "USDCHF", "AUDUSD", "EURGBP"],
            width=110, font=FONT_LABEL_SM,
            command=lambda _: self._refresh()
        )
        self._sym_menu.pack(side="left", padx=(0, PAD_X))

        ctk.CTkLabel(filter_frame, text="ТФ:",
                     font=FONT_LABEL).pack(side="left", padx=(0, 4))
        self._tf_var = ctk.StringVar(value="Все")
        ctk.CTkOptionMenu(
            filter_frame, variable=self._tf_var,
            values=["Все", "D", "H4", "H1"],
            width=80, font=FONT_LABEL_SM,
            command=lambda _: self._refresh()
        ).pack(side="left", padx=(0, PAD_X))

        ctk.CTkLabel(filter_frame, text="Класс:",
                     font=FONT_LABEL).pack(side="left", padx=(0, 4))
        self._cls_var = ctk.StringVar(value="Все")
        ctk.CTkOptionMenu(
            filter_frame, variable=self._cls_var,
            values=["Все", "Very Strong", "Strong", "Medium"],
            width=130, font=FONT_LABEL_SM,
            command=lambda _: self._refresh()
        ).pack(side="left", padx=(0, PAD_X))

        ctk.CTkLabel(filter_frame, text="Напр.:",
                     font=FONT_LABEL).pack(side="left", padx=(0, 4))
        self._dir_var = ctk.StringVar(value="Все")
        ctk.CTkOptionMenu(
            filter_frame, variable=self._dir_var,
            values=["Все", "Support", "Resistance"],
            width=120, font=FONT_LABEL_SM,
            command=lambda _: self._refresh()
        ).pack(side="left", padx=(0, PAD_X))

        # Кнопки управления
        ctk.CTkButton(
            filter_frame, text="↻ Обновить",
            font=FONT_LABEL_SM, width=100,
            fg_color=COLOR_OK, hover_color="#27AE60",
            command=self._refresh
        ).pack(side="left", padx=(PAD_X, 4))

        self._status_lbl = ctk.CTkLabel(
            filter_frame, text="", font=FONT_LABEL_SM,
            text_color=("gray50", "gray60")
        )
        self._status_lbl.pack(side="left", padx=(8, 0))

        # ── Заголовок таблицы ─────────────────────────────────
        hdr_frame = ctk.CTkFrame(self, fg_color=("gray85", "gray20"),
                                  corner_radius=4)
        hdr_frame.grid(row=1, column=0, sticky="ew",
                       padx=PAD_X, pady=(PAD_SM, 0))
        self._build_header(hdr_frame)

        # ── Прокручиваемая таблица ────────────────────────────
        self._scroll = ctk.CTkScrollableFrame(
            self, fg_color="transparent", corner_radius=4)
        self._scroll.grid(row=2, column=0, sticky="nsew",
                          padx=PAD_X, pady=(2, PAD_SM))
        self._scroll.grid_columnconfigure(0, weight=1)

        # ── Панель деталей (снизу) ────────────────────────────
        self._detail = ctk.CTkFrame(
            self, fg_color=("gray90", "gray18"), corner_radius=6)
        self._detail.grid(row=3, column=0, sticky="ew",
                          padx=PAD_X, pady=(0, PAD_SM))
        self._detail_lbl = ctk.CTkLabel(
            self._detail,
            text="Выбери уровень в таблице для просмотра деталей",
            font=FONT_LABEL_SM,
            text_color=("gray50", "gray60")
        )
        self._detail_lbl.pack(padx=PAD_X, pady=PAD_SM)

    def _build_header(self, parent):
        """Строка заголовков колонок."""
        cols = [
            ("Символ",  8,  "w"),
            ("ТФ",      4,  "w"),
            ("Напр.",   11, "w"),
            ("Зона",    10, "e"),
            ("S",        5, "e"),
            ("Класс",   12, "w"),
            ("B",        5, "e"),
            ("F",        5, "e"),
            ("C",        5, "e"),
            ("M",        5, "e"),
            ("ADX",      6, "e"),
            ("R",        3, "center"),
            ("#ТФ",      4, "center"),
        ]
        for i, (text, width, anchor) in enumerate(cols):
            parent.grid_columnconfigure(i, weight=1 if text in
                                         ("Символ","Класс","Напр.") else 0,
                                         minsize=width * 8)
            ctk.CTkLabel(
                parent, text=text, font=FONT_LABEL_SM,
                anchor=anchor
            ).grid(row=0, column=i, padx=(6, 2), pady=4, sticky="ew")

    # ----------------------------------------------------------
    # Загрузка данных
    # ----------------------------------------------------------

    def _refresh(self):
        """Загрузить данные из БД и перерисовать таблицу."""
        if not self._db_ok:
            self._show_no_db()
            return
        threading.Thread(target=self._load_data, daemon=True).start()

    def _load_data(self):
        """Фоновый поток: читает analyzed_levels из MySQL (без JOIN)."""
        try:
            from core.db_connection import get_db
            db = get_db()

            sym  = self._sym_var.get()
            tf   = self._tf_var.get()
            cls  = self._cls_var.get()
            dir_ = self._dir_var.get()

            # Кеш — не перезапрашиваем если фильтры не изменились
            cache_key = (sym, tf, cls, dir_)
            if (hasattr(self, '_cache_key')
                    and self._cache_key == cache_key
                    and hasattr(self, '_cache_time')
                    and (datetime.now(tz=timezone.utc)
                         - self._cache_time).seconds < 30):
                self.after(0, self._render_table)
                return

            conditions = ["strength_score >= 4.5"]
            params     = []

            if sym != "Все":
                conditions.append("symbol = %s")
                params.append(sym)
            if tf != "Все":
                conditions.append("timeframe = %s")
                params.append(tf)
            if cls != "Все":
                conditions.append("classification = %s")
                params.append(cls)
            if dir_ != "Все":
                conditions.append("direction = %s")
                params.append(dir_)

            where = " AND ".join(conditions)

            with db.cursor() as cur:
                cur.execute(f"""
                    SELECT symbol, timeframe, direction,
                           price_zone, strength_score,
                           classification,
                           f_bounce, f_freshness, f_confluence,
                           f_volume, f_multitf, f_reversal,
                           f_dynamics, f_stat,
                           tf_confirmed_count, last_touch_time
                    FROM analyzed_levels
                    WHERE {where}
                    ORDER BY strength_score DESC
                    LIMIT 50
                """, params)
                rows = cur.fetchall()

            self._rows      = rows
            self._cache_key  = cache_key
            self._cache_time = datetime.now(tz=timezone.utc)
            self._last_refresh = datetime.now(tz=timezone.utc)
            self.after(0, self._render_table)

        except Exception as e:
            err_msg = str(e)
            self.after(0, lambda m=err_msg: self._status_lbl.configure(
                text=f"✗ {m}", text_color=COLOR_TEXT_ERR))

    # ----------------------------------------------------------
    # Отрисовка таблицы
    # ----------------------------------------------------------

    def _render_table(self):
        """Перерисовать строки таблицы."""
        # Очищаем старые строки
        for widget in self._scroll.winfo_children():
            widget.destroy()

        if not self._rows:
            ctk.CTkLabel(
                self._scroll,
                text="Нет данных — запусти Collector и Analyzer",
                font=FONT_LABEL_SM,
                text_color=("gray50", "gray60")
            ).grid(row=0, column=0, pady=40)
            self._status_lbl.configure(
                text="Нет уровней по фильтру",
                text_color=COLOR_TEXT_WARN
            )
            return

        self._scroll.grid_columnconfigure(0, weight=1)

        for i, row in enumerate(self._rows):
            self._build_row(i, row)

        ts = self._last_refresh.strftime("%H:%M:%S") \
            if self._last_refresh else "—"
        self._status_lbl.configure(
            text=f"Обновлено {ts} UTC  |  {len(self._rows)} уровней",
            text_color=("gray50", "gray60")
        )

    def _build_row(self, idx: int, row: dict):
        """Одна строка таблицы."""
        bg_even = ("gray92", "gray17")
        bg_odd  = ("gray88", "gray21")
        bg      = bg_even if idx % 2 == 0 else bg_odd

        cls        = row["classification"]
        cls_colors = _CLASS_COLORS.get(cls, _CLASS_COLORS["Ignore"])
        s_score    = float(row["strength_score"])
        direction  = row["direction"]
        # f_dynamics содержит ADX фактор [0..1]
        f_dyn      = float(row.get("f_dynamics") or 0)
        # f_reversal: 1.0 если смена роли, 0.0 нет
        f_rev      = float(row.get("f_reversal") or 0)

        frame = ctk.CTkFrame(self._scroll, fg_color=bg,
                              corner_radius=3)
        frame.grid(row=idx, column=0, sticky="ew",
                   padx=2, pady=1)
        frame.grid_columnconfigure(5, weight=1)

        def _lbl(parent, text, col, fg=None, anchor="w", bold=False):
            font = (FONT_LABEL_SM[0], FONT_LABEL_SM[1],
                    "bold" if bold else "normal")
            kw = {"text_color": fg} if fg else {}
            ctk.CTkLabel(
                parent, text=str(text),
                font=font, anchor=anchor, **kw
            ).grid(row=0, column=col, padx=(6, 2), pady=3,
                   sticky="ew")

        # Колонки
        _lbl(frame, row["symbol"],                      0, bold=True)
        _lbl(frame, row["timeframe"],                   1)

        dir_text  = f"{_DIRECTION_EMOJI.get(direction,'')} {direction}"
        dir_color = COLOR_TEXT_OK if direction == "Support" \
                    else COLOR_TEXT_ERR
        _lbl(frame, dir_text,                           2, fg=dir_color)
        _lbl(frame, f"{float(row['price_zone']):.5f}",  3, anchor="e")

        # S score с цветом по классу
        _lbl(frame, f"{s_score:.2f}",                   4,
             fg=cls_colors[1], anchor="e", bold=True)

        # Класс — цветная метка
        ctk.CTkLabel(
            frame, text=cls,
            font=(FONT_LABEL_SM[0], FONT_LABEL_SM[1]-1, "bold"),
            fg_color=cls_colors[0],
            text_color="white", corner_radius=4
        ).grid(row=0, column=5, padx=(4, 6), pady=2, sticky="w")

        _lbl(frame, f"{float(row['f_bounce']):.2f}",    6, anchor="e")
        _lbl(frame, f"{float(row['f_freshness']):.2f}", 7, anchor="e")
        _lbl(frame, f"{float(row['f_confluence']):.2f}",8, anchor="e")
        _lbl(frame, f"{float(row['f_multitf']):.2f}",   9, anchor="e")

        # D фактор (ADX контекст) с цветом
        adx_color = (COLOR_TEXT_OK   if f_dyn >= 0.7
                     else COLOR_TEXT_WARN if f_dyn >= 0.4
                     else COLOR_TEXT_ERR)
        _lbl(frame, f"{f_dyn:.2f}",                    10,
             fg=adx_color, anchor="e")

        # R: смена роли
        r_text  = "✓" if f_rev > 0 else "—"
        r_color = COLOR_TEXT_OK if f_rev > 0 \
                  else ("gray50", "gray50")
        _lbl(frame, r_text,                            11,
             fg=r_color, anchor="center")

        _lbl(frame, str(row.get("tf_confirmed_count") or 1),
             12, anchor="center")

        # Клик на строку → детали
        frame.bind("<Button-1>",
                   lambda e, r=row: self._show_detail(r))
        for child in frame.winfo_children():
            child.bind("<Button-1>",
                       lambda e, r=row: self._show_detail(r))

    # ----------------------------------------------------------
    # Панель деталей
    # ----------------------------------------------------------

    def _show_detail(self, row: dict):
        """Показать детальную информацию об уровне внизу панели."""
        for w in self._detail.winfo_children():
            w.destroy()

        s         = float(row["strength_score"])
        f_dyn     = float(row.get("f_dynamics") or 0)
        f_rev     = float(row.get("f_reversal") or 0)
        touch     = row["last_touch_time"]

        if touch:
            touch_str = touch.strftime("%Y-%m-%d %H:%M UTC") \
                if hasattr(touch, "strftime") else str(touch)
        else:
            touch_str = "—"

        # ADX контекст через f_dynamics
        if f_dyn >= 0.7:
            adx_hint = "боковик ✓ (уровни работают хорошо)"
        elif f_dyn >= 0.4:
            adx_hint = "умеренный тренд"
        else:
            adx_hint = "сильный тренд ⚠ (уровни пробиваются)"

        # Примерный SL/TP
        pip_size  = 0.01 if row["symbol"].endswith("JPY") else 0.0001
        sl_pips   = 12
        tp_pips   = 16
        direction = row["direction"]
        zone      = float(row["price_zone"])
        entry     = zone
        sl_price  = entry - sl_pips * pip_size \
            if direction == "Support" \
            else entry + sl_pips * pip_size
        tp_price  = entry + tp_pips * pip_size \
            if direction == "Support" \
            else entry - tp_pips * pip_size

        reversal_str = "✓ была смена роли" if f_rev > 0 else "нет"

        text = (
            f"  {row['symbol']}  {row['timeframe']}  "
            f"{_DIRECTION_EMOJI.get(direction,'')} {direction}  "
            f"│  Зона: {zone:.5f}  "
            f"│  S = {s:.2f}  [{row['classification']}]  "
            f"│  ADX контекст: {adx_hint}  "
            f"│  Смена роли: {reversal_str}  "
            f"│  Последнее касание: {touch_str}  "
            f"│  Примерно: Вход {entry:.5f}  "
            f"SL {sl_price:.5f} (-{sl_pips}п)  "
            f"TP {tp_price:.5f} (+{tp_pips}п)  "
            f"│  Факторы: B={float(row['f_bounce']):.2f} "
            f"F={float(row['f_freshness']):.2f} "
            f"C={float(row['f_confluence']):.2f} "
            f"V={float(row['f_volume']):.2f} "
            f"M={float(row['f_multitf']):.2f} "
            f"D={f_dyn:.2f} "
            f"R={f_rev:.2f}"
        )
        ctk.CTkLabel(
            self._detail, text=text,
            font=FONT_LABEL_SM, anchor="w",
            wraplength=900
        ).pack(padx=PAD_X, pady=PAD_SM, anchor="w")

    # ----------------------------------------------------------
    # Вспомогательные
    # ----------------------------------------------------------

    def _show_no_db(self):
        for widget in self._scroll.winfo_children():
            widget.destroy()
        ctk.CTkLabel(
            self._scroll,
            text="MySQL недоступен — запусти XAMPP и перезапусти бот",
            font=FONT_LABEL,
            text_color=COLOR_TEXT_ERR
        ).grid(row=0, column=0, pady=40)

    def _schedule_refresh(self):
        """Автообновление каждые 60 секунд."""
        if self._auto_refresh:
            self._refresh()
        # Сохраняем ID чтобы отменить в destroy()
        self._after_id = self.after(60_000, self._schedule_refresh)

    def destroy(self):
        """Отменяем pending after перед уничтожением виджета."""
        self._auto_refresh = False
        if self._after_id:
            try:
                self.after_cancel(self._after_id)
            except Exception:
                pass
        super().destroy()
