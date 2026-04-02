# ============================================================
#  Scalping Strategy — панель аналитики  v2.0.0
#
#  3 вкладки:
#    1. Журнал    — все сигналы с виртуальным/реальным результатом
#    2. Статистика — Win Rate, Profit Factor, разбивка по символам
#    3. Параметры  — редактирование min_S, min_T, SL/TP, proximity
#
#  ИСПРАВЛЕНО: after(50) → не блокирует GUI, after_cancel при закрытии
# ============================================================
import configparser
import os
import tkinter as tk
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional

import customtkinter as ctk

from gui.theme import (
    FONT_TITLE, FONT_LABEL, FONT_LABEL_SM,
    COLOR_OK, COLOR_ERROR, COLOR_WARN, COLOR_STOP,
    COLOR_TEXT_OK, COLOR_TEXT_ERR, COLOR_TEXT_WARN,
    PAD_X, PAD_Y, PAD_SM
)

_FN  = ("Segoe UI", 12)
_FNB = ("Segoe UI", 12, "bold")
_FSM = ("Segoe UI", 11)

# __file__ = .../modules/strategies/scalping/panel.py
# dirname×4 → project root
_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.dirname(__file__)))),
    "config", "config.ini"
)

_OUTCOME_COLORS = {
    "tp_hit":          "#00A550",
    "sl_hit":          "#CC0000",
    "expired_neutral": "#888888",
    "unknown":         "#888888",
    None:              "#555555",
}

_OUTCOME_LABELS = {
    "tp_hit":          "✅ TP",
    "sl_hit":          "❌ SL",
    "expired_neutral": "⏳ —",
    "unknown":         "?",
    None:              "ожидание",
}

_STATUS_COLORS = {
    "pending":   COLOR_WARN,
    "confirmed": COLOR_OK,
    "opened":    COLOR_OK,
    "closed":    "#2A6B2A",
    "expired":   ("gray50", "gray50"),
    "cancelled": ("gray50", "gray50"),
}


def _load_signal_config() -> dict:
    cfg = configparser.ConfigParser()
    cfg.read(_CONFIG_PATH, encoding="utf-8")
    return {
        "min_s_score":    cfg.getfloat("SIGNAL","min_s_score",    fallback=7.5),
        "min_t_score":    cfg.getfloat("SIGNAL","min_t_score",    fallback=0.70),
        # getfloat + int — защита от "10.0" записанного слайдером
        "proximity_pips": int(cfg.getfloat("SIGNAL","proximity_pips", fallback=10)),
        "sl_atr_mult":    cfg.getfloat("SIGNAL","sl_atr_mult",    fallback=1.0),
        "tp_atr_mult":    cfg.getfloat("SIGNAL","tp_atr_mult",    fallback=1.5),
    }


def _save_signal_config(params: dict) -> None:
    cfg = configparser.ConfigParser()
    cfg.read(_CONFIG_PATH, encoding="utf-8")
    if not cfg.has_section("SIGNAL"):
        cfg.add_section("SIGNAL")
    for k, v in params.items():
        # proximity_pips должен быть целым числом
        if k == "proximity_pips":
            cfg.set("SIGNAL", k, str(int(round(v))))
        else:
            cfg.set("SIGNAL", k, str(round(float(v), 3)))
    with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
        cfg.write(f)


class ModulePanel(ctk.CTkToplevel):

    def __init__(self, parent, module_manager, **kwargs):
        super().__init__(parent, **kwargs)
        self._mm       = module_manager
        self._after_id = None
        self.title("Scalping — аналитика стратегии")
        self.geometry("900x560")
        self.minsize(800, 480)
        self.resizable(True, True)
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self._build()
        self.after(50, self._refresh)

    def _on_close(self):
        if self._after_id:
            self.after_cancel(self._after_id)
        self.destroy()

    # ----------------------------------------------------------
    # Построение UI
    # ----------------------------------------------------------
    def _build(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self._tabs = ctk.CTkTabview(self, anchor="nw")
        self._tabs.grid(row=0, column=0, sticky="nsew",
                        padx=6, pady=(4, 6))
        for name in ["📋 Журнал", "📊 Статистика", "⚙ Параметры"]:
            self._tabs.add(name)

        self._build_journal(self._tabs.tab("📋 Журнал"))
        self._build_stats(self._tabs.tab("📊 Статистика"))
        self._build_params(self._tabs.tab("⚙ Параметры"))

    # ----------------------------------------------------------
    # Вкладка 1: Журнал сигналов
    # ----------------------------------------------------------
    def _build_journal(self, parent):
        parent.grid_columnconfigure(0, weight=1)
        parent.grid_rowconfigure(1, weight=1)

        hdr = ctk.CTkFrame(parent, fg_color="transparent")
        hdr.grid(row=0, column=0, sticky="ew",
                 padx=PAD_X, pady=(PAD_SM, 2))
        ctk.CTkLabel(hdr, text="Все сигналы стратегии Scalping",
                     font=FONT_TITLE).pack(side="left")
        self._journal_stats = ctk.CTkLabel(
            hdr, text="", font=_FSM,
            text_color=("gray50", "gray55"))
        self._journal_stats.pack(side="right")

        self._journal_scroll = ctk.CTkScrollableFrame(parent)
        self._journal_scroll.grid(row=1, column=0, sticky="nsew",
                                   padx=PAD_X, pady=(0, PAD_Y))
        self._journal_scroll.grid_columnconfigure(0, weight=1)

    # ----------------------------------------------------------
    # Вкладка 2: Статистика
    # ----------------------------------------------------------
    def _build_stats(self, parent):
        parent.grid_columnconfigure(0, weight=1)
        parent.grid_rowconfigure(1, weight=1)
        ctk.CTkLabel(parent,
                     text="Статистика по оценённым сигналам",
                     font=FONT_TITLE).grid(
            row=0, column=0, sticky="w",
            padx=PAD_X, pady=(PAD_SM, 2))
        self._stats_scroll = ctk.CTkScrollableFrame(parent)
        self._stats_scroll.grid(row=1, column=0, sticky="nsew",
                                 padx=PAD_X, pady=(0, PAD_Y))
        self._stats_scroll.grid_columnconfigure(0, weight=1)

    # ----------------------------------------------------------
    # Вкладка 3: Параметры
    # ----------------------------------------------------------
    def _build_params(self, parent):
        parent.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(parent,
                     text="Параметры стратегии Scalping",
                     font=FONT_TITLE).grid(
            row=0, column=0, columnspan=3,
            sticky="w", padx=PAD_X, pady=(PAD_Y, PAD_SM))

        ctk.CTkLabel(parent,
            text="⚠  Изменения применяются на следующем цикле "
                 "Signal Engine (каждые 5 минут)",
            font=_FSM,
            text_color=("gray50","gray55")
        ).grid(row=1, column=0, columnspan=3,
               sticky="w", padx=PAD_X, pady=(0, PAD_Y))

        # Описания параметров
        param_info = [
            ("min_S — Порог силы уровня",
             "Минимальный S-score (0..10). Выше → меньше сигналов, "
             "но только очень сильные уровни.\n"
             "Текущее: 7.5. Рекомендуемый диапазон: 6.5–8.5",
             "min_s_score", 5.0, 9.5, 0.5, "{:.1f}"),
            ("min_T — Порог готовности сделки",
             "Trade Readiness Score (0..1). Выше → строже фильтр входа.\n"
             "Текущее: 0.70. Рекомендуемый диапазон: 0.55–0.85",
             "min_t_score", 0.40, 0.90, 0.05, "{:.2f}"),
            ("Proximity — зона входа (пипсы)",
             "Цена должна быть в ±N пипсах от уровня.\n"
             "Меньше → точнее вход но реже. Больше → чаще но хуже R:R.\n"
             "Текущее: 10п. Рекомендуемый диапазон: 5–20п",
             "proximity_pips", 3, 30, 1, "{:.0f}п"),
            ("SL — множитель ATR",
             "StopLoss = ATR × N. Меньше → плотный стоп (частые выбивы).\n"
             "Больше → редкие стопы, но бо́льший риск.\n"
             "Текущее: ATR×1.0. Рекомендуемый диапазон: 0.8–1.5",
             "sl_atr_mult", 0.5, 2.0, 0.1, "ATR×{:.1f}"),
            ("TP — множитель ATR",
             "TakeProfit = ATR × N. R:R = TP/SL. Важно: TP/SL > 1.3.\n"
             "Текущее: ATR×1.5 (R:R=1:1.5). Рекомендуемый: 1.3–2.5",
             "tp_atr_mult", 0.8, 3.0, 0.1, "ATR×{:.1f}"),
        ]

        cfg = _load_signal_config()
        self._param_vars = {}
        self._param_labels = {}

        for i, (title, desc, key, mn, mx, step, fmt) in \
                enumerate(param_info):
            row_base = 2 + i * 3

            # Заголовок параметра
            ctk.CTkLabel(parent, text=title,
                         font=_FNB).grid(
                row=row_base, column=0, columnspan=3,
                sticky="w", padx=PAD_X,
                pady=(PAD_Y if i > 0 else 0, 0))

            # Описание
            ctk.CTkLabel(parent, text=desc,
                         font=_FSM,
                         text_color=("gray45","gray60"),
                         justify="left",
                         wraplength=550).grid(
                row=row_base + 1, column=0,
                sticky="w", padx=PAD_X, pady=(0, 2))

            # Слайдер
            val = cfg.get(key, mn)
            var = tk.DoubleVar(value=val)
            self._param_vars[key] = (var, fmt)

            val_lbl = ctk.CTkLabel(
                parent, text=fmt.format(val),
                font=_FNB, width=90,
                text_color=COLOR_OK)
            val_lbl.grid(row=row_base + 2, column=2,
                         padx=(0, PAD_X), pady=(0, 2),
                         sticky="e")
            self._param_labels[key] = val_lbl

            slider = ctk.CTkSlider(
                parent,
                from_=mn, to=mx,
                number_of_steps=round((mx - mn) / step),
                variable=var,
                command=lambda v, k=key: self._on_slider(k, v)
            )
            slider.grid(row=row_base + 2, column=0,
                        columnspan=2, sticky="ew",
                        padx=PAD_X, pady=(0, 2))

        # Кнопки
        btn_row = 2 + len(param_info) * 3
        btn_frame = ctk.CTkFrame(parent, fg_color="transparent")
        btn_frame.grid(row=btn_row, column=0, columnspan=3,
                       sticky="w", padx=PAD_X,
                       pady=(PAD_Y, PAD_Y))

        ctk.CTkButton(
            btn_frame, text="💾  Сохранить параметры",
            font=FONT_LABEL, width=200,
            fg_color=COLOR_OK, hover_color="#27AE60",
            command=self._save_params
        ).pack(side="left", padx=(0, PAD_SM))

        ctk.CTkButton(
            btn_frame, text="↺  Сбросить к умолчаниям",
            font=FONT_LABEL_SM, width=180,
            fg_color=COLOR_STOP,
            command=self._reset_params
        ).pack(side="left", padx=(0, PAD_SM))

        self._param_status = ctk.CTkLabel(
            btn_frame, text="", font=FONT_LABEL_SM)
        self._param_status.pack(side="left")

        # R:R индикатор (обновляется при изменении SL/TP)
        self._rr_lbl = ctk.CTkLabel(
            parent,
            text="",
            font=_FNB,
            text_color=COLOR_OK
        )
        self._rr_lbl.grid(row=btn_row, column=2,
                          padx=PAD_X, pady=PAD_Y, sticky="e")
        self._update_rr_display()

    # ----------------------------------------------------------
    # Слайдер callback
    # ----------------------------------------------------------
    def _on_slider(self, key: str, value: float):
        var, fmt = self._param_vars[key]
        lbl = self._param_labels[key]
        lbl.configure(text=fmt.format(value))
        self._update_rr_display()

    def _update_rr_display(self):
        try:
            sl_var, _ = self._param_vars["sl_atr_mult"]
            tp_var, _ = self._param_vars["tp_atr_mult"]
            sl = sl_var.get()
            tp = tp_var.get()
            if sl > 0:
                rr = tp / sl
                color = COLOR_OK if rr >= 1.3 else COLOR_WARN \
                    if rr >= 1.0 else COLOR_ERROR
                self._rr_lbl.configure(
                    text=f"R:R = 1:{rr:.2f}",
                    text_color=color)
        except Exception:
            pass

    def _save_params(self):
        params = {}
        for key, (var, _) in self._param_vars.items():
            params[key] = round(var.get(), 3)
        try:
            _save_signal_config(params)
            self._param_status.configure(
                text="✓ Сохранено — применится на следующем цикле",
                text_color=COLOR_TEXT_OK)
            self.after(4000, lambda: self._param_status.configure(text=""))
        except Exception as e:
            self._param_status.configure(
                text=f"✗ {e}", text_color=COLOR_TEXT_ERR)

    def _reset_params(self):
        defaults = {
            "min_s_score": 7.5, "min_t_score": 0.70,
            "proximity_pips": 10, "sl_atr_mult": 1.0,
            "tp_atr_mult": 1.5
        }
        for key, val in defaults.items():
            if key in self._param_vars:
                var, _ = self._param_vars[key]
                var.set(val)
                self._on_slider(key, val)
        self._param_status.configure(
            text="↺ Сброшено. Нажми 'Сохранить' для применения",
            text_color=COLOR_TEXT_WARN)

    # ----------------------------------------------------------
    # Обновление данных
    # ----------------------------------------------------------
    def _refresh(self):
        threading.Thread(
            target=self._bg_load,
            daemon=True).start()
        self._after_id = self.after(10000, self._refresh)

    def _bg_load(self):
        """Фоновая загрузка данных из БД."""
        try:
            from core.db_connection import get_db
            db = get_db()

            # Журнал сигналов
            with db.cursor() as cur:
                cur.execute("""
                    SELECT
                        sq.id, sq.symbol, sq.timeframe,
                        sq.direction, sq.s_score, sq.t_score,
                        sq.sl_pips, sq.tp_pips, sq.rr_ratio,
                        sq.status, sq.created_at,
                        COALESCE(sq.rsi_at_signal, 50) AS rsi_at_signal,
                        sq.virtual_outcome,
                        sq.virtual_profit_pips,
                        t.profit_pips  AS real_profit_pips,
                        t.profit_usd   AS real_profit_usd,
                        t.close_reason AS real_close_reason,
                        t.status       AS trade_status
                    FROM signal_queue sq
                    LEFT JOIN trades t ON t.signal_id = sq.id
                    ORDER BY sq.created_at DESC
                    LIMIT 100
                """)
                journal = cur.fetchall()

            # Статистика
            with db.cursor() as cur:
                cur.execute("""
                    SELECT
                        symbol,
                        COUNT(*) AS total,
                        SUM(CASE WHEN virtual_outcome='tp_hit'
                            THEN 1 ELSE 0 END) AS virt_wins,
                        SUM(CASE WHEN virtual_outcome='sl_hit'
                            THEN 1 ELSE 0 END) AS virt_losses,
                        AVG(CASE WHEN virtual_outcome IS NOT NULL
                              AND virtual_outcome != 'unknown'
                            THEN virtual_profit_pips END) AS avg_pips,
                        SUM(CASE WHEN virtual_outcome='tp_hit'
                            THEN virtual_profit_pips ELSE 0 END) AS gross_tp,
                        SUM(CASE WHEN virtual_outcome='sl_hit'
                            THEN ABS(virtual_profit_pips) ELSE 0 END) AS gross_sl,
                        AVG(s_score) AS avg_s,
                        AVG(t_score) AS avg_t,
                        COUNT(CASE WHEN virtual_outcome IS NOT NULL
                              AND virtual_outcome != 'unknown'
                              THEN 1 END) AS evaluated
                    FROM signal_queue
                    WHERE strategy = 'scalping'
                    GROUP BY symbol
                    ORDER BY total DESC
                """)
                by_symbol = cur.fetchall()

            # Общая статистика
            with db.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) AS total,
                        SUM(CASE WHEN virtual_outcome='tp_hit'
                            THEN 1 ELSE 0 END) AS tp_count,
                        SUM(CASE WHEN virtual_outcome='sl_hit'
                            THEN 1 ELSE 0 END) AS sl_count,
                        SUM(CASE WHEN virtual_outcome='expired_neutral'
                            THEN 1 ELSE 0 END) AS neutral_count,
                        AVG(CASE WHEN virtual_outcome IN ('tp_hit','sl_hit')
                            THEN virtual_profit_pips END) AS avg_pips,
                        SUM(CASE WHEN virtual_outcome='tp_hit'
                            THEN virtual_profit_pips ELSE 0 END) AS gross_tp,
                        SUM(CASE WHEN virtual_outcome='sl_hit'
                            THEN ABS(virtual_profit_pips) ELSE 0 END) AS gross_sl,
                        COUNT(CASE WHEN virtual_outcome IS NOT NULL
                              AND virtual_outcome != 'unknown'
                              THEN 1 END) AS evaluated
                    FROM signal_queue
                    WHERE strategy = 'scalping'
                """)
                overall = cur.fetchone()

            self.after(0, lambda: self._render_journal(journal))
            self.after(0, lambda: self._render_stats(by_symbol, overall))

        except Exception as e:
            self.after(0, lambda: self._journal_stats.configure(
                text=f"Ошибка БД: {e}",
                text_color=COLOR_TEXT_ERR))

    # ----------------------------------------------------------
    # Отрисовка журнала
    # ----------------------------------------------------------
    def _render_journal(self, rows: list):
        for w in self._journal_scroll.winfo_children():
            w.destroy()

        total = len(rows)
        evaluated = sum(1 for r in rows
                        if r.get("virtual_outcome") and
                        r["virtual_outcome"] != "unknown")
        self._journal_stats.configure(
            text=f"Всего: {total}  │  Оценено: {evaluated}",
            text_color=("gray50", "gray55"))

        if not rows:
            ctk.CTkLabel(
                self._journal_scroll,
                text="Сигналов пока нет. Запусти модуль Scalping.",
                font=_FSM,
                text_color=("gray50", "gray55")
            ).grid(row=0, column=0, pady=40)
            return

        # Заголовки таблицы
        COL_DEFS = [
            ("Пара",     70), ("ТФ",  38), ("Напр.",    90),
            ("S",        48), ("T",   52), ("RSI",      52),
            ("SLп",      42), ("TPп", 42), ("R:R",      52),
            ("Статус",   80), ("Итог",65), ("P&L",      65),
            ("Время",   110),
        ]
        hdr_f = ctk.CTkFrame(
            self._journal_scroll,
            fg_color=("gray78", "gray22"))
        hdr_f.grid(row=0, column=0, sticky="ew", pady=(0, 2))
        for i, (t, w) in enumerate(COL_DEFS):
            ctk.CTkLabel(hdr_f, text=t,
                         font=("Segoe UI", 10, "bold"),
                         width=w).grid(
                row=0, column=i, padx=2, pady=3)

        for idx, r in enumerate(rows):
            outcome = r.get("virtual_outcome")
            # Реальный результат приоритетнее виртуального
            real_pips = r.get("real_profit_pips")
            virt_pips = r.get("virtual_profit_pips")
            pnl_pips  = real_pips if real_pips is not None else virt_pips

            # Цвет строки по результату
            if outcome == "tp_hit" or (real_pips and real_pips > 0):
                row_bg = ("gray94", "#1A2E1A")
            elif outcome == "sl_hit" or (real_pips and real_pips < 0):
                row_bg = ("gray94", "#2E1A1A")
            elif idx % 2 == 0:
                row_bg = ("gray90", "gray18")
            else:
                row_bg = ("gray86", "gray22")

            row_f = ctk.CTkFrame(
                self._journal_scroll, fg_color=row_bg)
            row_f.grid(row=idx + 1, column=0,
                       sticky="ew", pady=1)

            emoji = "▲" if r["direction"] == "Support" else "▼"
            created = str(r["created_at"] or "")[:16]
            status_color = _STATUS_COLORS.get(
                r["status"], ("gray50", "gray50"))

            outcome_lbl  = _OUTCOME_LABELS.get(outcome, "?")
            outcome_color = _OUTCOME_COLORS.get(outcome, "#888")

            pnl_text  = f"{pnl_pips:+.0f}п" \
                if pnl_pips is not None else "—"
            pnl_color = COLOR_OK if pnl_pips and pnl_pips > 0 else \
                        COLOR_ERROR if pnl_pips and pnl_pips < 0 else \
                        ("gray55", "gray55")

            rsi_val = float(r.get("rsi_at_signal") or 50)
            if rsi_val < 30:
                rsi_color = COLOR_OK    # перепроданность = хорошо для Buy
                rsi_txt   = f"↓{rsi_val:.0f}"
            elif rsi_val > 70:
                rsi_color = COLOR_OK    # перекупленность = хорошо для Sell
                rsi_txt   = f"↑{rsi_val:.0f}"
            elif rsi_val < 40 or rsi_val > 60:
                rsi_color = COLOR_WARN
                rsi_txt   = f"{rsi_val:.0f}"
            else:
                rsi_color = ("gray55", "gray55")
                rsi_txt   = f"{rsi_val:.0f}"

            vals = [
                (r["symbol"],                    70,  None),
                (r["timeframe"],                 38,  None),
                (f"{emoji} {r['direction'][:3]}", 90, None),
                (f"{float(r['s_score']):.2f}",   48,  None),
                (f"{float(r['t_score']):.3f}",   52,  None),
                (rsi_txt,                        52,  rsi_color),
                (str(r["sl_pips"]),              42,  None),
                (str(r["tp_pips"]),              42,  None),
                (f"1:{float(r['rr_ratio']):.1f}", 52, None),
                (r["status"],                    80,  status_color),
                (outcome_lbl,                    65,  outcome_color),
                (pnl_text,                       65,  pnl_color),
                (created,                        110, None),
            ]
            for i, (v, w, clr) in enumerate(vals):
                kw = {"text": str(v), "font": _FSM, "width": w}
                if clr:
                    kw["text_color"] = clr
                ctk.CTkLabel(row_f, **kw).grid(
                    row=0, column=i, padx=2, pady=2)

    # ----------------------------------------------------------
    # Отрисовка статистики
    # ----------------------------------------------------------
    def _render_stats(self, by_symbol: list, overall: dict):
        for w in self._stats_scroll.winfo_children():
            w.destroy()

        if not overall or not overall.get("total"):
            ctk.CTkLabel(
                self._stats_scroll,
                text="Сигналов пока нет. Запусти модуль Scalping.",
                font=_FSM,
                text_color=("gray50","gray55")
            ).grid(row=0, column=0, pady=40)
            return

        ev  = overall.get("evaluated") or 0
        tp  = overall.get("tp_count")  or 0
        sl  = overall.get("sl_count")  or 0
        avg = float(overall.get("avg_pips") or 0)
        gtp = float(overall.get("gross_tp") or 0)
        gsl = float(overall.get("gross_sl") or 0)
        wr  = (tp / ev * 100) if ev > 0 else 0
        pf  = (gtp / gsl) if gsl > 0 else 0.0

        # ── Сводная панель ────────────────────────────────────
        summary = ctk.CTkFrame(
            self._stats_scroll,
            fg_color=("gray85", "gray20"),
            corner_radius=8)
        summary.grid(row=0, column=0, sticky="ew",
                     padx=PAD_X, pady=(PAD_SM, PAD_Y))
        summary.grid_columnconfigure((0,1,2,3,4), weight=1)

        metrics = [
            ("Оценено сигналов", str(ev),         None),
            ("Win Rate",  f"{wr:.0f}%",
             COLOR_OK if wr >= 55 else
             COLOR_WARN if wr >= 45 else COLOR_ERROR),
            ("Avg P&L",   f"{avg:+.1f}п",
             COLOR_OK if avg > 0 else
             COLOR_WARN if avg > -3 else COLOR_ERROR),
            ("Profit Factor", f"{pf:.2f}",
             COLOR_OK if pf >= 1.3 else
             COLOR_WARN if pf >= 1.0 else COLOR_ERROR),
            ("TP/SL",     f"{tp}✅ / {sl}❌", None),
        ]
        for col, (lbl, val, clr) in enumerate(metrics):
            card = ctk.CTkFrame(summary,
                                fg_color=("gray90","gray18"),
                                corner_radius=6)
            card.grid(row=0, column=col,
                      padx=4, pady=6, sticky="ew")
            ctk.CTkLabel(card, text=lbl,
                         font=_FSM,
                         text_color=("gray50","gray55")
                         ).pack(pady=(4,0))
            kw = {"text": val, "font": _FNB}
            if clr:
                kw["text_color"] = clr
            ctk.CTkLabel(card, **kw).pack(pady=(0,4))

        # ── Разбивка по символам ──────────────────────────────
        ctk.CTkLabel(
            self._stats_scroll,
            text="По символам:",
            font=FONT_LABEL
        ).grid(row=1, column=0, sticky="w",
               padx=PAD_X, pady=(PAD_SM, 2))

        COL_SYM = [
            ("Символ",50),("Всего",52),("Оценено",65),
            ("Win%",  55),("AvgS",  50),("AvgT",  50),
            ("Avg P&L",70),("PF",  55),
        ]
        sym_hdr = ctk.CTkFrame(
            self._stats_scroll,
            fg_color=("gray78","gray22"))
        sym_hdr.grid(row=2, column=0, sticky="ew",
                     padx=PAD_X, pady=(0,2))
        for i, (t, w) in enumerate(COL_SYM):
            ctk.CTkLabel(sym_hdr, text=t,
                         font=("Segoe UI",10,"bold"),
                         width=w).grid(
                row=0, column=i, padx=3, pady=3)

        for idx, r in enumerate(by_symbol):
            ev_s  = int(r.get("evaluated") or 0)
            tp_s  = int(r.get("virt_wins") or 0)
            sl_s  = int(r.get("virt_losses") or 0)
            wr_s  = (tp_s / ev_s * 100) if ev_s > 0 else 0
            avg_s = float(r.get("avg_pips") or 0)
            gtp_s = float(r.get("gross_tp") or 0)
            gsl_s = float(r.get("gross_sl") or 0)
            pf_s  = (gtp_s / gsl_s) if gsl_s > 0 else 0.0

            bg = ("gray90","gray18") if idx % 2 == 0 \
                 else ("gray86","gray22")
            rf = ctk.CTkFrame(
                self._stats_scroll, fg_color=bg)
            rf.grid(row=idx + 3, column=0,
                    sticky="ew", padx=PAD_X, pady=1)

            wr_clr  = COLOR_OK if wr_s >= 55 else \
                      COLOR_WARN if wr_s >= 45 else COLOR_ERROR
            avg_clr = COLOR_OK if avg_s > 0 else \
                      COLOR_WARN if avg_s > -3 else COLOR_ERROR
            pf_clr  = COLOR_OK if pf_s >= 1.3 else \
                      COLOR_WARN if pf_s >= 1.0 else COLOR_ERROR

            vals = [
                (r["symbol"],                 50, None),
                (str(r.get("total",0)),        52, None),
                (str(ev_s),                    65, None),
                (f"{wr_s:.0f}%",               55, wr_clr),
                (f"{float(r.get('avg_s',0)):.2f}", 50, None),
                (f"{float(r.get('avg_t',0)):.3f}", 50, None),
                (f"{avg_s:+.1f}п",             70, avg_clr),
                (f"{pf_s:.2f}",                55, pf_clr),
            ]
            for i, (v, w, clr) in enumerate(vals):
                kw = {"text": str(v), "font": _FSM, "width": w}
                if clr:
                    kw["text_color"] = clr
                ctk.CTkLabel(rf, **kw).grid(
                    row=0, column=i, padx=3, pady=2)

        # ── Советы по настройке ───────────────────────────────
        if ev >= 10:
            tips = self._generate_tips(overall, by_symbol)
            if tips:
                ctk.CTkLabel(
                    self._stats_scroll,
                    text="💡 Рекомендации по настройке:",
                    font=FONT_LABEL
                ).grid(row=len(by_symbol) + 4, column=0,
                       sticky="w", padx=PAD_X,
                       pady=(PAD_Y, 2))
                for ti, tip in enumerate(tips):
                    ctk.CTkLabel(
                        self._stats_scroll,
                        text=f"  • {tip}",
                        font=_FSM,
                        text_color=("gray45","gray65"),
                        wraplength=750,
                        justify="left"
                    ).grid(row=len(by_symbol) + 5 + ti,
                           column=0, sticky="w",
                           padx=PAD_X, pady=1)
        else:
            ctk.CTkLabel(
                self._stats_scroll,
                text=f"  Оценено {ev} из 10 минимальных. "
                     f"Накапливаем статистику...",
                font=_FSM,
                text_color=("gray50","gray55")
            ).grid(row=len(by_symbol) + 4, column=0,
                   sticky="w", padx=PAD_X, pady=PAD_Y)

    def _generate_tips(self, overall: dict,
                        by_symbol: list) -> list:
        """Автоматические советы по настройке на основе данных."""
        tips = []
        ev  = int(overall.get("evaluated") or 0)
        tp  = int(overall.get("tp_count")  or 0)
        sl  = int(overall.get("sl_count")  or 0)
        wr  = (tp / ev * 100) if ev > 0 else 0
        avg = float(overall.get("avg_pips") or 0)
        gtp = float(overall.get("gross_tp") or 0)
        gsl = float(overall.get("gross_sl") or 0)
        pf  = (gtp / gsl) if gsl > 0 else 0.0

        cfg = _load_signal_config()

        if wr < 40:
            tips.append(
                f"Win Rate {wr:.0f}% слишком низкий. "
                f"Попробуй поднять min_S до {cfg['min_s_score']+0.5:.1f} "
                f"— брать только очень сильные уровни.")
        elif wr > 75 and ev < 20:
            tips.append(
                f"Win Rate {wr:.0f}% отличный, но мало сигналов ({ev}). "
                f"Можно попробовать снизить min_T до "
                f"{cfg['min_t_score']-0.05:.2f} для большей частоты.")

        if pf < 1.0 and sl > 0:
            tips.append(
                f"Profit Factor {pf:.2f} — стратегия убыточна. "
                f"Увеличь TP множитель до "
                f"{cfg['tp_atr_mult']+0.2:.1f} "
                f"(текущий R:R = 1:{cfg['tp_atr_mult']/cfg['sl_atr_mult']:.1f}).")

        if avg < -5:
            tips.append(
                "Средний P&L отрицательный. "
                "Уменьши SL множитель или увеличь proximity_pips — "
                "возможно входы слишком далеко от уровня.")

        # Анализ по символам
        worst = [r for r in by_symbol
                 if r.get("evaluated", 0) >= 5]
        worst.sort(key=lambda r: float(r.get("avg_pips") or 0))
        if worst and float(worst[0].get("avg_pips") or 0) < -8:
            tips.append(
                f"Символ {worst[0]['symbol']} даёт стабильные убытки "
                f"(avg {float(worst[0].get('avg_pips',0)):+.1f}п). "
                f"Рассмотри его исключение.")

        return tips
