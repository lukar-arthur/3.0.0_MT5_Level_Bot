# ============================================================
#  MT5_Level_Bot — gui/signals_panel.py  v1.0.0
#  Вкладка "Сигналы" — торговый центр.
#
#  Крупный шрифт — специально для удобного чтения.
#
#  Структура:
#    Верхняя часть  — активные сигналы (confirmed/pending)
#    Средняя часть  — история сделок с результатами
#    Нижняя часть   — статистика (Win Rate, Profit Factor)
# ============================================================

import threading
from datetime import datetime, timezone, timedelta
from typing import Optional

import customtkinter as ctk

# Крупные шрифты специально для этой вкладки
_F_TITLE   = ("Segoe UI", 15, "bold")
_F_HEADER  = ("Segoe UI", 13, "bold")
_F_ROW     = ("Segoe UI", 13)
_F_ROW_B   = ("Segoe UI", 13, "bold")
_F_SMALL   = ("Segoe UI", 12)
_F_STAT    = ("Segoe UI", 14, "bold")

_C_GREEN  = "#27AE60"
_C_RED    = "#E74C3C"
_C_BLUE   = "#2980B9"
_C_ORANGE = "#E67E22"
_C_GRAY   = ("gray55", "gray55")

_STATUS_COLOR = {
    "confirmed": _C_GREEN,
    "pending":   _C_ORANGE,
    "opened":    _C_BLUE,
    "open":      _C_BLUE,
    "closed":    ("gray60", "gray60"),
    "expired":   ("gray50", "gray50"),
    "cancelled": ("gray50", "gray50"),
    "error":     _C_RED,
}
_STATUS_EMOJI = {
    "confirmed": "✅",
    "pending":   "⏳",
    "opened":    "🔵",
    "open":      "🔵",
    "closed":    "⬜",
    "expired":   "⌛",
    "cancelled": "✖",
    "error":     "❌",
}


class SignalsPanel(ctk.CTkFrame):

    def __init__(self, parent, **kwargs):
        super().__init__(parent, fg_color="transparent", **kwargs)
        self._selected_signal = None
        self._trader          = None   # TraderModule — подключается позже
        self._build_ui()
        self._refresh()
        self.after(10_000, self._auto_refresh)

    # ----------------------------------------------------------
    # Построение UI
    # ----------------------------------------------------------

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=3)  # сигналы
        self.grid_rowconfigure(3, weight=2)  # история

        # ── Заголовок + кнопка обновить ──────────────────────
        top = ctk.CTkFrame(self, fg_color="transparent")
        top.grid(row=0, column=0, sticky="ew", padx=12, pady=(8,4))
        top.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(
            top, text="Торговые сигналы",
            font=_F_TITLE
        ).pack(side="left")

        self._status_lbl = ctk.CTkLabel(
            top, text="", font=_F_SMALL,
            text_color=_C_GRAY)
        self._status_lbl.pack(side="left", padx=16)

        ctk.CTkButton(
            top, text="↻ Обновить",
            font=_F_SMALL, width=110, height=30,
            fg_color=_C_GREEN, hover_color="#219A52",
            command=self._refresh
        ).pack(side="right")

        # ── Активные сигналы ─────────────────────────────────
        ctk.CTkLabel(
            self,
            text="Активные сигналы (confirmed / pending):",
            font=_F_HEADER,
            anchor="w"
        ).grid(row=0, column=0, sticky="sw",
               padx=12, pady=(0,2))

        self._signals_scroll = ctk.CTkScrollableFrame(
            self, corner_radius=6)
        self._signals_scroll.grid(
            row=1, column=0, sticky="nsew", padx=12, pady=(0,6))
        self._signals_scroll.grid_columnconfigure(0, weight=1)

        # ── Разделитель ───────────────────────────────────────
        ctk.CTkLabel(
            self,
            text="История сделок:",
            font=_F_HEADER, anchor="w"
        ).grid(row=2, column=0, sticky="sw", padx=12, pady=(4,2))

        # ── История сделок ────────────────────────────────────
        self._trades_scroll = ctk.CTkScrollableFrame(
            self, corner_radius=6)
        self._trades_scroll.grid(
            row=3, column=0, sticky="nsew", padx=12, pady=(0,6))
        self._trades_scroll.grid_columnconfigure(0, weight=1)

        # ── Статистика ────────────────────────────────────────
        self._stat_frame = ctk.CTkFrame(
            self, corner_radius=6,
            fg_color=("gray88", "gray18"))
        self._stat_frame.grid(
            row=4, column=0, sticky="ew", padx=12, pady=(0,8))

        self._stat_lbl = ctk.CTkLabel(
            self._stat_frame,
            text="Статистика появится после первых сделок",
            font=_F_SMALL,
            text_color=_C_GRAY)
        self._stat_lbl.pack(padx=16, pady=10)

    # ----------------------------------------------------------
    # Загрузка данных
    # ----------------------------------------------------------

    def _refresh(self):
        threading.Thread(
            target=self._load_data, daemon=True).start()

    def _load_data(self):
        try:
            from core.db_connection import get_db
            db = get_db()

            # Активные сигналы
            with db.cursor() as cur:
                cur.execute("""
                    SELECT id, symbol, timeframe, direction,
                           entry_price, sl_price, tp_price,
                           sl_pips, tp_pips, rr_ratio,
                           s_score, t_score, status,
                           created_at, expires_at
                    FROM signal_queue
                    WHERE status IN ('confirmed','pending')
                    ORDER BY
                        CASE status
                            WHEN 'confirmed' THEN 0
                            WHEN 'pending'   THEN 1
                        END,
                        t_score DESC
                    LIMIT 20
                """)
                signals = cur.fetchall()

            # История сделок
            with db.cursor() as cur:
                cur.execute("""
                    SELECT id, symbol, direction, timeframe,
                           entry_price, close_price,
                           profit_pips, profit_usd,
                           close_reason, status,
                           s_score, t_score,
                           open_time, close_time, lot_size
                    FROM trades
                    ORDER BY created_at DESC
                    LIMIT 50
                """)
                trades = cur.fetchall()

            # Статистика
            with db.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) as total,
                        SUM(CASE WHEN profit_pips > 0 THEN 1 ELSE 0 END) as wins,
                        SUM(CASE WHEN profit_pips <= 0 THEN 1 ELSE 0 END) as losses,
                        AVG(CASE WHEN profit_pips > 0 THEN profit_pips END) as avg_win,
                        AVG(CASE WHEN profit_pips <= 0 THEN profit_pips END) as avg_loss,
                        SUM(profit_usd) as total_usd,
                        AVG(s_score) as avg_s,
                        AVG(t_score) as avg_t
                    FROM trades
                    WHERE status = 'closed'
                """)
                stats = cur.fetchone()

            self.after(0, lambda: self._render(
                signals, trades, stats))

        except Exception as e:
            err = str(e)
            self.after(0, lambda m=err: self._status_lbl.configure(
                text=f"✗ {m}", text_color=_C_RED))

    # ----------------------------------------------------------
    # Отрисовка
    # ----------------------------------------------------------

    def _render(self, signals, trades, stats):
        self._render_signals(signals)
        self._render_trades(trades)
        self._render_stats(stats)
        ts = datetime.now(tz=timezone.utc).strftime("%H:%M:%S")
        self._status_lbl.configure(
            text=f"Обновлено {ts} UTC",
            text_color=_C_GRAY)

    def _render_signals(self, signals):
        for w in self._signals_scroll.winfo_children():
            w.destroy()

        if not signals:
            ctk.CTkLabel(
                self._signals_scroll,
                text="Нет активных сигналов — "
                     "запусти модули на Главной вкладке",
                font=_F_ROW,
                text_color=_C_GRAY
            ).grid(row=0, column=0, pady=20)
            return

        for i, sig in enumerate(signals):
            self._build_signal_card(i, sig)

    def _build_signal_card(self, idx: int, sig: dict):
        """Карточка одного активного сигнала."""
        status = sig["status"]
        is_confirmed = (status == "confirmed")

        # Фон: confirmed — чуть зеленее
        bg = ("gray90","gray17") if is_confirmed \
             else ("gray87","gray21")

        card = ctk.CTkFrame(
            self._signals_scroll,
            fg_color=bg, corner_radius=8)
        card.grid(row=idx, column=0, sticky="ew",
                  padx=4, pady=4)
        card.grid_columnconfigure(1, weight=1)

        # Левая полоса цвета статуса
        bar = ctk.CTkFrame(
            card,
            fg_color=_STATUS_COLOR.get(status, _C_GRAY),
            corner_radius=4, width=6)
        bar.grid(row=0, column=0, rowspan=3,
                 sticky="ns", padx=(6,8), pady=6)

        # Строка 1: символ + статус
        row1 = ctk.CTkFrame(card, fg_color="transparent")
        row1.grid(row=0, column=1, sticky="ew",
                  padx=(0,8), pady=(8,2))
        row1.grid_columnconfigure(0, weight=1)

        emoji = _STATUS_EMOJI.get(status, "")
        direction = sig["direction"]
        dir_arrow = "▲" if direction == "Support" else "▼"
        dir_color = _C_GREEN if direction == "Support" \
                    else _C_RED

        ctk.CTkLabel(
            row1,
            text=f"{sig['symbol']}  {sig['timeframe']}  "
                 f"{dir_arrow} {direction}",
            font=_F_ROW_B,
            text_color=dir_color
        ).pack(side="left")

        ctk.CTkLabel(
            row1,
            text=f"{emoji} {status.upper()}",
            font=_F_SMALL,
            text_color=_STATUS_COLOR.get(status, _C_GRAY)
        ).pack(side="right")

        # Строка 2: цены
        row2 = ctk.CTkFrame(card, fg_color="transparent")
        row2.grid(row=1, column=1, sticky="ew",
                  padx=(0,8), pady=2)

        entry = float(sig["entry_price"])
        sl    = float(sig["sl_price"])
        tp    = float(sig["tp_price"])
        rr    = float(sig["rr_ratio"])

        ctk.CTkLabel(
            row2,
            text=(f"Вход: {entry:.5f}   "
                  f"SL: {sl:.5f} (-{sig['sl_pips']}п)   "
                  f"TP: {tp:.5f} (+{sig['tp_pips']}п)   "
                  f"R:R = 1:{rr:.1f}"),
            font=_F_ROW
        ).pack(side="left")

        # Строка 3: метрики + кнопки
        row3 = ctk.CTkFrame(card, fg_color="transparent")
        row3.grid(row=2, column=1, sticky="ew",
                  padx=(0,8), pady=(2,8))

        ctk.CTkLabel(
            row3,
            text=(f"S = {float(sig['s_score']):.2f}   "
                  f"T = {float(sig['t_score']):.3f}"),
            font=_F_SMALL,
            text_color=_C_GRAY
        ).pack(side="left")

        # Кнопка "Открыть" только для confirmed
        if is_confirmed:
            ctk.CTkButton(
                row3,
                text="🚀  Открыть сделку",
                font=_F_HEADER,
                width=180, height=36,
                fg_color=_C_GREEN, hover_color="#219A52",
                command=lambda s=dict(sig):
                    self._on_open_trade(s)
            ).pack(side="right", padx=(8,0))

        ctk.CTkButton(
            row3,
            text="✖ Пропустить",
            font=_F_SMALL,
            width=110, height=36,
            fg_color=("gray60","gray40"),
            hover_color=("gray50","gray30"),
            command=lambda s=dict(sig):
                self._on_skip_signal(s)
        ).pack(side="right", padx=(8,0))

    def _render_trades(self, trades):
        for w in self._trades_scroll.winfo_children():
            w.destroy()

        if not trades:
            ctk.CTkLabel(
                self._trades_scroll,
                text="Сделок пока нет",
                font=_F_ROW,
                text_color=_C_GRAY
            ).grid(row=0, column=0, pady=16)
            return

        # Заголовок таблицы
        hdr = ctk.CTkFrame(
            self._trades_scroll,
            fg_color=("gray80","gray25"))
        hdr.grid(row=0, column=0, sticky="ew", pady=(0,2))

        cols = [
            ("Символ",  80), ("Напр.",  90), ("ТФ",   50),
            ("Вход",    90), ("Закрыт", 90),
            ("Пипсы",   70), ("USD",    75),
            ("Причина", 90), ("Статус", 90),
        ]
        for i,(t,w) in enumerate(cols):
            ctk.CTkLabel(
                hdr, text=t, font=_F_SMALL, width=w
            ).grid(row=0, column=i, padx=4, pady=4)

        for idx, tr in enumerate(trades):
            self._build_trade_row(idx+1, tr)

    def _build_trade_row(self, idx: int, tr: dict):
        bg = ("gray90","gray18") if idx%2==0 \
             else ("gray86","gray22")
        row = ctk.CTkFrame(
            self._trades_scroll, fg_color=bg)
        row.grid(row=idx, column=0, sticky="ew", pady=1)

        status = tr["status"]
        pips   = tr.get("profit_pips")
        usd    = tr.get("profit_usd")

        # Цвет пипсов
        if pips is not None:
            pip_color = _C_GREEN if pips > 0 else _C_RED
            pip_text  = f"+{pips}" if pips > 0 else str(pips)
            usd_text  = f"+{usd:.2f}" if usd and usd > 0 \
                        else f"{usd:.2f}" if usd else "—"
            usd_color = _C_GREEN if usd and usd > 0 else _C_RED
        else:
            pip_color = _C_GRAY
            pip_text  = "—"
            usd_text  = "—"
            usd_color = _C_GRAY

        direction = tr["direction"]
        dir_arrow = "▲Buy" if direction == "Buy" else "▼Sell"
        dir_color = _C_GREEN if direction == "Buy" else _C_RED

        close_p = f"{float(tr['close_price']):.5f}" \
            if tr.get("close_price") else "—"
        reason  = tr.get("close_reason") or "—"

        vals = [
            (tr["symbol"],                   80, None),
            (dir_arrow,                       90, dir_color),
            (tr.get("timeframe","—"),         50, None),
            (f"{float(tr['entry_price']):.5f}",90, None),
            (close_p,                         90, None),
            (pip_text,                        70, pip_color),
            (usd_text,                        75, usd_color),
            (reason,                          90, None),
            (f"{_STATUS_EMOJI.get(status,'')} {status}", 90,
             _STATUS_COLOR.get(status, _C_GRAY)),
        ]

        for i,(text,w,color) in enumerate(vals):
            kw = {"text_color": color} if color else {}
            ctk.CTkLabel(
                row, text=str(text),
                font=_F_ROW, width=w, **kw
            ).grid(row=0, column=i, padx=4, pady=4)

    def _render_stats(self, stats):
        for w in self._stat_frame.winfo_children():
            w.destroy()

        if not stats or not stats.get("total") \
                or stats["total"] == 0:
            ctk.CTkLabel(
                self._stat_frame,
                text="Статистика появится после первых закрытых сделок",
                font=_F_SMALL, text_color=_C_GRAY
            ).pack(padx=16, pady=10)
            return

        total    = stats["total"] or 0
        wins     = stats["wins"]  or 0
        losses   = stats["losses"] or 0
        avg_win  = float(stats["avg_win"]  or 0)
        avg_loss = float(stats["avg_loss"] or 0)
        total_usd = float(stats["total_usd"] or 0)
        avg_s    = float(stats["avg_s"] or 0)

        winrate  = (wins / total * 100) if total else 0
        pf = abs(avg_win * wins / (avg_loss * losses)) \
            if losses and avg_loss != 0 else 0

        # Цвет итога
        usd_color = _C_GREEN if total_usd >= 0 else _C_RED

        stat_row = ctk.CTkFrame(
            self._stat_frame, fg_color="transparent")
        stat_row.pack(padx=16, pady=10, fill="x")

        metrics = [
            ("Сделок",      f"{total}",          None),
            ("Win Rate",    f"{winrate:.0f}%",
             _C_GREEN if winrate >= 55 else _C_RED),
            ("Профит",      f"{avg_win:.0f}п",   _C_GREEN),
            ("Убыток",      f"{avg_loss:.0f}п",  _C_RED),
            ("Profit Factor", f"{pf:.2f}",
             _C_GREEN if pf >= 1.5 else _C_ORANGE),
            ("Итого $",     f"{total_usd:+.2f}", usd_color),
            ("Ср. S",       f"{avg_s:.2f}",      None),
        ]

        for label, value, color in metrics:
            cell = ctk.CTkFrame(
                stat_row, fg_color=("gray85","gray22"),
                corner_radius=6)
            cell.pack(side="left", padx=6)

            ctk.CTkLabel(
                cell, text=label,
                font=_F_SMALL, text_color=_C_GRAY
            ).pack(padx=12, pady=(6,0))

            kw = {"text_color": color} if color else {}
            ctk.CTkLabel(
                cell, text=value,
                font=_F_STAT, **kw
            ).pack(padx=12, pady=(0,6))

    # ----------------------------------------------------------
    # Действия
    # ----------------------------------------------------------

    def _on_open_trade(self, sig: dict):
        """Показать диалог подтверждения перед открытием."""
        dialog = _ConfirmTradeDialog(self, sig,
                                      on_confirm=self._do_open_trade)
        dialog.grab_set()

    def _do_open_trade(self, sig: dict):
        """Открыть сделку после подтверждения пользователем."""
        self._status_lbl.configure(
            text="Открываем сделку...", text_color=_C_ORANGE)

        def _run():
            try:
                # Получаем TraderModule
                trader = self._get_trader()
                if not trader:
                    self.after(0, lambda: self._status_lbl.configure(
                        text="✗ Trader модуль не запущен",
                        text_color=_C_RED))
                    return

                result = trader.open_trade(sig)

                if result["success"]:
                    msg = (f"✅ Открыто: {sig['symbol']} "
                           f"ticket={result.get('ticket')} "
                           f"@ {result.get('price', '—'):.5f}")
                    # Обновляем статус сигнала
                    self._mark_signal_opened(sig["id"])
                    self.after(0, lambda m=msg:
                        self._status_lbl.configure(
                            text=m, text_color=_C_GREEN))
                    self.after(2000, self._refresh)
                else:
                    err = result.get("error", "неизвестно")
                    self.after(0, lambda e=err:
                        self._status_lbl.configure(
                            text=f"✗ {e}", text_color=_C_RED))

            except Exception as e:
                err = str(e)
                self.after(0, lambda m=err:
                    self._status_lbl.configure(
                        text=f"✗ {m}", text_color=_C_RED))

        threading.Thread(target=_run, daemon=True).start()

    def _on_skip_signal(self, sig: dict):
        """Отменить сигнал."""
        try:
            from core.db_connection import get_db
            db = get_db()
            with db.cursor(commit=True) as cur:
                cur.execute("""
                    UPDATE signal_queue
                    SET status = 'cancelled'
                    WHERE id = %s
                """, (sig["id"],))
            self._refresh()
        except Exception as e:
            self._status_lbl.configure(
                text=f"✗ {e}", text_color=_C_RED)

    def _mark_signal_opened(self, signal_id: int):
        """Переводим сигнал в статус opened."""
        try:
            from core.db_connection import get_db
            db = get_db()
            with db.cursor(commit=True) as cur:
                cur.execute("""
                    UPDATE signal_queue
                    SET status = 'opened',
                        opened_at = %s
                    WHERE id = %s
                """, (datetime.now(tz=timezone.utc), signal_id))
        except Exception:
            pass

    def _get_trader(self):
        """Получить TraderModule из ModuleManager."""
        try:
            from core.module_manager import get_module_manager
            mm = get_module_manager()
            return mm.get_module("trader")
        except Exception:
            return None

    def _auto_refresh(self):
        self._refresh()
        self.after(10_000, self._auto_refresh)


# ------------------------------------------------------------------
# Диалог подтверждения сделки
# ------------------------------------------------------------------

class _ConfirmTradeDialog(ctk.CTkToplevel):
    """
    Модальное окно подтверждения перед открытием сделки.
    Показывает все параметры и ждёт явного согласия.
    """

    def __init__(self, parent, sig: dict, on_confirm):
        super().__init__(parent)
        self._sig        = sig
        self._on_confirm = on_confirm
        self.title("Подтверждение сделки")
        self.geometry("480x380")
        self.resizable(False, False)
        # Центрируем относительно родителя
        self.update_idletasks()
        px = parent.winfo_rootx() + \
             parent.winfo_width()//2 - 240
        py = parent.winfo_rooty() + \
             parent.winfo_height()//2 - 190
        self.geometry(f"+{px}+{py}")
        self._build()

    def _build(self):
        sig       = self._sig
        direction = sig["direction"]
        dir_word  = "BUY (покупка)" \
            if direction == "Support" else "SELL (продажа)"
        dir_color = _C_GREEN if direction == "Support" \
                    else _C_RED

        pad = 20

        ctk.CTkLabel(
            self,
            text="Открыть сделку?",
            font=("Segoe UI", 17, "bold")
        ).pack(pady=(pad, 8))

        # Основные параметры
        info_frame = ctk.CTkFrame(
            self, fg_color=("gray88","gray18"),
            corner_radius=8)
        info_frame.pack(padx=pad, fill="x", pady=4)

        rows = [
            ("Инструмент:",  f"{sig['symbol']}  {sig['timeframe']}"),
            ("Направление:", dir_word),
            ("Вход (≈):",    f"{float(sig['entry_price']):.5f}"),
            ("Stop Loss:",   f"{float(sig['sl_price']):.5f}  "
                             f"(-{sig['sl_pips']} пипсов)"),
            ("Take Profit:", f"{float(sig['tp_price']):.5f}  "
                             f"(+{sig['tp_pips']} пипсов)"),
            ("R:R:",         f"1 : {float(sig['rr_ratio']):.1f}"),
            ("Лот:",         "0.01  ($0.10 за пипс)"),
            ("S / T:",       f"{float(sig['s_score']):.2f} / "
                             f"{float(sig['t_score']):.3f}"),
        ]

        for label, value in rows:
            r = ctk.CTkFrame(
                info_frame, fg_color="transparent")
            r.pack(fill="x", padx=12, pady=2)
            ctk.CTkLabel(
                r, text=label,
                font=_F_SMALL,
                text_color=_C_GRAY,
                width=120, anchor="w"
            ).pack(side="left")
            color = dir_color if label == "Направление:" \
                    else None
            kw = {"text_color": color} if color else {}
            ctk.CTkLabel(
                r, text=value,
                font=_F_ROW_B, anchor="w", **kw
            ).pack(side="left")

        # Предупреждение
        ctk.CTkLabel(
            self,
            text="⚠  Сделка откроется на ДЕМО счёте автоматически",
            font=_F_SMALL,
            text_color=_C_ORANGE
        ).pack(pady=(8,4))

        # Кнопки
        btn_frame = ctk.CTkFrame(self, fg_color="transparent")
        btn_frame.pack(pady=12)

        ctk.CTkButton(
            btn_frame,
            text="✅  Да, открыть сделку",
            font=_F_HEADER,
            width=200, height=42,
            fg_color=_C_GREEN, hover_color="#219A52",
            command=self._confirm
        ).pack(side="left", padx=8)

        ctk.CTkButton(
            btn_frame,
            text="✖  Отмена",
            font=_F_HEADER,
            width=120, height=42,
            fg_color=("gray60","gray40"),
            hover_color=("gray50","gray30"),
            command=self.destroy
        ).pack(side="left", padx=8)

    def _confirm(self):
        self.destroy()
        self._on_confirm(self._sig)
