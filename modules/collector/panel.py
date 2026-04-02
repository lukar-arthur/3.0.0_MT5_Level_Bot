# ============================================================
#  Collector — панель статистики  v2.1.0
#  ИСПРАВЛЕНО: _refresh() через after(50) — не блокирует GUI
#  ИСПРАВЛЕНО: _auto_refresh отменяется при закрытии окна
# ============================================================
import customtkinter as ctk
from gui.theme import (FONT_TITLE, FONT_LABEL, FONT_LABEL_SM,
                       PAD_X, PAD_Y, PAD_SM)


class ModulePanel(ctk.CTkToplevel):
    def __init__(self, parent, module_manager, **kwargs):
        super().__init__(parent, **kwargs)
        self._mm       = module_manager
        self._after_id = None
        self.title("Collector — статистика сбора")
        self.geometry("540x400")
        self.resizable(True, True)
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self._build()
        # ВАЖНО: не вызывать _refresh() напрямую — блокирует GUI
        self.after(50, self._refresh)

    def _on_close(self):
        if self._after_id:
            self.after_cancel(self._after_id)
            self._after_id = None
        self.destroy()

    def _build(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)
        ctk.CTkLabel(self, text="Сбор данных — Collector",
                     font=FONT_TITLE).grid(
            row=0, column=0, padx=PAD_X, pady=PAD_Y, sticky="w")
        self._scroll = ctk.CTkScrollableFrame(self)
        self._scroll.grid(row=1, column=0, sticky="nsew",
                          padx=PAD_X, pady=(0, PAD_Y))
        self._scroll.grid_columnconfigure(0, weight=1)

    def _refresh(self):
        for w in self._scroll.winfo_children():
            w.destroy()
        try:
            from core.db_connection import get_db
            db = get_db()
            with db.cursor() as cur:
                cur.execute("""
                    SELECT symbol, timeframe,
                           COUNT(*) as cnt,
                           AVG(bounce_count) as avg_b,
                           AVG(adx_value) as avg_adx,
                           AVG(avg_bounce_pips) as avg_pips,
                           MAX(last_updated) as last_upd
                    FROM raw_levels
                    GROUP BY symbol, timeframe
                    ORDER BY symbol,
                        FIELD(timeframe,'D','H4','H1')
                """)
                rows = cur.fetchall()

            hdr = ctk.CTkFrame(self._scroll,
                                fg_color=("gray80", "gray25"))
            hdr.grid(row=0, column=0, sticky="ew", pady=(0, 2))
            for i, (t, w) in enumerate([
                ("Символ", 80), ("ТФ", 45), ("Уровней", 70),
                ("Ср.B", 60), ("ADX", 55),
                ("Ср.pips", 70), ("Обновлён", 120)
            ]):
                ctk.CTkLabel(hdr, text=t, font=FONT_LABEL_SM,
                             width=w).grid(row=0, column=i,
                                           padx=4, pady=3)

            for idx, r in enumerate(rows):
                bg = ("gray90", "gray18") if idx % 2 == 0 \
                     else ("gray86", "gray22")
                row_f = ctk.CTkFrame(self._scroll, fg_color=bg)
                row_f.grid(row=idx + 1, column=0,
                           sticky="ew", pady=1)
                upd  = str(r["last_upd"] or "—")[:16]
                vals = [r["symbol"], r["timeframe"], r["cnt"],
                        f"{float(r['avg_b']):.1f}",
                        f"{float(r['avg_adx']):.1f}",
                        f"{float(r['avg_pips']):.1f}", upd]
                for i, (v, w) in enumerate(
                        zip(vals, [80, 45, 70, 60, 55, 70, 120])):
                    ctk.CTkLabel(row_f, text=str(v),
                                 font=FONT_LABEL_SM,
                                 width=w).grid(row=0, column=i,
                                               padx=4, pady=2)

            ctk.CTkLabel(
                self._scroll,
                text=f"Всего групп: {len(rows)}",
                font=FONT_LABEL_SM,
                text_color=("gray50", "gray55")
            ).grid(row=len(rows) + 1, column=0,
                   sticky="w", padx=PAD_X, pady=PAD_SM)

        except Exception as e:
            ctk.CTkLabel(self._scroll, text=f"Ошибка: {e}",
                         font=FONT_LABEL_SM).grid(
                row=0, column=0, padx=PAD_X, pady=PAD_Y)

    def _auto_refresh(self):
        self._refresh()
        self._after_id = self.after(10000, self._auto_refresh)
