# ============================================================
#  Analyzer — панель статистики  v2.1.0
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
        self.title("Analyzer — статистика анализа")
        self.geometry("620x420")
        self.resizable(True, True)
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self._build()
        # ВАЖНО: не вызывать _refresh() напрямую — блокирует GUI
        self.after(50, self._refresh)

    def _on_close(self):
        """Отменяем отложенный вызов перед закрытием."""
        if self._after_id:
            self.after_cancel(self._after_id)
            self._after_id = None
        self.destroy()

    def _build(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)
        ctk.CTkLabel(self, text="Анализ уровней — Analyzer",
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

            # Распределение по классам
            with db.cursor() as cur:
                cur.execute("""
                    SELECT classification,
                           COUNT(*) as cnt,
                           AVG(strength_score) as avg_s,
                           MAX(strength_score) as max_s,
                           AVG(f_confluence) as avg_c,
                           AVG(f_multitf) as avg_m
                    FROM analyzed_levels
                    GROUP BY classification
                    ORDER BY avg_s DESC
                """)
                rows = cur.fetchall()

            ctk.CTkLabel(self._scroll,
                         text="Распределение по классам:",
                         font=FONT_LABEL).grid(
                row=0, column=0, sticky="w",
                padx=PAD_X, pady=(PAD_SM, 2))

            hdr = ctk.CTkFrame(self._scroll,
                                fg_color=("gray80", "gray25"))
            hdr.grid(row=1, column=0, sticky="ew", pady=(0, 2))
            for i, (t, w) in enumerate([("Класс", 130),
                ("Кол-во", 70), ("Ср.S", 65),
                ("Макс.S", 70), ("Ср.C", 65), ("Ср.M", 65)]):
                ctk.CTkLabel(hdr, text=t, font=FONT_LABEL_SM,
                             width=w).grid(row=0, column=i,
                                           padx=4, pady=3)

            _cls_colors = {
                "Very Strong": "#1A6B3A",
                "Strong":      "#1A4A6B",
                "Medium":      "#5A4A00",
                "Weak":        "#5A2A00",
                "Ignore":      "#3A3A3A",
            }
            for idx, r in enumerate(rows):
                bg = ("gray90", "gray18") if idx % 2 == 0 \
                     else ("gray86", "gray22")
                row_f = ctk.CTkFrame(self._scroll, fg_color=bg)
                row_f.grid(row=idx + 2, column=0,
                           sticky="ew", pady=1)
                cls   = r["classification"]
                color = _cls_colors.get(cls, "#3A3A3A")
                ctk.CTkLabel(
                    row_f, text=cls,
                    font=(FONT_LABEL_SM[0], FONT_LABEL_SM[1], "bold"),
                    fg_color=color,
                    text_color="white",
                    corner_radius=4, width=130
                ).grid(row=0, column=0, padx=4, pady=3)
                for i, v in enumerate([
                    r["cnt"],
                    f"{float(r['avg_s']):.2f}",
                    f"{float(r['max_s']):.2f}",
                    f"{float(r['avg_c']):.2f}",
                    f"{float(r['avg_m']):.2f}",
                ]):
                    ctk.CTkLabel(row_f, text=str(v),
                                 font=FONT_LABEL_SM,
                                 width=[70, 65, 70, 65, 65][i]
                                 ).grid(row=0, column=i + 1,
                                        padx=4, pady=2)

            # Топ-5
            with db.cursor() as cur:
                cur.execute("""
                    SELECT symbol, timeframe, direction,
                           price_zone, strength_score
                    FROM analyzed_levels
                    ORDER BY strength_score DESC
                    LIMIT 5
                """)
                top5 = cur.fetchall()

            offset = len(rows) + 3
            ctk.CTkLabel(self._scroll,
                         text="Топ-5 сильнейших уровней:",
                         font=FONT_LABEL).grid(
                row=offset, column=0, sticky="w",
                padx=PAD_X, pady=(PAD_Y, 2))

            for i, r in enumerate(top5):
                ctk.CTkLabel(
                    self._scroll,
                    text=f"  {r['symbol']:8} {r['timeframe']:5} "
                         f"{r['direction']:13} "
                         f"{float(r['price_zone']):.5f}  "
                         f"S={float(r['strength_score']):.2f}",
                    font=FONT_LABEL_SM, anchor="w"
                ).grid(row=offset + 1 + i, column=0,
                       sticky="w", padx=PAD_X, pady=1)

        except Exception as e:
            ctk.CTkLabel(self._scroll,
                         text=f"Ошибка: {e}",
                         font=FONT_LABEL_SM).grid(
                row=0, column=0, padx=PAD_X, pady=PAD_Y)

    def _auto_refresh(self):
        self._refresh()
        # Сохраняем ID — чтобы можно было отменить
        self._after_id = self.after(15000, self._auto_refresh)
