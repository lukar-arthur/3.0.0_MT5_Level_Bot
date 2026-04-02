# ============================================================
#  MT5_Level_Bot — gui/connection_monitor_widget.py
#  Version : 1.0.0
#  Desc    : Виджет статуса соединения MT5.
#            Отображается на главной панели.
#            Подписывается на события ConnectionMonitor.
#            Показывает: состояние, время без связи, кол-во переподключений.
# ============================================================

import time
import customtkinter as ctk
from core.mt5_bridge import ConnectionState


class ConnectionMonitorWidget(ctk.CTkFrame):
    """
    Компактный виджет статуса MT5 для главной панели.
    Встраивается в любой CTkFrame.
    """

    _STATE_CONFIG = {
        ConnectionState.CONNECTED: {
            "dot":   "#00A550",
            "text":  "MT5 подключён",
            "color": "#004D00",
        },
        ConnectionState.DISCONNECTED: {
            "dot":   "#CC0000",
            "text":  "MT5 отключён",
            "color": "#8B0000",
        },
        ConnectionState.RECONNECTING: {
            "dot":   "#E07B00",
            "text":  "MT5 переподключение...",
            "color": "#7A4400",
        },
    }

    def __init__(self, parent, monitor, **kwargs):
        super().__init__(parent, **kwargs)
        self._monitor = monitor
        self._build_ui()
        self._monitor.subscribe(self._on_state_change)
        # Начальное обновление
        self._refresh_display(self._monitor.state, "")
        # Таймер для обновления времени без связи
        self._tick()

    def _build_ui(self):
        self.grid_columnconfigure(1, weight=1)

        # Цветной индикатор
        self._dot = ctk.CTkLabel(
            self, text="●",
            font=("Segoe UI", 14),
            text_color="#CC0000"
        )
        self._dot.grid(row=0, column=0, padx=(6, 4))

        # Текст статуса
        self._status_lbl = ctk.CTkLabel(
            self, text="MT5 отключён",
            font=("Segoe UI", 10),
            text_color="#8B0000",
            anchor="w"
        )
        self._status_lbl.grid(row=0, column=1, sticky="w")

        # Дополнительная информация (время, счётчик)
        self._info_lbl = ctk.CTkLabel(
            self, text="",
            font=("Segoe UI", 9),
            text_color=("gray40", "gray60"),
            anchor="w"
        )
        self._info_lbl.grid(row=1, column=0, columnspan=2,
                             sticky="w", padx=(6, 0))

    def _on_state_change(self, state: ConnectionState, message: str):
        """Вызывается из фонового потока — используем after() для GUI."""
        self.after(0, lambda: self._refresh_display(state, message))

    def _refresh_display(self, state: ConnectionState, message: str):
        cfg = self._STATE_CONFIG.get(
            state, self._STATE_CONFIG[ConnectionState.DISCONNECTED]
        )
        self._dot.configure(text_color=cfg["dot"])
        self._status_lbl.configure(
            text=cfg["text"], text_color=cfg["color"]
        )

    def _tick(self):
        """Обновлять счётчик времени каждую секунду."""
        try:
            status = self._monitor.get_status_dict()
            parts  = []

            if status["reconnect_count"] > 0:
                parts.append(
                    f"Переподключений: {status['reconnect_count']}"
                )

            if (status["state"] == "reconnecting"
                    and status["disconnected_sec"] is not None):
                sec = status["disconnected_sec"]
                if sec < 60:
                    parts.append(f"Без связи: {sec}с")
                else:
                    parts.append(
                        f"Без связи: {sec // 60}м {sec % 60}с"
                    )

            self._info_lbl.configure(
                text="  ".join(parts) if parts else ""
            )
            self.after(1000, self._tick)
        except Exception:
            pass

    def destroy(self):
        self._monitor.unsubscribe(self._on_state_change)
        super().destroy()
