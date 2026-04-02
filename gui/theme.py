# ============================================================
#  MT5_Level_Bot — gui/theme.py  v2.0.0
#
#  ИЗМЕНЕНИЯ v2.0.0:
#  - Увеличены все размеры шрифтов (пользователь с плохим зрением)
#  - Размер окна увеличен: 900×640 → 1000×680
#  - Plug-and-Play: размеры читаются из config/gui.ini
# ============================================================

import customtkinter as ctk
from core.config_loader import load_module_config

# Plug-and-Play: GUI использует config/gui.ini
_GUI_CFG = load_module_config("gui")

ctk.set_appearance_mode(_GUI_CFG.get("GUI", "appearance_mode", fallback="system"))
ctk.set_default_color_theme("blue")

# ------------------------------------------------------------------
# Размеры окна — из gui.ini (увеличено по умолчанию)
# ------------------------------------------------------------------
WINDOW_WIDTH  = _GUI_CFG.getint("GUI", "window_width",  fallback=1000)
WINDOW_HEIGHT = _GUI_CFG.getint("GUI", "window_height", fallback=680)
WINDOW_MIN_W  = _GUI_CFG.getint("GUI", "window_min_w",  fallback=820)
WINDOW_MIN_H  = _GUI_CFG.getint("GUI", "window_min_h",  fallback=560)

# ------------------------------------------------------------------
# Шрифты — УВЕЛИЧЕНЫ для удобства чтения
# Все размеры берутся из config/gui.ini
# ------------------------------------------------------------------
_FS_TITLE  = _GUI_CFG.getint("GUI", "font_size_title",    fallback=15)
_FS_LABEL  = _GUI_CFG.getint("GUI", "font_size_label",    fallback=13)
_FS_LBL_SM = _GUI_CFG.getint("GUI", "font_size_label_sm", fallback=12)
_FS_BTN    = _GUI_CFG.getint("GUI", "font_size_button",   fallback=13)
_FS_CON    = _GUI_CFG.getint("GUI", "font_size_console",  fallback=12)
_FS_STATUS = _GUI_CFG.getint("GUI", "font_size_status",   fallback=12)

FONT_TITLE    = ("Segoe UI", _FS_TITLE,  "bold")
FONT_LABEL    = ("Segoe UI", _FS_LABEL)
FONT_LABEL_SM = ("Segoe UI", _FS_LBL_SM)
FONT_BUTTON   = ("Segoe UI", _FS_BTN,   "bold")
FONT_CONSOLE  = ("Consolas", _FS_CON)
FONT_STATUS   = ("Segoe UI", _FS_STATUS)

# ------------------------------------------------------------------
# Цвета кнопок и индикаторов
# ------------------------------------------------------------------
COLOR_OK    = "#00A550"   # насыщенный зелёный
COLOR_WARN  = "#E07B00"   # насыщенный оранжевый
COLOR_ERROR = "#CC0000"   # насыщенный красный
COLOR_STOP  = "#505050"   # тёмно-серый
COLOR_INFO  = "#0066CC"   # насыщенный синий

# ------------------------------------------------------------------
# Цвета текстовых статусных надписей — высокий контраст
# ------------------------------------------------------------------
COLOR_TEXT_OK   = "#004D00"
COLOR_TEXT_WARN = "#7A4400"
COLOR_TEXT_ERR  = "#8B0000"
COLOR_TEXT_STOP = "#2B2B2B"

TAB_TEXT_COLOR = ("black", "white")

# ------------------------------------------------------------------
# Цвета консоли логов
# ------------------------------------------------------------------
LOG_COLORS = {
    "DEBUG":    "#AAAAAA",
    "INFO":     "#FFFFFF",
    "WARNING":  "#FFB347",
    "ERROR":    "#FF4444",
    "CRITICAL": "#FF0000",
}

# ------------------------------------------------------------------
# Отступы (немного увеличены)
# ------------------------------------------------------------------
PAD_X  = 14
PAD_Y  = 10
PAD_SM = 5

# ------------------------------------------------------------------
# Иконки
# ------------------------------------------------------------------
ICONS = {
    "start":        "assets/icons/start.png",
    "stop":         "assets/icons/stop.png",
    "refresh":      "assets/icons/refresh.png",
    "settings":     "assets/icons/settings.png",
    "status_ok":    "assets/icons/status_ok.png",
    "status_error": "assets/icons/status_error.png",
    "status_warn":  "assets/icons/status_warn.png",
}
