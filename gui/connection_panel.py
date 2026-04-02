# ============================================================
#  MT5_Level_Bot — gui/connection_panel.py
#  Version : 2.0.0
#
#  Секции (сверху вниз):
#    1. XAMPP — путь к xampp-control.exe + кнопка "Запустить"
#    2. База данных MySQL — хост/порт/БД/пользователь/пароль
#    3. Терминал MetaTrader 5 — путь к terminal64.exe
#    4. Торговый счёт MT5 — логин/пароль/сервер
#    5. Кнопка "Сохранить все настройки"
# ============================================================

import configparser
import os
import subprocess
import sys
import threading

import customtkinter as ctk

from gui.theme import (
    FONT_TITLE, FONT_LABEL, FONT_LABEL_SM, FONT_BUTTON,
    COLOR_OK, COLOR_ERROR, COLOR_WARN, COLOR_STOP,
    COLOR_TEXT_OK, COLOR_TEXT_ERR, COLOR_TEXT_WARN, COLOR_TEXT_STOP,
    PAD_X, PAD_Y, PAD_SM
)

_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "config", "config.ini"
)


class ConnectionPanel(ctk.CTkFrame):
    """Вкладка 'Настройки соединения'."""

    def __init__(self, parent, on_db_ready=None,
                 on_mt5_ready=None, **kwargs):
        super().__init__(parent, **kwargs)
        self._on_db_ready  = on_db_ready   # callback когда MySQL ✓
        self._on_mt5_ready = on_mt5_ready  # callback когда MT5 ✓
        self._build_ui()
        self._load_config()

    # ----------------------------------------------------------
    # UI
    # ----------------------------------------------------------

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)

        # ══ 1. XAMPP ══════════════════════════════════════════
        self._section(row=0, title="XAMPP — управление сервером")

        xampp_frame = ctk.CTkFrame(self, fg_color="transparent")
        xampp_frame.grid(row=1, column=0, sticky="ew",
                         padx=PAD_X, pady=(0, PAD_SM))
        xampp_frame.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(
            xampp_frame,
            text="Путь к xampp-control.exe:",
            font=FONT_LABEL
        ).grid(row=0, column=0, sticky="w", padx=(0, PAD_SM), pady=(0, 2))

        # Строка: поле пути + Обзор
        path_row = ctk.CTkFrame(xampp_frame, fg_color="transparent")
        path_row.grid(row=1, column=0, columnspan=3, sticky="ew")
        path_row.grid_columnconfigure(0, weight=1)

        self._xampp_path = ctk.CTkEntry(
            path_row, font=FONT_LABEL,
            placeholder_text=r"D:\XAMPP\xampp-control.exe"
        )
        self._xampp_path.grid(row=0, column=0, sticky="ew",
                               padx=(0, PAD_SM))

        ctk.CTkButton(
            path_row, text="Обзор...", width=90, font=FONT_LABEL_SM,
            command=self._browse_xampp
        ).grid(row=0, column=1, padx=(0, PAD_SM))

        # Кнопка "Запустить сервер" + статус
        launch_frame = ctk.CTkFrame(self, fg_color="transparent")
        launch_frame.grid(row=2, column=0, sticky="w",
                          padx=PAD_X, pady=(PAD_SM, PAD_Y))

        ctk.CTkButton(
            launch_frame,
            text="▶  Запустить XAMPP",
            font=FONT_LABEL, width=200,
            fg_color="#1A6B3A", hover_color="#27AE60",
            command=self._launch_xampp
        ).pack(side="left", padx=(0, PAD_SM))

        self._xampp_status = ctk.CTkLabel(
            launch_frame, text="—", font=FONT_LABEL_SM)
        self._xampp_status.pack(side="left")

        # Подсказка
        ctk.CTkLabel(
            self,
            text="  После запуска XAMPP нажми Start у Apache и MySQL, "
                 "затем проверь соединение ниже.",
            font=FONT_LABEL_SM,
            text_color=("gray50", "gray55")
        ).grid(row=3, column=0, sticky="w", padx=PAD_X, pady=(0, PAD_Y))

        # ══ 2. MySQL ══════════════════════════════════════════
        self._section(row=4, title="База данных MySQL")

        # Host + Port + DB
        db_top = ctk.CTkFrame(self, fg_color="transparent")
        db_top.grid(row=5, column=0, sticky="ew",
                    padx=PAD_X, pady=(0, PAD_SM))

        ctk.CTkLabel(db_top, text="Хост:",
                     font=FONT_LABEL).pack(side="left", padx=(0, PAD_SM))
        self._db_host = ctk.CTkEntry(
            db_top, font=FONT_LABEL,
            placeholder_text="localhost", width=160)
        self._db_host.pack(side="left", padx=(0, PAD_X))

        ctk.CTkLabel(db_top, text="Порт:",
                     font=FONT_LABEL).pack(side="left", padx=(0, PAD_SM))
        self._db_port = ctk.CTkEntry(
            db_top, font=FONT_LABEL, width=70,
            placeholder_text="3306")
        self._db_port.pack(side="left", padx=(0, PAD_X))

        ctk.CTkLabel(db_top, text="БД:",
                     font=FONT_LABEL).pack(side="left", padx=(0, PAD_SM))
        self._db_name = ctk.CTkEntry(
            db_top, font=FONT_LABEL, width=180,
            placeholder_text="mt5_level_engine")
        self._db_name.pack(side="left")

        # User + Password
        db_cred = ctk.CTkFrame(self, fg_color="transparent")
        db_cred.grid(row=6, column=0, sticky="ew",
                     padx=PAD_X, pady=(0, PAD_SM))

        ctk.CTkLabel(db_cred, text="Пользователь:",
                     font=FONT_LABEL).pack(side="left", padx=(0, PAD_SM))
        self._db_user = ctk.CTkEntry(
            db_cred, font=FONT_LABEL, width=140,
            placeholder_text="root")
        self._db_user.pack(side="left", padx=(0, PAD_X))

        ctk.CTkLabel(db_cred, text="Пароль:",
                     font=FONT_LABEL).pack(side="left", padx=(0, PAD_SM))
        self._db_password = ctk.CTkEntry(
            db_cred, font=FONT_LABEL, show="*", width=140)
        self._db_password.pack(side="left")

        # Проверка MySQL
        db_check_frame = ctk.CTkFrame(self, fg_color="transparent")
        db_check_frame.grid(row=7, column=0, sticky="w",
                             padx=PAD_X, pady=(0, PAD_Y))

        ctk.CTkButton(
            db_check_frame, text="Проверить соединение MySQL",
            font=FONT_LABEL, width=230,
            command=self._check_mysql
        ).pack(side="left", padx=(0, PAD_SM))

        self._db_conn_status = ctk.CTkLabel(
            db_check_frame, text="—", font=FONT_LABEL_SM)
        self._db_conn_status.pack(side="left")

        # ══ 3. MetaTrader 5 ═══════════════════════════════════
        self._section(row=8, title="Терминал MetaTrader 5")

        path_frame = ctk.CTkFrame(self, fg_color="transparent")
        path_frame.grid(row=9, column=0, sticky="ew",
                        padx=PAD_X, pady=(0, PAD_SM))
        path_frame.grid_columnconfigure(0, weight=1)

        self._mt5_path = ctk.CTkEntry(
            path_frame, font=FONT_LABEL,
            placeholder_text=r"C:\MetaTrader5_Copy_1\terminal64.exe"
        )
        self._mt5_path.grid(row=0, column=0, sticky="ew", padx=(0, PAD_SM))

        ctk.CTkButton(
            path_frame, text="Обзор...", width=90, font=FONT_LABEL_SM,
            command=self._browse_mt5
        ).grid(row=0, column=1)

        self._mt5_path_status = ctk.CTkLabel(
            self, text="", font=FONT_LABEL_SM)
        self._mt5_path_status.grid(row=10, column=0, sticky="w",
                                    padx=PAD_X, pady=(0, PAD_SM))

        mt5_btn_frame = ctk.CTkFrame(self, fg_color="transparent")
        mt5_btn_frame.grid(row=11, column=0, sticky="w",
                           padx=PAD_X, pady=(0, PAD_Y))

        self._launch_mt5_btn = ctk.CTkButton(
            mt5_btn_frame, text="▶  Запустить Терминал MT5",
            font=FONT_LABEL, width=220,
            fg_color="#1A4A6B", hover_color="#2A7AB5",
            command=self._launch_mt5
        )
        self._launch_mt5_btn.pack(side="left", padx=(0, PAD_SM))

        ctk.CTkButton(
            mt5_btn_frame, text="Проверить соединение MT5",
            font=FONT_LABEL, width=220,
            command=self._check_mt5
        ).pack(side="left", padx=(0, PAD_SM))

        self._mt5_conn_status = ctk.CTkLabel(
            mt5_btn_frame, text="—", font=FONT_LABEL_SM)
        self._mt5_conn_status.pack(side="left")

        # ══ 4. Торговый счёт MT5 ══════════════════════════════
        self._section(row=12, title="Торговый счёт MT5")

        acc_frame = ctk.CTkFrame(self, fg_color="transparent")
        acc_frame.grid(row=13, column=0, sticky="ew",
                       padx=PAD_X, pady=PAD_SM)
        acc_frame.grid_columnconfigure((1, 3, 5), weight=1)

        ctk.CTkLabel(acc_frame, text="Логин:",
                     font=FONT_LABEL).grid(row=0, column=0, padx=(0, PAD_SM))
        self._mt5_login = ctk.CTkEntry(
            acc_frame, font=FONT_LABEL, placeholder_text="12345678", width=120)
        self._mt5_login.grid(row=0, column=1, padx=(0, PAD_X))

        ctk.CTkLabel(acc_frame, text="Пароль:",
                     font=FONT_LABEL).grid(row=0, column=2, padx=(0, PAD_SM))
        self._mt5_password = ctk.CTkEntry(
            acc_frame, font=FONT_LABEL, show="*", width=140)
        self._mt5_password.grid(row=0, column=3, padx=(0, PAD_X))

        ctk.CTkLabel(acc_frame, text="Сервер:",
                     font=FONT_LABEL).grid(row=0, column=4, padx=(0, PAD_SM))
        self._mt5_server = ctk.CTkEntry(
            acc_frame, font=FONT_LABEL,
            placeholder_text="RoboForex-ECN", width=180)
        self._mt5_server.grid(row=0, column=5, padx=(0, PAD_X))

        # Сохранить аккаунт
        acc_save_frame = ctk.CTkFrame(self, fg_color="transparent")
        acc_save_frame.grid(row=13, column=0, sticky="e",
                            padx=PAD_X, pady=PAD_SM)

        self._acc_save_status = ctk.CTkLabel(
            acc_save_frame, text="", font=FONT_LABEL_SM)
        self._acc_save_status.pack(side="left", padx=(0, PAD_SM))

        ctk.CTkButton(
            acc_save_frame, text="💾 Сохранить счёт",
            font=FONT_LABEL, width=140,
            fg_color=COLOR_OK, hover_color="#27AE60",
            command=self._save_account
        ).pack(side="left")

        # ══ 5. Сохранить все ══════════════════════════════════
        save_frame = ctk.CTkFrame(self, fg_color="transparent")
        save_frame.grid(row=14, column=0, sticky="e",
                        padx=PAD_X, pady=PAD_Y)

        self._save_status = ctk.CTkLabel(
            save_frame, text="", font=FONT_LABEL_SM)
        self._save_status.pack(side="left", padx=(0, PAD_X))

        ctk.CTkButton(
            save_frame, text="💾  Сохранить все настройки",
            font=FONT_BUTTON, width=220,
            fg_color=COLOR_OK, hover_color="#27AE60",
            command=self._save_config
        ).pack(side="left")

    def _section(self, row: int, title: str):
        """Заголовок секции с разделителем."""
        frame = ctk.CTkFrame(self, fg_color="transparent")
        frame.grid(row=row, column=0, sticky="ew",
                   padx=PAD_X, pady=(PAD_Y, 0))
        frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(frame, text=title, font=FONT_TITLE).grid(
            row=0, column=0, padx=(0, PAD_SM))
        ctk.CTkFrame(frame, height=2,
                     fg_color=("gray70", "gray30")).grid(
            row=0, column=1, sticky="ew")

    # ----------------------------------------------------------
    # XAMPP
    # ----------------------------------------------------------

    def _browse_xampp(self):
        from tkinter import filedialog
        path = filedialog.askopenfilename(
            title="Выбери xampp-control.exe",
            filetypes=[("Executable", "*.exe"), ("All files", "*.*")]
        )
        if path:
            self._xampp_path.delete(0, "end")
            self._xampp_path.insert(0, path)
            self._save_xampp_path(path)

    def _launch_xampp(self):
        """Открыть панель управления XAMPP."""
        path = self._xampp_path.get().strip()
        if not path:
            self._xampp_status.configure(
                text="✗ Укажи путь к xampp-control.exe",
                text_color=COLOR_TEXT_ERR)
            return
        if not os.path.isfile(path):
            self._xampp_status.configure(
                text=f"✗ Файл не найден: {path}",
                text_color=COLOR_TEXT_ERR)
            return
        try:
            subprocess.Popen(
                [path],
                creationflags=(subprocess.CREATE_NO_WINDOW
                               if sys.platform == "win32" else 0)
            )
            self._xampp_status.configure(
                text="✓ XAMPP запущен — нажми Start у Apache и MySQL",
                text_color=COLOR_TEXT_OK)
            self._save_xampp_path(path)
        except Exception as e:
            self._xampp_status.configure(
                text=f"✗ {e}", text_color=COLOR_TEXT_ERR)

    def _save_xampp_path(self, path: str):
        """Сохранить путь к XAMPP в config.ini."""
        cfg = configparser.ConfigParser()
        cfg.read(_CONFIG_PATH, encoding="utf-8")
        if not cfg.has_section("XAMPP"):
            cfg.add_section("XAMPP")
        cfg.set("XAMPP", "xampp_path", path)
        try:
            with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
                cfg.write(f)
        except Exception:
            pass

    # ----------------------------------------------------------
    # Обзор MT5
    # ----------------------------------------------------------

    def _browse_mt5(self):
        from tkinter import filedialog
        path = filedialog.askopenfilename(
            title="Выбери terminal64.exe",
            filetypes=[("Executable", "*.exe"), ("All files", "*.*")]
        )
        if path:
            self._mt5_path.delete(0, "end")
            self._mt5_path.insert(0, path)
            self._validate_mt5_path(path)

    def _validate_mt5_path(self, path: str):
        if os.path.isfile(path) and path.endswith(".exe"):
            self._mt5_path_status.configure(
                text="✓ Файл найден", text_color=COLOR_TEXT_OK)
        else:
            self._mt5_path_status.configure(
                text="✗ Файл не найден", text_color=COLOR_TEXT_ERR)

    # ----------------------------------------------------------
    # Проверки соединений
    # ----------------------------------------------------------

    def _launch_mt5(self):
        """Запуск terminal64.exe — ТОЛЬКО по команде пользователя."""
        path = self._mt5_path.get().strip()
        if not path:
            self._mt5_conn_status.configure(
                text="✗ Укажи путь к terminal64.exe",
                text_color=COLOR_TEXT_ERR)
            return
        if not os.path.isfile(path):
            self._mt5_conn_status.configure(
                text=f"✗ Файл не найден: {path}",
                text_color=COLOR_TEXT_ERR)
            return
        # Проверяем не запущен ли уже
        if self._is_mt5_running():
            self._mt5_conn_status.configure(
                text="ℹ Терминал уже запущен — нажми 'Проверить'",
                text_color=COLOR_TEXT_WARN)
            return
        try:
            subprocess.Popen(
                [path],
                creationflags=(subprocess.CREATE_NO_WINDOW
                               if sys.platform == "win32" else 0)
            )
            self._mt5_conn_status.configure(
                text="✓ Терминал запущен — подожди 5-10 сек, затем 'Проверить'",
                text_color=COLOR_TEXT_OK)
        except Exception as e:
            self._mt5_conn_status.configure(
                text=f"✗ {e}", text_color=COLOR_TEXT_ERR)

    def _is_mt5_running(self) -> bool:
        """Проверяет запущен ли terminal64.exe через tasklist."""
        try:
            if sys.platform == "win32":
                result = subprocess.run(
                    ["tasklist", "/FI", "IMAGENAME eq terminal64.exe"],
                    capture_output=True, text=True, timeout=5)
                return "terminal64.exe" in result.stdout
        except Exception:
            pass
        return False

    def _check_mt5(self):
        self._mt5_conn_status.configure(
            text="Проверка...", text_color=COLOR_TEXT_WARN)
        path = self._mt5_path.get().strip()
        if path:
            self._update_config_mt5_path(path)
        threading.Thread(target=self._do_check_mt5, daemon=True).start()

    def _do_check_mt5(self):
        # Используем bridge.ping() — безопасный таймаут, без зависания
        from core.mt5_bridge import get_mt5_bridge
        bridge = get_mt5_bridge()
        try:
            ok = bridge.ping()
            if ok:
                # Получаем данные аккаунта через session
                try:
                    with bridge.session() as mt5:
                        info = bridge.get_account_info(mt5)
                    if info:
                        text = (f"✓ #{info['login']}  |  "
                                f"{info['server']}  |  "
                                f"${info['balance']:.2f}")
                    else:
                        text = "✓ MT5 подключён"
                except Exception:
                    text = "✓ MT5 подключён"
                color = COLOR_TEXT_OK
            else:
                text  = "✗ MT5 не отвечает — проверь терминал"
                color = COLOR_TEXT_ERR
        except Exception as e:
            text, color = f"✗ {e}", COLOR_TEXT_ERR

        self.after(0, lambda: self._mt5_conn_status.configure(
            text=text, text_color=color))
        if color == COLOR_TEXT_OK and self._on_mt5_ready:
            self.after(0, self._on_mt5_ready)

    def _check_mysql(self):
        self._db_conn_status.configure(
            text="Проверка...", text_color=COLOR_TEXT_WARN)
        self._save_config(silent=True)
        threading.Thread(target=self._do_check_mysql, daemon=True).start()

    def _do_check_mysql(self):
        try:
            from core.db_connection import get_db, DBConnection
            DBConnection._instance._initialized = False
            db = get_db()
            db.init()
            ok = db.ping()
        except Exception:
            ok = False
        color = COLOR_TEXT_OK if ok else COLOR_TEXT_ERR
        text  = "✓ MySQL подключён" if ok else \
                "✗ MySQL недоступен — запусти XAMPP"
        self.after(0, lambda: self._db_conn_status.configure(
            text=text, text_color=color))
        # Уведомляем app.py
        if ok and self._on_db_ready:
            self.after(0, self._on_db_ready)

    # ----------------------------------------------------------
    # Config load / save
    # ----------------------------------------------------------

    def _load_config(self):
        cfg = configparser.ConfigParser()
        cfg.read(_CONFIG_PATH, encoding="utf-8")

        def s(section, key, fallback=""):
            return cfg.get(section, key, fallback=fallback)

        # XAMPP
        xampp_path = s("XAMPP", "xampp_path",
                       r"D:\XAMPP\xampp-control.exe")
        self._xampp_path.insert(0, xampp_path)

        # MT5
        self._mt5_path.insert(0, s("MT5", "terminal_path"))
        self._mt5_login.insert(0, s("MT5", "login", "0"))
        self._mt5_password.insert(0, s("MT5", "password"))
        self._mt5_server.insert(0, s("MT5", "server"))

        path = s("MT5", "terminal_path")
        if path:
            self._validate_mt5_path(path)
            # Шаг 3 ТЗ: автопроверка запущен ли терминал
            if self._is_mt5_running():
                self._mt5_conn_status.configure(
                    text="ℹ Терминал уже запущен — нажми 'Проверить соединение MT5'",
                    text_color=COLOR_TEXT_WARN)
                self._launch_mt5_btn.configure(state="disabled")
            else:
                self._launch_mt5_btn.configure(state="normal")

        # DB
        self._db_host.insert(0, s("DATABASE", "host", "localhost"))
        self._db_port.insert(0, s("DATABASE", "port", "3306"))
        self._db_name.insert(0, s("DATABASE", "db_name", "mt5_level_engine"))
        self._db_user.insert(0, s("DATABASE", "user", "root"))
        self._db_password.insert(0, s("DATABASE", "password"))

    def _save_config(self, silent: bool = False):
        cfg = configparser.ConfigParser()
        cfg.read(_CONFIG_PATH, encoding="utf-8")

        # XAMPP
        if not cfg.has_section("XAMPP"):
            cfg.add_section("XAMPP")
        cfg.set("XAMPP", "xampp_path", self._xampp_path.get().strip())

        # MT5
        if not cfg.has_section("MT5"):
            cfg.add_section("MT5")
        cfg.set("MT5", "terminal_path", self._mt5_path.get().strip())
        cfg.set("MT5", "login",         self._mt5_login.get().strip())
        cfg.set("MT5", "password",      self._mt5_password.get().strip())
        cfg.set("MT5", "server",        self._mt5_server.get().strip())

        # DATABASE
        if not cfg.has_section("DATABASE"):
            cfg.add_section("DATABASE")
        cfg.set("DATABASE", "host",     self._db_host.get().strip())
        cfg.set("DATABASE", "port",     self._db_port.get().strip())
        cfg.set("DATABASE", "db_name",  self._db_name.get().strip())
        cfg.set("DATABASE", "user",     self._db_user.get().strip())
        cfg.set("DATABASE", "password", self._db_password.get().strip())

        try:
            with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
                cfg.write(f)
            if not silent:
                self._save_status.configure(
                    text="✓ Сохранено", text_color=COLOR_TEXT_OK)
                self.after(3000, lambda: self._save_status.configure(text=""))
        except Exception as e:
            if not silent:
                self._save_status.configure(
                    text=f"✗ {e}", text_color=COLOR_TEXT_ERR)

    def _save_account(self):
        cfg = configparser.ConfigParser()
        cfg.read(_CONFIG_PATH, encoding="utf-8")
        if not cfg.has_section("MT5"):
            cfg.add_section("MT5")
        cfg.set("MT5", "login",    self._mt5_login.get().strip())
        cfg.set("MT5", "password", self._mt5_password.get().strip())
        cfg.set("MT5", "server",   self._mt5_server.get().strip())
        try:
            with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
                cfg.write(f)
            self._acc_save_status.configure(
                text="✓ Сохранено", text_color=COLOR_TEXT_OK)
            self.after(3000,
                       lambda: self._acc_save_status.configure(text=""))
        except Exception as e:
            self._acc_save_status.configure(
                text=f"✗ {e}", text_color=COLOR_TEXT_ERR)

    def _update_config_mt5_path(self, path: str):
        cfg = configparser.ConfigParser()
        cfg.read(_CONFIG_PATH, encoding="utf-8")
        if not cfg.has_section("MT5"):
            cfg.add_section("MT5")
        cfg.set("MT5", "terminal_path", path)
        with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
            cfg.write(f)
