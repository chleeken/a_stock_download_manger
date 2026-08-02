"""
股票数据下载器 - GUI界面模块
基于tkinter，封装StockDownloaderCore提供的所有功能。
"""
import os
import sys
import time
import threading
import datetime
import json
import logging
import traceback

import tkinter as tk
from tkinter import ttk, messagebox

try:
    import pyperclip
except ImportError:
    pyperclip = None

from stock_downloader_core import StockDownloaderCore


class StockDownloaderGUI(StockDownloaderCore):
    """带GUI的股票下载器"""

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("股票数据下载器 v18.01 - 优化版")
        self.root.geometry("1200x850")
        self.root.option_add('*Font', ('宋体', 10))

        # 获取程序目录和工作目录
        program_dir = self.get_program_dir()
        work_dir = self.get_work_dir()

        # 数据目录：程序同级目录 stock_data
        parent_dir = os.path.dirname(program_dir)
        data_dir = os.path.join(parent_dir, 'stock_data')
        os.makedirs(data_dir, exist_ok=True)

        # 数据库路径（全部在程序目录下）
        db_paths = {
            'day': os.path.join(data_dir, "stock_data.duckdb"),
            'week': os.path.join(data_dir, "weekly_stock_data.duckdb"),
            'month': os.path.join(data_dir, "monthly_stock_data.duckdb"),
            'season': os.path.join(data_dir, "seasonly_data.duckdb"),
            'year': os.path.join(data_dir, "yearly_data.duckdb"),
            '60min': os.path.join(data_dir, "minute60_stock_data.duckdb"),
            '30min': os.path.join(data_dir, "minute30_stock_data.duckdb"),
            '15min': os.path.join(data_dir, "minute15_stock_data.duckdb"),
            '5min': os.path.join(data_dir, "minute5_stock_data.duckdb"),
        }

        # 文件路径（全部在程序目录下）
        file_paths = {
            'table_txt': os.path.join(data_dir, "table.txt"),
            'failed_txt': os.path.join(data_dir, "failed_stocks.txt"),
            'config': os.path.join(data_dir, "stock_get_date_config.json"),
            'downloaded_record': os.path.join(data_dir, "downloaded_stocks.json"),
            'info_db': os.path.join(data_dir, "Table.csv"),
            'zixuan_csv': os.path.join(data_dir, "zixuan.csv"),
            'zixuangu_csv': os.path.join(data_dir, "zixuangu.csv"),
        }

        # 周期配置
        period_config = {
            'day': {
                'table': 'dayly_stock_data', 'limit': None, 'batch_size': 5000,
                'timeout': 90, 'retry_count': 5, 'request_delay': 0.2, 'enable_wal': True
            },
            'week': {
                'table': 'week_stock_data', 'limit': None, 'batch_size': 500,
                'timeout': 90, 'retry_count': 5, 'request_delay': 0.3, 'enable_wal': True
            },
            'month': {
                'table': 'month_stock_data', 'limit': None, 'batch_size': 500,
                'timeout': 90, 'retry_count': 5, 'request_delay': 0.4, 'enable_wal': True
            },
            'season': {
                'table': 'season_stock_data', 'limit': None, 'batch_size': 500,
                'timeout': 90, 'retry_count': 5, 'request_delay': 0.4, 'enable_wal': True
            },
            'year': {
                'table': 'year_stock_data', 'limit': None, 'batch_size': 500,
                'timeout': 90, 'retry_count': 5, 'request_delay': 0.4, 'enable_wal': True
            },
            '60min': {
                'table': '60minute_stock_data', 'limit': 2000, 'batch_size': 300,
                'timeout': 120, 'retry_count': 5, 'request_delay': 0.5, 'enable_wal': True
            },
            '30min': {
                'table': '30minute_stock_data', 'limit': 1000, 'batch_size': 300,
                'timeout': 120, 'retry_count': 5, 'request_delay': 0.5, 'enable_wal': True
            },
            '15min': {
                'table': '15minute_stock_data', 'limit': 1000, 'batch_size': 300,
                'timeout': 120, 'retry_count': 5, 'request_delay': 0.5, 'enable_wal': True
            },
            '5min': {
                'table': '5minute_stock_data', 'limit': 1000, 'batch_size': 300,
                'timeout': 120, 'retry_count': 5, 'request_delay': 0.5, 'enable_wal': True
            },
        }

        # 初始化核心类
        super().__init__(data_dir=data_dir, db_paths=db_paths, file_paths=file_paths,
                         period_config=period_config, callbacks=self._build_callbacks())

        # 初始化GUI状态变量
        self.is_running = False
        self.is_paused = False
        self.is_saving = False
        self.current_stock_var = tk.StringVar(value="等待处理...")
        self.progress_var = tk.DoubleVar()
        self.total_stocks = 0
        self.processed_stocks = 0
        self.start_time = 0
        self.last_login_time = 0
        self.download_btn = None
        self.pause_btn = None
        self.speed_label = None
        self.log_text = None
        self.status_label = None

        # 周期选项
        self.period_vars = {
            'day': tk.BooleanVar(value=True),
            'week': tk.BooleanVar(value=False),
            'month': tk.BooleanVar(value=False),
            'season': tk.BooleanVar(value=False),
            'year': tk.BooleanVar(value=False),
            '60min': tk.BooleanVar(value=False),
            '30min': tk.BooleanVar(value=False),
            '15min': tk.BooleanVar(value=False),
            '5min': tk.BooleanVar(value=False),
        }

        # 选项变量
        self.use_skip_mode = tk.BooleanVar(value=False)
        self.thread_count_var = tk.IntVar(value=1)
        self.batch_size_var = tk.IntVar(value=1000)
        self.auto_save_interval = tk.IntVar(value=3)
        self.auto_save_stock_count = tk.IntVar(value=100)
        self.save_batch_count = tk.IntVar(value=10)
        self.query_sample_count = tk.IntVar(value=1)
        self.enable_data_validation = tk.BooleanVar(value=True)
        self.max_retries_var = tk.IntVar(value=3)

        # 创建UI
        self.setup_ui()

        # 加载配置
        self._load_config()

        # 从本地数据库加载股票上市日期
        self.load_ipo_dates_from_info_db()

        # 显示数据库统计信息
        self.show_db_stats()

        # 预加载所有股票的最新日期
        self.preload_stock_max_dates()

        # 启动自动保存定时器
        self.start_auto_save_timer()

        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

        self._log(f"程序目录: {program_dir}")
        self._log(f"工作目录: {work_dir}")
        self._log(f"数据库目录: {self.data_dir}")

    def _build_callbacks(self):
        """构建回调字典"""
        return {
            'log': lambda msg, level="INFO": self.root.after(0, lambda: self._gui_log(msg, level)),
            'update_progress': lambda val: self.root.after(0, lambda: self.progress_var.set(val)),
            'update_speed': lambda text: self.root.after(0, lambda: self._set_speed_label(text)),
            'update_current_stock': lambda text: self.root.after(0, lambda: self.current_stock_var.set(text)),
            'show_message': lambda title, message, delay=1000: self.root.after(0, lambda: self._show_popup(title, message, delay)),
            'show_error': lambda title, message: self.root.after(0, lambda: messagebox.showerror(title, message)),
            'do_auto_save': lambda: self.root.after(0, lambda: self.do_auto_save()),
        }

    def _gui_log(self, message: str, level: str = "INFO"):
        """GUI日志输出"""
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        formatted_msg = f"[{timestamp}] {message}\n"
        if hasattr(self, 'log_text') and self.log_text:
            self.log_text.insert(tk.END, formatted_msg)
            self.log_text.see(tk.END)
            self.log_text.update_idletasks()
        if hasattr(self, 'status_label') and self.status_label:
            self.status_label.config(text=message[:50])

    def _set_speed_label(self, text: str):
        if hasattr(self, 'speed_label') and self.speed_label:
            self.speed_label.config(text=text)

    def _show_popup(self, title: str, message: str, delay: int = 1000):
        popup = tk.Toplevel(self.root)
        popup.title(title)
        popup.geometry("300x100")
        label = ttk.Label(popup, text=message)
        label.pack(pady=20)

        def close_popup():
            popup.destroy()
        popup.after(delay, close_popup)
        popup.transient(self.root)
        popup.grab_set()
        popup.focus_set()

    # ==================== UI设置 ====================

    def setup_ui(self):
        """设置用户界面"""
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 第一行：下载选项
        toolbar1 = ttk.LabelFrame(main_frame, text="下载选项", padding="5")
        toolbar1.grid(row=0, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=(0, 10))

        row1 = ttk.Frame(toolbar1)
        row1.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=5, pady=2)
        row2 = ttk.Frame(toolbar1)
        row2.grid(row=1, column=0, sticky=(tk.W, tk.E), padx=5, pady=2)
        row3 = ttk.Frame(toolbar1)
        row3.grid(row=2, column=0, sticky=(tk.W, tk.E), padx=5, pady=2)
        row4 = ttk.Frame(toolbar1)
        row4.grid(row=3, column=0, sticky=(tk.W, tk.E), padx=5, pady=2)
        row5 = ttk.Frame(toolbar1)
        row5.grid(row=4, column=0, sticky=(tk.W, tk.E), padx=5, pady=2)
        row6 = ttk.Frame(toolbar1)
        row6.grid(row=5, column=0, sticky=(tk.W, tk.E), padx=5, pady=2)

        periods_basic = [
            ('日线', 'day'),
            ('周线', 'week'),
            ('月线', 'month')
        ]

        for i, (label, key) in enumerate(periods_basic):
            cb = ttk.Checkbutton(row1, text=label, variable=self.period_vars[key])
            cb.grid(row=0, column=i, padx=5)

        periods_long = [
            ('季线', 'season'),
            ('年线', 'year')
        ]

        ttk.Label(row4, text="长期数据:").grid(row=0, column=0, padx=(0, 5))
        for i, (label, key) in enumerate(periods_long):
            cb = ttk.Checkbutton(row4, text=label, variable=self.period_vars[key])
            cb.grid(row=0, column=i+1, padx=5)

        minute_periods = [
            ('60分钟', '60min'),
            ('30分钟', '30min'),
            ('15分钟', '15min'),
            ('5分钟', '5min')
        ]

        ttk.Label(row2, text="分钟数据:").grid(row=0, column=0, padx=(0, 5))
        for i, (label, key) in enumerate(minute_periods):
            cb = ttk.Checkbutton(row2, text=label, variable=self.period_vars[key])
            cb.grid(row=0, column=i+1, padx=5)

        info_label = ttk.Label(
            row3,
            text="单线程模式 | 分批保存 | 自动保存 | 数据校验 | 重试机制",
            foreground="blue",
            font=('宋体', 9)
        )
        info_label.grid(row=0, column=0, padx=(5, 0), sticky=tk.W)
        row3.columnconfigure(0, weight=1)

        # 下载设置
        settings_frame = ttk.LabelFrame(row5, text="下载设置", padding="5")
        settings_frame.grid(row=0, column=0, padx=5, sticky=tk.W)

        ttk.Checkbutton(settings_frame, text="启用智能跳过",
                        variable=self.use_skip_mode).grid(row=0, column=0, columnspan=2, padx=5, pady=2, sticky=tk.W)

        ttk.Checkbutton(settings_frame, text="启用数据校验",
                        variable=self.enable_data_validation).grid(row=0, column=2, columnspan=2, padx=5, pady=2, sticky=tk.W)

        ttk.Label(settings_frame, text="线程数:").grid(row=1, column=0, padx=5, pady=2, sticky=tk.W)
        thread_spinbox = ttk.Spinbox(settings_frame, from_=1, to=20, textvariable=self.thread_count_var, width=5)
        thread_spinbox.grid(row=1, column=1, padx=5, pady=2, sticky=tk.W)

        ttk.Label(settings_frame, text="批量提交(行):").grid(row=1, column=2, padx=5, pady=2, sticky=tk.W)
        batch_spinbox = ttk.Spinbox(settings_frame, from_=100, to=10000, textvariable=self.batch_size_var, width=6)
        batch_spinbox.grid(row=1, column=3, padx=5, pady=2, sticky=tk.W)

        ttk.Label(settings_frame, text="最大重试次数:").grid(row=2, column=0, padx=5, pady=2, sticky=tk.W)
        retry_spinbox = ttk.Spinbox(settings_frame, from_=0, to=10, textvariable=self.max_retries_var, width=5)
        retry_spinbox.grid(row=2, column=1, padx=5, pady=2, sticky=tk.W)

        # 高级设置
        advanced_frame = ttk.LabelFrame(row6, text="高级设置", padding="5")
        advanced_frame.grid(row=0, column=0, padx=5, sticky=tk.W)

        ttk.Label(advanced_frame, text="自动保存(分钟):").grid(row=0, column=0, padx=5, pady=2, sticky=tk.W)
        save_spinbox = ttk.Spinbox(advanced_frame, from_=1, to=10, textvariable=self.auto_save_interval, width=5)
        save_spinbox.grid(row=0, column=1, padx=5, pady=2, sticky=tk.W)

        ttk.Label(advanced_frame, text="每X只保存:").grid(row=0, column=2, padx=5, pady=2, sticky=tk.W)
        stock_save_spinbox = ttk.Spinbox(advanced_frame, from_=10, to=500, textvariable=self.auto_save_stock_count, width=5)
        stock_save_spinbox.grid(row=0, column=3, padx=5, pady=2, sticky=tk.W)

        ttk.Label(advanced_frame, text="分批保存批数:").grid(row=1, column=0, padx=5, pady=2, sticky=tk.W)
        batch_save_spinbox = ttk.Spinbox(advanced_frame, from_=1, to=50, textvariable=self.save_batch_count, width=5)
        batch_save_spinbox.grid(row=1, column=1, padx=5, pady=2, sticky=tk.W)

        ttk.Label(advanced_frame, text="每批查询只数:").grid(row=1, column=2, padx=5, pady=2, sticky=tk.W)
        query_spinbox = ttk.Spinbox(advanced_frame, from_=1, to=10, textvariable=self.query_sample_count, width=5)
        query_spinbox.grid(row=1, column=3, padx=5, pady=2, sticky=tk.W)

        # 工具栏
        toolbar2 = ttk.Frame(main_frame)
        toolbar2.grid(row=1, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=(0, 10))

        ttk.Label(toolbar2, text="当前股票:").pack(side=tk.LEFT, padx=(0, 5))
        self.stock_label = ttk.Label(toolbar2, textvariable=self.current_stock_var, width=15)
        self.stock_label.pack(side=tk.LEFT, padx=(0, 20))

        self.download_btn = ttk.Button(toolbar2, text="下载📅", command=self.start_download)
        self.download_btn.pack(side=tk.LEFT, padx=5)

        self.pause_btn = ttk.Button(toolbar2, text="暂停", command=self.toggle_pause)
        self.pause_btn.pack(side=tk.LEFT, padx=5)

        self.retry_btn = ttk.Button(toolbar2, text="🔄 重跑失败", command=self.retry_failed)
        self.retry_btn.pack(side=tk.LEFT, padx=5)

        self.resume_btn = ttk.Button(toolbar2, text="⏯️ 断点续传", command=self.resume_download)
        self.resume_btn.pack(side=tk.LEFT, padx=5)

        self.save_btn = ttk.Button(toolbar2, text="💾 保存数据", command=self.manual_save_data)
        self.save_btn.pack(side=tk.LEFT, padx=5)

        self.update_stocks_btn = ttk.Button(toolbar2, text="更新股票列表", command=self.update_stock_list)
        self.update_stocks_btn.pack(side=tk.LEFT, padx=5)

        self.load_ipo_btn = ttk.Button(toolbar2, text="加载上市日期", command=self.load_ipo_dates_from_info_db)
        self.load_ipo_btn.pack(side=tk.LEFT, padx=5)

        self.view_ipo_btn = ttk.Button(toolbar2, text="查看上市日期", command=self.view_ipo_dates)
        self.view_ipo_btn.pack(side=tk.LEFT, padx=5)

        self.logout_btn = ttk.Button(toolbar2, text="登出", command=self.logout)
        self.logout_btn.pack(side=tk.LEFT, padx=5)

        # 进度条
        self.progress_bar = ttk.Progressbar(main_frame, variable=self.progress_var, maximum=100)
        self.progress_bar.grid(row=2, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=(0, 10))

        # 速度标签
        self.speed_label = ttk.Label(main_frame, text="速度: 0 只/秒 | 已处理: 0/0 | 缓存: 0行")
        self.speed_label.grid(row=3, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=(0, 5))

        # 日志框架
        log_frame = ttk.LabelFrame(main_frame, text="处理详情", padding="5")
        log_frame.grid(row=4, column=0, columnspan=4, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))

        log_toolbar = ttk.Frame(log_frame)
        log_toolbar.pack(side=tk.TOP, fill=tk.X, pady=(0, 5))

        self.copy_log_btn = ttk.Button(log_toolbar, text="📋 复制日志", command=self.copy_log_to_clipboard)
        self.copy_log_btn.pack(side=tk.LEFT, padx=5)

        self.clear_log_btn = ttk.Button(log_toolbar, text="清空日志", command=self.clear_log)
        self.clear_log_btn.pack(side=tk.LEFT, padx=5)

        text_frame = ttk.Frame(log_frame)
        text_frame.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=True)

        v_scrollbar = ttk.Scrollbar(text_frame)
        v_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        h_scrollbar = ttk.Scrollbar(text_frame, orient=tk.HORIZONTAL)
        h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)

        self.log_text = tk.Text(text_frame, height=20, width=100,
                                yscrollcommand=v_scrollbar.set,
                                xscrollcommand=h_scrollbar.set,
                                wrap=tk.NONE)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        v_scrollbar.config(command=self.log_text.yview)
        h_scrollbar.config(command=self.log_text.xview)

        # 状态栏
        status_frame = ttk.Frame(main_frame)
        status_frame.grid(row=5, column=0, columnspan=4, sticky=(tk.W, tk.E))

        self.status_label = ttk.Label(status_frame, text="就绪")
        self.status_label.pack(side=tk.LEFT)

        # 配置信息
        config_frame = ttk.LabelFrame(main_frame, text="当前配置", padding="5")
        config_frame.grid(row=6, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=(5, 0))

        config_text = f"日线数据库: {os.path.basename(self.db_paths['day'])} | 表名: dayly_stock_data"
        ttk.Label(config_frame, text=config_text, font=('宋体', 8)).pack(anchor=tk.W)

        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(4, weight=1)

    # ==================== 下载控制 ====================

    def start_download(self):
        """开始下载"""
        if self.is_running:
            return

        self._save_config()

        # 收集已选周期
        selected_periods = [p for p, var in self.period_vars.items() if var.get()]

        if not selected_periods:
            messagebox.showwarning("警告", "请至少选择一个下载周期")
            return

        if not os.path.exists(self.file_paths.get('table_txt', '')):
            messagebox.showwarning("警告", f"股票列表不存在: {self.file_paths.get('table_txt', '')}\n请先点击「更新股票列表」")
            return

        self.save_interval = self.auto_save_interval.get() * 60

        self.is_running = True
        self.is_paused = False
        self.failed_stocks = []
        self.processed_stocks = 0
        self.total_stocks = 0
        self.request_count = 0
        self.skip_count = 0
        self.stock_process_count = 0
        self.processed_codes.clear()
        self.missing_stocks.clear()
        self.stock_retry_count.clear()
        self.progress_var.set(0)
        self.speed_label.config(text="速度: 0 只/秒 | 已处理: 0/0 | 缓存: 0行")
        self.log_text.delete(1.0, tk.END)

        download_thread = threading.Thread(target=self.download_worker_single_thread, args=(selected_periods,), daemon=True)
        download_thread.start()

        self.download_btn.config(state=tk.DISABLED)

    def on_closing(self):
        """关闭窗口时的清理工作"""
        if self.is_running or self.is_saving:
            if messagebox.askyesno("确认", "任务正在进行中，确定要退出吗？\n数据将自动保存。"):
                self.is_running = False
                self.is_paused = False

                timeout = 30
                while self.is_saving and timeout > 0:
                    time.sleep(1)
                    timeout -= 1

                with self.temp_table_lock:
                    if self.temp_table_created and self.downloaded_rows > 0:
                        self._log("程序退出，保存临时数据...", "INFO")
                        try:
                            self.flush_buffer()
                            self.merge_temp_to_main(checkpoint=True, batch_save=True)
                        except Exception as e:
                            self._log(f"保存数据失败: {e}", "ERROR")

                self.close_db_connection(checkpoint=True)
            else:
                return
        else:
            with self.temp_table_lock:
                if self.temp_table_created and self.downloaded_rows > 0:
                    if messagebox.askyesno("确认", "有未合并的临时数据，是否保存？"):
                        try:
                            self.flush_buffer()
                            self.merge_temp_to_main(checkpoint=True, batch_save=True)
                        except Exception as e:
                            self._log(f"保存数据失败: {e}", "ERROR")

            self.close_db_connection(checkpoint=True)

        if self.bs_logged_in:
            try:
                import baostock as bs
                bs.logout()
            except Exception:
                pass

        self._save_config()
        self.root.destroy()

    # ==================== 辅助方法 ====================

    def _load_config(self):
        """加载配置"""
        config = self.load_config()
        if config:
            try:
                for key, var in self.period_vars.items():
                    if key in config:
                        var.set(config[key])
                if 'use_skip_mode' in config:
                    self.use_skip_mode.set(config['use_skip_mode'])
                if 'thread_count' in config:
                    self.thread_count_var.set(config['thread_count'])
                if 'batch_size' in config:
                    self.batch_size_var.set(config['batch_size'])
                if 'auto_save_interval' in config:
                    self.auto_save_interval.set(config['auto_save_interval'])
                if 'auto_save_stock_count' in config:
                    self.auto_save_stock_count.set(config['auto_save_stock_count'])
                if 'save_batch_count' in config:
                    self.save_batch_count.set(config['save_batch_count'])
                if 'query_sample_count' in config:
                    self.query_sample_count.set(config['query_sample_count'])
                if 'enable_data_validation' in config:
                    self.enable_data_validation.set(config['enable_data_validation'])
                if 'max_retries' in config:
                    self.max_retries_var.set(config['max_retries'])
                self._log("配置文件加载成功")
            except Exception as e:
                self._log(f"加载配置文件失败: {e}", "ERROR")

    def _save_config(self):
        """保存配置"""
        config = {key: var.get() for key, var in self.period_vars.items()}
        config['use_skip_mode'] = self.use_skip_mode.get()
        config['thread_count'] = self.thread_count_var.get()
        config['batch_size'] = self.batch_size_var.get()
        config['auto_save_interval'] = self.auto_save_interval.get()
        config['auto_save_stock_count'] = self.auto_save_stock_count.get()
        config['save_batch_count'] = self.save_batch_count.get()
        config['query_sample_count'] = self.query_sample_count.get()
        config['enable_data_validation'] = self.enable_data_validation.get()
        config['max_retries'] = self.max_retries_var.get()
        self.save_config(config)

    def copy_log_to_clipboard(self):
        try:
            log_content = self.log_text.get(1.0, tk.END).strip()
            if not log_content:
                messagebox.showinfo("提示", "日志为空")
                return
            if pyperclip:
                pyperclip.copy(log_content)
            self._log("日志已复制到剪贴板", "INFO")
        except Exception as e:
            self._log(f"复制日志失败: {e}", "ERROR")

    def clear_log(self):
        if messagebox.askyesno("确认", "确定要清空日志吗？"):
            self.log_text.delete(1.0, tk.END)
            self._log("日志已清空", "INFO")

    def resume_download(self):
        if self.is_running:
            return
        self.start_download()

    def toggle_pause(self):
        if not self.is_running:
            return
        self.is_paused = not self.is_paused
        if self.is_paused:
            self.pause_btn.config(text="继续")
            self._log("下载已暂停")
        else:
            self.pause_btn.config(text="暂停")
            self._log("下载继续")

    def view_ipo_dates(self):
        try:
            if not self.stock_ipo_dates:
                messagebox.showinfo("提示", "上市日期数据为空")
                return
            view_window = tk.Toplevel(self.root)
            view_window.title("股票上市日期表")
            view_window.geometry("600x600")
            toolbar = ttk.Frame(view_window)
            toolbar.pack(side=tk.TOP, fill=tk.X, padx=10, pady=10)
            rows_label = ttk.Label(toolbar, text=f"记录数: {len(self.stock_ipo_dates)}")
            rows_label.pack(side=tk.LEFT, padx=5)

            def copy_info_content():
                try:
                    content = "股票代码\t上市日期\n"
                    for code, ipo_date in sorted(self.stock_ipo_dates.items()):
                        content += f"{code}\t{ipo_date}\n"
                    if pyperclip:
                        pyperclip.copy(content)
                    self._log("上市日期已复制到剪贴板", "INFO")
                except Exception as e:
                    self._log(f"复制失败: {e}", "ERROR")

            copy_btn = ttk.Button(toolbar, text="📋 复制所有", command=copy_info_content)
            copy_btn.pack(side=tk.RIGHT, padx=5)

            def refresh_ipo():
                self.load_ipo_dates_from_info_db()
                view_window.destroy()
                self.view_ipo_dates()

            refresh_btn = ttk.Button(toolbar, text="🔄 刷新", command=refresh_ipo)
            refresh_btn.pack(side=tk.RIGHT, padx=5)

            scroll_frame = ttk.Frame(view_window)
            scroll_frame.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=True, padx=10, pady=10)
            v_scrollbar = ttk.Scrollbar(scroll_frame)
            v_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            h_scrollbar = ttk.Scrollbar(scroll_frame, orient=tk.HORIZONTAL)
            h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
            tree = ttk.Treeview(scroll_frame, columns=('code', 'ipo_date'), show='headings',
                                yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
            tree.heading('code', text='股票代码')
            tree.heading('ipo_date', text='上市日期')
            tree.column('code', width=200)
            tree.column('ipo_date', width=200)
            for code, ipo_date in sorted(self.stock_ipo_dates.items()):
                tree.insert('', tk.END, values=(code, ipo_date))
            tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            v_scrollbar.config(command=tree.yview)
            h_scrollbar.config(command=tree.xview)
        except Exception as e:
            self._log(f"查看上市日期失败: {e}", "ERROR")

    def run(self):
        """运行GUI"""
        self.root.mainloop()


if __name__ == "__main__":
    try:
        app = StockDownloaderGUI()
        app.run()
    except Exception as e:
        print(f"程序启动失败: {e}")
        input("按回车键退出...")
