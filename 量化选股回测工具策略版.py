"""
量化回测选股工具 v3.2 - 真正执行版
修复选股不执行的问题 + 添加清空按钮 + 左栏宽度减少25%
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import duckdb
import polars as pl
import matplotlib
matplotlib.use('TkAgg')
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from datetime import datetime
import os
import json
import traceback
import sys
import threading
import queue
import time
from pathlib import Path
import pyperclip
import psutil

# 设置默认编码
import locale
locale.setlocale(locale.LC_ALL, '')
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

class AutoCloseMessageBox:
    """自动关闭的消息框"""
    @staticmethod
    def showinfo(title, message, timeout=1000):
        msg = tk.Toplevel()
        msg.title(title)
        msg.configure(bg='#2b2b2b')
        msg.geometry('300x120')
        msg.resizable(False, False)
        
        msg.update_idletasks()
        x = (msg.winfo_screenwidth() // 2) - (msg.winfo_width() // 2)
        y = (msg.winfo_screenheight() // 2) - (msg.winfo_height() // 2)
        msg.geometry(f'+{x}+{y}')
        
        tk.Label(msg, text=message, bg='#2b2b2b', fg='#ffffff', 
                font=('微软雅黑', 10), wraplength=250).pack(expand=True, padx=10, pady=10)
        
        msg.after(timeout, msg.destroy)
        msg.grab_set()
        msg.focus_set()
    
    @staticmethod
    def showerror(title, message):
        messagebox.showerror(title, message)
    
    @staticmethod
    def showwarning(title, message, timeout=2000):
        AutoCloseMessageBox.showinfo(title, message, timeout)

class QuantTool:
    def __init__(self, root):
        self.root = root
        self.root.title("量化回测选股工具 v3.2 - 真正执行版")
        
        # 获取屏幕尺寸并全屏
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()
        self.root.geometry(f"{screen_width}x{screen_height}")
        
        # 设置暗色调主题
        self.setup_dark_theme()
        
        # 基础路径配置
        self.base_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        self.stock_data_dir = self.base_dir / "stock_data"
        self.config_dir = self.stock_data_dir / "stock_pakeing"
        self.result_dir = self.stock_data_dir / "xuangu"
        self.strategy_dir = self.stock_data_dir / "celue"
        
        # 创建必要目录
        for dir_path in [self.stock_data_dir, self.config_dir, self.result_dir, self.strategy_dir]:
            dir_path.mkdir(exist_ok=True)
        
        # 数据库配置（修正表名）
        self.db_configs = {
            '日线': {'path': self.stock_data_dir / 'stock_data.duckdb', 'table': 'dayly_stock_data'},
            '周线': {'path': self.stock_data_dir / 'weekly_stock_data.duckdb', 'table': 'weekly_data'},
            '月线': {'path': self.stock_data_dir / 'monthly_stock_data.duckdb', 'table': 'monthly_data'},
            '60分': {'path': self.stock_data_dir / 'minute60_stock_data.duckdb', 'table': 'kdata_60min'},
            '30分': {'path': self.stock_data_dir / 'minute30_stock_data.duckdb', 'table': 'kdata_30min'},
            '15分': {'path': self.stock_data_dir / 'minute15_stock_data.duckdb', 'table': 'kdata_15min'},
            '5分': {'path': self.stock_data_dir / 'minute5_stock_data.duckdb', 'table': 'kdata_5min'},
        }
        
        # 当前选中的周期
        self.selected_periods = []
        
        # 选股速度模式
        self.speed_modes = {
            '低': {'batch_size': 50, 'delay': 0.2, 'threads': 1},
            '中': {'batch_size': 100, 'delay': 0.1, 'threads': 2},
            '高': {'batch_size': 200, 'delay': 0.05, 'threads': 3},
        }
        
        # 线程控制
        self.stop_selection = False
        self.is_selecting = False
        
        # 日志队列
        self.log_queue = queue.Queue()
        
        # 初始化变量
        self.config = {}
        self.db_connections = {}
        self.last_results = []
        self.strategy_list = []
        
        # 构建界面
        self.setup_ui()
        
        # 加载策略列表和配置
        self.root.after(100, self.lazy_init)
        
        # 启动日志处理
        self.process_log_queue()
    
    def lazy_init(self):
        """延迟初始化"""
        self.log("🔄 正在初始化...")
        thread = threading.Thread(target=self._lazy_init_thread, daemon=True)
        thread.start()
    
    def _lazy_init_thread(self):
        """后台初始化"""
        try:
            # 加载策略列表
            self.load_strategy_list()
            
            # 加载配置
            self.config = self.load_config()
            
            # 连接数据库
            self.auto_connect_databases()
            
            # 加载上次设置
            self.load_last_settings()
            
            # 自动加载第一个策略
            if self.strategy_list:
                self.root.after(0, lambda: self.load_strategy_by_name(self.strategy_list[0]))
            
            self.root.after(0, lambda: self.log("✅ 初始化完成"))
        except Exception as e:
            self.root.after(0, lambda: self.log(f"⚠️ 初始化部分失败: {e}"))
    
    def setup_dark_theme(self):
        """设置暗色调主题"""
        self.root.configure(bg='#2b2b2b')
        
        style = ttk.Style()
        style.theme_use('clam')
        
        style.configure('TLabel', background='#2b2b2b', foreground='#ffffff', font=('微软雅黑', 9))
        style.configure('TFrame', background='#2b2b2b')
        style.configure('TLabelFrame', background='#2b2b2b', foreground='#ffffff')
        style.configure('TButton', background='#3c3c3c', foreground='#ffffff', font=('微软雅黑', 9))
        style.map('TButton', background=[('active', '#4a4a4a')])
        style.configure('TCheckbutton', background='#2b2b2b', foreground='#ffffff', font=('微软雅黑', 9))
        style.configure('TEntry', fieldbackground='#3c3c3c', foreground='#ffffff')
        style.configure('TCombobox', fieldbackground='#3c3c3c', foreground='#ffffff')
    
    def setup_ui(self):
        """构建用户界面 - 左栏宽度减少25%"""
        
        # 主布局
        main_paned = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        main_paned.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 左侧面板（宽度减少25% - 原来是8，现在改成6，相当于减少25%）
        left_frame = ttk.Frame(main_paned)
        main_paned.add(left_frame, weight=6)
        
        # 右侧面板
        right_frame = ttk.Frame(main_paned)
        main_paned.add(right_frame, weight=14)  # 增加右侧权重
        
        # ===== 左侧面板内容 =====
        
        # 1. 数据库连接面板
        db_frame = ttk.LabelFrame(left_frame, text="数据库连接", padding=5)
        db_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # 周期复选框
        period_frame = ttk.Frame(db_frame)
        period_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(period_frame, text="选择周期:").pack(side=tk.LEFT, padx=5)
        
        self.period_vars = {}
        period_row = ttk.Frame(period_frame)
        period_row.pack(side=tk.LEFT, padx=10)
        
        periods = list(self.db_configs.keys())
        for i, period in enumerate(periods):
            var = tk.BooleanVar()
            self.period_vars[period] = var
            cb = ttk.Checkbutton(period_row, text=period, variable=var,
                                command=self.on_period_selected)
            cb.grid(row=i//4, column=i%4, padx=5, pady=2, sticky='w')
        
        # 数据库状态
        db_btn_frame = ttk.Frame(db_frame)
        db_btn_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(db_btn_frame, text="🔄 刷新连接", 
                  command=self.auto_connect_databases).pack(side=tk.LEFT, padx=5)
        
        self.db_status_var = tk.StringVar(value="初始化中...")
        ttk.Label(db_btn_frame, textvariable=self.db_status_var, 
                 foreground='#ff6b6b').pack(side=tk.RIGHT, padx=10)
        
        # 2. 性能控制面板
        speed_frame = ttk.LabelFrame(left_frame, text="选股速度", padding=5)
        speed_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(speed_frame, text="运行模式:").pack(side=tk.LEFT, padx=5)
        
        self.speed_var = tk.StringVar(value="低")
        speed_low = ttk.Radiobutton(speed_frame, text="低 (稳定)", 
                                    variable=self.speed_var, value="低")
        speed_low.pack(side=tk.LEFT, padx=5)
        
        speed_med = ttk.Radiobutton(speed_frame, text="中 (平衡)", 
                                    variable=self.speed_var, value="中")
        speed_med.pack(side=tk.LEFT, padx=5)
        
        speed_high = ttk.Radiobutton(speed_frame, text="高 (快速)", 
                                     variable=self.speed_var, value="高")
        speed_high.pack(side=tk.LEFT, padx=5)
        
        # 缓存控制
        self.cache_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(speed_frame, text="启用缓存", 
                       variable=self.cache_var).pack(side=tk.LEFT, padx=10)
        
        # 3. 策略编辑面板
        strategy_frame = ttk.LabelFrame(left_frame, text="策略代码", padding=5)
        strategy_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 策略工具栏
        strategy_toolbar = ttk.Frame(strategy_frame)
        strategy_toolbar.pack(fill=tk.X, pady=2)
        
        ttk.Button(strategy_toolbar, text="📋 粘贴", 
                  command=self.paste_strategy).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(strategy_toolbar, text="💾 保存", 
                  command=self.save_strategy_dialog).pack(side=tk.LEFT, padx=2)
        
        # 策略下拉框
        self.strategy_var = tk.StringVar()
        self.strategy_combo = ttk.Combobox(strategy_toolbar, textvariable=self.strategy_var,
                                           values=self.strategy_list, width=18)
        self.strategy_combo.pack(side=tk.LEFT, padx=2)
        self.strategy_combo.bind('<<ComboboxSelected>>', self.on_strategy_selected)
        
        ttk.Button(strategy_toolbar, text="📚 示例", 
                  command=self.load_example_strategy).pack(side=tk.LEFT, padx=2)
        
        # 新增：清空按钮
        ttk.Button(strategy_toolbar, text="🗑️ 清空", 
                  command=self.clear_strategy_editor).pack(side=tk.LEFT, padx=2)
        
        # 策略输入框
        self.strategy_text = scrolledtext.ScrolledText(
            strategy_frame, height=15, bg='#1e1e1e', fg='#00ff00',
            font=('Consolas', 10), insertbackground='#ffffff',
            wrap=tk.NONE
        )
        self.strategy_text.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # 4. 操作按钮（底部）
        btn_frame = ttk.Frame(left_frame)
        btn_frame.pack(fill=tk.X, padx=5, pady=5, side=tk.BOTTOM)
        
        ttk.Button(btn_frame, text="💾 保存配置", 
                  command=self.save_config).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="📂 加载配置", 
                  command=self.load_config_dialog).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="⏹ 停止选股", 
                  command=self.stop_selection_process).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="▶ 回测", 
                  command=self.backtest_selection).pack(side=tk.RIGHT, padx=5)
        
        # ===== 右侧面板内容 =====
        
        # 结果标签页
        self.notebook = ttk.Notebook(right_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 选股结果页
        result_frame = ttk.Frame(self.notebook)
        self.notebook.add(result_frame, text="选股结果")
        
        # 结果表格
        self.select_columns = ('代码', '名称', '最新价', '涨跌幅', '成交量', '策略评分', '详情')
        self.backtest_columns = ('代码', '年化收益率', '总收益率', '胜率', '交易次数', '最大回撤')
        self.current_result_mode = 'select'
        self.result_tree = ttk.Treeview(result_frame, columns=self.select_columns, show='headings', height=20)
        
        for col in self.select_columns:
            self.result_tree.heading(col, text=col)
            self.result_tree.column(col, width=100, anchor='center')
        
        self.result_tree.column('代码', width=120)
        self.result_tree.column('名称', width=120)
        self.result_tree.column('详情', width=200)
        
        result_scrollbar = ttk.Scrollbar(result_frame, orient='vertical', command=self.result_tree.yview)
        self.result_tree.configure(yscrollcommand=result_scrollbar.set)
        
        self.result_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        result_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # 结果操作按钮
        result_btn_frame = ttk.Frame(result_frame)
        result_btn_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(result_btn_frame, text="📤 导出结果", 
                  command=self.export_results).pack(side=tk.LEFT, padx=5)
        ttk.Button(result_btn_frame, text="📈 回测选股", 
                  command=self.backtest_selection).pack(side=tk.LEFT, padx=5)
        ttk.Button(result_btn_frame, text="🧹 清空结果", 
                  command=self.clear_results).pack(side=tk.LEFT, padx=5)
        
        # 图表页
        chart_frame = ttk.Frame(self.notebook)
        self.notebook.add(chart_frame, text="分析图表")
        
        self.figure = Figure(figsize=(10, 8), dpi=80, facecolor='#2b2b2b')
        self.ax = self.figure.add_subplot(111)
        self.ax.set_facecolor('#1e1e1e')
        self.ax.tick_params(colors='#ffffff')
        
        self.canvas = FigureCanvasTkAgg(self.figure, chart_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # ===== 底部日志面板 =====
        log_frame = ttk.LabelFrame(self.root, text="处理日志", padding=5)
        log_frame.pack(fill=tk.BOTH, padx=5, pady=5)
        
        self.log_text = scrolledtext.ScrolledText(
            log_frame, height=8, bg='#1e1e1e', fg='#00ff00',
            font=('Consolas', 9), insertbackground='#ffffff',
            wrap=tk.WORD
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        log_btn_frame = ttk.Frame(log_frame)
        log_btn_frame.pack(fill=tk.X, pady=2)
        
        ttk.Button(log_btn_frame, text="📋 复制日志", 
                  command=self.copy_log).pack(side=tk.RIGHT, padx=2)
        ttk.Button(log_btn_frame, text="🧹 清空日志", 
                  command=self.clear_log).pack(side=tk.RIGHT, padx=2)
        
        self.progress = ttk.Progressbar(log_frame, mode='indeterminate')
        self.progress.pack(fill=tk.X, pady=2)
    
    def _auto_save_config(self):
        """自动保存配置到文件"""
        config = {
            'max_stocks': '50',
            'speed_mode': self.speed_var.get(),
            'last_periods': self.selected_periods
        }
        config_file = self.config_dir / "config.json"
        try:
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4)
        except:
            pass
    
    # ===== 新增：清空策略编辑器 =====
    def clear_strategy_editor(self):
        """清空策略编辑框内容，不影响保存的文件"""
        self.strategy_text.delete('1.0', tk.END)
        self.strategy_var.set('')  # 清空下拉框显示
        self.log("🗑️ 已清空策略编辑器")
    
    # ===== 策略相关方法 =====
    
    def load_strategy_list(self):
        """加载策略列表"""
        try:
            self.strategy_list = [f.stem for f in self.strategy_dir.glob("*.py")]
            self.strategy_list.sort()
            if hasattr(self, 'strategy_combo'):
                self.strategy_combo['values'] = self.strategy_list
        except Exception as e:
            self.log(f"⚠️ 加载策略列表失败: {e}")
    
    def load_strategy_by_name(self, name):
        """根据名称加载策略"""
        if not name:
            return
        
        file_path = self.strategy_dir / f"{name}.py"
        if file_path.exists():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    code = f.read()
                self.strategy_text.delete('1.0', tk.END)
                self.strategy_text.insert('1.0', code)
                self.strategy_var.set(name)
                self.log(f"📂 已加载策略: {name}")
            except Exception as e:
                self.log(f"⚠️ 加载策略失败: {e}")
    
    def on_strategy_selected(self, event=None):
        """策略下拉框选择事件"""
        name = self.strategy_var.get()
        self.load_strategy_by_name(name)
    
    def paste_strategy(self):
        """粘贴策略"""
        try:
            text = pyperclip.paste()
            if text:
                self.strategy_text.delete('1.0', tk.END)
                self.strategy_text.insert('1.0', text)
                self.log("📋 已粘贴策略代码")
        except Exception as e:
            messagebox.showerror("粘贴失败", str(e))
    
    def save_strategy_dialog(self):
        """保存策略对话框"""
        dialog = tk.Toplevel(self.root)
        dialog.title("保存策略")
        dialog.geometry("400x150")
        dialog.configure(bg='#2b2b2b')
        dialog.transient(self.root)
        dialog.grab_set()
        
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (dialog.winfo_width() // 2)
        y = (dialog.winfo_screenheight() // 2) - (dialog.winfo_height() // 2)
        dialog.geometry(f'+{x}+{y}')
        
        ttk.Label(dialog, text="策略名称:", font=('微软雅黑', 10)).pack(pady=10)
        
        name_var = tk.StringVar()
        name_entry = ttk.Entry(dialog, textvariable=name_var, width=40)
        name_entry.pack(pady=5)
        name_entry.focus()
        
        def do_save():
            name = name_var.get().strip()
            if not name:
                messagebox.showwarning("警告", "请输入策略名称")
                return
            
            code = self.strategy_text.get('1.0', tk.END).strip()
            if not code:
                messagebox.showwarning("警告", "策略代码不能为空")
                return
            
            file_path = self.strategy_dir / f"{name}.py"
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(code)
                
                self.load_strategy_list()
                self.strategy_var.set(name)
                
                AutoCloseMessageBox.showinfo("保存成功", f"策略已保存: {name}", 1000)
                dialog.destroy()
            except Exception as e:
                messagebox.showerror("保存失败", str(e))
        
        ttk.Button(dialog, text="确定", command=do_save).pack(pady=10)
    
    def load_example_strategy(self):
        """加载示例策略"""
        example_code = '''# ===== 宽松测试策略 =====
# 条件放得很宽，确保能选出股票测试程序

import polars as pl
import polars_ta
import numpy as np

def select_stock(df):
    """选股函数 - 宽松条件"""
    
    # 只要有点数据就行
    if df.height < 10:
        return False
    
    # 计算MACD
    df_with_ta = df.with_columns([
        polars_ta.trend.MACD(df['close']).alias('diff'),
        polars_ta.trend.MACDSignal(df['close']).alias('dea'),
    ])
    
    latest = df_with_ta.tail(1)
    if latest.height == 0:
        return False
    
    close = latest['close'].to_numpy()[0]
    diff = latest['diff'].to_numpy()[0]
    dea = latest['dea'].to_numpy()[0]
    
    # 放宽条件：只要股价>5元，MACD金叉即可
    if close > 5 and diff > dea:
        return True
    
    return False

def score_stock(df):
    """评分函数"""
    return 50
'''
        
        self.strategy_text.delete('1.0', tk.END)
        self.strategy_text.insert('1.0', example_code)
        self.log("📚 已加载宽松测试策略")
    
    # ===== 数据库相关方法 =====
    
    def auto_connect_databases(self):
        """自动连接数据库"""
        connected = 0
        self.db_connections = {}
        
        for period, config in self.db_configs.items():
            db_path = config['path']
            if db_path.exists():
                try:
                    conn = duckdb.connect(str(db_path), read_only=True)
                    table_name = config['table']
                    # 测试表是否存在
                    try:
                        conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()
                        self.db_connections[period] = {
                            'conn': conn,
                            'table': table_name
                        }
                        connected += 1
                        self.log(f"✅ 连接 {period} 成功")
                    except Exception as e:
                        conn.close()
                        self.log(f"⚠️ {period} 表不存在: {table_name}")
                except Exception as e:
                    self.log(f"❌ {period} 连接失败")
        
        self.db_status_var.set(f"已连接 {connected} 个")
        
        if connected > 0:
            AutoCloseMessageBox.showinfo("连接成功", f"已连接 {connected} 个数据库", 1500)
    
    def on_period_selected(self):
        """周期选择"""
        self.selected_periods = [
            period for period, var in self.period_vars.items() 
            if var.get() and period in self.db_connections
        ]
        if self.selected_periods:
            self.log(f"已选择周期: {', '.join(self.selected_periods)}")
        # 自动保存配置
        self._auto_save_config()
    
    # ===== 配置相关方法 =====
    
    def load_config(self):
        """加载配置"""
        config_file = self.config_dir / "config.json"
        default_config = {
            'max_stocks': '50',
            'speed_mode': '低',
            'last_periods': []
        }
        
        if config_file.exists():
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return default_config
        return default_config
    
    def save_config(self):
        """保存配置"""
        config = {
            'max_stocks': '50',
            'speed_mode': self.speed_var.get(),
            'last_periods': self.selected_periods
        }
        
        config_file = self.config_dir / "config.json"
        
        try:
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4)
            AutoCloseMessageBox.showinfo("保存成功", "配置已保存", 1000)
        except Exception as e:
            messagebox.showerror("保存失败", str(e))
    
    def load_config_dialog(self):
        """加载配置"""
        file_path = filedialog.askopenfilename(
            title="选择配置文件",
            initialdir=self.config_dir,
            filetypes=[("JSON文件", "*.json")]
        )
        
        if file_path:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                
                self.speed_var.set(config.get('speed_mode', '低'))
                
                last_periods = config.get('last_periods', [])
                for period, var in self.period_vars.items():
                    var.set(period in last_periods)
                
                self.on_period_selected()
                AutoCloseMessageBox.showinfo("加载成功", "配置已加载", 1500)
            except Exception as e:
                messagebox.showerror("加载失败", str(e))
    
    def load_last_settings(self):
        """加载上次设置"""
        if not self.config:
            return
        
        self.speed_var.set(self.config.get('speed_mode', '低'))
        
        last_periods = self.config.get('last_periods', [])
        for period, var in self.period_vars.items():
            var.set(period in last_periods)
        
        self.on_period_selected()
    
    # ===== 选股相关方法 =====
    
    def start_stock_selection(self):
        """开始选股"""
        if self.is_selecting:
            messagebox.showwarning("警告", "正在选股中，请勿重复启动")
            return
        
        if not self.selected_periods:
            messagebox.showwarning("警告", "请至少选择一个周期")
            return
        
        if not self.db_connections:
            messagebox.showwarning("警告", "没有可用的数据库连接")
            return
        
        strategy_code = self.strategy_text.get('1.0', tk.END).strip()
        if not strategy_code:
            messagebox.showwarning("警告", "请输入策略代码")
            return
        
        # 设置状态
        self.is_selecting = True
        self.stop_selection = False
        
        # 清空之前的结果
        self.clear_results()
        
        # 启动进度条
        self.progress.start()
        
        # 获取速度模式
        speed_mode = self.speed_var.get()
        mode_config = self.speed_modes.get(speed_mode, self.speed_modes['低'])
        
        self.log(f"🚀 开始选股 - 模式:{speed_mode} 批处理:{mode_config['batch_size']} 线程:{mode_config['threads']}")
        
        # 在后台线程执行
        thread = threading.Thread(target=self._run_selection, 
                                 args=(strategy_code, mode_config), 
                                 daemon=True)
        thread.start()
    
    def stop_selection_process(self):
        """停止选股"""
        self.stop_selection = True
        self.log("⏹ 正在停止选股...")
    
    def _run_selection(self, strategy_code, mode_config):
        """后台选股执行 - 修复版，确保真正执行"""
        try:
            # 动态编译策略
            strategy_globals = {}
            try:
                exec(strategy_code, strategy_globals)
                select_func = strategy_globals.get('select_stock')
                score_func = strategy_globals.get('score_stock', lambda x: 50)
                
                if not select_func:
                    self.root.after(0, lambda: self.log("❌ 策略必须包含 select_stock 函数"))
                    self.is_selecting = False
                    self.root.after(0, self.progress.stop)
                    return
            except Exception as e:
                self.root.after(0, lambda: self.log(f"❌ 策略编译错误: {e}"))
                self.is_selecting = False
                self.root.after(0, self.progress.stop)
                return
            
            all_results = []
            processed_count = 0
            selected_count = 0
            
            for period in self.selected_periods:
                if self.stop_selection:
                    break
                
                if period not in self.db_connections:
                    continue
                
                conn_info = self.db_connections[period]
                conn = conn_info['conn']
                table = conn_info['table']
                
                self.root.after(0, lambda p=period: self.log(f"📊 处理 {p} 周期"))
                
                # 获取所有股票代码
                try:
                    codes_df = conn.execute(f"SELECT DISTINCT code FROM {table}").pl()
                    if 'code' not in codes_df.columns:
                        continue
                    all_symbols = codes_df['code'].to_list()
                    total = len(all_symbols)
                    
                    self.root.after(0, lambda p=period, t=total: 
                                  self.log(f"📈 {p} 共 {t} 只股票"))
                    
                except Exception as e:
                    self.root.after(0, lambda: self.log(f"❌ 获取代码失败: {e}"))
                    continue
                
                # 分批处理
                batch_size = mode_config['batch_size']
                delay = mode_config['delay']
                
                for i in range(0, len(all_symbols), batch_size):
                    if self.stop_selection:
                        break
                    
                    batch = all_symbols[i:i+batch_size]
                    
                    # 获取这批股票的数据
                    placeholders = ','.join(['?'] * len(batch))
                    query = f"""
                        SELECT code, date, open, high, low, close, volume
                        FROM {table} 
                        WHERE code IN ({placeholders})
                        ORDER BY code, date
                    """
                    
                    try:
                        df = conn.execute(query, batch).pl()
                        
                        # 统一字段名
                        date_cols = [c for c in df.columns if 'date' in c.lower()]
                        if date_cols:
                            df = df.rename({date_cols[0]: 'date'})
                        
                        code_cols = [c for c in df.columns if 'code' in c.lower()]
                        if code_cols:
                            df = df.rename({code_cols[0]: 'code'})
                        
                    except Exception as e:
                        self.root.after(0, lambda: self.log(f"❌ 查询失败: {e}"))
                        continue
                    
                    # 处理这批股票
                    batch_results = []
                    
                    # 按代码分组处理
                    for symbol in batch:
                        if self.stop_selection:
                            break
                        
                        processed_count += 1
                        
                        # 过滤出该股票的数据
                        stock_df = df.filter(pl.col('code') == symbol).sort('date')
                        
                        if stock_df.height < 10:  # 数据太少跳过
                            continue
                        
                        try:
                            # 调用策略函数
                            if select_func(stock_df):
                                score = score_func(stock_df)
                                if score is None:
                                    score = 50
                                
                                # 获取最新价
                                latest = stock_df.tail(1)
                                price = float(latest['close'].to_numpy()[0])
                                
                                # 计算涨跌幅
                                if stock_df.height >= 2:
                                    prev_close = stock_df.tail(2)['close'].to_numpy()[0]
                                    change = (price / prev_close - 1) * 100
                                else:
                                    change = 0
                                
                                result = {
                                    'code': symbol,
                                    'name': f"{symbol[-6:]}",
                                    'price': price,
                                    'change': change,
                                    'volume': float(latest['volume'].to_numpy()[0]),
                                    'score': score,
                                    'detail': f"评分:{score:.0f}"
                                }
                                batch_results.append(result)
                                selected_count += 1
                        except Exception as e:
                            # 单个股票错误不影响整体
                            pass
                        
                        # 定期更新日志
                        if processed_count % 50 == 0:
                            self.root.after(0, lambda p=processed_count, s=selected_count, t=total: 
                                          self.log(f"⏳ 已处理 {p}/{t} 只，选中 {s} 只"))
                        
                        # 控制处理速度
                        time.sleep(0.001)
                    
                    # 添加本批结果
                    all_results.extend(batch_results)
                    
                    # 批次间延时
                    if delay > 0 and i + batch_size < len(all_symbols):
                        time.sleep(delay)
            
            # 更新结果
            self.root.after(0, self._update_results, all_results)
            
        except Exception as e:
            error_msg = traceback.format_exc()
            self.root.after(0, lambda: self.log(f"❌ 选股出错: {str(e)[:100]}"))
        finally:
            self.root.after(0, self.progress.stop)
            self.is_selecting = False
            self.log("✅ 选股结束")
    
    def _update_results(self, results):
        """更新结果显示"""
        # 恢复选股结果列
        if self.current_result_mode != 'select':
            self.current_result_mode = 'select'
            self.result_tree['columns'] = self.select_columns
            for col in self.select_columns:
                self.result_tree.heading(col, text=col)
                self.result_tree.column(col, width=100, anchor='center')
            self.result_tree.column('代码', width=120)
            self.result_tree.column('名称', width=120)
            self.result_tree.column('详情', width=200)
        
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)
        
        if not results:
            self.log("⚠️ 没有找到符合条件的股票")
            return
        
        # 按评分排序
        results.sort(key=lambda x: x['score'], reverse=True)
        
        for r in results:
            self.result_tree.insert('', 'end', values=(
                r['code'],
                r['name'],
                f"{r['price']:.2f}",
                f"{r['change']:.2f}%",
                f"{r['volume']:.0f}",
                f"{r['score']:.0f}",
                r.get('detail', '')
            ))
        
        self.log(f"✅ 选股完成，共找到 {len(results)} 只股票")
        self.last_results = results
        
        # 更新图表
        self.update_chart(results)
    
    def update_chart(self, results):
        """更新图表"""
        if not results:
            return
        
        self.ax.clear()
        self.ax.set_facecolor('#1e1e1e')
        
        scores = [r['score'] for r in results]
        self.ax.hist(scores, bins=10, color='#00ff00', alpha=0.7, edgecolor='#ffffff')
        self.ax.set_title('选股评分分布', color='#ffffff')
        self.ax.set_xlabel('评分', color='#ffffff')
        self.ax.set_ylabel('数量', color='#ffffff')
        
        self.canvas.draw()
    
    # ===== 结果操作 =====
    
    def export_results(self):
        """导出结果"""
        if not self.last_results:
            messagebox.showwarning("警告", "没有可导出的结果")
            return
        
        filename = f"xuangu_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = self.result_dir / filename
        
        try:
            df = pl.DataFrame(self.last_results)
            df.select('code').write_csv(filepath)
            AutoCloseMessageBox.showinfo("导出成功", f"已保存: {filename}", 1500)
        except Exception as e:
            messagebox.showerror("导出失败", str(e))
    
    def backtest_selection(self):
        """回测选股 - 读取table.txt中的股票代码，使用策略进行量化回测"""
        # 获取策略代码
        strategy_code = self.strategy_text.get('1.0', tk.END).strip()
        if not strategy_code:
            messagebox.showwarning("警告", "请先选择或输入策略代码")
            return
        
        # 验证策略包含backtest函数
        if 'def backtest(' not in strategy_code:
            messagebox.showwarning("警告", "策略必须包含 backtest(df) 函数用于回测")
            return
        
        if self.is_selecting:
            messagebox.showwarning("警告", "正在运行中，请等待完成")
            return
        
        # 取第一个选中的周期
        if not self.selected_periods:
            messagebox.showwarning("警告", "请至少选择一个周期")
            return
        
        period = self.selected_periods[0]
        if period not in self.db_connections:
            messagebox.showwarning("警告", f"未连接{period}数据库")
            return
        
        # 读取table.txt
        codes = self._read_codes_from_table_txt()
        if not codes:
            messagebox.showwarning("警告", f"table.txt中没有股票代码或文件不存在")
            return
        
        self.log(f"🚀 开始回测 - 周期:{period} 股票数:{len(codes)}")
        self.log(f"📋 股票列表: {', '.join(codes)}")
        
        # 设置状态
        self.is_selecting = True
        self.stop_selection = False
        self.progress.start()
        
        # 后台线程执行
        thread = threading.Thread(
            target=self._run_backtest_thread,
            args=(strategy_code, period, codes),
            daemon=True
        )
        thread.start()
    
    def _read_codes_from_table_txt(self):
        """从table.txt读取股票代码并规范化"""
        table_txt_path = self.base_dir / "table.txt"
        if not table_txt_path.exists():
            self.log(f"❌ 未找到 {table_txt_path}")
            return []
        
        codes = []
        try:
            with open(table_txt_path, 'r', encoding='utf-8') as f:
                for line in f:
                    code = line.strip()
                    if code and not code.startswith('#'):
                        norm = self._normalize_code(code)
                        if norm:
                            codes.append(norm)
        except Exception as e:
            self.log(f"❌ 读取table.txt失败: {e}")
            return []
        
        if not codes:
            self.log("⚠️ table.txt为空")
        
        return codes
    
    @staticmethod
    def _normalize_code(code):
        """规范化股票代码: sz.000001 → 000001, sh600000 → 600000"""
        if not code or not isinstance(code, str):
            return code
        c = code.strip().upper()
        if '.' in c:
            parts = c.split('.')
            if len(parts) == 2 and parts[1] in ('SH', 'SZ'):
                return parts[0]
            if len(parts) == 2 and parts[0] in ('SH', 'SZ'):
                return parts[1]
        if c.startswith('SH') or c.startswith('SZ'):
            import re
            return re.sub(r'^[A-Z]+\.?', '', c)
        return c
    
    def _run_backtest_thread(self, strategy_code, period, codes):
        """后台回测线程"""
        try:
            # 编译策略
            strategy_globals = {}
            exec(strategy_code, strategy_globals)
            backtest_func = strategy_globals.get('backtest')
            
            if not backtest_func:
                self.root.after(0, lambda: self.log("❌ 策略中未找到 backtest(df) 函数"))
                self.is_selecting = False
                self.root.after(0, self.progress.stop)
                return
            
            conn_info = self.db_connections[period]
            conn = conn_info['conn']
            table = conn_info['table']
            
            results = []
            total = len(codes)
            
            for idx, code in enumerate(codes):
                if self.stop_selection:
                    break
                
                try:
                    # 获取该股票全部历史数据
                    df = conn.execute(
                        f"SELECT date, open, high, low, close, volume FROM {table} WHERE code = ? ORDER BY date",
                        [code]
                    ).pl()
                    
                    if df.height < 30:
                        self.root.after(0, lambda c=code: self.log(f"⚠️ {c} 数据不足({df.height}条)"))
                        continue
                    
                    # 统一日期字段名
                    date_cols = [c for c in df.columns if 'date' in c.lower()]
                    if date_cols:
                        df = df.rename({date_cols[0]: 'date'})
                    
                    # 执行回测
                    bt_result = backtest_func(df)
                    
                    if bt_result is None:
                        continue
                    
                    annual_return = bt_result.get('annualized_return', 0) or 0
                    total_return = bt_result.get('total_return', 0) or 0
                    win_rate = bt_result.get('win_rate', 0) or 0
                    num_trades = bt_result.get('num_trades', 0) or 0
                    max_drawdown = bt_result.get('max_drawdown', 0) or 0
                    
                    results.append({
                        'code': code,
                        'annual_return': annual_return,
                        'total_return': total_return,
                        'win_rate': win_rate,
                        'num_trades': num_trades,
                        'max_drawdown': max_drawdown
                    })
                    
                    log_msg = f"📊 [{idx+1}/{total}] {code} 年化:{annual_return*100:.2f}% 总收益:{total_return*100:.2f}%"
                    self.root.after(0, lambda m=log_msg: self.log(m))
                    
                except Exception as e:
                    self.root.after(0, lambda c=code, err=str(e)[:60]: self.log(f"❌ {c} 回测失败: {err}"))
                
                time.sleep(0.02)
            
            # 剔除负收益
            positive = [r for r in results if r['annual_return'] > 0]
            # 按年化收益率从高到低排序
            positive.sort(key=lambda x: x['annual_return'], reverse=True)
            
            # 写出报告
            report_path = self._write_backtest_report(positive, period)
            
            # 更新界面
            self.root.after(0, self._update_backtest_results, positive)
            
            # 汇总日志
            self.root.after(0, lambda: self.log(f"📈 回测完成: 正收益 {len(positive)}/{len(results)} 只"))
            if positive:
                self.root.after(0, lambda r=positive[0]: self.log(f"🥇 最高: {r['code']} {r['annual_return']*100:.2f}%"))
            if report_path:
                self.root.after(0, lambda p=str(report_path): self.log(f"📄 报告已保存: {p}"))
            
        except Exception as e:
            error_msg = traceback.format_exc()
            self.root.after(0, lambda: self.log(f"❌ 回测线程出错: {str(e)[:100]}"))
        finally:
            self.root.after(0, self.progress.stop)
            self.is_selecting = False
            self.log("✅ 回测结束")
    
    def _write_backtest_report(self, results, period):
        """写出回测报告CSV"""
        if not results:
            return None
        
        report_dir = self.base_dir / "backtest_reports"
        report_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        strategy_name = self.strategy_var.get() or "unknown"
        safe_name = "".join(c for c in strategy_name if c.isalnum() or c in '_')
        report_path = report_dir / f"回测报告_{safe_name}_{period}_{timestamp}.csv"
        
        try:
            header = "代码,年化收益率,总收益率,胜率,交易次数,最大回撤"
            rows = []
            for r in results:
                rows.append(
                    f"{r['code']},"
                    f"{r['annual_return']*100:.2f}%,"
                    f"{r['total_return']*100:.2f}%,"
                    f"{r['win_rate']*100:.1f}%,"
                    f"{r['num_trades']},"
                    f"{r['max_drawdown']*100:.2f}%"
                )
            
            with open(report_path, 'w', encoding='utf-8-sig', newline='') as f:
                f.write(header + '\n')
                for row in rows:
                    f.write(row + '\n')
            
            self.log(f"📄 回测报告已保存: {report_path}")
            return report_path
        except Exception as e:
            self.log(f"❌ 写入报告失败: {e}")
            return None
    
    def _update_backtest_results(self, results):
        """在结果表格中显示回测结果"""
        # 切换为回测列
        self.current_result_mode = 'backtest'
        self.result_tree['columns'] = self.backtest_columns
        for col in self.backtest_columns:
            self.result_tree.heading(col, text=col)
            self.result_tree.column(col, width=100, anchor='center')
        self.result_tree.column('代码', width=120)
        self.result_tree.column('年化收益率', width=110)
        self.result_tree.column('总收益率', width=110)
        self.result_tree.column('胜率', width=80)
        self.result_tree.column('交易次数', width=80)
        self.result_tree.column('最大回撤', width=100)
        
        # 清空并填充
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)
        
        if not results:
            self.log("⚠️ 没有正收益的个股")
            return
        
        for r in results:
            self.result_tree.insert('', 'end', values=(
                r['code'],
                f"{r['annual_return']*100:.2f}%",
                f"{r['total_return']*100:.2f}%",
                f"{r['win_rate']*100:.1f}%",
                r['num_trades'],
                f"{r['max_drawdown']*100:.2f}%"
            ))
        
        self.log(f"✅ 共 {len(results)} 只正收益个股，已按年化收益率从高到低排列")
    
    def clear_results(self):
        """清空结果"""
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)
        self.last_results = []
        # 如果当前是回测模式，恢复为选股列
        if self.current_result_mode != 'select':
            self.current_result_mode = 'select'
            self.result_tree['columns'] = self.select_columns
            for col in self.select_columns:
                self.result_tree.heading(col, text=col)
                self.result_tree.column(col, width=100, anchor='center')
            self.result_tree.column('代码', width=120)
            self.result_tree.column('名称', width=120)
            self.result_tree.column('详情', width=200)
        self.log("🧹 已清空所有结果")
    
    # ===== 日志方法 =====
    
    def copy_log(self):
        """复制日志"""
        log_content = self.log_text.get('1.0', tk.END)
        try:
            pyperclip.copy(log_content)
            AutoCloseMessageBox.showinfo("成功", "日志已复制", 1000)
        except:
            self.root.clipboard_clear()
            self.root.clipboard_append(log_content)
            AutoCloseMessageBox.showinfo("成功", "日志已复制", 1000)
    
    def clear_log(self):
        """清空日志"""
        self.log_text.delete('1.0', tk.END)
    
    def log(self, message):
        """添加日志"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        log_msg = f"[{timestamp}] {message}\n"
        self.log_queue.put(log_msg)
    
    def process_log_queue(self):
        """处理日志队列"""
        try:
            count = 0
            while count < 20:
                msg = self.log_queue.get_nowait()
                self.log_text.insert(tk.END, msg)
                
                if float(self.log_text.index('end-1c')) > 2000:
                    self.log_text.delete('1.0', '2.0')
                
                self.log_text.see(tk.END)
                count += 1
                self.log_queue.task_done()
        except queue.Empty:
            pass
        finally:
            self.root.after(200, self.process_log_queue)

def main():
    """主函数"""
    root = tk.Tk()
    
    try:
        p = psutil.Process(os.getpid())
        p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
    except:
        pass
    
    app = QuantTool(root)
    root.mainloop()

if __name__ == "__main__":
    main()