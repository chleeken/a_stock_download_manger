import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import threading
import queue
import json
import os
import time
import datetime
import logging
from typing import List, Dict, Optional, Tuple, Set
import duckdb
import baostock as bs
import re
import warnings
import pyperclip
import sys
import traceback

# 模块接口导入（剥离GUI的模块）
from minute60_stock_data_download import run_module as run_60min_module
from monthly_stock_data_download import run_module as run_monthly_module
from seasonly_stock_data_download import run_module as run_seasonly_module
from weekly_stock_data_download import run_module as run_weekly_module

warnings.filterwarnings('ignore')


class StockDownloader:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("股票数据下载器 v18.01 - 优化版")
        self.root.geometry("1200x850")
        
        self.root.option_add('*Font', ('宋体', 10))
        
        # 获取程序目录和工作目录
        self.program_dir = self.get_program_dir()
        self.work_dir = self.get_work_dir()
        
        # 数据库路径（直接指定）
        self.db_dir = os.path.join(self.program_dir, "stock_data")
        self.day_db_path = os.path.join(self.db_dir, "stock_data.duckdb")
        self.info_db_path = os.path.join(self.db_dir, "stock_info_data.duckdb")
        
        # 确保数据库目录存在
        os.makedirs(self.db_dir, exist_ok=True)
        
        # 表名配置
        self.day_table_name = "dayly_stock_data"
        self.info_table_name = "stock_data"
        
        # 文件路径
        self.table_txt_path = os.path.join(self.work_dir, "table.txt")
        self.failed_txt_path = os.path.join(self.work_dir, "failed_stocks.txt")
        self.config_file = os.path.join(self.work_dir, "stock_get_date_config.json")
        self.downloaded_record_file = os.path.join(self.work_dir, "downloaded_stocks.json")
        
        # 变量初始化
        self.is_running = False
        self.is_paused = False
        self.is_saving = False
        self.task_queue = queue.Queue()
        self.failed_stocks = []
        self.bs_logged_in = False
        self.bs_session = None
        self.current_stock = tk.StringVar(value="等待处理...")
        self.progress_var = tk.DoubleVar()
        self.total_stocks = 0
        self.processed_stocks = 0
        self.start_time = 0
        self.last_login_time = 0
        
        # 多线程相关
        self.thread_count = 1
        self.lock = threading.Lock()
        
        # 缓存股票上市日期
        self.stock_ipo_dates = {}
        
        # 数据库连接
        self.db_conn = None
        self.db_lock = threading.Lock()
        
        # 临时数据表
        self.temp_table_name = None
        self.temp_table_created = False
        self.temp_table_lock = threading.RLock()
        
        # 网络请求统计
        self.request_count = 0
        self.last_request_time = time.time()
        
        # 智能跳过计数
        self.skip_count = 0
        self.skip_threshold = 50
        
        # 已下载记录
        self.downloaded_stocks = self.load_downloaded_records()
        
        # 当前批次已下载的数据行数
        self.downloaded_rows = 0
        self.stock_process_count = 0
        
        # 数据校验相关
        self.data_validation_errors = []
        
        # 数据库统计信息
        self.db_max_date = None
        self.db_min_date = None
        self.db_stock_count = 0
        self.db_row_count = 0
        
        # 股票最新日期缓存
        self.stock_max_date_cache = {}
        self.cache_lock = threading.Lock()
        
        # 可下载日期缓存
        self.available_dates_cache = {}
        self.available_dates_lock = threading.Lock()
        
        # 定时保存相关
        self.last_save_time = time.time()
        self.save_interval = 180
        self.save_timer = None
        self.auto_save_trigger_count = 100
        
        # 重试机制
        self.max_retries = 3
        self.retry_delay = 5
        self.stock_retry_count = {}
        
        # 缺失股票记录
        self.missing_stocks = set()
        
        # 当前批次处理的股票列表
        self.current_batch_stocks = []
        self.processed_codes = set()
        
        # 结果队列
        self.result_queue = queue.Queue()
        
        # 数据缓冲区
        self.data_buffer = []
        self.data_buffer_lock = threading.Lock()
        self.buffer_size = 0
        
        # 自动保存标志
        self.is_auto_saving = False
        self.auto_save_requested = False
        self.last_save_check = 0
        
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
            '5min': tk.BooleanVar(value=False)
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
        
        # 周期配置
        self.period_config = {
            'day': {
                'table': self.day_table_name,
                'limit': None, 
                'batch_size': 5000,
                'timeout': 90,
                'retry_count': 5,
                'request_delay': 0.2,
                'enable_wal': True
            },
            'week': {
                'table': 'week_stock_data', 
                'limit': None, 
                'batch_size': 500,
                'timeout': 90,
                'retry_count': 5,
                'request_delay': 0.3,
                'enable_wal': True
            },
            'month': {
                'table': 'month_stock_data', 
                'limit': None, 
                'batch_size': 500,
                'timeout': 90,
                'retry_count': 5,
                'request_delay': 0.4,
                'enable_wal': True
            },
            'season': {
                'table': 'season_stock_data', 
                'limit': None, 
                'batch_size': 500,
                'timeout': 90,
                'retry_count': 5,
                'request_delay': 0.4,
                'enable_wal': True
            },
            'year': {
                'table': 'year_stock_data', 
                'limit': None, 
                'batch_size': 500,
                'timeout': 90,
                'retry_count': 5,
                'request_delay': 0.4,
                'enable_wal': True
            },
            '60min': {
                'table': '60minute_stock_data', 
                'limit': 2000, 
                'batch_size': 300,
                'timeout': 120,
                'retry_count': 5,
                'request_delay': 0.5,
                'enable_wal': True
            },
            '30min': {
                'table': '30minute_stock_data', 
                'limit': 1000, 
                'batch_size': 300,
                'timeout': 120,
                'retry_count': 5,
                'request_delay': 0.5,
                'enable_wal': True
            },
            '15min': {
                'table': '15minute_stock_data', 
                'limit': 1000, 
                'batch_size': 300,
                'timeout': 120,
                'retry_count': 5,
                'request_delay': 0.5,
                'enable_wal': True
            },
            '5min': {
                'table': '5minute_stock_data', 
                'limit': 1000, 
                'batch_size': 300,
                'timeout': 120,
                'retry_count': 5,
                'request_delay': 0.5,
                'enable_wal': True
            }
        }
        
        # 创建UI
        self.setup_ui()
        
        # 加载配置
        self.load_config()
        
        # 从本地数据库加载股票上市日期
        self.load_ipo_dates_from_info_db()
        
        # 显示数据库统计信息
        self.show_db_stats()
        
        # 预加载所有股票的最新日期
        self.preload_stock_max_dates()
        
        # 启动自动保存定时器
        self.start_auto_save_timer()
        
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        self.log_message(f"程序目录: {self.program_dir}")
        self.log_message(f"工作目录: {self.work_dir}")
        self.log_message(f"数据库目录: {self.db_dir}")
    
    def get_program_dir(self) -> str:
        """获取程序所在目录（兼容打包环境）"""
        if getattr(sys, 'frozen', False):
            return os.path.dirname(sys.executable)
        else:
            return os.path.dirname(os.path.abspath(__file__))
    
    def is_temp_directory(self, path: str) -> bool:
        """检测路径是否是临时目录"""
        temp_patterns = ['temp', 'tmp', 'Temp', 'Tmp', 'AppData\\Local\\Temp', 
                         '/tmp', '/var/tmp', '~/.cache']
        path_lower = path.lower()
        
        for pattern in temp_patterns:
            if pattern.lower() in path_lower:
                return True
        
        if getattr(sys, 'frozen', False):
            if '\\_MEI' in path or '/_MEI' in path:
                return True
        
        return False
    
    def get_work_dir(self) -> str:
        """智能获取工作目录"""
        # 优先使用当前工作目录（如果不在临时目录）
        cwd = os.getcwd()
        if not self.is_temp_directory(cwd):
            return cwd
        
        # 其次使用用户文档目录
        try:
            import ctypes.wintypes
            CSIDL_PERSONAL = 5
            buf = ctypes.create_unicode_buffer(ctypes.wintypes.MAX_PATH)
            ctypes.windll.shell32.SHGetFolderPathW(None, CSIDL_PERSONAL, None, 0, buf)
            docs_dir = buf.value
            if docs_dir and not self.is_temp_directory(docs_dir):
                return docs_dir
        except:
            pass
        
        # 然后使用桌面目录
        try:
            desktop = os.path.join(os.path.expanduser("~"), "Desktop")
            if os.path.exists(desktop) and not self.is_temp_directory(desktop):
                return desktop
        except:
            pass
        
        # 最后使用用户主目录
        home = os.path.expanduser("~")
        if not self.is_temp_directory(home):
            return home
        
        # 如果都不行，使用程序目录
        return self.program_dir
    
    def get_db_connection(self):
        """获取日线数据库连接"""
        with self.db_lock:
            try:
                if self.db_conn is not None:
                    try:
                        self.db_conn.execute("SELECT 1").fetchone()
                        return self.db_conn
                    except Exception as e:
                        self.log_message(f"数据库连接失效，重新连接: {e}", "DEBUG")
                        try:
                            self.db_conn.close()
                        except:
                            pass
                        self.db_conn = None
                
                # 创建新连接
                os.makedirs(os.path.dirname(self.day_db_path), exist_ok=True)
                
                self.db_conn = duckdb.connect(self.day_db_path)
                
                # 启用WAL模式
                try:
                    self.db_conn.execute("PRAGMA wal_autocheckpoint = 0")
                    self.db_conn.execute("PRAGMA checkpoint_threshold = 1000000")
                    self.db_conn.execute("PRAGMA synchronous = NORMAL")
                except:
                    pass
                
                # 设置内存限制
                try:
                    self.db_conn.execute("PRAGMA memory_limit='4GB'")
                except:
                    pass
                
                # 设置线程数
                try:
                    self.db_conn.execute("PRAGMA threads=4")
                except:
                    pass
                
                self.log_message(f"数据库连接成功: {self.day_db_path}", "DEBUG")
                return self.db_conn
                
            except Exception as e:
                self.log_message(f"数据库连接失败: {e}", "ERROR")
                return None
    
    def close_db_connection(self, checkpoint=True):
        """关闭数据库连接"""
        with self.db_lock:
            if self.db_conn:
                try:
                    if checkpoint:
                        self.log_message("正在执行检查点，合并WAL文件...", "INFO")
                        self.db_conn.execute("CHECKPOINT")
                        self.log_message("WAL文件合并完成", "INFO")
                    
                    self.db_conn.close()
                except Exception as e:
                    self.log_message(f"关闭数据库连接时出错: {e}", "ERROR")
                self.db_conn = None
                self.log_message("数据库连接已关闭", "DEBUG")
    
    def load_ipo_dates_from_info_db(self):
        """从上市日期数据库加载上市日期"""
        try:
            if not os.path.exists(self.info_db_path):
                self.log_message(f"上市日期数据库不存在: {self.info_db_path}", "WARNING")
                return False
            
            self.log_message(f"正在从 {self.info_db_path} 加载上市日期...")
            
            try:
                info_conn = duckdb.connect(self.info_db_path, read_only=True)
            except Exception as e:
                self.log_message(f"无法打开上市日期数据库: {e}", "ERROR")
                return False
            
            # 获取表结构
            tables = info_conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            
            if self.info_table_name not in table_names:
                self.log_message(f"表 {self.info_table_name} 不存在", "ERROR")
                info_conn.close()
                return False
            
            # 获取列名
            columns = info_conn.execute(f"PRAGMA table_info({self.info_table_name})").fetchall()
            column_names = [col[1] for col in columns]
            
            # 查找上市日期字段
            ipo_field = None
            for name in column_names:
                name_lower = name.lower()
                if 'ipo' in name_lower or '上市日期' in name or 'list_date' in name_lower or 'listing' in name_lower:
                    ipo_field = name
                    break
            
            if not ipo_field:
                self.log_message("未找到上市日期字段", "WARNING")
                info_conn.close()
                return False
            
            # 查询上市日期
            result = info_conn.execute(f"SELECT code, \"{ipo_field}\" FROM {self.info_table_name} WHERE \"{ipo_field}\" IS NOT NULL").fetchall()
            
            if not result:
                self.log_message("没有上市日期记录", "WARNING")
                info_conn.close()
                return False
            
            # 更新内存缓存
            ipo_count = 0
            for code, ipo_date in result:
                if code and ipo_date:
                    normalized_code = self.normalize_stock_code(str(code))
                    date_str = str(ipo_date).strip()
                    if date_str and len(date_str) >= 10:
                        date_str = date_str[:10]
                        self.stock_ipo_dates[normalized_code] = date_str
                        ipo_count += 1
            
            info_conn.close()
            
            self.log_message(f"✅ 成功加载 {ipo_count} 只股票的上市日期到内存缓存")
            return True
            
        except Exception as e:
            self.log_message(f"加载上市日期失败: {e}", "ERROR")
            traceback.print_exc()
            return False
    
    def ensure_table_exists(self):
        """确保数据表存在"""
        try:
            conn = self.get_db_connection()
            if conn is None:
                self.log_message("无法获取数据库连接", "ERROR")
                return False
            
            # 检查表是否存在
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            
            if self.day_table_name not in table_names:
                self.log_message(f"创建 {self.day_table_name} 表...", "INFO")
                conn.execute(f"""
                    CREATE TABLE {self.day_table_name} (
                        code VARCHAR,
                        date DATE,
                        open DOUBLE,
                        high DOUBLE,
                        low DOUBLE,
                        close DOUBLE,
                        volume DOUBLE,
                        amount DOUBLE
                    )
                """)
                
                # 创建索引
                try:
                    conn.execute(f"CREATE INDEX idx_{self.day_table_name}_code ON {self.day_table_name}(code)")
                    conn.execute(f"CREATE INDEX idx_{self.day_table_name}_date ON {self.day_table_name}(date)")
                except:
                    pass
                
                self.log_message(f"✅ {self.day_table_name} 表创建完成", "INFO")
                return True
            
            return True
            
        except Exception as e:
            self.log_message(f"检查/创建表失败: {e}", "ERROR")
            return False
    
    def create_temp_table(self):
        """创建临时数据表"""
        with self.temp_table_lock:
            try:
                conn = self.get_db_connection()
                if conn is None:
                    self.log_message("无法获取数据库连接，无法创建临时表", "ERROR")
                    return False
                
                # 如果已有临时表，先删除
                if self.temp_table_created and self.temp_table_name:
                    try:
                        conn.execute(f"DROP TABLE IF EXISTS {self.temp_table_name}")
                    except:
                        pass
                
                self.temp_table_name = f"temp_download_{int(time.time())}"
                conn.execute(f"""
                    CREATE TABLE {self.temp_table_name} (
                        code VARCHAR,
                        date DATE,
                        open DOUBLE,
                        high DOUBLE,
                        low DOUBLE,
                        close DOUBLE,
                        volume DOUBLE,
                        amount DOUBLE
                    )
                """)
                
                self.temp_table_created = True
                self.downloaded_rows = 0
                self.stock_process_count = 0
                self.data_buffer = []
                self.buffer_size = 0
                self.log_message(f"创建临时表: {self.temp_table_name}", "INFO")
                return True
                
            except Exception as e:
                self.log_message(f"创建临时表失败: {e}", "ERROR")
                return False
    
    def recreate_temp_table(self):
        """重新创建临时表"""
        with self.temp_table_lock:
            try:
                conn = self.get_db_connection()
                if conn is None:
                    self.log_message("无法获取数据库连接，无法重建临时表", "ERROR")
                    return False
                
                # 删除旧的临时表
                if self.temp_table_name:
                    try:
                        conn.execute(f"DROP TABLE IF EXISTS {self.temp_table_name}")
                    except:
                        pass
                
                # 创建新的临时表
                self.temp_table_name = f"temp_download_{int(time.time())}"
                conn.execute(f"""
                    CREATE TABLE {self.temp_table_name} (
                        code VARCHAR,
                        date DATE,
                        open DOUBLE,
                        high DOUBLE,
                        low DOUBLE,
                        close DOUBLE,
                        volume DOUBLE,
                        amount DOUBLE
                    )
                """)
                
                self.temp_table_created = True
                self.downloaded_rows = 0
                self.log_message(f"重新创建临时表: {self.temp_table_name}", "INFO")
                return True
                
            except Exception as e:
                self.log_message(f"重建临时表失败: {e}", "ERROR")
                self.temp_table_created = False
                return False
    
    def merge_temp_to_main(self, checkpoint=True, batch_save=False):
        """将临时表数据合并到主表"""
        with self.temp_table_lock:
            if not self.temp_table_created:
                self.log_message("临时表不存在，无需合并", "INFO")
                return
            
            try:
                conn = self.get_db_connection()
                if conn is None:
                    self.log_message("无法获取数据库连接", "ERROR")
                    return
                
                # 检查临时表是否存在
                tables = conn.execute("SHOW TABLES").fetchall()
                table_names = [t[0] for t in tables]
                
                if self.temp_table_name not in table_names:
                    self.log_message(f"临时表 {self.temp_table_name} 不存在", "WARNING")
                    self.temp_table_created = False
                    return
                
                # 获取临时表数据量
                temp_count = conn.execute(f"SELECT COUNT(*) FROM {self.temp_table_name}").fetchone()[0]
                
                if temp_count == 0:
                    self.log_message("临时表为空，无需合并", "INFO")
                    conn.execute(f"DROP TABLE {self.temp_table_name}")
                    self.temp_table_created = False
                    self.downloaded_rows = 0
                    return
                
                self.log_message(f"开始合并数据到主表 {self.day_table_name}，临时表共 {temp_count} 行...", "INFO")
                
                # 使用ANTI JOIN找出不存在的记录
                inserted_count = conn.execute(f"""
                    SELECT COUNT(*) FROM {self.temp_table_name} t
                    ANTI JOIN {self.day_table_name} d 
                    ON t.code = d.code AND t.date = d.date
                """).fetchone()[0]
                
                if inserted_count > 0:
                    # 插入数据
                    conn.execute(f"""
                        INSERT INTO {self.day_table_name}
                        SELECT t.* FROM {self.temp_table_name} t
                        ANTI JOIN {self.day_table_name} d 
                        ON t.code = d.code AND t.date = d.date
                    """)
                    
                    self.log_message(f"✅ 合并完成：临时表 {temp_count} 行，新增 {inserted_count} 行到主表")
                else:
                    self.log_message(f"临时表 {temp_count} 行数据都已存在，无需新增")
                
                # 如果启用了检查点，执行WAL合并
                if checkpoint:
                    self.log_message("执行检查点，合并WAL文件...", "INFO")
                    conn.execute("CHECKPOINT")
                    self.log_message("WAL文件合并完成", "INFO")
                
                # 清理原始临时表
                conn.execute(f"DROP TABLE {self.temp_table_name}")
                self.temp_table_created = False
                self.downloaded_rows = 0
                self.stock_process_count = 0
                
                # 更新缓存
                self.update_cache_after_save(conn)
                
                # 更新统计信息
                self.show_db_stats_with_connection(conn)
                
            except Exception as e:
                self.log_message(f"合并数据失败: {e}", "ERROR")
                traceback.print_exc()
    
    def update_cache_after_save(self, conn):
        """保存后更新缓存"""
        try:
            # 检查表是否存在
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            
            if self.day_table_name not in table_names:
                return
            
            self.log_message("正在更新股票最新日期缓存...", "INFO")
            
            # 查询所有股票的最新日期
            result = conn.execute(f"""
                SELECT code, MAX(date) as max_date
                FROM {self.day_table_name}
                GROUP BY code
            """).fetchall()
            
            with self.cache_lock:
                self.stock_max_date_cache.clear()
                for code, max_date in result:
                    if max_date:
                        if isinstance(max_date, str):
                            date_str = max_date[:10]
                        elif hasattr(max_date, 'strftime'):
                            date_str = max_date.strftime("%Y-%m-%d")
                        else:
                            date_str = str(max_date)[:10]
                        self.stock_max_date_cache[code] = date_str
            
            self.log_message(f"✅ 缓存更新完成，共 {len(self.stock_max_date_cache)} 只股票的最新日期", "INFO")
            
        except Exception as e:
            self.log_message(f"更新缓存失败: {e}", "WARNING")
    
    def add_to_buffer(self, data):
        """将数据添加到缓冲区"""
        with self.data_buffer_lock:
            self.data_buffer.extend(data)
            self.buffer_size += len(data)
    
    def flush_buffer(self):
        """将缓冲区数据写入临时表"""
        with self.data_buffer_lock:
            if not self.data_buffer:
                return 0
            
            with self.temp_table_lock:
                if not self.temp_table_created:
                    self.log_message("临时表不存在，无法写入数据", "ERROR")
                    return 0
                
                conn = self.get_db_connection()
                if conn is None:
                    self.log_message("无法获取数据库连接", "ERROR")
                    return 0
                
                # 分批插入
                chunk_size = 5000
                total_inserted = 0
                
                for i in range(0, len(self.data_buffer), chunk_size):
                    chunk = self.data_buffer[i:i+chunk_size]
                    placeholders = ','.join(['(?,?,?,?,?,?,?,?)'] * len(chunk))
                    flat_values = []
                    for row in chunk:
                        flat_values.extend(row)
                    
                    try:
                        conn.execute(
                            f"INSERT INTO {self.temp_table_name} VALUES {placeholders}",
                            flat_values
                        )
                        total_inserted += len(chunk)
                    except Exception as e:
                        self.log_message(f"批量插入失败: {e}，尝试逐条插入", "WARNING")
                        for row in chunk:
                            try:
                                conn.execute(
                                    f"INSERT INTO {self.temp_table_name} VALUES (?,?,?,?,?,?,?,?)",
                                    row
                                )
                                total_inserted += 1
                            except Exception as e2:
                                self.log_message(f"逐条插入失败: {e2}", "ERROR")
                
                self.downloaded_rows += total_inserted
                self.log_message(f"批量提交 {total_inserted} 行到临时表，累计 {self.downloaded_rows} 行", "DEBUG")
                
                self.data_buffer = []
                self.buffer_size = 0
                
                return total_inserted
    
    def start_auto_save_timer(self):
        """启动自动保存定时器"""
        def auto_save():
            while self.is_running:
                time.sleep(5)
                current_time = time.time()
                if current_time - self.last_save_time >= self.save_interval:
                    if self.downloaded_rows > 0:
                        if not self.is_auto_saving and not self.auto_save_requested:
                            self.auto_save_requested = True
                            self.log_message("自动保存触发（定时）：将临时数据合并到主表", "INFO")
                            self.root.after(0, self.do_auto_save)
            
            if self.save_timer:
                self.save_timer = None
        
        self.save_timer = threading.Thread(target=auto_save, daemon=True)
        self.save_timer.start()
    
    def do_auto_save(self):
        """执行自动保存"""
        if self.is_auto_saving:
            return
        
        self.is_auto_saving = True
        try:
            self.flush_buffer()
            if self.downloaded_rows > 0:
                self.merge_temp_to_main(checkpoint=True)
                self.recreate_temp_table()
            self.last_save_time = time.time()
        except Exception as e:
            self.log_message(f"自动保存失败: {e}", "ERROR")
        finally:
            self.is_auto_saving = False
            self.auto_save_requested = False
    
    def manual_save_data(self):
        """手动保存数据"""
        if self.is_saving:
            messagebox.showinfo("提示", "正在保存中，请稍候...")
            return
        
        if not self.temp_table_created or self.downloaded_rows == 0:
            messagebox.showinfo("提示", "没有临时数据需要保存")
            return
        
        save_thread = threading.Thread(target=self.manual_save_worker, daemon=True)
        save_thread.start()
    
    def manual_save_worker(self):
        """手动保存工作线程"""
        self.is_saving = True
        try:
            self.log_message("手动保存触发：开始合并临时数据到主表", "INFO")
            with self.temp_table_lock:
                self.flush_buffer()
                self.merge_temp_to_main(checkpoint=True, batch_save=True)
                self.recreate_temp_table()
            self.log_message("手动保存完成，已重建临时表", "INFO")
            self.root.after(0, lambda: messagebox.showinfo("保存完成", "数据保存完成"))
        except Exception as e:
            self.log_message(f"手动保存失败: {e}", "ERROR")
            self.root.after(0, lambda: messagebox.showerror("保存失败", f"保存失败: {e}"))
        finally:
            self.is_saving = False
    
    def get_stock_max_date(self, stock_code: str, use_cache: bool = True) -> Optional[str]:
        """获取股票最大日期"""
        if use_cache:
            with self.cache_lock:
                if stock_code in self.stock_max_date_cache:
                    return self.stock_max_date_cache[stock_code]
        
        try:
            conn = self.get_db_connection()
            if conn is None:
                return None
            
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            
            if self.day_table_name not in table_names:
                return None
            
            result = conn.execute(
                f"SELECT MAX(date) FROM {self.day_table_name} WHERE code = ?",
                [stock_code]
            ).fetchone()
            
            if result and result[0]:
                max_date = result[0]
                if isinstance(max_date, str):
                    date_str = max_date[:10]
                elif hasattr(max_date, 'strftime'):
                    date_str = max_date.strftime("%Y-%m-%d")
                else:
                    date_str = str(max_date)[:10]
                
                with self.cache_lock:
                    self.stock_max_date_cache[stock_code] = date_str
                return date_str
        except Exception as e:
            if "does not exist" not in str(e):
                self.log_message(f"获取最大日期失败 {stock_code}: {e}", "DEBUG")
        return None
    
    def get_stock_ipo_date(self, code: str) -> Optional[str]:
        """获取股票上市日期"""
        return self.stock_ipo_dates.get(code)
    
    def get_stock_max_dates_batch(self, codes: List[str]) -> Dict[str, Optional[str]]:
        """批量获取股票的最大日期"""
        if not codes:
            return {}
        
        try:
            conn = self.get_db_connection()
            if conn is None:
                return {}
            
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            
            if self.day_table_name not in table_names:
                return {code: None for code in codes}
            
            placeholders = ','.join(['?'] * len(codes))
            query = f"""
                SELECT code, MAX(date) as max_date
                FROM {self.day_table_name}
                WHERE code IN ({placeholders})
                GROUP BY code
            """
            
            result = conn.execute(query, codes).fetchall()
            
            max_dates = {}
            for code, max_date in result:
                if max_date:
                    if isinstance(max_date, str):
                        date_str = max_date[:10]
                    elif hasattr(max_date, 'strftime'):
                        date_str = max_date.strftime("%Y-%m-%d")
                    else:
                        date_str = str(max_date)[:10]
                    max_dates[code] = date_str
                else:
                    max_dates[code] = None
            
            for code in codes:
                if code not in max_dates:
                    max_dates[code] = None
            
            return max_dates
            
        except Exception as e:
            self.log_message(f"批量获取最大日期失败: {e}", "ERROR")
            return {code: None for code in codes}
    
    def get_available_dates_from_baostock(self, stock_code: str, start_date: str, end_date: str) -> List[str]:
        """从baostock查询指定股票的可下载日期"""
        cache_key = (stock_code, start_date, end_date)
        
        with self.available_dates_lock:
            if cache_key in self.available_dates_cache:
                return self.available_dates_cache[cache_key]
        
        try:
            # 构建baostock代码格式
            if stock_code.startswith('6'):
                bs_code = f"sh.{stock_code}"
            elif stock_code.startswith('0') or stock_code.startswith('3'):
                bs_code = f"sz.{stock_code}"
            else:
                bs_code = f"sh.{stock_code}"
            
            self.safe_request_delay()
            
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3"
            )
            
            if rs.error_code != '0':
                return []
            
            dates = []
            while (rs.error_code == '0') and rs.next():
                row_data = rs.get_row_data()
                if row_data and row_data[0]:
                    date_str = row_data[0].strip()
                    if date_str and len(date_str) >= 10:
                        dates.append(date_str[:10])
            
            with self.available_dates_lock:
                self.available_dates_cache[cache_key] = dates
            
            return dates
            
        except Exception as e:
            self.log_message(f"查询可下载日期时出错 {stock_code}: {e}", "ERROR")
            return []
    
    def get_common_available_dates(self, codes: List[str], start_date: str, end_date: str) -> List[str]:
        """从多个股票中获取共同的可下载日期"""
        sample_count = min(self.query_sample_count.get(), len(codes))
        sample_codes = codes[:sample_count]
        
        all_dates_sets = []
        for code in sample_codes:
            dates = self.get_available_dates_from_baostock(code, start_date, end_date)
            if dates:
                all_dates_sets.append(set(dates))
        
        if not all_dates_sets:
            return []
        
        common_dates = set()
        for dates_set in all_dates_sets:
            common_dates.update(dates_set)
        
        return sorted(list(common_dates))
    
    def check_batch_need_update(self, stocks: List[str], end_date: str) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
        """批量检查股票是否需要更新"""
        max_dates = self.get_stock_max_dates_batch(stocks)
        
        need_update = {}
        all_available_dates = {}
        
        stocks_by_start_date = {}
        
        for code in stocks:
            max_date = max_dates.get(code)
            
            if max_date is None:
                ipo_date = self.get_stock_ipo_date(code)
                start_date = ipo_date if ipo_date else "2000-01-01"
                stocks_by_start_date.setdefault(start_date, []).append(code)
            else:
                if max_date < end_date:
                    try:
                        last_date = datetime.datetime.strptime(max_date, "%Y-%m-%d")
                        next_day = last_date + datetime.timedelta(days=1)
                        start_date = next_day.strftime("%Y-%m-%d")
                        stocks_by_start_date.setdefault(start_date, []).append(code)
                    except Exception as e:
                        self.log_message(f"解析日期失败 {max_date}: {e}", "WARNING")
                        stocks_by_start_date.setdefault(max_date, []).append(code)
        
        for start_date, code_list in stocks_by_start_date.items():
            if not code_list:
                continue
            
            available_dates = self.get_common_available_dates(code_list, start_date, end_date)
            
            if available_dates:
                all_available_dates[(start_date, end_date)] = available_dates
                for code in code_list:
                    need_update[code] = start_date
            else:
                self.log_message(f"开始日期 {start_date} 无可下载数据", "WARNING")
        
        return need_update, all_available_dates
    
    def validate_stock_data(self, code: str, data: List[tuple]) -> bool:
        """验证股票数据的完整性和正确性"""
        if not data:
            return True
        
        try:
            # 验证所有数据行的股票代码是否一致
            for row in data:
                if row[0] != code:
                    self.log_message(f"数据校验失败: {code} 的数据中包含其他股票代码 {row[0]}", "ERROR")
                    return False
            
            # 验证日期是否在合理范围内
            current_year = datetime.datetime.now().year
            for row in data:
                date_str = row[1]
                if date_str:
                    try:
                        date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                        if date_obj.year < 1990 or date_obj.year > current_year + 1:
                            self.log_message(f"数据校验失败: {code} 的日期 {date_str} 不在合理范围内", "ERROR")
                            return False
                    except:
                        self.log_message(f"数据校验失败: {code} 的日期格式错误 {date_str}", "ERROR")
                        return False
            
            # 验证价格数据
            for row in data:
                open_val = row[2]
                high_val = row[3]
                low_val = row[4]
                close_val = row[5]
                
                for price in [open_val, high_val, low_val, close_val]:
                    if price < 0 or price > 100000:
                        self.log_message(f"数据校验失败: {code} 的价格 {price} 超出合理范围", "ERROR")
                        return False
                
                if high_val < low_val:
                    self.log_message(f"数据校验失败: {code} 的最高价 {high_val} 小于最低价 {low_val}", "ERROR")
                    return False
            
            # 验证成交量和成交额
            for row in data:
                volume = row[6]
                amount = row[7]
                
                if volume < 0 or volume > 1e12:
                    self.log_message(f"数据校验失败: {code} 的成交量 {volume} 超出合理范围", "ERROR")
                    return False
                
                if amount < 0 or amount > 1e15:
                    self.log_message(f"数据校验失败: {code} 的成交额 {amount} 超出合理范围", "ERROR")
                    return False
            
            return True
            
        except Exception as e:
            self.log_message(f"数据校验过程出错: {e}", "ERROR")
            return False
    
    def download_single_stock(self, code: str, start_date: str, end_date: str) -> Tuple[str, bool, List, bool, str]:
        """下载单只股票"""
        config = self.period_config['day']
        retry_count = config.get('retry_count', 5)
        
        for attempt in range(retry_count):
            try:
                self.safe_request_delay()
                
                # 构建baostock代码格式
                # 注意：code 已经是6位数字格式，如 '000001'
                if code.startswith('6') or code.startswith('688'):
                    bs_code = f"sh.{code}"
                elif code.startswith('0') or code.startswith('3'):
                    bs_code = f"sz.{code}"
                else:
                    # 默认使用sh
                    bs_code = f"sh.{code}"
                
                # 调试信息
                if code == '000001':
                    self.log_message(f"平安银行({code})转换为: {bs_code}", "INFO")
                
                self.log_message(f"下载{code} {start_date}~{end_date}", "INFO")
                
                # 添加超时控制
                import signal
                
                def timeout_handler(signum, frame):
                    raise TimeoutError("查询超时")
                
                # 设置超时（仅在Unix系统有效，Windows需要其他方法）
                try:
                    signal.signal(signal.SIGALRM, timeout_handler)
                    signal.alarm(60)  # 60秒超时
                except:
                    pass  # Windows系统不支持SIGALRM
                
                try:
                    rs = bs.query_history_k_data_plus(
                        bs_code,
                        "date,open,high,low,close,volume,amount",
                        start_date=start_date,
                        end_date=end_date,
                        frequency="d",
                        adjustflag="3"
                    )
                finally:
                    # 取消超时
                    try:
                        signal.alarm(0)
                    except:
                        pass
                
                # 检查结果
                if code == '000001':
                    self.log_message(f"平安银行查询结果: error_code={rs.error_code}, error_msg={rs.error_msg}", "INFO")
                
                if rs.error_code != '0':
                    if "没有数据" in rs.error_msg:
                        return code, False, None, True, "无数据"
                    raise Exception(f"获取数据失败: {rs.error_msg}")
                
                data_list = []
                error_count = 0
                max_errors = 5
                
                # 添加进度显示
                self.log_message(f"正在获取{code}的数据...", "INFO")
                
                while (rs.error_code == '0') and rs.next():
                    try:
                        row_data = rs.get_row_data()
                        safe_row = []
                        for item in row_data:
                            if item is None:
                                safe_row.append("")
                            elif isinstance(item, bytes):
                                decoded = self.safe_decode_binary(item)
                                safe_row.append(decoded)
                            else:
                                safe_row.append(str(item))
                        
                        if len(safe_row) >= 7 and safe_row[0] and safe_row[0].strip():
                            data_list.append(safe_row)
                        else:
                            error_count += 1
                            if error_count > max_errors:
                                break
                        
                        # 显示进度
                        if len(data_list) % 1000 == 0 and len(data_list) > 0:
                            self.log_message(f"已获取 {len(data_list)} 条数据...", "INFO")
                        
                        if len(data_list) >= 10000:
                            self.log_message(f"已达到单次下载上限10000条，停止获取", "WARNING")
                            break
                            
                    except Exception as e:
                        error_count += 1
                        if error_count > max_errors:
                            break
                        continue
                
                self.log_message(f"{code} 共获取到 {len(data_list)} 条原始数据", "INFO")
                
                if not data_list:
                    return code, False, None, True, "无数据"
                
                processed_data = []
                for row in data_list:
                    if len(row) >= 7:
                        try:
                            date_str = row[0].strip()[:10] if row[0] else ""
                            
                            open_val = float(row[1]) if row[1] and row[1] not in ['', 'None'] else 0.0
                            high_val = float(row[2]) if row[2] and row[2] not in ['', 'None'] else 0.0
                            low_val = float(row[3]) if row[3] and row[3] not in ['', 'None'] else 0.0
                            close_val = float(row[4]) if row[4] and row[4] not in ['', 'None'] else 0.0
                            volume_val = float(row[5]) if row[5] and row[5] not in ['', 'None'] else 0.0
                            amount_val = float(row[6]) if row[6] and row[6] not in ['', 'None'] else 0.0
                            
                            processed_data.append((
                                code, date_str, open_val, high_val, low_val, 
                                close_val, volume_val, amount_val
                            ))
                        except ValueError as e:
                            self.log_message(f"数据转换错误: {e}, 行数据: {row}", "DEBUG")
                            continue
                
                self.log_message(f"{code} 处理完成，有效数据 {len(processed_data)} 条", "INFO")
                
                if processed_data:
                    if self.enable_data_validation.get():
                        is_valid = self.validate_stock_data(code, processed_data)
                        if not is_valid:
                            return code, False, None, False, "数据校验失败"
                    
                    self.log_message(f"{code} 下载成功 {len(processed_data)} 条数据", "INFO")
                    return code, True, processed_data, True, "成功"
                else:
                    return code, False, None, True, "无有效数据"
                    
            except TimeoutError as e:
                self.log_message(f"{code} 查询超时，重试 {attempt+1}/{retry_count}", "WARNING")
                if attempt < retry_count - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue
                return code, False, None, True, f"下载超时"
                
            except Exception as e:
                error_msg = str(e)
                self.log_message(f"{code} 下载出错: {error_msg}", "ERROR")
                self.check_and_relogin(error_msg)
                
                if "网络" in error_msg or "timeout" in error_msg.lower() or "连接" in error_msg:
                    if attempt < retry_count - 1:
                        wait_time = (attempt + 1) * 5
                        self.log_message(f"{code} 网络错误，{wait_time}秒后重试...", "WARNING")
                        time.sleep(wait_time)
                        continue
                
                return code, False, None, True, f"下载错误: {error_msg[:50]}"
        
        return code, False, None, True, "超过最大重试次数"
    
    def process_dayly_stocks_single_thread(self, stocks: List[str]):
        """单线程处理日线数据"""
        skip_mode = "启用" if self.use_skip_mode.get() else "禁用"
        batch_size = self.batch_size_var.get()
        auto_save_stock_count = self.auto_save_stock_count.get()
        
        self.log_message(f"开始单线程处理{len(stocks)}只股票的日线数据... [智能跳过: {skip_mode}]")
        self.log_message(f"[每{auto_save_stock_count}只自动保存]")
        
        self.skip_count = 0
        self.stock_process_count = 0
        self.data_validation_errors = []
        self.stock_retry_count.clear()
        self.missing_stocks.clear()
        self.processed_codes.clear()
        
        if not self.ensure_table_exists():
            self.log_message("无法确保数据表存在，退出下载", "ERROR")
            return
        
        if not self.create_temp_table():
            self.log_message("无法创建临时表，退出下载", "ERROR")
            return
        
        end_date = self.get_previous_trade_day()
        self.log_message(f"本次下载目标日期: {end_date}", "INFO")
        
        self.log_message("批量检查股票更新需求...", "INFO")
        need_update, _ = self.check_batch_need_update(stocks, end_date)
        
        stocks_to_download = list(need_update.keys())
        self.log_message(f"需要更新的股票: {len(stocks_to_download)}/{len(stocks)} 只", "INFO")
        
        total_pending = len(stocks_to_download)
        success_count = 0
        fail_count = 0
        validation_fail_count = 0
        missing_count = 0
        
        for idx, code in enumerate(stocks_to_download):
            if not self.is_running:
                break
            
            if self.is_paused:
                while self.is_paused and self.is_running:
                    time.sleep(0.5)
            
            start_date = need_update[code]
            self.current_stock.set(f"{code} ({idx+1}/{total_pending})")
            
            result_code, success, data, is_valid, reason = self.download_single_stock(
                code, start_date, end_date
            )
            
            if success and data:
                if is_valid:
                    self.add_to_buffer(data)
                    self.save_downloaded_record(code)
                    success_count += 1
                    self.processed_codes.add(code)
                    self.log_message(f"{code} 下载成功 ({len(data)}条)", "INFO")
                else:
                    validation_fail_count += 1
                    self.failed_stocks.append(f"{code}|day|validation_fail")
                    with open(self.failed_txt_path, 'a', encoding='utf-8') as f:
                        f.write(f"{code}|day|validation_fail\n")
                    self.log_message(f"{code} 数据校验失败: {reason}", "WARNING")
                    self.processed_codes.add(code)
            else:
                if reason == "无数据" or reason == "无有效数据":
                    missing_count += 1
                    self.missing_stocks.add(code)
                    self.log_message(f"{code} 无数据，已记录到缺失列表", "WARNING")
                    self.processed_codes.add(code)
                else:
                    fail_count += 1
                    self.failed_stocks.append(f"{code}|day|{reason}")
                    with open(self.failed_txt_path, 'a', encoding='utf-8') as f:
                        f.write(f"{code}|day|{reason}\n")
                    self.log_message(f"{code} 下载失败: {reason}", "ERROR")
                    self.processed_codes.add(code)
            
            self.processed_stocks = len(self.processed_codes)
            
            if self.buffer_size >= batch_size:
                self.flush_buffer()
            
            if self.processed_stocks % auto_save_stock_count == 0 and self.processed_stocks > 0:
                if not self.is_auto_saving and not self.auto_save_requested:
                    self.auto_save_requested = True
                    self.log_message(f"已处理 {self.processed_stocks} 只股票，触发自动保存", "INFO")
                    self.root.after(0, self.do_auto_save)
            
            progress = (self.processed_stocks / total_pending) * 100 if total_pending > 0 else 0
            self.progress_var.set(progress)
            
            elapsed_time = time.time() - self.start_time
            if elapsed_time > 0:
                speed = self.processed_stocks / elapsed_time
                self.speed_label.config(
                    text=f"速度: {speed:.2f} 只/秒 | 已处理: {self.processed_stocks}/{total_pending} | "
                         f"缓存: {self.downloaded_rows}行 | 成功: {success_count} | 失败: {fail_count} | "
                         f"缺失: {missing_count} | 校验失败: {validation_fail_count}"
                )
        
        with self.temp_table_lock:
            self.flush_buffer()
            
            if self.downloaded_rows > 0:
                self.merge_temp_to_main(checkpoint=True, batch_save=True)
                self.recreate_temp_table()
            else:
                self.log_message("没有新数据需要合并", "INFO")
        
        if fail_count > 0 or validation_fail_count > 0 or missing_count > 0:
            self.log_message(f"日线处理完成：成功{success_count}只，失败{fail_count}只，"
                           f"校验失败{validation_fail_count}只，缺失{missing_count}只", "WARNING")
        else:
            self.log_message(f"日线处理完成：成功{success_count}只", "INFO")
        
        if self.missing_stocks:
            self.log_message(f"缺失股票列表 ({len(self.missing_stocks)}只): {sorted(list(self.missing_stocks))}", "WARNING")
    
    def preload_stock_max_dates(self):
        """预加载所有股票的最新日期到缓存"""
        try:
            conn = self.get_db_connection()
            if conn is None:
                return
            
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            
            if self.day_table_name not in table_names:
                return
            
            self.log_message("正在预加载股票最新日期...", "INFO")
            
            result = conn.execute(f"""
                SELECT code, MAX(date) as max_date
                FROM {self.day_table_name}
                GROUP BY code
            """).fetchall()
            
            with self.cache_lock:
                for code, max_date in result:
                    if max_date:
                        if isinstance(max_date, str):
                            date_str = max_date[:10]
                        elif hasattr(max_date, 'strftime'):
                            date_str = max_date.strftime("%Y-%m-%d")
                        else:
                            date_str = str(max_date)[:10]
                        self.stock_max_date_cache[code] = date_str
            
            self.log_message(f"✅ 预加载完成，共 {len(self.stock_max_date_cache)} 只股票的最新日期", "INFO")
            
        except Exception as e:
            self.log_message(f"预加载股票最新日期失败: {e}", "WARNING")
    
    def show_db_stats(self):
        """显示数据库统计信息"""
        try:
            if self.db_conn is not None:
                try:
                    self.show_db_stats_with_connection(self.db_conn)
                    return
                except Exception as e:
                    self.log_message(f"使用现有连接获取统计信息失败: {e}", "DEBUG")
            
            if not os.path.exists(self.day_db_path):
                self.log_message("数据库文件不存在，尚无数据", "INFO")
                return
            
            try:
                conn = duckdb.connect(self.day_db_path, read_only=True)
            except Exception as e:
                self.log_message(f"无法连接数据库: {e}", "WARNING")
                return
            
            try:
                self.show_db_stats_with_connection(conn)
            finally:
                conn.close()
            
        except Exception as e:
            self.log_message(f"获取数据库统计信息失败: {e}", "WARNING")
    
    def show_db_stats_with_connection(self, conn):
        """使用指定连接显示数据库统计信息"""
        try:
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            
            if self.day_table_name not in table_names:
                self.log_message(f"数据表 {self.day_table_name} 不存在，尚无数据", "INFO")
                return
            
            result = conn.execute(f"""
                SELECT 
                    MIN(date) as min_date,
                    MAX(date) as max_date,
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT code) as stock_count
                FROM {self.day_table_name}
            """).fetchone()
            
            if result:
                min_date, max_date, total_rows, stock_count = result
                
                self.db_row_count = total_rows
                self.db_stock_count = stock_count
                
                if min_date:
                    if isinstance(min_date, str):
                        min_date_str = min_date[:10]
                    elif hasattr(min_date, 'strftime'):
                        min_date_str = min_date.strftime("%Y-%m-%d")
                    else:
                        min_date_str = str(min_date)[:10]
                    self.db_min_date = min_date_str
                
                if max_date:
                    if isinstance(max_date, str):
                        max_date_str = max_date[:10]
                    elif hasattr(max_date, 'strftime'):
                        max_date_str = max_date.strftime("%Y-%m-%d")
                    else:
                        max_date_str = str(max_date)[:10]
                    self.db_max_date = max_date_str
                    
                    self.log_message(f"📅 数据库统计信息:", "INFO")
                    self.log_message(f"   • 数据库: {self.day_db_path}", "INFO")
                    self.log_message(f"   • 表名: {self.day_table_name}", "INFO")
                    self.log_message(f"   • 数据范围: {min_date_str} 至 {max_date_str}", "INFO")
                    self.log_message(f"   • 总数据量: {total_rows:,} 行", "INFO")
                    self.log_message(f"   • 股票数量: {stock_count} 只", "INFO")
            
        except Exception as e:
            self.log_message(f"获取统计信息失败: {e}", "DEBUG")
    
    def download_worker_single_thread(self):
        """工作线程"""
        try:
            self.start_time = time.time()
            self.request_count = 0
            self.skip_count = 0
            self.last_save_time = time.time()
            self.stock_process_count = 0
            self.processed_codes.clear()
            self.missing_stocks.clear()
            
            if not self.stock_ipo_dates:
                self.load_ipo_dates_from_info_db()
            
            if not self.stock_ipo_dates:
                self.log_message("警告：没有上市日期数据，将使用默认起始日期 2000-01-01", "WARNING")
            
            if not os.path.exists(self.table_txt_path):
                self.log_message(f"table.txt文件不存在: {self.table_txt_path}，请先更新股票列表", "ERROR")
                self.show_auto_close_message("提示", "请先点击「更新股票列表」获取股票代码", 3000)
                return
                    
            with open(self.table_txt_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                    
            stocks = []
            for line in lines:
                line = line.strip()
                if line and not line.startswith('#'):
                    # 处理可能的格式：sz.000001 或 000001
                    code_part = line.split(',')[0].strip()
                    
                    # 关键修复：标准化代码
                    normalized = self.normalize_stock_code(code_part)
                    
                    # 排除指数（注意：平安银行000001不是指数，上证指数才是）
                    # 只有在带sh.前缀的000001才是上证指数
                    if not self.is_index_stock(code_part, normalized):
                        stocks.append(normalized)
                        if normalized == '000001':
                            self.log_message(f"✅ 已添加平安银行(000001)到下载列表", "INFO")
            
            stocks = list(dict.fromkeys(stocks))
            
            if not stocks:
                self.log_message("没有有效的股票代码", "WARNING")
                return
                
            self.total_stocks = len(stocks)
            self.processed_stocks = 0
            
            self.log_message(f"总共加载了 {len(stocks)} 只股票")
            # 显示前10只股票以便调试
            if stocks:
                self.log_message(f"前10只股票: {stocks[:10]}")
            
            os.makedirs(self.db_dir, exist_ok=True)
            
            thread_count = self.thread_count_var.get()
            batch_size = self.batch_size_var.get()
            
            self.log_message(f"开始下载股票数据，共{len(stocks)}只股票...")
            self.log_message(f"[线程数: {thread_count}] [批量提交: {batch_size}行]")
            self.log_message(f"[上市日期] 从本地数据库加载了 {len(self.stock_ipo_dates)} 只股票的上市日期")
            self.log_message(f"[断点续传] 已下载 {len(self.downloaded_stocks)} 只股票记录")
            
            if not self.ensure_login():
                self.log_message("登录失败，无法继续下载", "ERROR")
                return
            
            if self.period_vars['day'].get() and self.is_running:
                self.process_dayly_stocks_single_thread(stocks)
            
            other_periods = ['week', 'month', 'season', 'year', '60min', '30min', '15min', '5min']
            for period in other_periods:
                if self.period_vars[period].get() and self.is_running:
                    self.log_message(f"{period}选项已勾选，运行模块...")
                    try:
                        if period == '60min':
                            run_60min_module(
                                stocks=stocks,
                                force=False,
                                log_callback=lambda msg, p=period: self.log_message(f"[{p}] {msg}")
                            )
                        elif period == 'week':
                            run_weekly_module(
                                log_callback=lambda msg, p=period: self.log_message(f"[{p}] {msg}")
                            )
                        elif period == 'month':
                            run_monthly_module(
                                log_callback=lambda msg, p=period: self.log_message(f"[{p}] {msg}")
                            )
                        elif period == 'season':
                            run_seasonly_module(
                                log_callback=lambda msg, p=period: self.log_message(f"[{p}] {msg}")
                            )
                        elif period == 'year':
                            self.log_message("年线模块暂未实现", "WARNING")
                        else:
                            self.log_message(f"{period}模块暂未实现", "WARNING")
                    except Exception as e:
                        self.log_message(f"运行{period}模块失败: {e}", "ERROR")
            
            with self.temp_table_lock:
                if self.temp_table_created and self.downloaded_rows > 0:
                    self.log_message("下载完成，执行最终保存", "INFO")
                    self.flush_buffer()
                    self.merge_temp_to_main(checkpoint=True, batch_save=True)
                    self.recreate_temp_table()
            
            self.close_db_connection(checkpoint=True)
            
            total_time = time.time() - self.start_time
            avg_speed = self.total_stocks / total_time if total_time > 0 else 0
            
            if self.failed_stocks:
                self.log_message(f"处理完成，失败{len(self.failed_stocks)}只股票，缺失{len(self.missing_stocks)}只，总耗时{total_time:.1f}秒", "WARNING")
                self.show_auto_close_message("完成", f"处理完成，失败{len(self.failed_stocks)}只，缺失{len(self.missing_stocks)}只")
            else:
                self.log_message(f"所有股票处理完成，总耗时{total_time:.1f}秒", "INFO")
                self.show_auto_close_message("完成", "所有股票处理完成")
                
        except Exception as e:
            self.log_message(f"下载过程出错: {e}", "ERROR")
            traceback.print_exc()
            
            with self.temp_table_lock:
                if self.temp_table_created and self.downloaded_rows > 0:
                    self.log_message("下载出错，尝试保存已下载数据...", "WARNING")
                    try:
                        self.flush_buffer()
                        self.merge_temp_to_main(checkpoint=True, batch_save=True)
                    except:
                        pass
            
            self.close_db_connection(checkpoint=True)
        finally:
            self.is_running = False
            self.download_btn.config(state=tk.NORMAL)
            self.pause_btn.config(text="暂停")
            self.is_paused = False
    
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
        self.stock_label = ttk.Label(toolbar2, textvariable=self.current_stock, width=15)
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
        
        config_text = f"日线数据库: {os.path.basename(self.day_db_path)} | 表名: {self.day_table_name}"
        ttk.Label(config_frame, text=config_text, font=('宋体', 8)).pack(anchor=tk.W)
        
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(4, weight=1)
    
    def start_download(self):
        """开始下载"""
        if self.is_running:
            return
            
        self.save_config()
        
        if not any(var.get() for var in self.period_vars.values()):
            messagebox.showwarning("警告", "请至少选择一个下载周期")
            return
        
        if not os.path.exists(self.table_txt_path):
            messagebox.showwarning("警告", f"股票列表不存在: {self.table_txt_path}\n请先点击「更新股票列表」")
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
        
        if os.path.exists(self.failed_txt_path):
            os.remove(self.failed_txt_path)
        
        download_thread = threading.Thread(target=self.download_worker_single_thread, daemon=True)
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
                        self.log_message("程序退出，保存临时数据...", "INFO")
                        try:
                            self.flush_buffer()
                            self.merge_temp_to_main(checkpoint=True, batch_save=True)
                        except Exception as e:
                            self.log_message(f"保存数据失败: {e}", "ERROR")
                
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
                            self.log_message(f"保存数据失败: {e}", "ERROR")
            
            self.close_db_connection(checkpoint=True)
        
        if self.bs_logged_in:
            try:
                bs.logout()
            except:
                pass
                
        self.save_config()
        
        self.root.destroy()
    
    # ==================== 辅助方法 ====================
    
    def get_previous_trade_day(self) -> str:
        current_date = datetime.datetime.now()
        current_hour = current_date.hour
        current_minute = current_date.minute
        
        if current_hour < 15 or (current_hour == 15 and current_minute < 30):
            if current_date.weekday() == 0:
                previous_date = current_date - datetime.timedelta(days=3)
            elif current_date.weekday() == 6:
                previous_date = current_date - datetime.timedelta(days=2)
            else:
                previous_date = current_date - datetime.timedelta(days=1)
        else:
            if current_date.weekday() < 5:
                return current_date.strftime("%Y-%m-%d")
            else:
                days_to_subtract = current_date.weekday() - 4
                previous_date = current_date - datetime.timedelta(days=days_to_subtract)
        
        return previous_date.strftime("%Y-%m-%d")
    
    def load_config(self):
        """加载配置"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
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
                self.log_message("配置文件加载成功")
            except Exception as e:
                self.log_message(f"加载配置文件失败: {e}", "ERROR")
    
    def save_config(self):
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
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.log_message(f"保存配置文件失败: {e}", "ERROR")
    
    def copy_log_to_clipboard(self):
        try:
            log_content = self.log_text.get(1.0, tk.END).strip()
            if not log_content:
                messagebox.showinfo("提示", "日志为空")
                return
            pyperclip.copy(log_content)
            self.log_message("日志已复制到剪贴板", "INFO")
        except Exception as e:
            self.log_message(f"复制日志失败: {e}", "ERROR")
    
    def clear_log(self):
        if messagebox.askyesno("确认", "确定要清空日志吗？"):
            self.log_text.delete(1.0, tk.END)
            self.log_message("日志已清空", "INFO")
    
    def log_message(self, message: str, level: str = "INFO"):
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        formatted_msg = f"[{timestamp}] {message}\n"
        if hasattr(self, 'log_text') and self.log_text:
            self.log_text.insert(tk.END, formatted_msg)
            self.log_text.see(tk.END)
            self.status_label.config(text=message[:50])
        if level == "ERROR":
            logging.error(message)
        elif level == "WARNING":
            logging.warning(message)
        else:
            logging.info(message)
    
    def show_auto_close_message(self, title: str, message: str, delay: int = 1000):
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
    
    def safe_request_delay(self):
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        min_delay = 0.1
        if time_since_last < min_delay:
            time.sleep(min_delay - time_since_last)
        self.last_request_time = time.time()
    
    def ensure_login(self):
        current_time = time.time()
        if not self.bs_logged_in or (current_time - self.last_login_time) > 1800:
            try:
                if self.bs_logged_in:
                    try:
                        bs.logout()
                    except:
                        pass
                    self.bs_logged_in = False
                self.log_message("正在登录baostock...")
                lg = bs.login()
                if lg.error_code != '0':
                    raise Exception(f"登录失败: {lg.error_msg}")
                self.bs_logged_in = True
                self.bs_session = lg
                self.last_login_time = current_time
                self.log_message("baostock登录成功")
            except Exception as e:
                self.log_message(f"登录失败: {e}", "ERROR")
                return False
        return True
    
    def check_and_relogin(self, error_msg: str) -> bool:
        error_msg_lower = error_msg.lower()
        login_errors = ['登录', 'login', 'auth', '认证', 'session', '未登录', 'token', 'expired', '过期', '无效', 'invalid']
        for error_keyword in login_errors:
            if error_keyword in error_msg_lower:
                self.log_message(f"检测到登录相关错误，尝试重新登录: {error_msg}", "WARNING")
                self.bs_logged_in = False
                return self.ensure_login()
        return True
    
    def extract_digits(self, code: str) -> str:
        """提取代码中的数字部分"""
        digits = re.findall(r'\d+', code)
        if digits:
            return digits[0]
        return code
    
    def normalize_stock_code(self, code: str) -> str:
        """标准化股票代码为6位数字"""
        code = code.strip().upper()
        
        # 如果已经是6位数字，直接返回
        if re.match(r'^\d{6}$', code):
            return code
        
        # 处理带前缀的格式，如 sh.600000 或 sz.000001
        if '.' in code:
            parts = code.split('.')
            if len(parts) == 2:
                # 提取数字部分
                digits = self.extract_digits(parts[1])
                if len(digits) >= 6:
                    return digits[:6]
                elif len(digits) > 0:
                    return digits.zfill(6)
        
        # 提取所有数字
        digits = self.extract_digits(code)
        
        # 确保是6位数字
        if len(digits) >= 6:
            return digits[:6]
        
        # 补足到6位
        if len(digits) > 0:
            return digits.zfill(6)
        
        # 如果什么都没有，返回原代码
        return code
    
    def is_index_stock(self, code_with_prefix: str, normalized_code: str = None) -> bool:
        """判断是否为指数
        Args:
            code_with_prefix: 原始代码，可能带前缀如'sh.000001'或'sz.000001'
            normalized_code: 标准化后的6位数字代码
        """
        # 如果有带前缀的代码，优先使用带前缀的判断
        if code_with_prefix:
            # 带sh.前缀的000001才是上证指数
            if code_with_prefix == 'sh.000001':
                return True
            # 带sz.前缀的000001是平安银行，不是指数
            if code_with_prefix == 'sz.000001':
                return False
        
        # 如果没有带前缀的代码或需要进一步判断，使用标准化代码
        if normalized_code is None:
            if code_with_prefix:
                normalized_code = self.normalize_stock_code(code_with_prefix)
            else:
                return False
        
        # 深圳指数代码（这些是真正的指数）
        sz_index_codes = {
            '399001',   # 深证成指
            '399006',   # 创业板指
            '399005',   # 中小板指
            '399300',   # 沪深300（深圳）
        }
        
        # 上海指数代码
        sh_index_codes = {
            '000300',   # 沪深300
            '000016',   # 上证50
            '000905',   # 中证500
            '000688',   # 科创50
            '000852',   # 中证1000
            '000903',   # 中证100
            '000010',   # 上证180
            '000009',   # 上证380
            '000015',   # 上证红利
            '000922',   # 中证红利
        }
        
        # 注意：000001是平安银行，不是指数，所以不在这里排除
        # 上证指数的代码是sh.000001，已经在上面处理了
        
        return normalized_code in sz_index_codes or normalized_code in sh_index_codes
    
    def load_downloaded_records(self) -> Set[str]:
        if os.path.exists(self.downloaded_record_file):
            try:
                with open(self.downloaded_record_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return set(data.get('downloaded', []))
            except Exception as e:
                self.log_message(f"加载下载记录失败: {e}", "WARNING")
        return set()
    
    def save_downloaded_record(self, code: str):
        with self.lock:
            self.downloaded_stocks.add(code)
            try:
                with open(self.downloaded_record_file, 'w', encoding='utf-8') as f:
                    json.dump({'downloaded': list(self.downloaded_stocks)}, f, ensure_ascii=False, indent=2)
            except Exception as e:
                self.log_message(f"保存下载记录失败: {e}", "WARNING")
    
    def update_stock_list(self):
        """更新股票列表"""
        try:
            self.log_message("正在更新股票列表...")
            if not self.ensure_login():
                return
            
            rs = bs.query_all_stock()
            if rs.error_code != '0':
                raise Exception(f"获取股票列表失败: {rs.error_msg}")
            
            data_list = []
            stock_codes = []
            
            while (rs.error_code == '0') and rs.next():
                data_list.append(rs.get_row_data())
            
            if data_list:
                self.log_message(f"获取到 {len(data_list)} 只股票基本信息...")
                
                for i, row in enumerate(data_list):
                    if len(row) >= 1:
                        code = row[0]
                        
                        # 识别A股股票
                        # 上海：6开头，688开头
                        # 深圳：0开头，3开头
                        is_a_share = (
                            code.startswith('sh.6') or      # 上海主板
                            code.startswith('sh.688') or    # 上海科创板
                            code.startswith('sz.0') or      # 深圳主板
                            code.startswith('sz.3')         # 深圳创业板
                        )
                        
                        if is_a_share:
                            # 排除指数代码
                            exclude_codes = [
                                'sh.000001',   # 上证指数
                                'sz.399001',   # 深证成指
                                'sh.000300',   # 沪深300
                                'sh.000016',   # 上证50
                                'sh.000905',   # 中证500
                                'sh.000688',   # 科创50
                                'sz.399006',   # 创业板指
                                'sz.399005',   # 中小板指
                                'sh.000852',   # 中证1000
                                'sh.000010',   # 上证180
                            ]
                            
                            if code not in exclude_codes:
                                normalized = self.normalize_stock_code(code)
                                stock_codes.append(normalized)
                    
                    # 进度提示
                    if (i + 1) % 100 == 0:
                        self.log_message(f"已处理 {i+1}/{len(data_list)} 只股票...")
                
                # 去重并排序
                stock_codes = sorted(list(set(stock_codes)))
                
                # 保存到文件
                with open(self.table_txt_path, 'w', encoding='utf-8') as f:
                    for code in stock_codes:
                        f.write(f"{code}\n")
                
                self.log_message(f"✅ 股票列表更新完成，共{len(stock_codes)}只股票")
                self.show_auto_close_message("成功", f"股票列表更新完成，共{len(stock_codes)}只股票")
            else:
                self.log_message("未获取到股票数据", "WARNING")
                
        except Exception as e:
            self.log_message(f"更新股票列表失败: {e}", "ERROR")
            self.show_auto_close_message("错误", f"更新失败: {e}")
    
    def safe_decode_binary(self, data):
        if data is None:
            return ""
        if isinstance(data, str):
            return data
        if isinstance(data, bytes):
            encodings = ['utf-8', 'gbk', 'gb2312', 'latin1', 'cp1252']
            for enc in encodings:
                try:
                    return data.decode(enc)
                except UnicodeDecodeError:
                    continue
            try:
                return data.decode('utf-8', errors='ignore')
            except:
                pass
            try:
                return data.hex()
            except:
                return str(data)
        return str(data)
    

    
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
            self.log_message("下载已暂停")
        else:
            self.pause_btn.config(text="暂停")
            self.log_message("下载继续")
    
    def retry_failed(self):
        if not os.path.exists(self.failed_txt_path):
            messagebox.showinfo("提示", "没有失败的股票需要重试")
            return
        with open(self.failed_txt_path, 'r', encoding='utf-8') as f:
            failed_items = [line.strip() for line in f if line.strip()]
        if not failed_items:
            messagebox.showinfo("提示", "没有失败的股票需要重试")
            return
        failed_stocks = []
        for item in failed_items:
            if '|' in item:
                failed_stocks.append(item.split('|')[0])
            else:
                failed_stocks.append(item)
        failed_stocks = list(set(failed_stocks))
        temp_file = os.path.join(self.work_dir, 'table_temp.txt')
        with open(temp_file, 'w', encoding='utf-8') as f:
            for stock in failed_stocks:
                f.write(f"{stock}\n")
        backup_file = os.path.join(self.work_dir, 'table_backup.txt')
        if os.path.exists(self.table_txt_path):
            os.rename(self.table_txt_path, backup_file)
        os.rename(temp_file, self.table_txt_path)
        self.start_download()
        def restore_file():
            while self.is_running:
                time.sleep(1)
            if os.path.exists(backup_file):
                os.rename(backup_file, self.table_txt_path)
            if os.path.exists(self.failed_txt_path):
                os.remove(self.failed_txt_path)
        threading.Thread(target=restore_file, daemon=True).start()
    
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
                    pyperclip.copy(content)
                    self.log_message("上市日期已复制到剪贴板", "INFO")
                except Exception as e:
                    self.log_message(f"复制失败: {e}", "ERROR")
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
            self.log_message(f"查看上市日期失败: {e}", "ERROR")
    
    def logout(self):
        if self.bs_logged_in:
            try:
                bs.logout()
                self.bs_logged_in = False
                self.log_message("baostock已登出")
                self.show_auto_close_message("成功", "登出成功")
            except Exception as e:
                self.log_message(f"登出失败: {e}", "ERROR")
        else:
            self.log_message("当前未登录")
    
    def run(self):
        """运行程序"""
        self.root.mainloop()


if __name__ == "__main__":
    try:
        app = StockDownloader()
        app.run()
    except Exception as e:
        print(f"程序启动失败: {e}")
        input("按回车键退出...")