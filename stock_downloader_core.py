"""
股票数据下载器 - 核心业务逻辑模块
所有下载、数据库操作、登录等业务逻辑在此模块中实现。
GUI通过回调函数与核心交互，不直接依赖tkinter。
"""
import os
import re
import sys
import time
import json
import csv
import signal
import queue
import threading
import datetime
import logging
import traceback
from typing import List, Dict, Optional, Tuple, Set

import duckdb
import baostock as bs
import warnings

warnings.filterwarnings('ignore')


class StockDownloaderCore:
    """股票下载核心逻辑类，无GUI依赖"""

    def __init__(self, data_dir: str, db_paths: dict, file_paths: dict,
                 period_config: dict, callbacks: dict):
        """
        data_dir: 数据目录
        db_paths: 各周期数据库路径 {key: path}
        file_paths: 配置文件路径 {key: path}
        period_config: 周期配置
        callbacks: {
            'log': callable(message, level),
            'update_progress': callable(value),
            'update_speed': callable(text),
            'update_current_stock': callable(text),
            'show_message': callable(title, message, delay),
            'show_error': callable(title, message),
        }
        """
        self.data_dir = data_dir
        self.db_paths = db_paths
        self.file_paths = file_paths
        self.period_config = period_config
        self.callbacks = callbacks

        # 内部日志
        self._logger = logging.getLogger('stock_core')
        self._logger.setLevel(logging.INFO)
        self._logger.handlers.clear()
        self._logger.propagate = False
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self._logger.addHandler(handler)

        # 状态变量
        self.is_running = False
        self.is_paused = False
        self.is_saving = False
        self.failed_stocks = []
        self.bs_logged_in = False
        self.bs_session = None

        # 线程相关
        self.lock = threading.Lock()
        self.db_lock = threading.Lock()
        self.temp_table_lock = threading.RLock()
        self.cache_lock = threading.Lock()
        self.available_dates_lock = threading.Lock()
        self.data_buffer_lock = threading.Lock()

        # 缓存
        self.stock_ipo_dates = {}
        self.stock_max_date_cache = {}
        self.available_dates_cache = {}
        self.downloaded_stocks = set()

        # 临时数据表
        self.temp_table_name = None
        self.temp_table_created = False
        self.downloaded_rows = 0
        self.stock_process_count = 0
        self.data_buffer = []
        self.buffer_size = 0

        # 数据库连接
        self.db_conn = None

        # 网络请求统计
        self.request_count = 0
        self.last_request_time = time.time()

        # 智能跳过计数
        self.skip_count = 0
        self.skip_threshold = 50

        # 当前批次
        self.current_batch_stocks = []
        self.processed_codes = set()

        # 结果队列
        self.result_queue = queue.Queue()

        # 数据校验
        self.data_validation_errors = []

        # 数据库统计
        self.db_max_date = None
        self.db_min_date = None
        self.db_stock_count = 0
        self.db_row_count = 0

        # 定时保存
        self.last_save_time = time.time()
        self.save_interval = 180
        self.auto_save_trigger_count = 100
        self.is_auto_saving = False
        self.auto_save_requested = False

        # 重试机制
        self.max_retries = 3
        self.retry_delay = 5
        self.stock_retry_count = {}

        # 缺失股票记录
        self.missing_stocks = set()

        # 下载统计
        self.start_time = 0
        self.total_stocks = 0
        self.processed_stocks = 0

    # ==================== 日志与UI回调 ====================

    def _log(self, message: str, level: str = "INFO"):
        cb = self.callbacks.get('log')
        if cb:
            cb(message, level)
        else:
            self._logger.log(getattr(logging, level.upper(), logging.INFO), message)

    def _update_progress(self, value: float):
        cb = self.callbacks.get('update_progress')
        if cb:
            cb(value)

    def _update_speed(self, text: str):
        cb = self.callbacks.get('update_speed')
        if cb:
            cb(text)

    def _update_current_stock(self, text: str):
        cb = self.callbacks.get('update_current_stock')
        if cb:
            cb(text)

    def _show_message(self, title: str, message: str, delay: int = 1000):
        cb = self.callbacks.get('show_message')
        if cb:
            cb(title, message, delay)

    def _show_error(self, title: str, message: str):
        cb = self.callbacks.get('show_error')
        if cb:
            cb(title, message)

    # ==================== 路径工具 ====================

    @staticmethod
    def get_program_dir() -> str:
        if getattr(sys, 'frozen', False):
            return os.path.dirname(sys.executable)
        return os.path.dirname(os.path.abspath(__file__))

    @staticmethod
    def is_temp_directory(path: str) -> bool:
        temp_patterns = ['temp', 'tmp', 'Temp', 'Tmp', 'AppData\\Local\\Temp',
                         '/tmp', '/var/tmp', '~/.cache']
        path_lower = path.lower()
        return any(p.lower() in path_lower for p in temp_patterns)

    def get_work_dir(self) -> str:
        cwd = os.getcwd()
        if not self.is_temp_directory(cwd):
            return cwd
        try:
            import ctypes.wintypes
            CSIDL_PERSONAL = 5
            buf = ctypes.create_unicode_buffer(ctypes.wintypes.MAX_PATH)
            ctypes.windll.shell32.SHGetFolderPathW(None, CSIDL_PERSONAL, None, 0, buf)
            docs_dir = buf.value
            if docs_dir and not self.is_temp_directory(docs_dir):
                return docs_dir
        except Exception:
            pass
        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        if os.path.exists(desktop) and not self.is_temp_directory(desktop):
            return desktop
        home = os.path.expanduser("~")
        if not self.is_temp_directory(home):
            return home
        return self.get_program_dir()

    # ==================== 数据库操作 ====================

    def get_db_connection(self, db_path: str = None):
        """获取数据库连接"""
        db_path = db_path or self.db_paths.get('day', '')
        with self.db_lock:
            try:
                if self.db_conn is not None:
                    try:
                        self.db_conn.execute("SELECT 1").fetchone()
                        return self.db_conn
                    except Exception:
                        self._log(f"数据库连接失效，重新连接", "DEBUG")
                        try:
                            self.db_conn.close()
                        except Exception:
                            pass
                        self.db_conn = None

                os.makedirs(os.path.dirname(db_path), exist_ok=True)
                self.db_conn = duckdb.connect(db_path)
                try:
                    self.db_conn.execute("PRAGMA wal_autocheckpoint = 0")
                    self.db_conn.execute("PRAGMA checkpoint_threshold = 1000000")
                    self.db_conn.execute("PRAGMA synchronous = NORMAL")
                except Exception:
                    pass
                try:
                    self.db_conn.execute("PRAGMA memory_limit='4GB'")
                except Exception:
                    pass
                try:
                    self.db_conn.execute("PRAGMA threads=4")
                except Exception:
                    pass
                self._log(f"数据库连接成功: {db_path}", "DEBUG")
                return self.db_conn
            except Exception as e:
                self._log(f"数据库连接失败: {e}", "ERROR")
                return None

    def close_db_connection(self, db_path: str = None, checkpoint: bool = True):
        """关闭数据库连接"""
        with self.db_lock:
            if self.db_conn:
                try:
                    if checkpoint:
                        self._log("正在执行检查点，合并WAL文件...", "INFO")
                        self.db_conn.execute("CHECKPOINT")
                        self._log("WAL文件合并完成", "INFO")
                    self.db_conn.close()
                except Exception as e:
                    self._log(f"关闭数据库连接时出错: {e}", "ERROR")
                self.db_conn = None
                self._log("数据库连接已关闭", "DEBUG")

    def ensure_table_exists(self, table_name: str = None, db_path: str = None):
        """确保日线数据表存在"""
        table_name = table_name or 'dayly_stock_data'
        db_path = db_path or self.db_paths.get('day', '')
        try:
            conn = self.get_db_connection(db_path)
            if conn is None:
                self._log("无法获取数据库连接", "ERROR")
                return False
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            if table_name not in table_names:
                self._log(f"创建 {table_name} 表...", "INFO")
                conn.execute(f"""
                    CREATE TABLE {table_name} (
                        code VARCHAR, date DATE, open DOUBLE, high DOUBLE,
                        low DOUBLE, close DOUBLE, volume DOUBLE, amount DOUBLE
                    )
                """)
                try:
                    conn.execute(f"CREATE INDEX idx_{table_name}_code ON {table_name}(code)")
                    conn.execute(f"CREATE INDEX idx_{table_name}_date ON {table_name}(date)")
                except Exception:
                    pass
                self._log(f"✅ {table_name} 表创建完成", "INFO")
            return True
        except Exception as e:
            self._log(f"检查/创建表失败: {e}", "ERROR")
            return False

    def create_temp_table(self, db_path: str = None):
        """创建临时数据表"""
        db_path = db_path or self.db_paths.get('day', '')
        with self.temp_table_lock:
            try:
                conn = self.get_db_connection(db_path)
                if conn is None:
                    self._log("无法获取数据库连接，无法创建临时表", "ERROR")
                    return False
                if self.temp_table_created and self.temp_table_name:
                    try:
                        conn.execute(f"DROP TABLE IF EXISTS {self.temp_table_name}")
                    except Exception:
                        pass
                self.temp_table_name = f"temp_download_{int(time.time())}"
                conn.execute(f"""
                    CREATE TABLE {self.temp_table_name} (
                        code VARCHAR, date DATE, open DOUBLE, high DOUBLE,
                        low DOUBLE, close DOUBLE, volume DOUBLE, amount DOUBLE
                    )
                """)
                self.temp_table_created = True
                self.downloaded_rows = 0
                self.stock_process_count = 0
                self.data_buffer = []
                self.buffer_size = 0
                self._log(f"创建临时表: {self.temp_table_name}", "INFO")
                return True
            except Exception as e:
                self._log(f"创建临时表失败: {e}", "ERROR")
                return False

    def recreate_temp_table(self, db_path: str = None):
        """重新创建临时表"""
        db_path = db_path or self.db_paths.get('day', '')
        with self.temp_table_lock:
            try:
                conn = self.get_db_connection(db_path)
                if conn is None:
                    self._log("无法获取数据库连接，无法重建临时表", "ERROR")
                    return False
                if self.temp_table_name:
                    try:
                        conn.execute(f"DROP TABLE IF EXISTS {self.temp_table_name}")
                    except Exception:
                        pass
                self.temp_table_name = f"temp_download_{int(time.time())}"
                conn.execute(f"""
                    CREATE TABLE {self.temp_table_name} (
                        code VARCHAR, date DATE, open DOUBLE, high DOUBLE,
                        low DOUBLE, close DOUBLE, volume DOUBLE, amount DOUBLE
                    )
                """)
                self.temp_table_created = True
                self.downloaded_rows = 0
                self._log(f"重新创建临时表: {self.temp_table_name}", "INFO")
                return True
            except Exception as e:
                self._log(f"重建临时表失败: {e}", "ERROR")
                self.temp_table_created = False
                return False

    def merge_temp_to_main(self, table_name: str = None, db_path: str = None,
                           checkpoint: bool = True, batch_save: bool = False):
        """将临时表数据合并到主表"""
        table_name = table_name or 'dayly_stock_data'
        db_path = db_path or self.db_paths.get('day', '')
        with self.temp_table_lock:
            if not self.temp_table_created:
                self._log("临时表不存在，无需合并", "INFO")
                return
            try:
                conn = self.get_db_connection(db_path)
                if conn is None:
                    self._log("无法获取数据库连接", "ERROR")
                    return
                tables = conn.execute("SHOW TABLES").fetchall()
                table_names = [t[0] for t in tables]
                if self.temp_table_name not in table_names:
                    self._log(f"临时表 {self.temp_table_name} 不存在", "WARNING")
                    self.temp_table_created = False
                    return
                temp_count = conn.execute(f"SELECT COUNT(*) FROM {self.temp_table_name}").fetchone()[0]
                if temp_count == 0:
                    self._log("临时表为空，无需合并", "INFO")
                    conn.execute(f"DROP TABLE {self.temp_table_name}")
                    self.temp_table_created = False
                    self.downloaded_rows = 0
                    return
                self._log(f"开始合并数据到主表 {table_name}，临时表共 {temp_count} 行...", "INFO")
                inserted_count = conn.execute(f"""
                    SELECT COUNT(*) FROM {self.temp_table_name} t
                    ANTI JOIN {table_name} d ON t.code = d.code AND t.date = d.date
                """).fetchone()[0]
                if inserted_count > 0:
                    conn.execute(f"""
                        INSERT INTO {table_name}
                        SELECT t.* FROM {self.temp_table_name} t
                        ANTI JOIN {table_name} d ON t.code = d.code AND t.date = d.date
                    """)
                    self._log(f"✅ 合并完成：临时表 {temp_count} 行，新增 {inserted_count} 行到主表")
                else:
                    self._log(f"临时表 {temp_count} 行数据都已存在，无需新增")
                if checkpoint:
                    self._log("执行检查点，合并WAL文件...", "INFO")
                    conn.execute("CHECKPOINT")
                    self._log("WAL文件合并完成", "INFO")
                conn.execute(f"DROP TABLE {self.temp_table_name}")
                self.temp_table_created = False
                self.downloaded_rows = 0
                self.stock_process_count = 0
                self.update_cache_after_save(conn, table_name)
                self.show_db_stats_with_connection(conn, table_name)
            except Exception as e:
                self._log(f"合并数据失败: {e}", "ERROR")
                traceback.print_exc()

    def update_cache_after_save(self, conn, table_name: str):
        """保存后更新缓存"""
        try:
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            if table_name not in table_names:
                return
            self._log("正在更新股票最新日期缓存...", "INFO")
            result = conn.execute(f"""
                SELECT code, MAX(date) as max_date FROM {table_name} GROUP BY code
            """).fetchall()
            with self.cache_lock:
                self.stock_max_date_cache.clear()
                for code, max_date in result:
                    if max_date:
                        date_str = self._format_date(max_date)
                        self.stock_max_date_cache[code] = date_str
            self._log(f"✅ 缓存更新完成，共 {len(self.stock_max_date_cache)} 只股票的最新日期", "INFO")
        except Exception as e:
            self._log(f"更新缓存失败: {e}", "WARNING")

    def add_to_buffer(self, data):
        with self.data_buffer_lock:
            self.data_buffer.extend(data)
            self.buffer_size += len(data)

    def flush_buffer(self, db_path: str = None):
        """将缓冲区数据写入临时表"""
        try:
            db_path = db_path or self.db_paths.get('day', '')
            with self.data_buffer_lock:
                if not self.data_buffer:
                    return 0
                with self.temp_table_lock:
                    if not self.temp_table_created:
                        self._log("临时表不存在，无法写入数据", "ERROR")
                        return 0
                    conn = self.get_db_connection(db_path)
                    if conn is None:
                        self._log("无法获取数据库连接", "ERROR")
                        return 0
                    chunk_size = 5000
                    total_inserted = 0
                    for i in range(0, len(self.data_buffer), chunk_size):
                        chunk = self.data_buffer[i:i + chunk_size]
                        placeholders = ','.join(['(?,?,?,?,?,?,?,?)'] * len(chunk))
                        flat_values = []
                        for row in chunk:
                            flat_values.extend(row)
                        try:
                            conn.execute(f"INSERT INTO {self.temp_table_name} VALUES {placeholders}", flat_values)
                            total_inserted += len(chunk)
                        except Exception as e:
                            self._log(f"批量插入失败: {e}，尝试逐条插入", "WARNING")
                            for row in chunk:
                                try:
                                    conn.execute(
                                        f"INSERT INTO {self.temp_table_name} VALUES (?,?,?,?,?,?,?,?)", row)
                                    total_inserted += 1
                                except Exception as e2:
                                    self._log(f"逐条插入失败: {e2}", "ERROR")
                    self.downloaded_rows += total_inserted
                    self._log(f"批量提交 {total_inserted} 行到临时表，累计 {self.downloaded_rows} 行", "DEBUG")
                    self.data_buffer = []
                    self.buffer_size = 0
                    return total_inserted
        except Exception as e:
            self._log(f"下载工作线程异常: {e}", "ERROR")
            import traceback
            self._log(traceback.format_exc(), "DEBUG")
            return 0

    # ==================== 自动保存 ====================

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
                            self._log("自动保存触发（定时）：将临时数据合并到主表", "INFO")
                            cb = self.callbacks.get('do_auto_save')
                            if cb:
                                cb()
            self._log("自动保存定时器已停止", "DEBUG")

        t = threading.Thread(target=auto_save, daemon=True)
        t.start()
        return t

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
            self._log(f"自动保存失败: {e}", "ERROR")
        finally:
            self.is_auto_saving = False
            self.auto_save_requested = False

    def manual_save_data(self):
        """手动保存数据"""
        if self.is_saving:
            self._show_message("提示", "正在保存中，请稍候...")
            return
        if not self.temp_table_created or self.downloaded_rows == 0:
            self._show_message("提示", "没有临时数据需要保存")
            return
        self.is_saving = True
        try:
            self._log("手动保存触发：开始合并临时数据到主表", "INFO")
            with self.temp_table_lock:
                self.flush_buffer()
                self.merge_temp_to_main(checkpoint=True, batch_save=True)
                self.recreate_temp_table()
            self._log("手动保存完成，已重建临时表", "INFO")
            self._show_message("保存完成", "数据保存完成")
        except Exception as e:
            self._log(f"手动保存失败: {e}", "ERROR")
            self._show_error("保存失败", f"保存失败: {e}")
        finally:
            self.is_saving = False

    # ==================== 股票日期查询 ====================

    def get_stock_max_date(self, stock_code: str, db_path: str = None, use_cache: bool = True) -> Optional[str]:
        """获取股票最大日期"""
        db_path = db_path or self.db_paths.get('day', '')
        if use_cache:
            with self.cache_lock:
                if stock_code in self.stock_max_date_cache:
                    return self.stock_max_date_cache[stock_code]
        try:
            conn = self.get_db_connection(db_path)
            if conn is None:
                return None
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            if 'dayly_stock_data' not in table_names:
                return None
            result = conn.execute(
                "SELECT MAX(date) FROM dayly_stock_data WHERE code = ?", [stock_code]).fetchone()
            if result and result[0]:
                date_str = self._format_date(result[0])
                with self.cache_lock:
                    self.stock_max_date_cache[stock_code] = date_str
                return date_str
        except Exception as e:
            if "does not exist" not in str(e):
                self._log(f"获取最大日期失败 {stock_code}: {e}", "DEBUG")
        return None

    def get_stock_ipo_date(self, code: str) -> Optional[str]:
        return self.stock_ipo_dates.get(code)

    def get_stock_max_dates_batch(self, codes: List[str], db_path: str = None) -> Dict[str, Optional[str]]:
        """批量获取股票的最大日期"""
        db_path = db_path or self.db_paths.get('day', '')
        if not codes:
            return {}
        try:
            conn = self.get_db_connection(db_path)
            if conn is None:
                return {}
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            if 'dayly_stock_data' not in table_names:
                return {code: None for code in codes}
            placeholders = ','.join(['?'] * len(codes))
            result = conn.execute(f"""
                SELECT code, MAX(date) as max_date FROM dayly_stock_data
                WHERE code IN ({placeholders}) GROUP BY code
            """, codes).fetchall()
            max_dates = {}
            for code, max_date in result:
                max_dates[code] = self._format_date(max_date) if max_date else None
            for code in codes:
                if code not in max_dates:
                    max_dates[code] = None
            return max_dates
        except Exception as e:
            self._log(f"批量获取最大日期失败: {e}", "ERROR")
            return {code: None for code in codes}

    def get_available_dates_from_baostock(self, stock_code: str, start_date: str, end_date: str,
                                          frequency: str = "d") -> List[str]:
        """从baostock查询指定股票的可下载日期"""
        cache_key = (stock_code, start_date, end_date, frequency)
        with self.available_dates_lock:
            if cache_key in self.available_dates_cache:
                return self.available_dates_cache[cache_key]
        try:
            bs_code = self._to_bs_code(stock_code)
            self.safe_request_delay()
            rs = bs.query_history_k_data_plus(bs_code, "date", start_date=start_date, end_date=end_date,
                                              frequency=frequency, adjustflag="3")
            if rs.error_code != '0':
                return []
            dates = []
            while rs.error_code == '0' and rs.next():
                row_data = rs.get_row_data()
                if row_data and row_data[0]:
                    date_str = row_data[0].strip()
                    if date_str and len(date_str) >= 10:
                        dates.append(date_str[:10])
            with self.available_dates_lock:
                self.available_dates_cache[cache_key] = dates
            return dates
        except Exception as e:
            self._log(f"查询可下载日期时出错 {stock_code}: {e}", "ERROR")
            return []

    def _group_stocks_by_last_date(self, stocks: List[str], end_date: str,
                                   sample_size: int = 10) -> Tuple[List[Tuple[str, List[str]]], List[str]]:
        """按最后下载日期分组股票，每组抽样查询baostock可下载日期"""
        max_dates = self.get_stock_max_dates_batch(stocks)
        MIN_IPO_DATE = "2010-01-01"
        groups: Dict[str, List[str]] = {}
        no_ipo_stocks = []
        no_db_stocks = []
        for code in stocks:
            max_date = max_dates.get(code)
            if max_date is None:
                ipo_date = self.get_stock_ipo_date(code)
                if ipo_date:
                    start = MIN_IPO_DATE if ipo_date < MIN_IPO_DATE else ipo_date
                    groups.setdefault(start, []).append(code)
                else:
                    no_ipo_stocks.append(code)
            else:
                try:
                    last_dt = datetime.datetime.strptime(max_date, "%Y-%m-%d")
                    next_day = (last_dt + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                    groups.setdefault(next_day, []).append(code)
                except Exception:
                    groups.setdefault(max_date, []).append(code)
        all_groups = []
        all_no_data = []
        for start_date, code_list in groups.items():
            if not code_list:
                continue
            sample = code_list[:sample_size] if len(code_list) > sample_size else code_list
            has_data = False
            available_dates = []
            for sc in sample:
                dates = self.get_available_dates_from_baostock(sc, start_date, end_date, "d")
                if dates:
                    has_data = True
                    available_dates.extend(dates)
            if has_data:
                all_groups.append((start_date, code_list))
            else:
                all_no_data.extend(code_list)
        if no_ipo_stocks:
            all_no_data.extend(no_ipo_stocks)
        return all_groups, all_no_data

    # ==================== 数据校验与下载 ====================

    def validate_stock_data(self, code: str, data: List[tuple]) -> bool:
        """验证股票数据的完整性和正确性"""
        if not data:
            return True
        try:
            for row in data:
                if row[0] != code:
                    self._log(f"数据校验失败: {code} 的数据中包含其他股票代码 {row[0]}", "ERROR")
                    return False
            current_year = datetime.datetime.now().year
            for row in data:
                date_str = row[1]
                if date_str:
                    try:
                        date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                        if date_obj.year < 1990 or date_obj.year > current_year + 1:
                            self._log(f"数据校验失败: {code} 的日期 {date_str} 不在合理范围内", "ERROR")
                            return False
                    except Exception:
                        self._log(f"数据校验失败: {code} 的日期格式错误 {date_str}", "ERROR")
                        return False
            for row in data:
                open_val, high_val, low_val, close_val = row[2], row[3], row[4], row[5]
                for price in [open_val, high_val, low_val, close_val]:
                    if price < 0 or price > 100000:
                        self._log(f"数据校验失败: {code} 的价格 {price} 超出合理范围", "ERROR")
                        return False
                if high_val < low_val:
                    self._log(f"数据校验失败: {code} 的最高价 {high_val} 小于最低价 {low_val}", "ERROR")
                    return False
            for row in data:
                volume, amount = row[6], row[7]
                if volume < 0 or volume > 1e12:
                    self._log(f"数据校验失败: {code} 的成交量 {volume} 超出合理范围", "ERROR")
                    return False
                if amount < 0 or amount > 1e15:
                    self._log(f"数据校验失败: {code} 的成交额 {amount} 超出合理范围", "ERROR")
                    return False
            return True
        except Exception as e:
            self._log(f"数据校验过程出错: {e}", "ERROR")
            return False

    def download_single_stock(self, code: str, start_date: str, end_date: str,
                              frequency: str = "d") -> Tuple[str, bool, Optional[list], bool, str]:
        """下载单只股票数据"""
        config = self.period_config.get('day', {})
        retry_count = config.get('retry_count', 5)
        bs_field = "date" if frequency == "d" else f"{frequency}date"
        if frequency == "d":
            bs_columns = "date,open,high,low,close,volume,amount"
        else:
            bs_columns = "open,high,low,close,volume,amount"

        for attempt in range(retry_count):
            try:
                self.safe_request_delay()
                bs_code = self._to_bs_code(code)
                self._log(f"下载{code} {start_date}~{end_date}", "INFO")

                try:
                    signal.signal(signal.SIGALRM, lambda signum, frame: (_ for _ in ()).throw(TimeoutError("查询超时")))
                    signal.alarm(60)
                except Exception:
                    pass

                try:
                    rs = bs.query_history_k_data_plus(
                        bs_code, bs_columns, start_date=start_date, end_date=end_date,
                        frequency=frequency, adjustflag="3")
                finally:
                    try:
                        signal.alarm(0)
                    except Exception:
                        pass

                if rs.error_code != '0':
                    if "没有数据" in rs.error_msg:
                        return code, False, None, True, "无数据"
                    raise Exception(f"获取数据失败: {rs.error_msg}")

                data_list = []
                error_count = 0
                max_errors = 5
                self._log(f"正在获取{code}的数据...", "INFO")

                while rs.error_code == '0' and rs.next():
                    try:
                        row_data = rs.get_row_data()
                        safe_row = []
                        for item in row_data:
                            if item is None:
                                safe_row.append("")
                            elif isinstance(item, bytes):
                                safe_row.append(self.safe_decode_binary(item))
                            else:
                                safe_row.append(str(item))
                        if len(safe_row) >= 7 and safe_row[0] and safe_row[0].strip():
                            data_list.append(safe_row)
                        else:
                            error_count += 1
                            if error_count > max_errors:
                                break
                        if len(data_list) % 1000 == 0 and len(data_list) > 0:
                            self._log(f"已获取 {len(data_list)} 条数据...", "INFO")
                        if len(data_list) >= 10000:
                            self._log("已达到单次下载上限10000条，停止获取", "WARNING")
                            break
                    except Exception:
                        error_count += 1
                        if error_count > max_errors:
                            break
                        continue

                self._log(f"{code} 共获取到 {len(data_list)} 条原始数据", "INFO")
                if not data_list:
                    return code, False, None, True, "无数据"

                processed_data = []
                for row in data_list:
                    if len(row) >= 7:
                        try:
                            date_str = row[0].strip()[:10] if row[0] else ""
                            vals = []
                            for val in row[1:]:
                                vals.append(float(val) if val and val not in ['', 'None'] else 0.0)
                            processed_data.append((code, date_str, *vals))
                        except ValueError as e:
                            self._log(f"数据转换错误: {e}, 行数据: {row}", "DEBUG")
                            continue

                self._log(f"{code} 处理完成，有效数据 {len(processed_data)} 条", "INFO")
                if processed_data:
                    return code, True, processed_data, True, "成功"
                else:
                    return code, False, None, True, "无有效数据"

            except TimeoutError:
                self._log(f"{code} 查询超时，重试 {attempt + 1}/{retry_count}", "WARNING")
                if attempt < retry_count - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue
                return code, False, None, True, "下载超时"
            except Exception as e:
                error_msg = str(e)
                self._log(f"{code} 下载出错: {error_msg}", "ERROR")
                self.check_and_relogin(error_msg)
                if "网络" in error_msg or "timeout" in error_msg.lower() or "连接" in error_msg:
                    if attempt < retry_count - 1:
                        wait_time = (attempt + 1) * 5
                        self._log(f"{code} 网络错误，{wait_time}秒后重试...", "WARNING")
                        time.sleep(wait_time)
                        continue
                return code, False, None, True, f"下载错误: {error_msg[:50]}"

        return code, False, None, True, "超过最大重试次数"

    # ==================== 下载工作流 ====================

    def process_dayly_stocks_single_thread(self, stocks: List[str]):
        """单线程处理日线数据"""
        batch_size = self.period_config['day'].get('batch_size', 5000)
        auto_save_stock_count = self.auto_save_trigger_count
        self._log(f"开始单线程处理{len(stocks)}只股票的日线数据...", "INFO")

        self.skip_count = 0
        self.stock_process_count = 0
        self.data_validation_errors = []
        self.stock_retry_count.clear()
        self.missing_stocks.clear()
        self.processed_codes.clear()

        if not self.ensure_table_exists():
            self._log("无法确保数据表存在，退出下载", "ERROR")
            return

        if not self.create_temp_table():
            self._log("无法创建临时表，退出下载", "ERROR")
            return

        end_date = self.get_previous_trade_day()
        self._log(f"本次下载目标日期: {end_date}", "INFO")

        self._log("按最后下载日期分组并抽样查询baostock...", "INFO")
        groups, no_data_stocks = self._group_stocks_by_last_date(stocks, end_date, sample_size=10)

        total_pending = 0
        success_count = 0
        fail_count = 0
        validation_fail_count = 0
        missing_count = 0
        skipped_count = 0

        for start_date, code_list in groups:
            if not self.is_running:
                break
            while self.is_paused and self.is_running:
                time.sleep(0.5)

            self._log(f"批次 [{start_date}] 共 {len(code_list)} 只股票，开始下载...", "INFO")
            batch_success = 0
            batch_fail = 0
            batch_missing = 0
            batch_validation_fail = 0

            for idx, code in enumerate(code_list):
                if not self.is_running:
                    break
                while self.is_paused and self.is_running:
                    time.sleep(0.5)

                self._update_current_stock(f"{code} ({total_pending + idx + 1})")
                result_code, success, data, is_valid, reason = self.download_single_stock(code, start_date, end_date)

                if success and data:
                    if is_valid:
                        self.add_to_buffer(data)
                        self.save_downloaded_record(code)
                        batch_success += 1
                        success_count += 1
                        self.processed_codes.add(code)
                    else:
                        batch_validation_fail += 1
                        validation_fail_count += 1
                        self.failed_stocks.append(f"{code}|day|validation_fail")
                        self._append_failed(code, "day", "validation_fail")
                        self.processed_codes.add(code)
                else:
                    if reason in ("无数据", "无有效数据"):
                        batch_missing += 1
                        missing_count += 1
                        self.missing_stocks.add(code)
                        self.processed_codes.add(code)
                    else:
                        batch_fail += 1
                        fail_count += 1
                        self.failed_stocks.append(f"{code}|day|{reason}")
                        self._append_failed(code, "day", reason)
                        self.processed_codes.add(code)

                self.processed_stocks = len(self.processed_codes)
                elapsed = time.time() - self.start_time if self.start_time > 0 else 1
                speed = self.processed_stocks / elapsed if elapsed > 0 else 0
                total = max(total_pending, len(stocks))
                self._update_progress((self.processed_stocks / total) * 100 if total > 0 else 0)
                self._update_speed(f"速度: {speed:.1f} 只/秒 | 已处理: {self.processed_stocks}/{total} | 缓存: {self.buffer_size}行")

                if self.buffer_size >= batch_size:
                    self.flush_buffer()

                if self.processed_stocks % auto_save_stock_count == 0 and self.processed_stocks > 0:
                    if not self.is_auto_saving and not self.auto_save_requested:
                        self.auto_save_requested = True
                        self._log(f"已处理 {self.processed_stocks} 只股票，触发自动保存", "INFO")
                        cb = self.callbacks.get('do_auto_save')
                        if cb:
                            cb()

            total_pending += len(code_list)
            self._log(f"批次 [{start_date}] 完成: 成功{batch_success} 失败{batch_fail} "
                      f"缺失{batch_missing} 校验失败{batch_validation_fail}", "INFO")

        if no_data_stocks:
            skipped_count = len(no_data_stocks)
            self._log(f"跳过 {skipped_count} 只股票（抽样查询无可用数据）", "INFO")
            total_pending += skipped_count
            self.processed_codes.update(no_data_stocks)

        progress = (self.processed_stocks / max(total_pending, 1)) * 100
        self._update_progress(progress)

        with self.temp_table_lock:
            self.flush_buffer()
            if self.downloaded_rows > 0:
                self.merge_temp_to_main(checkpoint=True, batch_save=True)
                self.recreate_temp_table()
            else:
                self._log("没有新数据需要合并", "INFO")

        if fail_count > 0 or validation_fail_count > 0 or missing_count > 0:
            self._log(f"日线处理完成：成功{success_count}只，失败{fail_count}只，"
                      f"校验失败{validation_fail_count}只，缺失{missing_count}只，跳过{skipped_count}只", "WARNING")
        else:
            self._log(f"日线处理完成：成功{success_count}只", "INFO")

        if self.missing_stocks:
            self._log(f"缺失股票列表 ({len(self.missing_stocks)}只): {sorted(list(self.missing_stocks))}", "WARNING")

    def download_worker_single_thread(self, selected_periods: List[str] = None):
        """主下载工作线程"""
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
                self._log("警告：没有上市日期数据，将使用默认起始日期 2000-01-01", "WARNING")

            table_txt_path = self.file_paths.get('table_txt', '')
            if not os.path.exists(table_txt_path):
                self._log(f"table.txt文件不存在: {table_txt_path}，请先更新股票列表", "ERROR")
                self._show_message("提示", "请先点击「更新股票列表」获取股票代码", 3000)
                return

            lines = self._read_file_multi_encoding(table_txt_path)
            stocks_filtered = []
            for line in lines:
                line = line.strip()
                if line and not line.startswith('#'):
                    code_part = line.split(',')[0].strip()
                    normalized = self.normalize_stock_code(code_part)
                    if not self.is_index_stock(code_part, normalized):
                        stocks_filtered.append(normalized)
            stocks_filtered = list(dict.fromkeys(stocks_filtered))

            if not stocks_filtered:
                self._log("没有有效的股票代码", "WARNING")
                return

            self.total_stocks = len(stocks_filtered)
            self.processed_stocks = 0
            self._log(f"总共加载了 {len(stocks_filtered)} 只股票")
            if stocks_filtered:
                self._log(f"前10只股票: {stocks_filtered[:10]}")

            os.makedirs(self.data_dir, exist_ok=True)
            self._log(f"开始下载股票数据，共{len(stocks_filtered)}只股票...")

            # ====== 周线/月线/季线/年线：从日线数据库聚合，无需baostock登录 ======
            long_periods = [p for p in ['week', 'month', 'season', 'year'] if p in selected_periods]
            _long_period_module_map = {
                'week': 'weekly_stock_data_download',
                'month': 'monthly_stock_data_download',
                'season': 'seasonly_stock_data_download',
                'year': 'yearly_stock_data_download',
            }
            for period in long_periods:
                if not self.is_running:
                    break
                self._log(f"[{period}] 运行模块...", "INFO")
                try:
                    mod_name = _long_period_module_map[period]
                    mod = __import__(mod_name)
                    func = getattr(mod, 'run_module')
                    func(log_callback=lambda msg, p=period: self._log(f"[{p}] {msg}"))
                except Exception as e:
                    self._log(f"运行{period}模块失败: {e}", "ERROR")
                if self.is_running:
                    self.cleanup_temp_tables(period)

            # ====== 日线 + 分钟线：需要baostock登录 ======
            if 'day' in selected_periods:
                if not self.ensure_login():
                    self._log("登录失败，无法继续下载", "ERROR")
                    return
                if self.is_running:
                    self.process_dayly_stocks_single_thread(stocks_filtered)

            minute_periods = [p for p in ['60min', '30min', '15min', '5min'] if p in selected_periods]
            _minute_period_module_map = {
                '60min': 'minute60_stock_data_download',
                '30min': 'minute30_stock_data_download',
                '15min': 'minute15_stock_data_download',
                '5min': 'minute5_stock_data_download',
            }
            if minute_periods and not self.bs_logged_in:
                if not self.ensure_login():
                    self._log("登录失败，无法继续下载", "ERROR")
                    return
            for period in minute_periods:
                if not self.is_running:
                    break
                self._log(f"[{period}] 运行模块...", "INFO")
                try:
                    mod_name = _minute_period_module_map[period]
                    mod = __import__(mod_name)
                    func = getattr(mod, 'run_module')
                    func(
                        stocks=stocks_filtered,
                        force=False,
                        log_callback=lambda msg, p=period: self._log(f"[{p}] {msg}")
                    )
                except Exception as e:
                    self._log(f"运行{period}模块失败: {e}", "ERROR")
                if self.is_running:
                    self.cleanup_temp_tables(period)

            with self.temp_table_lock:
                if not self.temp_table_created:
                    self._log("临时表不存在，无法写入数据", "ERROR")
                    return 0
                conn = self.get_db_connection(db_path)
                if conn is None:
                    self._log("无法获取数据库连接", "ERROR")
                    return 0
                chunk_size = 5000
                total_inserted = 0
                for i in range(0, len(self.data_buffer), chunk_size):
                    chunk = self.data_buffer[i:i + chunk_size]
                    placeholders = ','.join(['(?,?,?,?,?,?,?,?)'] * len(chunk))
                    flat_values = []
                    for row in chunk:
                        flat_values.extend(row)
                    try:
                        conn.execute(f"INSERT INTO {self.temp_table_name} VALUES {placeholders}", flat_values)
                        total_inserted += len(chunk)
                    except Exception as e:
                        self._log(f"批量插入失败: {e}，尝试逐条插入", "WARNING")
                        for row in chunk:
                            try:
                                conn.execute(
                                    f"INSERT INTO {self.temp_table_name} VALUES (?,?,?,?,?,?,?,?)", row)
                                total_inserted += 1
                            except Exception as e2:
                                self._log(f"逐条插入失败: {e2}", "ERROR")
                self.downloaded_rows += total_inserted
                self._log(f"批量提交 {total_inserted} 行到临时表，累计 {self.downloaded_rows} 行", "DEBUG")
                self.data_buffer = []
                self.buffer_size = 0
                return total_inserted
        except Exception as e:
            self._log(f"下载工作线程异常: {e}", "ERROR")
            import traceback
            self._log(traceback.format_exc(), "DEBUG")

    # ==================== 自动保存 ====================

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
                            self._log("自动保存触发（定时）：将临时数据合并到主表", "INFO")
                            cb = self.callbacks.get('do_auto_save')
                            if cb:
                                cb()
            self._log("自动保存定时器已停止", "DEBUG")

        t = threading.Thread(target=auto_save, daemon=True)
        t.start()
        return t

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
            self._log(f"自动保存失败: {e}", "ERROR")
        finally:
            self.is_auto_saving = False
            self.auto_save_requested = False

    def manual_save_data(self):
        """手动保存数据"""
        if self.is_saving:
            self._show_message("提示", "正在保存中，请稍候...")
            return
        if not self.temp_table_created or self.downloaded_rows == 0:
            self._show_message("提示", "没有临时数据需要保存")
            return
        self.is_saving = True
        try:
            self._log("手动保存触发：开始合并临时数据到主表", "INFO")
            with self.temp_table_lock:
                self.flush_buffer()
                self.merge_temp_to_main(checkpoint=True, batch_save=True)
                self.recreate_temp_table()
            self._log("手动保存完成，已重建临时表", "INFO")
            self._show_message("保存完成", "数据保存完成")
        except Exception as e:
            self._log(f"手动保存失败: {e}", "ERROR")
            self._show_error("保存失败", f"保存失败: {e}")
        finally:
            self.is_saving = False

    # ==================== 股票日期查询 ====================

    def get_stock_max_date(self, stock_code: str, db_path: str = None, use_cache: bool = True) -> Optional[str]:
        """获取股票最大日期"""
        db_path = db_path or self.db_paths.get('day', '')
        if use_cache:
            with self.cache_lock:
                if stock_code in self.stock_max_date_cache:
                    return self.stock_max_date_cache[stock_code]
        try:
            conn = self.get_db_connection(db_path)
            if conn is None:
                return None
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            if 'dayly_stock_data' not in table_names:
                return None
            result = conn.execute(
                "SELECT MAX(date) FROM dayly_stock_data WHERE code = ?", [stock_code]).fetchone()
            if result and result[0]:
                date_str = self._format_date(result[0])
                with self.cache_lock:
                    self.stock_max_date_cache[stock_code] = date_str
                return date_str
        except Exception as e:
            if "does not exist" not in str(e):
                self._log(f"获取最大日期失败 {stock_code}: {e}", "DEBUG")
        return None

    def get_stock_ipo_date(self, code: str) -> Optional[str]:
        return self.stock_ipo_dates.get(code)

    def get_stock_max_dates_batch(self, codes: List[str], db_path: str = None) -> Dict[str, Optional[str]]:
        """批量获取股票的最大日期"""
        db_path = db_path or self.db_paths.get('day', '')
        if not codes:
            return {}
        try:
            conn = self.get_db_connection(db_path)
            if conn is None:
                return {}
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            if 'dayly_stock_data' not in table_names:
                return {code: None for code in codes}
            placeholders = ','.join(['?'] * len(codes))
            result = conn.execute(f"""
                SELECT code, MAX(date) as max_date FROM dayly_stock_data
                WHERE code IN ({placeholders}) GROUP BY code
            """, codes).fetchall()
            max_dates = {}
            for code, max_date in result:
                max_dates[code] = self._format_date(max_date) if max_date else None
            for code in codes:
                if code not in max_dates:
                    max_dates[code] = None
            return max_dates
        except Exception as e:
            self._log(f"批量获取最大日期失败: {e}", "ERROR")
            return {code: None for code in codes}

    def get_available_dates_from_baostock(self, stock_code: str, start_date: str, end_date: str,
                                          frequency: str = "d") -> List[str]:
        """从baostock查询指定股票的可下载日期"""
        cache_key = (stock_code, start_date, end_date, frequency)
        with self.available_dates_lock:
            if cache_key in self.available_dates_cache:
                return self.available_dates_cache[cache_key]
        try:
            bs_code = self._to_bs_code(stock_code)
            self.safe_request_delay()
            rs = bs.query_history_k_data_plus(bs_code, "date", start_date=start_date, end_date=end_date,
                                              frequency=frequency, adjustflag="3")
            if rs.error_code != '0':
                return []
            dates = []
            while rs.error_code == '0' and rs.next():
                row_data = rs.get_row_data()
                if row_data and row_data[0]:
                    date_str = row_data[0].strip()
                    if date_str and len(date_str) >= 10:
                        dates.append(date_str[:10])
            with self.available_dates_lock:
                self.available_dates_cache[cache_key] = dates
            return dates
        except Exception as e:
            self._log(f"查询可下载日期时出错 {stock_code}: {e}", "ERROR")
            return []

    def _group_stocks_by_last_date(self, stocks: List[str], end_date: str,
                                   sample_size: int = 10) -> Tuple[List[Tuple[str, List[str]]], List[str]]:
        """按最后下载日期分组股票，每组抽样查询baostock可下载日期"""
        max_dates = self.get_stock_max_dates_batch(stocks)
        MIN_IPO_DATE = "2010-01-01"
        groups: Dict[str, List[str]] = {}
        no_ipo_stocks = []
        no_db_stocks = []
        for code in stocks:
            max_date = max_dates.get(code)
            if max_date is None:
                ipo_date = self.get_stock_ipo_date(code)
                if ipo_date:
                    start = MIN_IPO_DATE if ipo_date < MIN_IPO_DATE else ipo_date
                    groups.setdefault(start, []).append(code)
                else:
                    no_ipo_stocks.append(code)
            else:
                try:
                    last_dt = datetime.datetime.strptime(max_date, "%Y-%m-%d")
                    next_day = (last_dt + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                    groups.setdefault(next_day, []).append(code)
                except Exception:
                    groups.setdefault(max_date, []).append(code)
        all_groups = []
        all_no_data = []
        for start_date, code_list in groups.items():
            if not code_list:
                continue
            sample = code_list[:sample_size] if len(code_list) > sample_size else code_list
            has_data = False
            available_dates = []
            for sc in sample:
                dates = self.get_available_dates_from_baostock(sc, start_date, end_date, "d")
                if dates:
                    has_data = True
                    available_dates.extend(dates)
            if has_data:
                all_groups.append((start_date, code_list))
            else:
                all_no_data.extend(code_list)
        if no_ipo_stocks:
            all_no_data.extend(no_ipo_stocks)
        return all_groups, all_no_data

    # ==================== 数据校验与下载 ====================

    def validate_stock_data(self, code: str, data: List[tuple]) -> bool:
        """验证股票数据的完整性和正确性"""
        if not data:
            return True
        try:
            for row in data:
                if row[0] != code:
                    self._log(f"数据校验失败: {code} 的数据中包含其他股票代码 {row[0]}", "ERROR")
                    return False
            current_year = datetime.datetime.now().year
            for row in data:
                date_str = row[1]
                if date_str:
                    try:
                        date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                        if date_obj.year < 1990 or date_obj.year > current_year + 1:
                            self._log(f"数据校验失败: {code} 的日期 {date_str} 不在合理范围内", "ERROR")
                            return False
                    except Exception:
                        self._log(f"数据校验失败: {code} 的日期格式错误 {date_str}", "ERROR")
                        return False
            for row in data:
                open_val, high_val, low_val, close_val = row[2], row[3], row[4], row[5]
                for price in [open_val, high_val, low_val, close_val]:
                    if price < 0 or price > 100000:
                        self._log(f"数据校验失败: {code} 的价格 {price} 超出合理范围", "ERROR")
                        return False
                if high_val < low_val:
                    self._log(f"数据校验失败: {code} 的最高价 {high_val} 小于最低价 {low_val}", "ERROR")
                    return False
            for row in data:
                volume, amount = row[6], row[7]
                if volume < 0 or volume > 1e12:
                    self._log(f"数据校验失败: {code} 的成交量 {volume} 超出合理范围", "ERROR")
                    return False
                if amount < 0 or amount > 1e15:
                    self._log(f"数据校验失败: {code} 的成交额 {amount} 超出合理范围", "ERROR")
                    return False
            return True
        except Exception as e:
            self._log(f"数据校验过程出错: {e}", "ERROR")
            return False

    def download_single_stock(self, code: str, start_date: str, end_date: str,
                              frequency: str = "d") -> Tuple[str, bool, Optional[list], bool, str]:
        """下载单只股票数据"""
        config = self.period_config.get('day', {})
        retry_count = config.get('retry_count', 5)
        bs_field = "date" if frequency == "d" else f"{frequency}date"
        if frequency == "d":
            bs_columns = "date,open,high,low,close,volume,amount"
        else:
            bs_columns = "open,high,low,close,volume,amount"

        for attempt in range(retry_count):
            try:
                self.safe_request_delay()
                bs_code = self._to_bs_code(code)
                self._log(f"下载{code} {start_date}~{end_date}", "INFO")

                try:
                    signal.signal(signal.SIGALRM, lambda signum, frame: (_ for _ in ()).throw(TimeoutError("查询超时")))
                    signal.alarm(60)
                except Exception:
                    pass

                try:
                    rs = bs.query_history_k_data_plus(
                        bs_code, bs_columns, start_date=start_date, end_date=end_date,
                        frequency=frequency, adjustflag="3")
                finally:
                    try:
                        signal.alarm(0)
                    except Exception:
                        pass

                if rs.error_code != '0':
                    if "没有数据" in rs.error_msg:
                        return code, False, None, True, "无数据"
                    raise Exception(f"获取数据失败: {rs.error_msg}")

                data_list = []
                error_count = 0
                max_errors = 5
                self._log(f"正在获取{code}的数据...", "INFO")

                while rs.error_code == '0' and rs.next():
                    try:
                        row_data = rs.get_row_data()
                        safe_row = []
                        for item in row_data:
                            if item is None:
                                safe_row.append("")
                            elif isinstance(item, bytes):
                                safe_row.append(self.safe_decode_binary(item))
                            else:
                                safe_row.append(str(item))
                        if len(safe_row) >= 7 and safe_row[0] and safe_row[0].strip():
                            data_list.append(safe_row)
                        else:
                            error_count += 1
                            if error_count > max_errors:
                                break
                        if len(data_list) % 1000 == 0 and len(data_list) > 0:
                            self._log(f"已获取 {len(data_list)} 条数据...", "INFO")
                        if len(data_list) >= 10000:
                            self._log("已达到单次下载上限10000条，停止获取", "WARNING")
                            break
                    except Exception:
                        error_count += 1
                        if error_count > max_errors:
                            break
                        continue

                self._log(f"{code} 共获取到 {len(data_list)} 条原始数据", "INFO")
                if not data_list:
                    return code, False, None, True, "无数据"

                processed_data = []
                for row in data_list:
                    if len(row) >= 7:
                        try:
                            date_str = row[0].strip()[:10] if row[0] else ""
                            vals = []
                            for val in row[1:]:
                                vals.append(float(val) if val and val not in ['', 'None'] else 0.0)
                            processed_data.append((code, date_str, *vals))
                        except ValueError as e:
                            self._log(f"数据转换错误: {e}, 行数据: {row}", "DEBUG")
                            continue

                self._log(f"{code} 处理完成，有效数据 {len(processed_data)} 条", "INFO")
                if processed_data:
                    return code, True, processed_data, True, "成功"
                else:
                    return code, False, None, True, "无有效数据"

            except TimeoutError:
                self._log(f"{code} 查询超时，重试 {attempt + 1}/{retry_count}", "WARNING")
                if attempt < retry_count - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue
                return code, False, None, True, "下载超时"
            except Exception as e:
                error_msg = str(e)
                self._log(f"{code} 下载出错: {error_msg}", "ERROR")
                self.check_and_relogin(error_msg)
                if "网络" in error_msg or "timeout" in error_msg.lower() or "连接" in error_msg:
                    if attempt < retry_count - 1:
                        wait_time = (attempt + 1) * 5
                        self._log(f"{code} 网络错误，{wait_time}秒后重试...", "WARNING")
                        time.sleep(wait_time)
                        continue
                return code, False, None, True, f"下载错误: {error_msg[:50]}"

        return code, False, None, True, "超过最大重试次数"

    # ==================== 下载工作流 ====================

    def process_dayly_stocks_single_thread(self, stocks: List[str]):
        """单线程处理日线数据"""
        batch_size = self.period_config['day'].get('batch_size', 5000)
        auto_save_stock_count = self.auto_save_trigger_count
        self._log(f"开始单线程处理{len(stocks)}只股票的日线数据...", "INFO")

        self.skip_count = 0
        self.stock_process_count = 0
        self.data_validation_errors = []
        self.stock_retry_count.clear()
        self.missing_stocks.clear()
        self.processed_codes.clear()

        if not self.ensure_table_exists():
            self._log("无法确保数据表存在，退出下载", "ERROR")
            return

        if not self.create_temp_table():
            self._log("无法创建临时表，退出下载", "ERROR")
            return

        end_date = self.get_previous_trade_day()
        self._log(f"本次下载目标日期: {end_date}", "INFO")

        self._log("按最后下载日期分组并抽样查询baostock...", "INFO")
        groups, no_data_stocks = self._group_stocks_by_last_date(stocks, end_date, sample_size=10)

        total_pending = 0
        success_count = 0
        fail_count = 0
        validation_fail_count = 0
        missing_count = 0
        skipped_count = 0

        for start_date, code_list in groups:
            if not self.is_running:
                break
            while self.is_paused and self.is_running:
                time.sleep(0.5)

            self._log(f"批次 [{start_date}] 共 {len(code_list)} 只股票，开始下载...", "INFO")
            batch_success = 0
            batch_fail = 0
            batch_missing = 0
            batch_validation_fail = 0

            for idx, code in enumerate(code_list):
                if not self.is_running:
                    break
                while self.is_paused and self.is_running:
                    time.sleep(0.5)

                self._update_current_stock(f"{code} ({total_pending + idx + 1})")
                result_code, success, data, is_valid, reason = self.download_single_stock(code, start_date, end_date)

                if success and data:
                    if is_valid:
                        self.add_to_buffer(data)
                        self.save_downloaded_record(code)
                        batch_success += 1
                        success_count += 1
                        self.processed_codes.add(code)
                    else:
                        batch_validation_fail += 1
                        validation_fail_count += 1
                        self.failed_stocks.append(f"{code}|day|validation_fail")
                        self._append_failed(code, "day", "validation_fail")
                        self.processed_codes.add(code)
                else:
                    if reason in ("无数据", "无有效数据"):
                        batch_missing += 1
                        missing_count += 1
                        self.missing_stocks.add(code)
                        self.processed_codes.add(code)
                    else:
                        batch_fail += 1
                        fail_count += 1
                        self.failed_stocks.append(f"{code}|day|{reason}")
                        self._append_failed(code, "day", reason)
                        self.processed_codes.add(code)

                self.processed_stocks = len(self.processed_codes)
                elapsed = time.time() - self.start_time if self.start_time > 0 else 1
                speed = self.processed_stocks / elapsed if elapsed > 0 else 0
                total = max(total_pending, len(stocks))
                self._update_progress((self.processed_stocks / total) * 100 if total > 0 else 0)
                self._update_speed(f"速度: {speed:.1f} 只/秒 | 已处理: {self.processed_stocks}/{total} | 缓存: {self.buffer_size}行")

                if self.buffer_size >= batch_size:
                    self.flush_buffer()

                if self.processed_stocks % auto_save_stock_count == 0 and self.processed_stocks > 0:
                    if not self.is_auto_saving and not self.auto_save_requested:
                        self.auto_save_requested = True
                        self._log(f"已处理 {self.processed_stocks} 只股票，触发自动保存", "INFO")
                        cb = self.callbacks.get('do_auto_save')
                        if cb:
                            cb()

            total_pending += len(code_list)
            self._log(f"批次 [{start_date}] 完成: 成功{batch_success} 失败{batch_fail} "
                      f"缺失{batch_missing} 校验失败{batch_validation_fail}", "INFO")

        if no_data_stocks:
            skipped_count = len(no_data_stocks)
            self._log(f"跳过 {skipped_count} 只股票（抽样查询无可用数据）", "INFO")
            total_pending += skipped_count
            self.processed_codes.update(no_data_stocks)

        progress = (self.processed_stocks / max(total_pending, 1)) * 100
        self._update_progress(progress)

        with self.temp_table_lock:
            self.flush_buffer()
            if self.downloaded_rows > 0:
                self.merge_temp_to_main(checkpoint=True, batch_save=True)
                self.recreate_temp_table()
            else:
                self._log("没有新数据需要合并", "INFO")

        if fail_count > 0 or validation_fail_count > 0 or missing_count > 0:
            self._log(f"日线处理完成：成功{success_count}只，失败{fail_count}只，"
                      f"校验失败{validation_fail_count}只，缺失{missing_count}只，跳过{skipped_count}只", "WARNING")
        else:
            self._log(f"日线处理完成：成功{success_count}只", "INFO")

        if self.missing_stocks:
            self._log(f"缺失股票列表 ({len(self.missing_stocks)}只): {sorted(list(self.missing_stocks))}", "WARNING")

    def download_worker_single_thread(self, selected_periods: List[str] = None):
        """主下载工作线程"""
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
                self._log("警告：没有上市日期数据，将使用默认起始日期 2000-01-01", "WARNING")

            table_txt_path = self.file_paths.get('table_txt', '')
            if not os.path.exists(table_txt_path):
                self._log(f"table.txt文件不存在: {table_txt_path}，请先更新股票列表", "ERROR")
                self._show_message("提示", "请先点击「更新股票列表」获取股票代码", 3000)
                return

            lines = self._read_file_multi_encoding(table_txt_path)
            stocks_filtered = []
            for line in lines:
                line = line.strip()
                if line and not line.startswith('#'):
                    code_part = line.split(',')[0].strip()
                    normalized = self.normalize_stock_code(code_part)
                    if not self.is_index_stock(code_part, normalized):
                        stocks_filtered.append(normalized)
            stocks_filtered = list(dict.fromkeys(stocks_filtered))

            if not stocks_filtered:
                self._log("没有有效的股票代码", "WARNING")
                return

            self.total_stocks = len(stocks_filtered)
            self.processed_stocks = 0
            self._log(f"总共加载了 {len(stocks_filtered)} 只股票")
            if stocks_filtered:
                self._log(f"前10只股票: {stocks_filtered[:10]}")

            os.makedirs(self.data_dir, exist_ok=True)
            self._log(f"开始下载股票数据，共{len(stocks_filtered)}只股票...")

            # ====== 周线/月线/季线/年线：从日线数据库聚合，无需baostock登录 ======
            long_periods = [p for p in ['week', 'month', 'season', 'year'] if p in selected_periods]
            _long_period_module_map = {
                'week': 'weekly_stock_data_download',
                'month': 'monthly_stock_data_download',
                'season': 'seasonly_stock_data_download',
                'year': 'yearly_stock_data_download',
            }
            for period in long_periods:
                if not self.is_running:
                    break
                self._log(f"[{period}] 运行模块...", "INFO")
                try:
                    mod_name = _long_period_module_map[period]
                    mod = __import__(mod_name)
                    func = getattr(mod, 'run_module')
                    func(log_callback=lambda msg, p=period: self._log(f"[{p}] {msg}"))
                except Exception as e:
                    self._log(f"运行{period}模块失败: {e}", "ERROR")
                if self.is_running:
                    self.cleanup_temp_tables(period)

            # ====== 日线 + 分钟线：需要baostock登录 ======
            if 'day' in selected_periods:
                if not self.ensure_login():
                    self._log("登录失败，无法继续下载", "ERROR")
                    return
                if self.is_running:
                    self.process_dayly_stocks_single_thread(stocks_filtered)

            minute_periods = [p for p in ['60min', '30min', '15min', '5min'] if p in selected_periods]
            _minute_period_module_map = {
                '60min': 'minute60_stock_data_download',
                '30min': 'minute30_stock_data_download',
                '15min': 'minute15_stock_data_download',
                '5min': 'minute5_stock_data_download',
            }
            if minute_periods and not self.bs_logged_in:
                if not self.ensure_login():
                    self._log("登录失败，无法继续下载", "ERROR")
                    return
            for period in minute_periods:
                if not self.is_running:
                    break
                self._log(f"[{period}] 运行模块...", "INFO")
                try:
                    mod_name = _minute_period_module_map[period]
                    mod = __import__(mod_name)
                    func = getattr(mod, 'run_module')
                    func(
                        stocks=stocks_filtered,
                        force=False,
                        log_callback=lambda msg, p=period: self._log(f"[{p}] {msg}")
                    )
                except Exception as e:
                    self._log(f"运行{period}模块失败: {e}", "ERROR")
                if self.is_running:
                    self.cleanup_temp_tables(period)

            with self.temp_table_lock:
                if not self.temp_table_created:
                    self._log("临时表不存在，无法写入数据", "ERROR")
                    return 0
                conn = self.get_db_connection(db_path)
                if conn is None:
                    self._log("无法获取数据库连接", "ERROR")
                    return 0
                chunk_size = 5000
                total_inserted = 0
                for i in range(0, len(self.data_buffer), chunk_size):
                    chunk = self.data_buffer[i:i + chunk_size]
                    placeholders = ','.join(['(?,?,?,?,?,?,?,?)'] * len(chunk))
                    flat_values = []
                    for row in chunk:
                        flat_values.extend(row)
                    try:
                        conn.execute(f"INSERT INTO {self.temp_table_name} VALUES {placeholders}", flat_values)
                        total_inserted += len(chunk)
                    except Exception as e:
                        self._log(f"批量插入失败: {e}，尝试逐条插入", "WARNING")
                        for row in chunk:
                            try:
                                conn.execute(
                                    f"INSERT INTO {self.temp_table_name} VALUES (?,?,?,?,?,?,?,?)", row)
                                total_inserted += 1
                            except Exception as e2:
                                self._log(f"逐条插入失败: {e2}", "ERROR")
                self.downloaded_rows += total_inserted
                self._log(f"批量提交 {total_inserted} 行到临时表，累计 {self.downloaded_rows} 行", "DEBUG")
                self.data_buffer = []
                self.buffer_size = 0
                return total_inserted
        except Exception as e:
            self._log(f"下载工作线程异常: {e}", "ERROR")
            import traceback
            self._log(traceback.format_exc(), "DEBUG")

    # ==================== 自动保存 ====================

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
                            self._log("自动保存触发（定时）：将临时数据合并到主表", "INFO")
                            cb = self.callbacks.get('do_auto_save')
                            if cb:
                                cb()
            self._log("自动保存定时器已停止", "DEBUG")

        t = threading.Thread(target=auto_save, daemon=True)
        t.start()
        return t

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
            self._log(f"自动保存失败: {e}", "ERROR")
        finally:
            self.is_auto_saving = False
            self.auto_save_requested = False

    def manual_save_data(self):
        """手动保存数据"""
        if self.is_saving:
            self._show_message("提示", "正在保存中，请稍候...")
            return
        if not self.temp_table_created or self.downloaded_rows == 0:
            self._show_message("提示", "没有临时数据需要保存")
            return
        self.is_saving = True
        try:
            self._log("手动保存触发：开始合并临时数据到主表", "INFO")
            with self.temp_table_lock:
                self.flush_buffer()
                self.merge_temp_to_main(checkpoint=True, batch_save=True)
                self.recreate_temp_table()
            self._log("手动保存完成，已重建临时表", "INFO")
            self._show_message("保存完成", "数据保存完成")
        except Exception as e:
            self._log(f"手动保存失败: {e}", "ERROR")
            self._show_error("保存失败", f"保存失败: {e}")
        finally:
            self.is_saving = False

    # ==================== 股票日期查询 ====================

    def get_stock_max_date(self, stock_code: str, db_path: str = None, use_cache: bool = True) -> Optional[str]:
        """获取股票最大日期"""
        db_path = db_path or self.db_paths.get('day', '')
        if use_cache:
            with self.cache_lock:
                if stock_code in self.stock_max_date_cache:
                    return self.stock_max_date_cache[stock_code]
        try:
            conn = self.get_db_connection(db_path)
            if conn is None:
                return None
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            if 'dayly_stock_data' not in table_names:
                return None
            result = conn.execute(
                "SELECT MAX(date) FROM dayly_stock_data WHERE code = ?", [stock_code]).fetchone()
            if result and result[0]:
                date_str = self._format_date(result[0])
                with self.cache_lock:
                    self.stock_max_date_cache[stock_code] = date_str
                return date_str
        except Exception as e:
            if "does not exist" not in str(e):
                self._log(f"获取最大日期失败 {stock_code}: {e}", "DEBUG")
        return None

    def get_stock_ipo_date(self, code: str) -> Optional[str]:
        return self.stock_ipo_dates.get(code)

    def get_stock_max_dates_batch(self, codes: List[str], db_path: str = None) -> Dict[str, Optional[str]]:
        """批量获取股票的最大日期"""
        db_path = db_path or self.db_paths.get('day', '')
        if not codes:
            return {}
        try:
            conn = self.get_db_connection(db_path)
            if conn is None:
                return {}
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            if 'dayly_stock_data' not in table_names:
                return {code: None for code in codes}
            placeholders = ','.join(['?'] * len(codes))
            result = conn.execute(f"""
                SELECT code, MAX(date) as max_date FROM dayly_stock_data
                WHERE code IN ({placeholders}) GROUP BY code
            """, codes).fetchall()
            max_dates = {}
            for code, max_date in result:
                max_dates[code] = self._format_date(max_date) if max_date else None
            for code in codes:
                if code not in max_dates:
                    max_dates[code] = None
            return max_dates
        except Exception as e:
            self._log(f"批量获取最大日期失败: {e}", "ERROR")
            return {code: None for code in codes}

    def get_available_dates_from_baostock(self, stock_code: str, start_date: str, end_date: str,
                                          frequency: str = "d") -> List[str]:
        """从baostock查询指定股票的可下载日期"""
        cache_key = (stock_code, start_date, end_date, frequency)
        with self.available_dates_lock:
            if cache_key in self.available_dates_cache:
                return self.available_dates_cache[cache_key]
        try:
            bs_code = self._to_bs_code(stock_code)
            self.safe_request_delay()
            rs = bs.query_history_k_data_plus(bs_code, "date", start_date=start_date, end_date=end_date,
                                              frequency=frequency, adjustflag="3")
            if rs.error_code != '0':
                return []
            dates = []
            while rs.error_code == '0' and rs.next():
                row_data = rs.get_row_data()
                if row_data and row_data[0]:
                    date_str = row_data[0].strip()
                    if date_str and len(date_str) >= 10:
                        dates.append(date_str[:10])
            with self.available_dates_lock:
                self.available_dates_cache[cache_key] = dates
            return dates
        except Exception as e:
            self._log(f"查询可下载日期时出错 {stock_code}: {e}", "ERROR")
            return []

    def _group_stocks_by_last_date(self, stocks: List[str], end_date: str,
                                   sample_size: int = 10) -> Tuple[List[Tuple[str, List[str]]], List[str]]:
        """按最后下载日期分组股票，每组抽样查询baostock可下载日期"""
        max_dates = self.get_stock_max_dates_batch(stocks)
        MIN_IPO_DATE = "2010-01-01"
        groups: Dict[str, List[str]] = {}
        no_ipo_stocks = []
        no_db_stocks = []
        for code in stocks:
            max_date = max_dates.get(code)
            if max_date is None:
                ipo_date = self.get_stock_ipo_date(code)
                if ipo_date:
                    start = MIN_IPO_DATE if ipo_date < MIN_IPO_DATE else ipo_date
                    groups.setdefault(start, []).append(code)
                else:
                    no_ipo_stocks.append(code)
            else:
                try:
                    last_dt = datetime.datetime.strptime(max_date, "%Y-%m-%d")
                    next_day = (last_dt + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                    groups.setdefault(next_day, []).append(code)
                except Exception:
                    groups.setdefault(max_date, []).append(code)
        all_groups = []
        all_no_data = []
        for start_date, code_list in groups.items():
            if not code_list:
                continue
            sample = code_list[:sample_size] if len(code_list) > sample_size else code_list
            has_data = False
            available_dates = []
            for sc in sample:
                dates = self.get_available_dates_from_baostock(sc, start_date, end_date, "d")
                if dates:
                    has_data = True
                    available_dates.extend(dates)
            if has_data:
                all_groups.append((start_date, code_list))
            else:
                all_no_data.extend(code_list)
        if no_ipo_stocks:
            all_no_data.extend(no_ipo_stocks)
        return all_groups, all_no_data

    # ==================== 数据校验与下载 ====================

    def validate_stock_data(self, code: str, data: List[tuple]) -> bool:
        """验证股票数据的完整性和正确性"""
        if not data:
            return True
        try:
            for row in data:
                if row[0] != code:
                    self._log(f"数据校验失败: {code} 的数据中包含其他股票代码 {row[0]}", "ERROR")
                    return False
            current_year = datetime.datetime.now().year
            for row in data:
                date_str = row[1]
                if date_str:
                    try:
                        date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                        if date_obj.year < 1990 or date_obj.year > current_year + 1:
                            self._log(f"数据校验失败: {code} 的日期 {date_str} 不在合理范围内", "ERROR")
                            return False
                    except Exception:
                        self._log(f"数据校验失败: {code} 的日期格式错误 {date_str}", "ERROR")
                        return False
            for row in data:
                open_val, high_val, low_val, close_val = row[2], row[3], row[4], row[5]
                for price in [open_val, high_val, low_val, close_val]:
                    if price < 0 or price > 100000:
                        self._log(f"数据校验失败: {code} 的价格 {price} 超出合理范围", "ERROR")
                        return False
                if high_val < low_val:
                    self._log(f"数据校验失败: {code} 的最高价 {high_val} 小于最低价 {low_val}", "ERROR")
                    return False
            for row in data:
                volume, amount = row[6], row[7]
                if volume < 0 or volume > 1e12:
                    self._log(f"数据校验失败: {code} 的成交量 {volume} 超出合理范围", "ERROR")
                    return False
                if amount < 0 or amount > 1e15:
                    self._log(f"数据校验失败: {code} 的成交额 {amount} 超出合理范围", "ERROR")
                    return False
            return True
        except Exception as e:
            self._log(f"数据校验过程出错: {e}", "ERROR")
            return False

    def download_single_stock(self, code: str, start_date: str, end_date: str,
                              frequency: str = "d") -> Tuple[str, bool, Optional[list], bool, str]:
        """下载单只股票数据"""
        config = self.period_config.get('day', {})
        retry_count = config.get('retry_count', 5)
        bs_field = "date" if frequency == "d" else f"{frequency}date"
        if frequency == "d":
            bs_columns = "date,open,high,low,close,volume,amount"
        else:
            bs_columns = "open,high,low,close,volume,amount"

        for attempt in range(retry_count):
            try:
                self.safe_request_delay()
                bs_code = self._to_bs_code(code)
                self._log(f"下载{code} {start_date}~{end_date}", "INFO")

                try:
                    signal.signal(signal.SIGALRM, lambda signum, frame: (_ for _ in ()).throw(TimeoutError("查询超时")))
                    signal.alarm(60)
                except Exception:
                    pass

                try:
                    rs = bs.query_history_k_data_plus(
                        bs_code, bs_columns, start_date=start_date, end_date=end_date,
                        frequency=frequency, adjustflag="3")
                finally:
                    try:
                        signal.alarm(0)
                    except Exception:
                        pass

                if rs.error_code != '0':
                    if "没有数据" in rs.error_msg:
                        return code, False, None, True, "无数据"
                    raise Exception(f"获取数据失败: {rs.error_msg}")

                data_list = []
                error_count = 0
                max_errors = 5
                self._log(f"正在获取{code}的数据...", "INFO")

                while rs.error_code == '0' and rs.next():
                    try:
                        row_data = rs.get_row_data()
                        safe_row = []
                        for item in row_data:
                            if item is None:
                                safe_row.append("")
                            elif isinstance(item, bytes):
                                safe_row.append(self.safe_decode_binary(item))
                            else:
                                safe_row.append(str(item))
                        if len(safe_row) >= 7 and safe_row[0] and safe_row[0].strip():
                            data_list.append(safe_row)
                        else:
                            error_count += 1
                            if error_count > max_errors:
                                break
                        if len(data_list) % 1000 == 0 and len(data_list) > 0:
                            self._log(f"已获取 {len(data_list)} 条数据...", "INFO")
                        if len(data_list) >= 10000:
                            self._log("已达到单次下载上限10000条，停止获取", "WARNING")
                            break
                    except Exception:
                        error_count += 1
                        if error_count > max_errors:
                            break
                        continue

                self._log(f"{code} 共获取到 {len(data_list)} 条原始数据", "INFO")
                if not data_list:
                    return code, False, None, True, "无数据"

                processed_data = []
                for row in data_list:
                    if len(row) >= 7:
                        try:
                            date_str = row[0].strip()[:10] if row[0] else ""
                            vals = []
                            for val in row[1:]:
                                vals.append(float(val) if val and val not in ['', 'None'] else 0.0)
                            processed_data.append((code, date_str, *vals))
                        except ValueError as e:
                            self._log(f"数据转换错误: {e}, 行数据: {row}", "DEBUG")
                            continue

                self._log(f"{code} 处理完成，有效数据 {len(processed_data)} 条", "INFO")
                if processed_data:
                    return code, True, processed_data, True, "成功"
                else:
                    return code, False, None, True, "无有效数据"

            except TimeoutError:
                self._log(f"{code} 查询超时，重试 {attempt + 1}/{retry_count}", "WARNING")
                if attempt < retry_count - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue
                return code, False, None, True, "下载超时"
            except Exception as e:
                error_msg = str(e)
                self._log(f"{code} 下载出错: {error_msg}", "ERROR")
                self.check_and_relogin(error_msg)
                if "网络" in error_msg or "timeout" in error_msg.lower() or "连接" in error_msg:
                    if attempt < retry_count - 1:
                        wait_time = (attempt + 1) * 5
                        self._log(f"{code} 网络错误，{wait_time}秒后重试...", "WARNING")
                        time.sleep(wait_time)
                        continue
                return code, False, None, True, f"下载错误: {error_msg[:50]}"

        return code, False, None, True, "超过最大重试次数"

    # ==================== 下载工作流 ====================

    def process_dayly_stocks_single_thread(self, stocks: List[str]):
        """单线程处理日线数据"""
        batch_size = self.period_config['day'].get('batch_size', 5000)
        auto_save_stock_count = self.auto_save_trigger_count
        self._log(f"开始单线程处理{len(stocks)}只股票的日线数据...", "INFO")

        self.skip_count = 0
        self.stock_process_count = 0
        self.data_validation_errors = []
        self.stock_retry_count.clear()
        self.missing_stocks.clear()
        self.processed_codes.clear()

        if not self.ensure_table_exists():
            self._log("无法确保数据表存在，退出下载", "ERROR")
            return

        if not self.create_temp_table():
            self._log("无法创建临时表，退出下载", "ERROR")
            return

        end_date = self.get_previous_trade_day()
        self._log(f"本次下载目标日期: {end_date}", "INFO")

        self._log("按最后下载日期分组并抽样查询baostock...", "INFO")
        groups, no_data_stocks = self._group_stocks_by_last_date(stocks, end_date, sample_size=10)

        total_pending = 0
        success_count = 0
        fail_count = 0
        validation_fail_count = 0
        missing_count = 0
        skipped_count = 0

        for start_date, code_list in groups:
            if not self.is_running:
                break
            while self.is_paused and self.is_running:
                time.sleep(0.5)

            self._log(f"批次 [{start_date}] 共 {len(code_list)} 只股票，开始下载...", "INFO")
            batch_success = 0
            batch_fail = 0
            batch_missing = 0
            batch_validation_fail = 0

            for idx, code in enumerate(code_list):
                if not self.is_running:
                    break
                while self.is_paused and self.is_running:
                    time.sleep(0.5)

                self._update_current_stock(f"{code} ({total_pending + idx + 1})")
                result_code, success, data, is_valid, reason = self.download_single_stock(code, start_date, end_date)

                if success and data:
                    if is_valid:
                        self.add_to_buffer(data)
                        self.save_downloaded_record(code)
                        batch_success += 1
                        success_count += 1
                        self.processed_codes.add(code)
                    else:
                        batch_validation_fail += 1
                        validation_fail_count += 1
                        self.failed_stocks.append(f"{code}|day|validation_fail")
                        self._append_failed(code, "day", "validation_fail")
                        self.processed_codes.add(code)
                else:
                    if reason in ("无数据", "无有效数据"):
                        batch_missing += 1
                        missing_count += 1
                        self.missing_stocks.add(code)
                        self.processed_codes.add(code)
                    else:
                        batch_fail += 1
                        fail_count += 1
                        self.failed_stocks.append(f"{code}|day|{reason}")
                        self._append_failed(code, "day", reason)
                        self.processed_codes.add(code)

                self.processed_stocks = len(self.processed_codes)
                elapsed = time.time() - self.start_time if self.start_time > 0 else 1
                speed = self.processed_stocks / elapsed if elapsed > 0 else 0
                total = max(total_pending, len(stocks))
                self._update_progress((self.processed_stocks / total) * 100 if total > 0 else 0)
                self._update_speed(f"速度: {speed:.1f} 只/秒 | 已处理: {self.processed_stocks}/{total} | 缓存: {self.buffer_size}行")

                if self.buffer_size >= batch_size:
                    self.flush_buffer()

                if self.processed_stocks % auto_save_stock_count == 0 and self.processed_stocks > 0:
                    if not self.is_auto_saving and not self.auto_save_requested:
                        self.auto_save_requested = True
                        self._log(f"已处理 {self.processed_stocks} 只股票，触发自动保存", "INFO")
                        cb = self.callbacks.get('do_auto_save')
                        if cb:
                            cb()

            total_pending += len(code_list)
            self._log(f"批次 [{start_date}] 完成: 成功{batch_success} 失败{batch_fail} "
                      f"缺失{batch_missing} 校验失败{batch_validation_fail}", "INFO")

        if no_data_stocks:
            skipped_count = len(no_data_stocks)
            self._log(f"跳过 {skipped_count} 只股票（抽样查询无可用数据）", "INFO")
            total_pending += skipped_count
            self.processed_codes.update(no_data_stocks)

        progress = (self.processed_stocks / max(total_pending, 1)) * 100
        self._update_progress(progress)

        with self.temp_table_lock:
            self.flush_buffer()
            if self.downloaded_rows > 0:
                self.merge_temp_to_main(checkpoint=True, batch_save=True)
                self.recreate_temp_table()
            else:
                self._log("没有新数据需要合并", "INFO")

        if fail_count > 0 or validation_fail_count > 0 or missing_count > 0:
            self._log(f"日线处理完成：成功{success_count}只，失败{fail_count}只，"
                      f"校验失败{validation_fail_count}只，缺失{missing_count}只，跳过{skipped_count}只", "WARNING")
        else:
            self._log(f"日线处理完成：成功{success_count}只", "INFO")

        if self.missing_stocks:
            self._log(f"缺失股票列表 ({len(self.missing_stocks)}只): {sorted(list(self.missing_stocks))}", "WARNING")

    def download_worker_single_thread(self, selected_periods: List[str] = None):
        """主下载工作线程"""
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
                self._log("警告：没有上市日期数据，将使用默认起始日期 2000-01-01", "WARNING")

            table_txt_path = self.file_paths.get('table_txt', '')
            if not os.path.exists(table_txt_path):
                self._log(f"table.txt文件不存在: {table_txt_path}，请先更新股票列表", "ERROR")
                self._show_message("提示", "请先点击「更新股票列表」获取股票代码", 3000)
                return

            lines = self._read_file_multi_encoding(table_txt_path)
            stocks_filtered = []
            for line in lines:
                line = line.strip()
                if line and not line.startswith('#'):
                    code_part = line.split(',')[0].strip()
                    normalized = self.normalize_stock_code(code_part)
                    if not self.is_index_stock(code_part, normalized):
                        stocks_filtered.append(normalized)
            stocks_filtered = list(dict.fromkeys(stocks_filtered))

            if not stocks_filtered:
                self._log("没有有效的股票代码", "WARNING")
                return

            self.total_stocks = len(stocks_filtered)
            self.processed_stocks = 0
            self._log(f"总共加载了 {len(stocks_filtered)} 只股票")
            if stocks_filtered:
                self._log(f"前10只股票: {stocks_filtered[:10]}")

            os.makedirs(self.data_dir, exist_ok=True)
            self._log(f"开始下载股票数据，共{len(stocks_filtered)}只股票...")

            # ====== 周线/月线/季线/年线：从日线数据库聚合，无需baostock登录 ======
            long_periods = [p for p in ['week', 'month', 'season', 'year'] if p in selected_periods]
            _long_period_module_map = {
                'week': 'weekly_stock_data_download',
                'month': 'monthly_stock_data_download',
                'season': 'seasonly_stock_data_download',
                'year': 'yearly_stock_data_download',
            }
            for period in long_periods:
                if not self.is_running:
                    break
                self._log(f"[{period}] 运行模块...", "INFO")
                try:
                    mod_name = _long_period_module_map[period]
                    mod = __import__(mod_name)
                    func = getattr(mod, 'run_module')
                    func(log_callback=lambda msg, p=period: self._log(f"[{p}] {msg}"))
                except Exception as e:
                    self._log(f"运行{period}模块失败: {e}", "ERROR")
                if self.is_running:
                    self.cleanup_temp_tables(period)

            # ====== 日线 + 分钟线：需要baostock登录 ======
            if 'day' in selected_periods:
                if not self.ensure_login():
                    self._log("登录失败，无法继续下载", "ERROR")
                    return
                if self.is_running:
                    self.process_dayly_stocks_single_thread(stocks_filtered)

            minute_periods = [p for p in ['60min', '30min', '15min', '5min'] if p in selected_periods]
            _minute_period_module_map = {
                '60min': 'minute60_stock_data_download',
                '30min': 'minute30_stock_data_download',
                '15min': 'minute15_stock_data_download',
                '5min': 'minute5_stock_data_download',
            }
            if minute_periods and not self.bs_logged_in:
                if not self.ensure_login():
                    self._log("登录失败，无法继续下载", "ERROR")
                    return
            for period in minute_periods:
                if not self.is_running:
                    break
                self._log(f"[{period}] 运行模块...", "INFO")
                try:
                    mod_name = _minute_period_module_map[period]
                    mod = __import__(mod_name)
                    func = getattr(mod, 'run_module')
                    func(
                        stocks=stocks_filtered,
                        force=False,
                        log_callback=lambda msg, p=period: self._log(f"[{p}] {msg}")
                    )
                except Exception as e:
                    self._log(f"运行{period}模块失败: {e}", "ERROR")
                if self.is_running:
                    self.cleanup_temp_tables(period)

            with self.temp_table_lock:
                if self.temp_table_created and self.downloaded_rows > 0:
                    self._log("下载完成，执行最终保存", "INFO")
                    self.flush_buffer()
                    self.merge_temp_to_main(checkpoint=True, batch_save=True)
                    self.recreate_temp_table()

            self.close_db_connection(checkpoint=True)

            total_time = time.time() - self.start_time
            avg_speed = self.total_stocks / total_time if total_time > 0 else 0
            if self.failed_stocks:
                self._log(f"处理完成，失败{len(self.failed_stocks)}只股票，缺失{len(self.missing_stocks)}只，总耗时{total_time:.1f}秒", "WARNING")
                self._show_message("完成", f"处理完成，失败{len(self.failed_stocks)}只，缺失{len(self.missing_stocks)}只")
            else:
                self._log(f"所有股票处理完成，总耗时{total_time:.1f}秒", "INFO")
                self._show_message("完成", "所有股票处理完成")

        except Exception as e:
            self._log(f"下载过程出错: {e}", "ERROR")
            traceback.print_exc()
            with self.temp_table_lock:
                if self.temp_table_created and self.downloaded_rows > 0:
                    self._log("下载出错，尝试保存已下载数据...", "WARNING")
                    try:
                        self.flush_buffer()
                        self.merge_temp_to_main(checkpoint=True, batch_save=True)
                    except Exception:
                        pass
            self.close_db_connection(checkpoint=True)
        finally:
            self.is_running = False

    # ==================== 周期清理temp缓存表 ====================

    def cleanup_temp_tables(self, period: str):
        """清除指定周期数据库中带temp字样的缓存表"""
        db_path = self.db_paths.get(period, '')
        if not db_path or not os.path.exists(db_path):
            return
        try:
            conn = duckdb.connect(db_path)
            tables = conn.execute("SHOW TABLES").fetchall()
            temp_tables = [t[0] for t in tables if 'temp' in t[0].lower()]
            if temp_tables:
                self._log(f"[{period}] 发现 {len(temp_tables)} 个temp缓存表，开始清理: {temp_tables}", "INFO")
                for tbl in temp_tables:
                    try:
                        conn.execute(f"DROP TABLE IF EXISTS {tbl}")
                        self._log(f"[{period}] 已删除临时表: {tbl}", "INFO")
                    except Exception as e:
                        self._log(f"[{period}] 删除临时表 {tbl} 失败: {e}", "ERROR")
                conn.execute("CHECKPOINT")
                self._log(f"[{period}] temp缓存表清理完成", "INFO")
            else:
                self._log(f"[{period}] 无temp缓存表，无需清理", "DEBUG")
            conn.close()
        except Exception as e:
            self._log(f"[{period}] 清理temp缓存表失败: {e}", "ERROR")

    # ==================== 登录与请求控制 ====================

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
                    except Exception:
                        pass
                    self.bs_logged_in = False
                self._log("正在登录baostock...")
                lg = bs.login()
                if lg.error_code != '0':
                    raise Exception(f"登录失败: {lg.error_msg}")
                self.bs_logged_in = True
                self.bs_session = lg
                self.last_login_time = current_time
                self._log("baostock登录成功")
            except Exception as e:
                self._log(f"登录失败: {e}", "ERROR")
                return False
        return True

    def check_and_relogin(self, error_msg: str) -> bool:
        error_msg_lower = error_msg.lower()
        login_errors = ['登录', 'login', 'auth', '认证', 'session', '未登录', 'token', 'expired', '过期', '无效', 'invalid']
        for keyword in login_errors:
            if keyword in error_msg_lower:
                self._log(f"检测到登录相关错误，尝试重新登录: {error_msg}", "WARNING")
                self.bs_logged_in = False
                return self.ensure_login()
        return True

    def logout(self):
        if self.bs_logged_in:
            try:
                bs.logout()
                self.bs_logged_in = False
                self._log("baostock已登出")
                self._show_message("成功", "登出成功")
            except Exception as e:
                self._log(f"登出失败: {e}", "ERROR")
        else:
            self._log("当前未登录")

    # ==================== 股票代码工具 ====================

    def _to_bs_code(self, code: str) -> str:
        if code.startswith(('6', '688')):
            return f"sh.{code}"
        elif code.startswith(('0', '3')):
            return f"sz.{code}"
        else:
            return f"sh.{code}"

    def extract_digits(self, code: str) -> str:
        digits = re.findall(r'\d+', code)
        return digits[0] if digits else code

    def normalize_stock_code(self, code: str) -> str:
        code = code.strip().upper()
        if re.match(r'^\d{6}$', code):
            return code
        if '.' in code:
            parts = code.split('.')
            if len(parts) == 2:
                digits = self.extract_digits(parts[1])
                if len(digits) >= 6:
                    return digits[:6]
                elif len(digits) > 0:
                    return digits.zfill(6)
        digits = self.extract_digits(code)
        if len(digits) >= 6:
            return digits[:6]
        if len(digits) > 0:
            return digits.zfill(6)
        return code

    def is_index_stock(self, code_with_prefix: str, normalized_code: str = None) -> bool:
        if code_with_prefix == 'sh.000001':
            return True
        if code_with_prefix == 'sz.000001':
            return False
        if normalized_code is None:
            if code_with_prefix:
                normalized_code = self.normalize_stock_code(code_with_prefix)
            else:
                return False
        sz_index_codes = {'399001', '399006', '399005', '399300'}
        sh_index_codes = {'000300', '000016', '000905', '000688', '000852', '000903', '000100', '000010', '000009', '000015', '000922'}
        return normalized_code in sz_index_codes or normalized_code in sh_index_codes

    # ==================== 文件操作 ====================

    def load_ipo_dates_from_info_db(self):
        """从上市日期CSV加载上市日期"""
        info_db_path = self.file_paths.get('info_db', '')
        try:
            if not os.path.exists(info_db_path):
                self._log(f"上市日期CSV不存在: {info_db_path}", "WARNING")
                return False
            self._log(f"正在从 {info_db_path} 加载上市日期...")
            ipo_field = None
            row_count = 0
            ipo_count = 0
            with open(info_db_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if header is None:
                    self._log("CSV文件为空", "WARNING")
                    return False
                for name in header:
                    name_lower = name.strip().lower()
                    if 'ipo' in name_lower or '上市日期' in name or 'list_date' in name_lower or 'listing' in name_lower:
                        ipo_field = name.strip()
                        break
                if not ipo_field:
                    self._log("未找到上市日期字段", "WARNING")
                    return False
                ipo_col_idx = header.index(ipo_field)
                code_field = None
                for name in header:
                    nl = name.strip().lower()
                    if nl in ('code', '股票代码', 'stock_code', 'symbol'):
                        code_field = name.strip()
                        break
                if not code_field:
                    code_field = header[0]
                code_col_idx = header.index(code_field)
                for row in reader:
                    row_count += 1
                    if len(row) <= max(code_col_idx, ipo_col_idx):
                        continue
                    code = row[code_col_idx].strip()
                    date_str = row[ipo_col_idx].strip()
                    if code and date_str:
                        normalized_code = self.normalize_stock_code(code)
                        if len(date_str) >= 10:
                            date_str = date_str[:10]
                        self.stock_ipo_dates[normalized_code] = date_str
                        ipo_count += 1
            self._log(f"✅ 成功加载 {ipo_count} 只股票的上市日期到内存缓存 (共解析 {row_count} 行)")
            return True
        except Exception as e:
            self._log(f"加载上市日期失败: {e}", "ERROR")
            traceback.print_exc()
            return False

    def load_downloaded_records(self) -> Set[str]:
        record_file = self.file_paths.get('downloaded_record', '')
        if os.path.exists(record_file):
            try:
                with open(record_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return set(data.get('downloaded', []))
            except Exception as e:
                self._log(f"加载下载记录失败: {e}", "WARNING")
        return set()

    def save_downloaded_record(self, code: str):
        with self.lock:
            self.downloaded_stocks.add(code)
            record_file = self.file_paths.get('downloaded_record', '')
            try:
                with open(record_file, 'w', encoding='utf-8') as f:
                    json.dump({'downloaded': list(self.downloaded_stocks)}, f, ensure_ascii=False, indent=2)
            except Exception as e:
                self._log(f"保存下载记录失败: {e}", "WARNING")

    def _append_failed(self, code: str, period: str, reason: str):
        failed_txt_path = self.file_paths.get('failed_txt', '')
        try:
            with open(failed_txt_path, 'a', encoding='utf-8') as f:
                f.write(f"{code}|{period}|{reason}\n")
        except Exception as e:
            self._log(f"写入失败记录失败: {e}", "ERROR")

    def _read_file_multi_encoding(self, filepath: str) -> List[str]:
        """尝试多种编码读取文件"""
        for enc in ('utf-8', 'gbk', 'gb2312', 'latin1'):
            try:
                with open(filepath, 'r', encoding=enc) as f:
                    return f.readlines()
            except (UnicodeDecodeError, UnicodeError):
                continue
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            return f.readlines()

    # ==================== 数据库统计 ====================

    def preload_stock_max_dates(self):
        """预加载所有股票的最新日期到缓存"""
        try:
            conn = self.get_db_connection()
            if conn is None:
                return
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            if 'dayly_stock_data' not in table_names:
                return
            self._log("正在预加载股票最新日期...", "INFO")
            result = conn.execute("""
                SELECT code, MAX(date) as max_date FROM dayly_stock_data GROUP BY code
            """).fetchall()
            with self.cache_lock:
                for code, max_date in result:
                    if max_date:
                        self.stock_max_date_cache[code] = self._format_date(max_date)
            self._log(f"✅ 预加载完成，共 {len(self.stock_max_date_cache)} 只股票的最新日期", "INFO")
        except Exception as e:
            self._log(f"预加载股票最新日期失败: {e}", "WARNING")

    def show_db_stats(self):
        try:
            if self.db_conn is not None:
                try:
                    self.show_db_stats_with_connection(self.db_conn, 'dayly_stock_data')
                    return
                except Exception:
                    pass
            day_db_path = self.db_paths.get('day', '')
            if not os.path.exists(day_db_path):
                self._log("数据库文件不存在，尚无数据", "INFO")
                return
            try:
                conn = duckdb.connect(day_db_path, read_only=True)
            except Exception as e:
                self._log(f"无法连接数据库: {e}", "WARNING")
                return
            try:
                self.show_db_stats_with_connection(conn, 'dayly_stock_data')
            finally:
                conn.close()
        except Exception as e:
            self._log(f"获取数据库统计信息失败: {e}", "WARNING")

    def show_db_stats_with_connection(self, conn, table_name: str):
        try:
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            if table_name not in table_names:
                self._log(f"数据表 {table_name} 不存在，尚无数据", "INFO")
                return
            result = conn.execute(f"""
                SELECT MIN(date), MAX(date), COUNT(*), COUNT(DISTINCT code) FROM {table_name}
            """).fetchone()
            if result:
                min_date, max_date, total_rows, stock_count = result
                self.db_row_count = total_rows
                self.db_stock_count = stock_count
                if min_date:
                    self.db_min_date = self._format_date(min_date)
                if max_date:
                    self.db_max_date = self._format_date(max_date)
                    self._log(f"📅 数据库统计信息:", "INFO")
                    self._log(f"   • 数据库: {self.db_paths.get('day', '')}", "INFO")
                    self._log(f"   • 表名: {table_name}", "INFO")
                    self._log(f"   • 数据范围: {self.db_min_date} 至 {self.db_max_date}", "INFO")
                    self._log(f"   • 总数据量: {total_rows:,} 行", "INFO")
                    self._log(f"   • 股票数量: {stock_count} 只", "INFO")
        except Exception as e:
            self._log(f"获取统计信息失败: {e}", "DEBUG")

    @staticmethod
    def _format_date(value) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value[:10]
        if hasattr(value, 'strftime'):
            return value.strftime("%Y-%m-%d")
        return str(value)[:10]

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

    def safe_decode_binary(self, data) -> str:
        if data is None:
            return ""
        if isinstance(data, str):
            return data
        if isinstance(data, bytes):
            for enc in ['utf-8', 'gbk', 'gb2312', 'latin1', 'cp1252']:
                try:
                    return data.decode(enc)
                except UnicodeDecodeError:
                    continue
            try:
                return data.decode('utf-8', errors='ignore')
            except Exception:
                pass
            try:
                return data.hex()
            except Exception:
                return str(data)
        return str(data)

    def load_config(self) -> dict:
        """加载配置文件，返回配置字典"""
        config_file = self.file_paths.get('config', '')
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self._log(f"加载配置文件失败: {e}", "ERROR")
        return {}

    def save_config(self, config: dict):
        """保存配置"""
        config_file = self.file_paths.get('config', '')
        try:
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self._log(f"保存配置文件失败: {e}", "ERROR")

    def update_stock_list(self):
        """更新股票列表"""
        try:
            self._log("正在更新股票列表...")
            if not self.ensure_login():
                return
            rs = bs.query_all_stock()
            if rs.error_code != '0':
                raise Exception(f"获取股票列表失败: {rs.error_msg}")
            data_list = []
            stock_codes = []
            while rs.error_code == '0' and rs.next():
                data_list.append(rs.get_row_data())
            if data_list:
                self._log(f"获取到 {len(data_list)} 只股票基本信息...")
                for i, row in enumerate(data_list):
                    if len(row) >= 1:
                        code = row[0]
                        is_a_share = (
                            code.startswith('sh.6') or code.startswith('sh.688') or
                            code.startswith('sz.0') or code.startswith('sz.3')
                        )
                        if is_a_share:
                            exclude_codes = [
                                'sh.000001', 'sz.399001', 'sh.000300', 'sh.000016',
                                'sh.000905', 'sh.000688', 'sz.399006', 'sz.399005',
                                'sh.000852', 'sh.000010',
                            ]
                            if code not in exclude_codes:
                                normalized = self.normalize_stock_code(code)
                                stock_codes.append(normalized)
                    if (i + 1) % 100 == 0:
                        self._log(f"已处理 {i + 1}/{len(data_list)} 只股票...")
                stock_codes = sorted(list(set(stock_codes)))
                table_txt_path = self.file_paths.get('table_txt', '')
                with open(table_txt_path, 'w', encoding='utf-8') as f:
                    for code in stock_codes:
                        f.write(f"{code}\n")
                self._log(f"✅ 股票列表更新完成，共{len(stock_codes)}只股票")
                self._show_message("成功", f"股票列表更新完成，共{len(stock_codes)}只股票")
            else:
                self._log("未获取到股票数据", "WARNING")
        except Exception as e:
            self._log(f"更新股票列表失败: {e}", "ERROR")
            self._show_message("错误", f"更新失败: {e}")

    def retry_failed(self):
        """重试失败的股票"""
        failed_txt_path = self.file_paths.get('failed_txt', '')
        if not os.path.exists(failed_txt_path):
            self._show_message("提示", "没有失败的股票需要重试")
            return
        with open(failed_txt_path, 'r', encoding='utf-8') as f:
            failed_items = [line.strip() for line in f if line.strip()]
        if not failed_items:
            self._show_message("提示", "没有失败的股票需要重试")
            return
        failed_stocks = []
        for item in failed_items:
            if '|' in item:
                failed_stocks.append(item.split('|')[0])
            else:
                failed_stocks.append(item)
        failed_stocks = list(set(failed_stocks))
        table_txt_path = self.file_paths.get('table_txt', '')
        data_dir = self.data_dir
        temp_file = os.path.join(data_dir, 'table_temp.txt')
        backup_file = os.path.join(data_dir, 'table_backup.txt')
        with open(temp_file, 'w', encoding='utf-8') as f:
            for stock in failed_stocks:
                f.write(f"{stock}\n")
        if os.path.exists(table_txt_path):
            os.rename(table_txt_path, backup_file)
        os.rename(temp_file, table_txt_path)
        self.start_download()
        def restore_file():
            while self.is_running:
                time.sleep(1)
            if os.path.exists(backup_file):
                os.rename(backup_file, table_txt_path)
            if os.path.exists(failed_txt_path):
                os.remove(failed_txt_path)
        threading.Thread(target=restore_file, daemon=True).start()

    def start_download(self, selected_periods: List[str], save_interval_seconds: int = 180):
        """开始下载（由GUI调用，在后台线程中执行）"""
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
        self.save_interval = save_interval_seconds
        failed_txt = self.file_paths.get('failed_txt', '')
        if os.path.exists(failed_txt):
            try:
                os.remove(failed_txt)
            except Exception:
                pass
        download_thread = threading.Thread(target=self.download_worker_single_thread, args=(selected_periods,), daemon=True)
        download_thread.start()
        return download_thread
