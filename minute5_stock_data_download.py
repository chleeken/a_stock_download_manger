"""
5分钟股票数据下载工具（智能版，模块化）
功能：下载A股5分钟K线数据，自动增量更新，每只股票保存最近400条记录
作为模块被 a_stock_download_manger.py 调用
"""
import baostock as bs
import duckdb
import datetime
import time
import gc
import logging
import random
from pathlib import Path
from functools import wraps
import sys
import atexit
import threading
import queue
import traceback

# ==================== 线程化下载包装器（带超时） ====================
class _DownloadResult:
    __slots__ = ('data', 'fields', 'msg')
    def __init__(self):
        self.data = None
        self.fields = None
        self.msg = None


def _download_with_timeout(code, max_date, force, timeout, retry_times):
    """在线程中执行下载，超时后返回失败"""
    result = _DownloadResult()
    completed = threading.Event()

    def worker():
        try:
            data, msg, fields = _download_single(code, max_date, force, timeout, retry_times)
            result.data = data
            result.fields = fields
            result.msg = msg
        except Exception as e:
            result.msg = str(e)
        finally:
            completed.set()

    thread = threading.Thread(target=worker)
    thread.daemon = True
    thread.start()
    completed.wait(timeout)

    if not completed.is_set():
        return None, None, f"超时({timeout}s)"
    return result.data, result.fields, result.msg

# ==================== 配置 ====================
class Config:
    __slots__ = ('max_records', 'tolerance', 'db_dir', 'db_name', 'stock_file',
                 'request_delay', 'retry_times', 'batch_size', 'skip_threshold',
                 'wal_auto_checkpoint', 'wal_checkpoint_size', 'enable_wal',
                 'market_hours_only', 'no_data_threshold', 'batch_check_size',
                 'max_workers', 'timeout_per_stock',
                 'group_sample_size', 'group_query_timeout')

    def __init__(self):
        self.max_records = 400
        self.tolerance = 50
        self.db_dir = Path(__file__).parent.parent / "stock_data"
        self.db_name = "minute5_stock_data.duckdb"
        self.stock_file = "table.txt"
        self.request_delay = 0.05
        self.retry_times = 2
        self.batch_size = 100
        self.skip_threshold = 500
        self.wal_auto_checkpoint = True
        self.wal_checkpoint_size = 50
        self.enable_wal = True
        self.market_hours_only = False
        self.no_data_threshold = 3
        self.batch_check_size = 500
        self.max_workers = 5
        self.timeout_per_stock = 12
        self.group_sample_size = 5
        self.group_query_timeout = 15

    @property
    def default_db_path(self):
        return self.db_dir / self.db_name

config = Config()

# ==================== 日志 ====================
logger = logging.getLogger('stock_5min')
logger.setLevel(logging.INFO)
logger.handlers.clear()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False


# ==================== 下载函数（子进程安全） ====================
_bs_logged_in = False

def _ensure_baostock_login():
    global _bs_logged_in
    if not _bs_logged_in:
        lg = bs.login()
        if lg.error_code == '0':
            _bs_logged_in = True
        elif '已经登录' in lg.error_msg or '已登录' in lg.error_msg:
            _bs_logged_in = True

def _is_socket_error(err_str):
    """判断是否为 socket 层错误（需要强制重置连接）"""
    return any(k in err_str for k in ['10057', '10061', '10053', '10054', '10060'])

def _is_network_error(err_str):
    """判断是否为网络/数据损坏错误（可重试）"""
    return any(k in err_str for k in [
        '10060', '10053', '10054', '10061', '10057',
        'socket', '连接', '网络', '超时',
        'decompressing', 'codec', 'utf-8', 'encode', 'decode', 'binary', 'truncated', 'incomplete',
        '连接失败', '接收数据异常', '发送数据异常', '套接字', 'connection', 'refused', '服务器连接失败'
    ])

def _download_single(code, max_date, force, timeout, retry_times):
    """下载单只股票数据（线程安全版本）"""
    global _bs_logged_in
    c = ''.join(filter(str.isdigit, str(code)))
    c = c.zfill(6) if len(c) == 6 else ""
    if not c:
        return None, "无效代码", None

    full_code = f"sh.{c}" if c[0] == '6' else f"sz.{c}"

    now = datetime.datetime.now()
    if force or max_date is None:
        sd = (now - datetime.timedelta(days=15)).strftime("%Y-%m-%d")
        ed = now.strftime("%Y-%m-%d")
    else:
        sd = (max_date + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        ed = now.strftime("%Y-%m-%d")
        if sd > ed:
            return None, "已是最新", None

    for attempt in range(retry_times):
        try:
            _ensure_baostock_login()

            rs = bs.query_history_k_data_plus(
                full_code, "date,time,open,high,low,close,volume,amount",
                start_date=sd, end_date=ed, frequency="5", adjustflag="2"
            )

            if rs.error_code != '0':
                is_net = _is_network_error(rs.error_msg)
                if is_net and attempt < retry_times - 1:
                    time.sleep(1.0)
                    _bs_logged_in = False
                    _ensure_baostock_login()
                    continue
                if attempt < retry_times - 1:
                    time.sleep(0.3)
                    continue
                return None, rs.error_msg, None

            data_rows = []
            fields = None
            try:
                while rs.next():
                    try:
                        row_data = rs.get_row_data()
                        data_rows.append(row_data)
                        if fields is None:
                            fields = rs.fields
                    except Exception as inner_e:
                        inner_str = str(inner_e)
                        if any(k in inner_str for k in ['decompressing', 'codec', 'utf-8', 'binary', 'truncated', 'incomplete']):
                            raise
                        continue
            except Exception as e:
                err_str = str(e)
                is_net = _is_network_error(err_str)
                if is_net and attempt < retry_times - 1:
                    wait = 1.0 if _is_socket_error(err_str) else 0.5
                    time.sleep(wait)
                    _bs_logged_in = False
                    _ensure_baostock_login()
                    continue
                return None, err_str, None

            if not data_rows:
                return None, "无新数据", None

            return data_rows, "成功", fields

        except Exception as e:
            err_str = str(e)
            if _is_network_error(err_str) and attempt < retry_times - 1:
                wait = 1.0 if _is_socket_error(err_str) else 0.5
                time.sleep(wait)
                _bs_logged_in = False
                _ensure_baostock_login()
                continue
            return None, str(e), None

    return None, "达到最大重试次数", None


def _process_data(data_rows, fields, code, max_date):
    """处理数据"""
    if not data_rows or not fields:
        return []

    try:
        date_idx = fields.index('date')
        time_idx = fields.index('time')
        open_idx = fields.index('open')
        high_idx = fields.index('high')
        low_idx = fields.index('low')
        close_idx = fields.index('close')
        volume_idx = fields.index('volume')
        amount_idx = fields.index('amount')
    except ValueError:
        return []

    processed = []
    for row in data_rows:
        try:
            date_str = row[date_idx]  # "2026-07-28"
            time_str = row[time_idx]  # "20260728093500000" 或 "09:35:00"

            # 解析日期
            year = int(date_str[:4])
            month = int(date_str[5:7])
            day = int(date_str[8:10])

            # 解析时间 - 支持多种格式
            if len(time_str) >= 12 and time_str[8:10].isdigit():
                # 格式: 20260728093500000
                hhmm = time_str[8:12]
            elif ':' in time_str:
                # 格式: HH:MM:SS
                parts = time_str.split(':')
                hhmm = f"{parts[0]}{parts[1]}"
            else:
                # 直接是 HH:MM 或 HHMM
                hhmm = time_str[:4]

            hour = int(hhmm[:2])
            minute = int(hhmm[2:4]) if len(hhmm) >= 4 else 0

            dt = datetime.datetime(year, month, day, hour, minute)

            if max_date and dt <= max_date:
                continue

            processed.append((
                code, dt,
                float(row[open_idx]) if row[open_idx] else 0.0,
                float(row[high_idx]) if row[high_idx] else 0.0,
                float(row[low_idx]) if row[low_idx] else 0.0,
                float(row[close_idx]) if row[close_idx] else 0.0,
                float(row[volume_idx]) if row[volume_idx] else 0.0,
                float(row[amount_idx]) if row[amount_idx] else 0.0
            ))
        except (ValueError, IndexError):
            continue

    return processed


def _filter_time_points(data_records):
    """过滤时间点"""
    if not data_records:
        return []
    
    date_groups = {}
    for record in data_records:
        date_key = record[1].date()
        if date_key not in date_groups:
            date_groups[date_key] = []
        date_groups[date_key].append(record)
    
    standard_times = [(9, 30), (9, 35), (9, 40), (9, 45), (9, 50), (9, 55),
                      (10, 0), (10, 5), (10, 10), (10, 15), (10, 20), (10, 25),
                      (10, 30), (10, 35), (10, 40), (10, 45), (10, 50), (10, 55),
                      (11, 0), (11, 5), (11, 10), (11, 15), (11, 20), (11, 25),
                      (11, 30), (13, 0), (13, 5), (13, 10), (13, 15), (13, 20),
                      (13, 25), (13, 30), (13, 35), (13, 40), (13, 45), (13, 50),
                      (13, 55), (14, 0), (14, 5), (14, 10), (14, 15), (14, 20),
                      (14, 25), (14, 30), (14, 35), (14, 40), (14, 45), (14, 50),
                      (14, 55), (15, 0)]
    
    result = []
    for date, records in date_groups.items():
        records.sort(key=lambda x: x[1])
        for i, record in enumerate(records[:50]):
            if i < 50:
                h, m = standard_times[i]
                new_record = list(record)
                new_record[1] = datetime.datetime.combine(date, datetime.time(h, m))
                result.append(tuple(new_record))
    
    result.sort(key=lambda x: x[1])
    return result


# ==================== WAL管理器 ====================
class WALManager:
    def __init__(self, db_path):
        self.db_path = Path(db_path)
        self.checkpoint_counter = 0
        self.con = None
        self.wal_enabled = False

    def setup_wal(self, con):
        self.con = con
        try:
            if config.enable_wal:
                self.wal_enabled = True
                logger.info("WAL模式已启用")
        except Exception as e:
            logger.error(f"WAL配置失败: {e}")

    def checkpoint(self, con=None, force=False):
        if con is None:
            con = self.con
        if con is None:
            return
        try:
            if force:
                con.execute("CHECKPOINT")
                logger.info("强制WAL检查点执行完成")
                self.checkpoint_counter = 0
            elif self.checkpoint_counter >= config.wal_checkpoint_size:
                con.execute("CHECKPOINT")
                logger.info("定期WAL检查点执行完成")
                self.checkpoint_counter = 0
            else:
                self.checkpoint_counter += 1
        except Exception as e:
            logger.error(f"检查点执行失败: {e}")

    def cleanup(self, con=None):
        if con is None:
            con = self.con
        if con is None:
            return
        try:
            con.execute("CHECKPOINT")
        except Exception as e:
            logger.error(f"WAL清理失败: {e}")


# ==================== 核心类 ====================
class StockDownloader:
    __slots__ = ('db_path', 'con', '_lg', '_login_time', '_login_retry',
                 'wal_manager', '_stock_cache', '_need_update_list',
                 '_all_stocks_list', '_db_lock')

    def __init__(self, db_path=None):
        self.db_path = Path(db_path or config.default_db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.wal_manager = WALManager(self.db_path)
        self.con = duckdb.connect(str(self.db_path))
        self.wal_manager.setup_wal(self.con)

        try:
            self.con.execute("PRAGMA threads=4")
            self.con.execute("PRAGMA memory_limit='2GB'")
        except:
            pass

        self._init_db()
        self._lg = False
        self._login_time = None
        self._login_retry = 0
        self._stock_cache = {}
        self._need_update_list = []
        self._all_stocks_list = []
        self._db_lock = threading.Lock()

        atexit.register(self.cleanup_wal)
        logger.info(f"数据库: {self.db_path}")

    def cleanup_wal(self):
        try:
            if hasattr(self, 'con') and self.con:
                self.wal_manager.cleanup(self.con)
        except:
            pass

    def _init_db(self):
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS minute5_data (
                code VARCHAR,
                date_time TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                amount DOUBLE
            )
        """)
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_5min_c_t ON minute5_data(code, date_time)")

        count = self.con.execute("SELECT COUNT(*) FROM minute5_data").fetchone()[0]
        stock_count = self.con.execute("SELECT COUNT(DISTINCT code) FROM minute5_data").fetchone()[0]
        logger.info(f"数据库状态: {stock_count}只股票, {count}条数据")

    def _check_login(self):
        if not self._lg:
            return False
        if self._login_time and (datetime.datetime.now() - self._login_time).seconds > 3600:
            logger.info("登录可能已过期，重新登录...")
            self._logout()
            return False
        return True

    def _login(self):
        if self._check_login():
            return True
        max_retries = 3
        for retry in range(max_retries):
            try:
                lg = bs.login()
                if lg.error_code == '0':
                    self._lg = True
                    self._login_time = datetime.datetime.now()
                    self._login_retry = 0
                    logger.info("登录成功")
                    return True
                elif '已经登录' in lg.error_msg or '已登录' in lg.error_msg:
                    self._lg = True
                    self._login_time = datetime.datetime.now()
                    logger.info("检测到已登录会话，复用现有连接")
                    return True
                else:
                    logger.error(f"登录失败:{lg.error_msg}")
            except Exception as e:
                logger.error(f"登录异常:{e}")
            if retry < max_retries - 1:
                wait_time = 2 ** retry
                time.sleep(wait_time)
        raise Exception("登录失败，请检查网络")

    def _logout(self):
        if self._lg:
            try:
                bs.logout()
            except:
                pass
            self._lg = False
            self._login_time = None

    def _full_code(self, code):
        return f"sh.{code}" if code[0] == '6' else f"sz.{code}"

    def read_file(self, f=None):
        base_dir = Path(__file__).parent
        p = Path(f or (base_dir / config.stock_file))
        if not p.exists():
            logger.error(f"文件不存在:{p}")
            return []
        codes = []
        try:
            with open(p, 'r', encoding='gbk') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        c = ''.join(filter(str.isdigit, str(line)))
                        c = c.zfill(6) if len(c) == 6 else ""
                        if c:
                            codes.append(c)
            logger.info(f"从文件读取{len(codes)}只股票: {p}")
            self._all_stocks_list = codes
            return codes
        except Exception as e:
            logger.error(f"读取失败:{e}")
            return []

    def update_cache_from_db(self):
        start_time = time.time()
        if not self._all_stocks_list:
            self.read_file()
        if not self._all_stocks_list:
            logger.warning("股票列表为空")
            return {}, []

        placeholders = ','.join(['?'] * len(self._all_stocks_list))
        result = self.con.execute(f"""
            SELECT code, MAX(date_time) as max_date, COUNT(*) as cnt
            FROM minute5_data WHERE code IN ({placeholders})
            GROUP BY code
        """, self._all_stocks_list).fetchall()

        self._stock_cache = {row[0]: {'max_date': row[1], 'count': row[2]} for row in result}

        now_date = datetime.datetime.now().date()
        self._need_update_list = []

        for code in self._all_stocks_list:
            info = self._stock_cache.get(code)
            if info is None:
                self._need_update_list.append(code)
            else:
                max_date = info['max_date']
                cnt = info['count']
                if cnt < config.max_records * 0.8:
                    self._need_update_list.append(code)
                elif cnt < config.max_records and max_date and (now_date - max_date.date()).days >= 1:
                    self._need_update_list.append(code)

        elapsed = time.time() - start_time
        logger.info(f"缓存更新: 需更新{len(self._need_update_list)}只, 耗时{elapsed:.2f}秒")
        return self._stock_cache, self._need_update_list

    def save_single_stock(self, code, records):
        try:
            c = self.norm(code)
            if not records:
                return True, "无数据"
            date_times = [r[1] for r in records]
            min_dt = min(date_times)
            max_dt = max(date_times)

            with self._db_lock:
                existing = {e[0] for e in self.con.execute(
                    "SELECT date_time FROM minute5_data WHERE code=? AND date_time BETWEEN ? AND ?",
                    [c, min_dt, max_dt]
                ).fetchall()}
                new_records = [r for r in records if r[1] not in existing]
                if not new_records:
                    return True, "无新数据"

                self.con.executemany(
                    "INSERT INTO minute5_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)", new_records
                )
                current_count = self.con.execute(
                    "SELECT COUNT(*) FROM minute5_data WHERE code=?", [c]
                ).fetchone()[0]

                if current_count > config.max_records + config.tolerance:
                    to_delete = current_count - config.max_records
                    self.con.execute("""
                        DELETE FROM minute5_data WHERE code=? AND date_time IN (
                            SELECT date_time FROM minute5_data WHERE code=? ORDER BY date_time LIMIT ?
                        )
                    """, [c, c, to_delete])
                
                return True, f"保存{len(new_records)}条"
        except Exception as e:
            logger.error(f"{code} 保存失败:{e}")
            return False, str(e)

    def norm(self, code):
        if not code:
            return ""
        c = ''.join(filter(str.isdigit, str(code)))
        return c.zfill(6) if len(c) == 6 else ""

    def _query_sample_available_dates(self, sample_codes, days=60):
        """查询样本股票的可下载日期，用于判断分组是否需要下载"""
        try:
            if not self._check_login():
                self._login()

            available_dates = set()
            for code in sample_codes:
                c = self.norm(code)
                full_code = self._full_code(c)
                now = datetime.datetime.now()
                end_date = now.strftime("%Y-%m-%d")
                start_date = (now - datetime.timedelta(days=days)).strftime("%Y-%m-%d")

                rs = bs.query_history_k_data_plus(
                    full_code, "date",
                    start_date=start_date, end_date=end_date, frequency="5", adjustflag="2"
                )
                if rs.error_code != '0':
                    continue
                while rs.next():
                    available_dates.add(rs.get_row_data()[0])
                time.sleep(config.request_delay)

            return available_dates
        except Exception as e:
            logger.error(f"查询样本日期失败: {e}")
            return set()

    def _build_download_groups(self):
        """按最后下载日期分组股票"""
        if not self._stock_cache:
            return []

        groups = {}
        for code, info in self._stock_cache.items():
            max_date = info.get('max_date')
            if max_date is None:
                continue
            date_key = max_date.date() if hasattr(max_date, 'date') else max_date
            if date_key not in groups:
                groups[date_key] = []
            groups[date_key].append(code)

        result = []
        for date_key, codes in groups.items():
            for code in codes:
                if code in self._need_update_list:
                    result.append((date_key, code))

        result.sort(key=lambda x: x[0], reverse=True)

        group_list = []
        current_group = []
        current_date = None
        for date_key, code in result:
            if date_key != current_date:
                if current_group:
                    group_list.append(current_group)
                current_group = [code]
                current_date = date_key
            else:
                current_group.append(code)
        if current_group:
            group_list.append(current_group)

        return group_list

    def _get_group_download_dates(self, group_codes):
        """获取分组可下载日期（线程+Event+超时）"""
        event = threading.Event()
        result = {'dates': set(), 'error': None, 'completed': False}
        timeout = config.group_query_timeout

        def query_worker():
            try:
                sample_size = min(config.group_sample_size, len(group_codes))
                if sample_size <= 0:
                    return
                sample_codes = random.sample(group_codes, sample_size)
                dates = self._query_sample_available_dates(sample_codes)
                result['dates'] = dates
            except Exception as e:
                result['error'] = str(e)
            finally:
                result['completed'] = True
                event.set()

        thread = threading.Thread(target=query_worker)
        thread.daemon = True
        thread.start()

        if not event.wait(timeout):
            logger.warning(f"查询分组可下载日期超时({timeout}s)")
            return None, f"超时({timeout}s)"

        if result['error']:
            logger.error(f"查询分组日期失败: {result['error']}")
            return None, result['error']

        return result['dates'], None

    def download_optimized(self, stocks=None, force=False, progress=None, log_callback=None):
        if stocks is None:
            stocks = self.read_file()
        if not stocks:
            logger.warning("股票列表为空")
            return {'t': 0, 's': 0, 'k': 0, 'f': 0}

        self._all_stocks_list = [self.norm(code) for code in stocks if self.norm(code)]
        self.update_cache_from_db()

        if not force:
            download_list = self._need_update_list
            logger.info(f"快速过滤: 需下载{len(download_list)}只")
        else:
            download_list = self._all_stocks_list
            logger.info(f"强制下载全部: {len(download_list)}只")

        res = {'t': len(self._all_stocks_list), 's': 0, 'k': len(self._all_stocks_list) - len(download_list), 'f': 0}

        if not download_list:
            if log_callback:
                log_callback("所有股票都已是最新，无需下载")
            return res

        if log_callback:
            log_callback(f"开始下载任务: 需下载{len(download_list)}只")
            log_callback(f"单只超时: {config.timeout_per_stock}秒, 重试: {config.retry_times}次")

        group_list = self._build_download_groups()
        if group_list and len(group_list) > 1:
            if log_callback:
                log_callback(f"检测到{len(group_list)}个日期分组，使用分组下载")

            start_time = time.time()
            for group_idx, group in enumerate(group_list):
                if log_callback:
                    log_callback(f"处理第{group_idx + 1}/{len(group_list)}组, {len(group)}只股票")

                if progress:
                    progress(0, len(download_list), "", f"分组{group_idx + 1}/{len(group_list)}")

                sample_codes = random.sample(group, min(config.group_sample_size, len(group)))
                available_dates, err = self._get_group_download_dates(sample_codes)

                if err:
                    if log_callback:
                        log_callback(f"第{group_idx + 1}组查询失败，跳过该组")
                    continue

                if not available_dates:
                    if log_callback:
                        log_callback(f"第{group_idx + 1}组无可下载数据，跳过{len(group)}只")
                    res['k'] += len(group)
                    continue

                for i, code in enumerate(group):
                    max_date = self._stock_cache.get(code, {}).get('max_date') if not force else None
                    data, fields, msg = _download_with_timeout(
                        code, max_date, force,
                        config.timeout_per_stock, config.retry_times
                    )
                    if data is None:
                        res['f'] += 1
                        if msg and "已是最新" not in msg and "无新数据" not in msg:
                            if log_callback:
                                log_callback(f"{code}: {msg}")
                    else:
                        processed = _process_data(data, fields, code, max_date)
                        if not processed:
                            res['f'] += 1
                            continue
                        filtered = _filter_time_points(processed)
                        if filtered:
                            success, save_msg = self.save_single_stock(code, filtered)
                            if success:
                                res['s'] += 1
                                if log_callback:
                                    log_callback(f"✓ {code}: {save_msg}")
                            else:
                                res['f'] += 1
                        else:
                            res['f'] += 1
                    time.sleep(config.request_delay)

                if progress:
                    progress(len(download_list), len(download_list), "",
                            f"分组{group_idx + 1}/{len(group_list)}完成")

                if config.wal_auto_checkpoint:
                    self.wal_manager.checkpoint(self.con)
                gc.collect()
        else:
            if log_callback and group_list:
                log_callback("仅1个日期分组，使用串行下载")
            elif not group_list:
                if log_callback:
                    log_callback("无历史数据，使用串行下载")

            total = len(download_list)
            start_time = time.time()

            success_count = 0
            skip_count = 0
            fail_count = 0
            fail_list = []
            count_since_last_save = 0

            # baostock 是单连接协议，使用串行下载保证稳定
            for i, code in enumerate(download_list):
                max_date = self._stock_cache.get(code, {}).get('max_date') if not force else None

                # 使用线程包装实现超时控制
                data, fields, msg = _download_with_timeout(
                    code, max_date, force,
                    config.timeout_per_stock, config.retry_times
                )

                if data is None:
                    fail_count += 1
                    if msg and "已是最新" not in msg and "无新数据" not in msg:
                        fail_list.append((code, msg))
                        if log_callback:
                            log_callback(f"{code}: {msg}")
                else:
                    processed = _process_data(data, fields, code, max_date)
                    if not processed:
                        fail_count += 1
                        continue

                    filtered = _filter_time_points(processed)
                    if filtered:
                        success, save_msg = self.save_single_stock(code, filtered)
                        if success:
                            success_count += 1
                            count_since_last_save += 1
                            if log_callback:
                                log_callback(f"✓ {code}: {save_msg}")
                        else:
                            fail_count += 1
                            fail_list.append((code, save_msg))
                    else:
                        fail_count += 1

                # 进度报告
                if log_callback and (i + 1) % 50 == 0:
                    elapsed = time.time() - start_time
                    speed = (i + 1) / elapsed if elapsed > 0 else 0
                    log_callback(f"进度: {i+1}/{total} ({(i+1)*100//total}%), {speed:.2f}只/秒")

                if progress:
                    progress(i + 1, total, "", f"已处理 {i+1}/{total}")

                # 每200只股票保存一次数据库
                if count_since_last_save >= 200:
                    self.wal_manager.checkpoint(self.con, force=True)
                    count_since_last_save = 0
                    if log_callback:
                        log_callback(f"已下载{success_count}只，强制保存数据库")

                # 定期检查点
                if (i + 1) % config.batch_size == 0:
                    self.wal_manager.checkpoint(self.con)

                # 短暂间隔，避免服务器压力
                time.sleep(config.request_delay)

            # 最终检查点
            self.wal_manager.checkpoint(self.con, force=True)

            elapsed = time.time() - start_time
            res['s'] = success_count
            res['k'] = skip_count
            res['f'] = fail_count

            if log_callback:
                log_callback(f"\n下载完成: 成功:{res['s']} 跳过:{res['k']} 失败:{res['f']} 耗时:{elapsed:.1f}秒")
                if elapsed > 0 and res['s'] > 0:
                    speed = res['s'] / elapsed
                    log_callback(f"平均速度: {speed:.2f}只/秒")
                    remaining = total - success_count - skip_count
                    if remaining > 0:
                        log_callback(f"预计剩余: {remaining/speed/60:.1f}分钟")
                if fail_list:
                    log_callback(f"失败股票数: {len(fail_list)}")
                    for code, msg in fail_list[:10]:
                        log_callback(f"  {code}: {msg}")

            if progress:
                progress(total, total, "", f"完成 (成功:{res['s']}, 耗时:{elapsed:.1f}秒)")

        return res

    def get_stats(self, code=None):
        if code:
            c = self.norm(code)
            return self.con.execute("""
                SELECT code, COUNT(*), COUNT(DISTINCT DATE(date_time)),
                       MIN(date_time), MAX(date_time)
                FROM minute5_data WHERE code=? GROUP BY code
            """, [c]).fetchone()
        else:
            return self.con.execute("""
                SELECT code, COUNT(*) FROM minute5_data GROUP BY code ORDER BY code
            """).fetchall()

    def close(self):
        logger.info("关闭数据库连接...")
        self._logout()
        if hasattr(self, 'wal_manager') and self.wal_manager:
            try:
                self.wal_manager.cleanup(self.con)
            except Exception as e:
                logger.error(f"WAL清理失败: {e}")
        try:
            self.con.close()
            logger.info("数据库连接已关闭")
        except Exception as e:
            logger.error(f"关闭连接失败: {e}")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ==================== 模块接口 ====================
def run_module(stocks=None, force=False, progress_callback=None, log_callback=None):
    """被主程序调用的模块入口"""
    if log_callback:
        log_callback("开始5分钟K线数据下载...")
    with StockDownloader() as d:
        return d.download_optimized(stocks, force, progress_callback, log_callback)


# ==================== 主程序 ====================
def main():
    import argparse
    parser = argparse.ArgumentParser(description="A股5分钟K线下载器")
    parser.add_argument("--cli", action="store_true", help="命令行模式")
    parser.add_argument("--workers", type=int, default=5, help="并发线程数")
    parser.add_argument("--timeout", type=int, default=12, help="单只股票超时时间(秒)")
    args = parser.parse_args()

    if args.cli:
        config.max_workers = args.workers
        config.timeout_per_stock = args.timeout
        
        print("=" * 80)
        print("A股5分钟K线下载器")
        print("=" * 80)
        with StockDownloader() as d:
            stocks = d.read_file()
            print(f"从文件读取 {len(stocks)} 只股票")
            start_time = time.time()
            res = d.download_optimized(stocks, False)
            elapsed = time.time() - start_time
            print(f"\n完成! 成功:{res['s']} 失败:{res['f']} 耗时:{elapsed:.1f}秒")
            if elapsed > 0:
                print(f"平均速度: {res['s']/elapsed:.2f} 只/秒")
                print(f"预计5000只: {5000/max(res['s'],1)*elapsed/60:.1f}分钟")
    else:
        print("5分钟K线数据下载模块")
        print("作为模块被主程序调用，或使用 --cli 参数命令行运行")


if __name__ == "__main__":
    main()
