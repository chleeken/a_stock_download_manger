"""
30分钟股票数据下载工具（智能版，模块化）
功能：下载A股30分钟K线数据，自动增量更新，每只股票保存最近400条记录
作为模块被 a_stock_download_manger.py 调用
"""
import baostock as bs
import duckdb
import datetime
import time
import gc
import logging
import random
import threading
from pathlib import Path
from functools import wraps
import sys
import atexit

# ==================== 配置 ====================
class Config:
    __slots__ = ('max_records', 'tolerance', 'db_dir', 'db_name', 'stock_file',
                 'request_delay', 'retry_times', 'batch_size', 'skip_threshold',
                 'wal_auto_checkpoint', 'wal_checkpoint_size', 'enable_wal',
                 'market_hours_only', 'no_data_threshold', 'batch_check_size',
                 'group_sample_size', 'group_query_timeout')

    def __init__(self):
        self.max_records = 400
        self.tolerance = 50
        self.db_dir = Path(__file__).parent.parent / "stock_data"
        self.db_name = "minute30_stock_data.duckdb"
        self.stock_file = "table.txt"
        self.request_delay = 0.01
        self.retry_times = 2
        self.batch_size = 200
        self.skip_threshold = 500
        self.wal_auto_checkpoint = True
        self.wal_checkpoint_size = 200
        self.enable_wal = True
        self.market_hours_only = False
        self.no_data_threshold = 3
        self.batch_check_size = 500
        self.group_sample_size = 5
        self.group_query_timeout = 15

    @property
    def default_db_path(self):
        return self.db_dir / self.db_name

config = Config()

# ==================== 日志 ====================
logger = logging.getLogger('stock_30min')
logger.setLevel(logging.INFO)
logger.handlers.clear()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False


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

    def get_wal_info(self, con=None):
        return {
            'enabled': self.wal_enabled,
            'status': '已启用' if self.wal_enabled else '未启用',
            'checkpoint_counter': self.checkpoint_counter
        }

    def cleanup(self, con=None):
        if con is None:
            con = self.con
        if con is None:
            return
        try:
            con.execute("CHECKPOINT")
            logger.info("退出前执行WAL检查点")
        except Exception as e:
            logger.error(f"WAL清理失败: {e}")


# ==================== 全局登录状态 ====================
_bs_logged_in = False

def _ensure_baostock_login():
    global _bs_logged_in
    if not _bs_logged_in:
        lg = bs.login()
        if lg.error_code == '0':
            _bs_logged_in = True
        elif '已经登录' in lg.error_msg or '已登录' in lg.error_msg:
            _bs_logged_in = True


# ==================== 核心类 ====================
def ensure_login(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if not self._lg:
            self._login()
        return f(self, *args, **kwargs)
    return wrapper


# ==================== 核心类 ====================
class StockDownloader:
    __slots__ = ('db_path', 'con', '_lg', '_login_time', '_login_retry',
                 'wal_manager', '_batch_saved_count', '_stock_cache',
                 '_need_update_list', '_last_cache_update', '_no_data_counter',
                 '_last_check_date', '_all_stocks_list', '_pending_data',
                 '_batch_size')

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
        self._batch_saved_count = 0
        self._stock_cache = {}
        self._need_update_list = []
        self._last_cache_update = None
        self._no_data_counter = 0
        self._last_check_date = None
        self._all_stocks_list = []
        self._pending_data = []
        self._batch_size = 200

        atexit.register(self.cleanup_wal)
        logger.info(f"数据库: {self.db_path}")
        wal_info = self.wal_manager.get_wal_info(self.con)
        if wal_info['enabled']:
            logger.info(f"WAL模式: {wal_info['status']}")

    def cleanup_wal(self):
        try:
            if hasattr(self, 'con') and self.con:
                try:
                    self.con.execute("SELECT 1")
                    self.wal_manager.cleanup(self.con)
                except:
                    pass
        except:
            pass

    def _init_db(self):
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS minute30_data (
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
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_30min_c_t ON minute30_data(code, date_time)")

        self.con.execute("""
            CREATE TEMP TABLE IF NOT EXISTS temp_stock_data (
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

        count = self.con.execute("SELECT COUNT(*) FROM minute30_data").fetchone()[0]
        stock_count = self.con.execute("SELECT COUNT(DISTINCT code) FROM minute30_data").fetchone()[0]
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

    def norm(self, code):
        if not code:
            return ""
        c = ''.join(filter(str.isdigit, str(code)))
        return c.zfill(6) if len(c) == 6 else ""

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
                        c = self.norm(line)
                        if c:
                            codes.append(c)
            logger.info(f"从文件读取{len(codes)}只股票: {p}")
            self._all_stocks_list = codes
            return codes
        except Exception as e:
            logger.error(f"读取失败:{e}")
            return []

    def update_cache_from_db(self, force=False):
        start_time = time.time()
        if not self._all_stocks_list:
            self.read_file()
        if not self._all_stocks_list:
            logger.warning("股票列表为空")
            return {}, []

        placeholders = ','.join(['?'] * len(self._all_stocks_list))
        result = self.con.execute(f"""
            SELECT code, MAX(date_time) as max_date, COUNT(*) as cnt
            FROM minute30_data WHERE code IN ({placeholders})
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
                elif max_date and (now_date - max_date.date()).days >= 1:
                    self._need_update_list.append(code)

        self._last_cache_update = datetime.datetime.now()
        elapsed = time.time() - start_time
        logger.info(f"缓存更新: 文件中有{len(self._all_stocks_list)}只, 数据库中有{len(self._stock_cache)}只, 需更新{len(self._need_update_list)}只, 耗时{elapsed:.2f}秒")
        return self._stock_cache, self._need_update_list

    def need_download_fast(self, code, force=False):
        c = self.norm(code)
        if not c:
            return False, "无效代码"
        if force:
            return True, "强制下载"
        if c in self._need_update_list:
            return True, "需要更新"
        return False, "已是最新"

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

                logger.info(f"查询样本[{i+1}/{len(sample_codes)}]: {full_code}")
                rs = bs.query_history_k_data_plus(
                    full_code, "date",
                    start_date=start_date, end_date=end_date, frequency="30", adjustflag="2"
                )
                if rs.error_code != '0':
                    logger.warning(f"查询失败: {rs.error_msg}")
                    continue
                count = 0
                max_sample_rows = 100  # 查询日期只需要少量数据
                while count < max_sample_rows:
                    if rs.next():
                        available_dates.add(rs.get_row_data()[0])
                        count += 1
                    else:
                        break
                logger.info(f"样本{full_code}: {count}条数据")
                time.sleep(config.request_delay)

            logger.info(f"查询完成，共{len(available_dates)}个日期")
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
        """获取分组可下载日期（直接查询）"""
        try:
            logger.info(f"查询分组可下载日期，样本数: {len(group_codes)}")
            if not self._check_login():
                self._login()
            sample_size = min(config.group_sample_size, len(group_codes))
            if sample_size <= 0:
                return set(), None
            sample_codes = random.sample(group_codes, sample_size)
            logger.info(f"采样股票: {sample_codes[:3]}...")
            dates = self._query_sample_available_dates(sample_codes)
            logger.info(f"查询结果: 找到{len(dates)}个可下载日期")
            return dates, None
        except Exception as e:
            logger.error(f"查询分组日期失败: {e}")
            return None, str(e)

    def is_trading_time(self):
        now = datetime.datetime.now()
        current_hour = now.hour
        current_minute = now.minute
        if now.weekday() >= 5:
            return False
        if (current_hour == 9 and current_minute >= 30) or (current_hour == 10) or (current_hour == 11 and current_minute <= 30):
            return True
        if (current_hour == 13) or (current_hour == 14) or (current_hour == 15 and current_minute == 0):
            return True
        return False

    def check_market_has_today_data(self):
        try:
            test_code = self._all_stocks_list[0] if self._all_stocks_list else "600000"
            c = self.norm(test_code)
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            rs = bs.query_history_k_data_plus(
                self._full_code(c), "date",
                start_date=today, end_date=today, frequency="30", adjustflag="2"
            )
            if rs.error_code != '0':
                return True
            has_data = False
            while rs.next():
                has_data = True
                break
            return has_data
        except:
            return True

    def parse_date_time(self, date_str, time_str):
        try:
            year = int(date_str[:4])
            month = int(date_str[5:7])
            day = int(date_str[8:10])
            if len(time_str) >= 12:
                hhmm = time_str[8:12]
            elif len(time_str) >= 4:
                hhmm = time_str[:4]
            else:
                hhmm = time_str
            hour = int(hhmm[:2])
            minute = int(hhmm[2:4])
            return datetime.datetime(year, month, day, hour, minute)
        except:
            return None

    def process_stock_data_batch(self, data_rows, fields, code, max_date=None):
        if not data_rows:
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
        except ValueError as e:
            logger.error(f"字段索引错误: {e}")
            return []

        processed_data = []
        for row in data_rows:
            try:
                dt = self.parse_date_time(row[date_idx], row[time_idx])
                if dt is None:
                    continue
                if max_date and dt <= max_date:
                    continue
                processed_data.append((
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
        return processed_data

    def filter_time_points_batch(self, data_records):
        if not data_records:
            return []
        date_groups = {}
        for record in data_records:
            date_key = record[1].date()
            if date_key not in date_groups:
                date_groups[date_key] = []
            date_groups[date_key].append(record)
        standard_times = [(9, 30), (10, 0), (10, 30), (11, 0), (13, 0), (13, 30), (14, 0), (14, 30), (15, 0)]
        result = []
        for date, records in date_groups.items():
            records.sort(key=lambda x: x[1])
            for i, record in enumerate(records[:9]):
                if i < 9:
                    h, m = standard_times[i]
                    new_record = list(record)
                    new_record[1] = datetime.datetime.combine(date, datetime.time(h, m))
                    result.append(tuple(new_record))
        result.sort(key=lambda x: x[1])
        return result

    def save_single_stock(self, code, records):
        try:
            c = self.norm(code)
            if not records:
                return True, "无数据"
            date_times = [r[1] for r in records]
            min_dt = min(date_times)
            max_dt = max(date_times)
            existing = {e[0] for e in self.con.execute(
                "SELECT date_time FROM minute30_data WHERE code=? AND date_time BETWEEN ? AND ?",
                [c, min_dt, max_dt]
            ).fetchall()}
            new_records = [r for r in records if r[1] not in existing]
            if not new_records:
                return True, "无新数据"
            self._pending_data.extend(new_records)
            if len(self._pending_data) >= config.batch_size:
                self._flush_pending_data()
            current_count = self.con.execute(
                "SELECT COUNT(*) FROM minute30_data WHERE code=?", [c]
            ).fetchone()[0]
            logger.info(f"{c} 已缓存{len(new_records)}条, 待保存总数{len(self._pending_data)}")
            return True, f"缓存{len(new_records)}条"
        except Exception as e:
            logger.error(f"{code} 保存失败:{e}")
            return False, str(e)

    def _flush_pending_data(self):
        if not self._pending_data:
            return 0
        try:
            self.con.executemany(
                "INSERT INTO minute30_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                self._pending_data
            )
            count = len(self._pending_data)
            self._pending_data = []
            logger.info(f"批量保存{count}条数据到数据库")
            return count
        except Exception as e:
            logger.error(f"批量保存失败:{e}")
            return 0

    def download_stock(self, code, force=False, days=365):
        max_retries = config.retry_times
        for attempt in range(max_retries):
            try:
                c = self.norm(code)
                if not c:
                    return None, "无效代码"
                need, reason = self.need_download_fast(c, force)
                if not need and not force:
                    return None, reason
                if not self._check_login():
                    self._login()
                max_date = self._stock_cache.get(c, {}).get('max_date') if not force else None
                now = datetime.datetime.now()
                if force or max_date is None:
                    download_days = 60 if max_date is None else days
                    sd = (now - datetime.timedelta(days=download_days)).strftime("%Y-%m-%d")
                    ed = now.strftime("%Y-%m-%d")
                else:
                    sd = (max_date + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                    ed = now.strftime("%Y-%m-%d")
                    if sd > ed:
                        return None, "已是最新"
                rs = bs.query_history_k_data_plus(
                    self._full_code(c), "date,time,open,high,low,close,volume,amount",
                    start_date=sd, end_date=ed, frequency="30", adjustflag="2"
                )
                if rs.error_code != '0':
                    is_network_error = any(k in rs.error_msg for k in ['网络', '连接', 'socket', '超时', '10060', '10053', '10054', '10061', '10038', 'decompressing', 'codec', 'utf-8', 'encode', 'decode', 'binary', 'truncated'])
                    if is_network_error and attempt < max_retries - 1:
                        wait = min(2 ** attempt, 10)
                        msg = f"{c} 网络错误({rs.error_msg[:30]}...)，{wait}秒后重试 ({attempt+1}/{max_retries})"
                        logger.warning(msg)
                        time.sleep(wait)
                        continue
                    logger.error(f"{c} 下载失败:{rs.error_msg}")
                    return None, rs.error_msg
                data_rows = []
                fields = rs.fields
                max_rows = 10000  # 限制最大读取条数
                row_count = 0
                while row_count < max_rows:
                    if rs.next():
                        data_rows.append(rs.get_row_data())
                        row_count += 1
                        if row_count % 1000 == 0:
                            logger.info(f"下载中... 已读取{row_count}条")
                    else:
                        break
                if row_count >= max_rows:
                    logger.warning(f"{c} 数据量超过{max_rows}条，已截断")
                if not data_rows:
                    return None, "无新数据"
                processed_data = self.process_stock_data_batch(data_rows, fields, c, max_date if not force else None)
                if not processed_data:
                    return None, "处理后无新数据"
                filtered_data = self.filter_time_points_batch(processed_data)
                if filtered_data:
                    return filtered_data, "成功"
                else:
                    return None, "无有效数据"
            except Exception as e:
                err_str = str(e)
                is_network = any(k in err_str for k in ['10060', '10053', '10054', '10061', '10038', 'socket', '连接', '网络', '超时', 'decompressing', 'codec', 'utf-8', 'encode', 'decode', 'binary', 'truncated'])
                if is_network and attempt < max_retries - 1:
                    wait = 2 ** attempt
                    logger.warning(f"{code} 网络异常，{wait}秒后重试 ({attempt+1}/{max_retries})")
                    time.sleep(wait)
                    continue
                logger.error(f"{code} 处理异常:{str(e)}")
                _ensure_baostock_login()
                return None, str(e)
        return None, "重试次数已达上限"

    def download_optimized(self, stocks=None, force=False, progress=None, log_callback=None):
        if stocks is None:
            stocks = self.read_file()
        if not stocks:
            logger.warning("股票列表为空")
            return {'t': 0, 's': 0, 'k': 0, 'f': 0}

        self._all_stocks_list = [self.norm(code) for code in stocks if self.norm(code)]
        self.update_cache_from_db()
        self._login()

        if not force:
            download_list = self._need_update_list
            logger.info(f"快速过滤: 文件中有{len(self._all_stocks_list)}只, 需下载{len(download_list)}只")
        else:
            download_list = self._all_stocks_list
            logger.info(f"强制下载全部: {len(download_list)}只")

        res = {'t': len(self._all_stocks_list), 's': 0, 'k': len(self._all_stocks_list) - len(download_list), 'f': 0}

        if not download_list:
            if log_callback:
                log_callback("所有股票都已是最新，无需下载")
            if progress:
                progress(0, 0, "", "完成 (无需下载)")
            return res

        is_trading = self.is_trading_time()
        has_today_data = self.check_market_has_today_data() if is_trading else False

        if log_callback:
            log_callback(f"开始下载任务: 需下载{len(download_list)}只")

        group_list = self._build_download_groups()
        logger.info(f"分组列表: {len(group_list)}个组")
        if group_list and len(group_list) > 1:
            if log_callback:
                log_callback(f"检测到{len(group_list)}个日期分组，使用分组下载")

            for group_idx, group in enumerate(group_list):
                if log_callback:
                    log_callback(f"处理第{group_idx + 1}/{len(group_list)}组, {len(group)}只股票")

                if progress:
                    progress(0, len(download_list), "", f"分组{group_idx + 1}/{len(group_list)}")

                sample_codes = random.sample(group, min(config.group_sample_size, len(group)))
                logger.info(f"第{group_idx + 1}组: 查询{len(sample_codes)}只样本股票可下载日期...")
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

                for code in group:
                    logger.info(f"下载股票: {code}")
                    try:
                        data, msg = self.download_stock(code, days=len(available_dates))
                        if data is None:
                            if "无新数据" in msg or "已是最新" in msg:
                                res['k'] += 1
                            else:
                                res['f'] += 1
                                logger.error(f"{code} 下载失败: {msg}")
                        else:
                            success, save_msg = self.save_single_stock(code, data)
                            if success:
                                res['s'] += 1
                                if log_callback:
                                    log_callback(f"{code}: {save_msg}")
                            else:
                                res['f'] += 1
                    except Exception as e:
                        logger.error(f"{code} 处理异常: {e}")
                        res['f'] += 1
                    time.sleep(config.request_delay)

                if progress:
                    progress(len(download_list), len(download_list), "",
                            f"分组{group_idx + 1}/{len(group_list)}完成")

                if config.wal_auto_checkpoint:
                    self.wal_manager.checkpoint(self.con, force=True)
                    logger.info(f"分组保存完成，已checkpoint")
                gc.collect()
        else:
            if log_callback and group_list:
                log_callback("仅1个日期分组，使用批量下载")
            elif not group_list:
                if log_callback:
                    log_callback("无历史数据，使用批量下载")

            batch_size = config.batch_size
            total_batches = (len(download_list) + batch_size - 1) // batch_size

            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(download_list))
                batch_codes = download_list[start_idx:end_idx]

                if log_callback:
                    log_callback(f"处理第{batch_idx + 1}/{total_batches}批, {len(batch_codes)}只股票")

                if progress:
                    progress(start_idx, len(download_list), "", f"批{batch_idx + 1}/{total_batches}")

                batch_success = 0
                batch_skip = 0
                batch_fail = 0

                for code in batch_codes:
                    logger.info(f"下载股票: {code}")
                    try:
                        data, msg = self.download_stock(code, force)
                        if data is None:
                            if "无新数据" in msg or "已是最新" in msg:
                                batch_skip += 1
                            else:
                                batch_fail += 1
                                logger.error(f"{code} 下载失败: {msg}")
                        else:
                            success, save_msg = self.save_single_stock(code, data)
                            if success:
                                batch_success += 1
                                if log_callback:
                                    log_callback(f"{code}: {save_msg}")
                            else:
                                batch_fail += 1
                    except Exception as e:
                        logger.error(f"{code} 处理异常: {e}")
                        batch_fail += 1
                    time.sleep(config.request_delay)

                res['s'] += batch_success
                res['k'] += batch_skip
                res['f'] += batch_fail

                if progress:
                    progress(end_idx, len(download_list), "",
                            f"批{batch_idx + 1}/{total_batches} 成功:{batch_success} 跳过:{batch_skip} 失败:{batch_fail}")

                if config.wal_auto_checkpoint:
                    self.wal_manager.checkpoint(self.con, force=True)
                    logger.info(f"批次保存完成，已checkpoint ({end_idx}/{len(download_list)})")
                gc.collect()

        self._flush_pending_data()
        self.wal_manager.checkpoint(self.con, force=True)

        if log_callback:
            log_callback(f"下载完成: 成功:{res['s']} 跳过:{res['k']} 失败:{res['f']}")

        if progress:
            progress(len(download_list), len(download_list), "", f"完成 (成功:{res['s']})")

        return res

    def get_stats(self, code=None):
        if code:
            c = self.norm(code)
            return self.con.execute("""
                SELECT code, COUNT(*), COUNT(DISTINCT DATE(date_time)),
                       MIN(date_time), MAX(date_time)
                FROM minute30_data WHERE code=? GROUP BY code
            """, [c]).fetchone()
        else:
            return self.con.execute("""
                SELECT code, COUNT(*) FROM minute30_data GROUP BY code ORDER BY code
            """).fetchall()

    def close(self):
        logger.info("关闭数据库连接...")
        self._logout()
        if hasattr(self, 'wal_manager') and self.wal_manager:
            self.wal_manager.cleanup(self.con)
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
        log_callback("开始30分钟K线数据下载...")
    with StockDownloader() as d:
        return d.download_optimized(stocks, force, progress_callback, log_callback)


# ==================== 主程序 ====================
def main():
    import argparse
    parser = argparse.ArgumentParser(description="A股30分钟K线下载器")
    parser.add_argument("--cli", action="store_true", help="命令行模式")
    args = parser.parse_args()

    if args.cli:
        print("=" * 80)
        print("A股30分钟K线下载器")
        print("=" * 80)
        with StockDownloader() as d:
            stocks = d.read_file()
            print(f"从文件读取 {len(stocks)} 只股票")
            start_time = time.time()
            res = d.download_optimized(stocks, False)
            elapsed = time.time() - start_time
            print(f"\n完成! 成功:{res['s']} 跳过:{res['k']} 失败:{res['f']} 耗时:{elapsed:.1f}秒")
    else:
        print("30分钟K线数据下载模块")
        print("作为模块被主程序调用，或使用 --cli 参数命令行运行")


if __name__ == "__main__":
    main()
