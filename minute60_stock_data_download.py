"""
60分钟股票数据下载工具（智能版，模块化）
功能：下载A股60分钟K线数据，自动增量更新，每只股票保存最近500条记录
作为模块被 a_stock_download_manger.py 调用
"""
import baostock as bs
import duckdb
import datetime
import time
import gc
import logging
from pathlib import Path
from functools import wraps
import sys
import atexit

# ==================== 配置 ====================
class Config:
    __slots__ = ('max_records', 'tolerance', 'db_dir', 'db_name', 'stock_file',
                 'request_delay', 'retry_times', 'batch_size', 'skip_threshold',
                 'wal_auto_checkpoint', 'wal_checkpoint_size', 'enable_wal',
                 'market_hours_only', 'no_data_threshold', 'batch_check_size')

    def __init__(self):
        self.max_records = 400
        self.tolerance = 50
        self.db_dir = "stock_data"
        self.db_name = "minute60_stock_data.duckdb"
        self.stock_file = "table.txt"
        self.request_delay = 0.01
        self.retry_times = 2
        self.batch_size = 200
        self.skip_threshold = 500
        self.wal_auto_checkpoint = True
        self.wal_checkpoint_size = 20
        self.enable_wal = True
        self.market_hours_only = False
        self.no_data_threshold = 3
        self.batch_check_size = 500

    @property
    def default_db_path(self):
        return Path(__file__).parent / self.db_dir / self.db_name

config = Config()

# ==================== 日志 ====================
logger = logging.getLogger('stock_60min')
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


# ==================== 装饰器 ====================
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
                 '_last_check_date', '_all_stocks_list')

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

        atexit.register(self.cleanup_wal)
        logger.info(f"数据库: {self.db_path}")
        wal_info = self.wal_manager.get_wal_info(self.con)
        if wal_info['enabled']:
            logger.info(f"WAL模式: {wal_info['status']}")

    def cleanup_wal(self):
        try:
            if hasattr(self, 'con') and self.con:
                self.wal_manager.cleanup(self.con)
        except:
            pass

    def _init_db(self):
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS minute60_data (
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
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_60min_c_t ON minute60_data(code, date_time)")

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

        count = self.con.execute("SELECT COUNT(*) FROM minute60_data").fetchone()[0]
        stock_count = self.con.execute("SELECT COUNT(DISTINCT code) FROM minute60_data").fetchone()[0]
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
        p = Path(f or config.stock_file)
        if not p.exists():
            logger.error(f"文件不存在:{p}")
            return []
        codes = []
        try:
            with open(p, 'r', encoding='utf-8') as f:
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
            FROM minute60_data WHERE code IN ({placeholders})
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
                start_date=today, end_date=today, frequency="60", adjustflag="2"
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
            time_part = time_str[:4] if len(time_str) >= 4 else time_str
            hour = int(time_part[:2])
            minute = int(time_part[2:4])
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
        standard_times = [(10, 30), (11, 30), (14, 0), (15, 0)]
        result = []
        for date, records in date_groups.items():
            records.sort(key=lambda x: x[1])
            for i, record in enumerate(records[:4]):
                if i < 4:
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
                "SELECT date_time FROM minute60_data WHERE code=? AND date_time BETWEEN ? AND ?",
                [c, min_dt, max_dt]
            ).fetchall()}
            new_records = [r for r in records if r[1] not in existing]
            if not new_records:
                return True, "无新数据"
            self.con.executemany(
                "INSERT INTO minute60_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)", new_records
            )
            current_count = self.con.execute(
                "SELECT COUNT(*) FROM minute60_data WHERE code=?", [c]
            ).fetchone()[0]
            if current_count > config.max_records + config.tolerance:
                to_delete = current_count - config.max_records
                self.con.execute("""
                    DELETE FROM minute60_data WHERE code=? AND date_time IN (
                        SELECT date_time FROM minute60_data WHERE code=? ORDER BY date_time LIMIT ?
                    )
                """, [c, c, to_delete])
            logger.info(f"{c} 保存{len(new_records)}条, 当前{current_count}条")
            return True, f"保存{len(new_records)}条"
        except Exception as e:
            logger.error(f"{code} 保存失败:{e}")
            return False, str(e)

    def download_stock(self, code, force=False, days=365):
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
                sd = (now - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
                ed = now.strftime("%Y-%m-%d")
            else:
                sd = (max_date + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                ed = now.strftime("%Y-%m-%d")
                if sd > ed:
                    return None, "已是最新"
            rs = bs.query_history_k_data_plus(
                self._full_code(c), "date,time,open,high,low,close,volume,amount",
                start_date=sd, end_date=ed, frequency="60", adjustflag="2"
            )
            if rs.error_code != '0':
                logger.error(f"{c} 下载失败:{rs.error_msg}")
                return None, rs.error_msg
            data_rows = []
            fields = rs.fields
            while rs.next():
                data_rows.append(rs.get_row_data())
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
            logger.error(f"{code} 处理异常:{str(e)}")
            return None, str(e)

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
                try:
                    data, msg = self.download_stock(code, force)
                    if data is None:
                        if "无新数据" in msg:
                            batch_skip += 1
                        else:
                            batch_fail += 1
                    else:
                        success, save_msg = self.save_single_stock(code, data)
                        if success:
                            batch_success += 1
                            if log_callback:
                                log_callback(f"{code}: {save_msg}")
                        else:
                            batch_fail += 1
                    time.sleep(config.request_delay)
                except Exception as e:
                    logger.error(f"{code} 处理异常:{str(e)}")
                    batch_fail += 1

            res['s'] += batch_success
            res['k'] += batch_skip
            res['f'] += batch_fail

            if progress:
                progress(end_idx, len(download_list), "",
                        f"批{batch_idx + 1}/{total_batches} 成功:{batch_success} 跳过:{batch_skip} 失败:{batch_fail}")

            if config.wal_auto_checkpoint:
                self.wal_manager.checkpoint(self.con)
            gc.collect()

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
                FROM minute60_data WHERE code=? GROUP BY code
            """, [c]).fetchone()
        else:
            return self.con.execute("""
                SELECT code, COUNT(*) FROM minute60_data GROUP BY code ORDER BY code
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
        log_callback("开始60分钟K线数据下载...")
    with StockDownloader() as d:
        return d.download_optimized(stocks, force, progress_callback, log_callback)


# ==================== 主程序 ====================
def main():
    import argparse
    parser = argparse.ArgumentParser(description="A股60分钟K线下载器")
    parser.add_argument("--cli", action="store_true", help="命令行模式")
    args = parser.parse_args()

    if args.cli:
        print("=" * 80)
        print("A股60分钟K线下载器")
        print("=" * 80)
        with StockDownloader() as d:
            stocks = d.read_file()
            print(f"从文件读取 {len(stocks)} 只股票")
            start_time = time.time()
            res = d.download_optimized(stocks, False)
            elapsed = time.time() - start_time
            print(f"\n完成! 成功:{res['s']} 跳过:{res['k']} 失败:{res['f']} 耗时:{elapsed:.1f}秒")
    else:
        print("60分钟K线数据下载模块")
        print("作为模块被主程序调用，或使用 --cli 参数命令行运行")


if __name__ == "__main__":
    main()
