"""
股票日线数据转季线数据工具（模块化）
使用DuckDB数据库，作为模块被 a_stock_download_manger.py 调用
"""

import duckdb
from datetime import datetime, timedelta
import logging
import threading
import time
import os
import sys
from functools import wraps
import gc
from contextlib import contextmanager

# ==================== 日志 ====================
logger = logging.getLogger('stock_seasonly')
logger.setLevel(logging.INFO)
logger.handlers.clear()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

# 常量定义
PROGRAM_DIR = os.path.dirname(os.path.abspath(__file__))
STOCK_DATA_DIR = os.path.join(PROGRAM_DIR, "stock_data")
DAILY_DB_PATH = os.path.join(STOCK_DATA_DIR, "stock_data.duckdb")
SEASONLY_DB_PATH = os.path.join(STOCK_DATA_DIR, "seasonly_data.duckdb")
BATCH_SIZE = 50
MAX_SKIP_CONTINUOUS = 50

os.makedirs(STOCK_DATA_DIR, exist_ok=True)


def handle_errors(func):
    """错误处理装饰器"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"函数 {func.__name__} 执行出错: {str(e)}")
            raise
    return wrapper


class DatabaseManager:
    """数据库连接管理器"""
    def __init__(self):
        self.daily_conn = None
        self.seasonly_conn = None
        self.lock = threading.Lock()

    def connect_daily(self):
        if self.daily_conn is None:
            self.daily_conn = duckdb.connect(DAILY_DB_PATH)
        return self.daily_conn

    def connect_seasonly(self):
        if self.seasonly_conn is None:
            self.seasonly_conn = duckdb.connect(SEASONLY_DB_PATH)
        return self.seasonly_conn

    def close_all(self):
        if self.daily_conn:
            self.daily_conn.close()
            self.daily_conn = None
        if self.seasonly_conn:
            self.seasonly_conn.close()
            self.seasonly_conn = None

    @contextmanager
    def daily_connection(self):
        conn = self.connect_daily()
        try:
            yield conn
        except Exception as e:
            logger.error(f"日线数据库操作失败: {str(e)}")
            raise

    @contextmanager
    def seasonly_connection(self):
        conn = self.connect_seasonly()
        try:
            yield conn
        except Exception as e:
            logger.error(f"季线数据库操作失败: {str(e)}")
            raise


class DataConverter:
    """数据转换核心类（纯DuckDB实现）"""
    def __init__(self, db_manager, progress_callback=None):
        self.db_manager = db_manager
        self.progress_callback = progress_callback
        self.paused = False
        self.stopped = False
        self.skipped_count = 0
        self.processed_count = 0
        self.continuous_skip = 0
        self.start_time = None

    def pause(self):
        self.paused = True

    def resume(self):
        self.paused = False

    def stop(self):
        self.stopped = True

    def init_seasonly_database(self, seasonly_conn):
        seasonly_conn.execute("""
            CREATE TABLE IF NOT EXISTS seasonly_stock_data (
                code VARCHAR, date DATE, open DOUBLE, high DOUBLE, low DOUBLE,
                close DOUBLE, volume DOUBLE, amount DOUBLE,
                PRIMARY KEY (code, date)
            )
        """)
        seasonly_conn.execute("""
            CREATE TABLE IF NOT EXISTS seasonly_last_download_time (
                code VARCHAR PRIMARY KEY,
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def resolve_column_names(self, conn, table_name):
        try:
            columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
            column_names = [col[0].lower() for col in columns]
            mapping = {}
            field_variants = {
                'code': ['code', 'stock_code', 'symbol', 'ts_code', 'stockcode'],
                'date': ['date', 'trade_date', 'datetime', 'date_time'],
                'open': ['open', 'open_price'],
                'high': ['high', 'high_price'],
                'low': ['low', 'low_price'],
                'close': ['close', 'close_price'],
                'volume': ['volume', 'vol'],
                'amount': ['amount', 'turnover', 'amt']
            }
            for std_field, variants in field_variants.items():
                for variant in variants:
                    if variant in column_names:
                        mapping[std_field] = variant
                        break
            return mapping
        except Exception as e:
            logger.error(f"列名解析失败: {str(e)}")
            return {}

    def get_stock_list(self, daily_conn):
        try:
            data_mapping = self.resolve_column_names(daily_conn, 'dayly_stock_data')
            if not data_mapping or 'code' not in data_mapping:
                logger.error("无法解析数据表列名")
                return []
            code_col = data_mapping['code']
            result = daily_conn.execute(f"SELECT DISTINCT {code_col} FROM dayly_stock_data").fetchall()
            stock_codes = [row[0] for row in result]
            logger.info(f"从日线数据库获取到 {len(stock_codes)} 只股票")
            return stock_codes
        except Exception as e:
            logger.error(f"获取股票列表失败: {str(e)}")
            return []

    def needs_update(self, seasonly_conn, stock_code, daily_conn, data_mapping):
        try:
            result = seasonly_conn.execute(
                "SELECT last_update FROM seasonly_last_download_time WHERE code = ?",
                [stock_code]
            ).fetchone()
            if not result:
                return True, None
            last_update = result[0]
            code_col = data_mapping['code']
            date_col = data_mapping['date']
            latest_daily = daily_conn.execute(f"""
                SELECT MAX({date_col}) FROM dayly_stock_data WHERE {code_col} = ?
            """, [stock_code]).fetchone()[0]
            if not latest_daily:
                return False, None
            if latest_daily > last_update.date():
                return True, last_update
            else:
                return False, None
        except Exception as e:
            logger.error(f"检查更新状态失败 {stock_code}: {str(e)}")
            return True, None

    def convert_to_quarterly(self, daily_conn, seasonly_conn, stock_code, data_mapping, last_update=None):
        try:
            code_col = data_mapping['code']
            date_col = data_mapping['date']
            open_col = data_mapping.get('open', 'open')
            high_col = data_mapping.get('high', 'high')
            low_col = data_mapping.get('low', 'low')
            close_col = data_mapping.get('close', 'close')
            volume_col = data_mapping.get('volume', 'volume')
            amount_col = data_mapping.get('amount', 'amount')

            where_clause = f"{code_col} = '{stock_code}'"
            if last_update:
                where_clause += f" AND {date_col} > '{last_update}'"

            query = f"""
            WITH daily_data AS (
                SELECT {date_col} as date, {open_col} as open, {high_col} as high,
                       {low_col} as low, {close_col} as close, {volume_col} as volume, {amount_col} as amount
                FROM dayly_stock_data WHERE {where_clause}
            ),
            quarterly_base AS (
                SELECT DATE_TRUNC('quarter', date) as quarter_start,
                       FIRST(open ORDER BY date) as quarter_open,
                       MAX(high) as quarter_high, MIN(low) as quarter_low,
                       LAST(close ORDER BY date) as quarter_close,
                       SUM(volume) as quarter_volume, SUM(amount) as quarter_amount
                FROM daily_data GROUP BY DATE_TRUNC('quarter', date)
            )
            SELECT quarter_start as date, quarter_open as open, quarter_high as high,
                   quarter_low as low, quarter_close as close, quarter_volume as volume, quarter_amount as amount
            FROM quarterly_base ORDER BY quarter_start
            """
            result = daily_conn.execute(query).fetchall()
            if not result:
                return None
            return result
        except Exception as e:
            logger.error(f"季度转换失败 {stock_code}: {str(e)}")
            return None

    def save_quarterly_data(self, seasonly_conn, stock_code, quarterly_data):
        if not quarterly_data:
            return False
        try:
            seasonly_conn.execute("BEGIN TRANSACTION")
            for row in quarterly_data:
                seasonly_conn.execute("""
                    INSERT OR REPLACE INTO seasonly_stock_data (code, date, open, high, low, close, volume, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [stock_code, row[0], row[1], row[2], row[3], row[4], row[5], row[6]])
            seasonly_conn.execute("""
                INSERT OR REPLACE INTO seasonly_last_download_time (code, last_update)
                VALUES (?, CURRENT_TIMESTAMP)
            """, [stock_code])
            seasonly_conn.execute("COMMIT")
            return True
        except Exception as e:
            seasonly_conn.execute("ROLLBACK")
            logger.error(f"保存季线数据失败 {stock_code}: {str(e)}")
            return False

    def process_stock(self, stock_code):
        try:
            with self.db_manager.daily_connection() as daily_conn:
                data_mapping = self.resolve_column_names(daily_conn, 'dayly_stock_data')
                if not data_mapping or 'code' not in data_mapping or 'date' not in data_mapping:
                    logger.error(f"无法解析日线数据表列名，跳过股票 {stock_code}")
                    return False
                with self.db_manager.seasonly_connection() as seasonly_conn:
                    self.init_seasonly_database(seasonly_conn)
                    need_update, last_update = self.needs_update(seasonly_conn, stock_code, daily_conn, data_mapping)
                    if not need_update:
                        self.skipped_count += 1
                        self.continuous_skip += 1
                        return False
                    quarterly_data = self.convert_to_quarterly(daily_conn, seasonly_conn, stock_code, data_mapping, last_update)
                    if quarterly_data is None:
                        self.skipped_count += 1
                        self.continuous_skip += 1
                        return False
                    success = self.save_quarterly_data(seasonly_conn, stock_code, quarterly_data)
                    if success:
                        self.processed_count += 1
                        self.continuous_skip = 0
                        return True
                    else:
                        return False
        except Exception as e:
            logger.error(f"处理股票 {stock_code} 时出错: {str(e)}")
            return False

    @handle_errors
    def run_conversion(self, use_index=True, skip_optimize=True, log_callback=None):
        self.paused = False
        self.stopped = False
        self.skipped_count = 0
        self.processed_count = 0
        self.continuous_skip = 0
        self.start_time = time.time()

        try:
            with self.db_manager.daily_connection() as daily_conn:
                all_stocks = self.get_stock_list(daily_conn)
                if not all_stocks:
                    if log_callback:
                        log_callback("日线数据库中没有股票数据")
                    return

                stocks_to_process = []
                if use_index:
                    with self.db_manager.seasonly_connection() as seasonly_conn:
                        self.init_seasonly_database(seasonly_conn)
                        data_mapping = self.resolve_column_names(daily_conn, 'dayly_stock_data')
                        for stock_code in all_stocks:
                            need_update, _ = self.needs_update(seasonly_conn, stock_code, daily_conn, data_mapping)
                            if need_update:
                                stocks_to_process.append(stock_code)
                else:
                    stocks_to_process = all_stocks

                total_stocks = len(stocks_to_process)
                if total_stocks == 0:
                    if log_callback:
                        log_callback("所有股票都已是最新，无需处理")
                    return

                if log_callback:
                    log_callback(f"开始处理 {total_stocks} 只股票（共 {len(all_stocks)} 只）")

                for i, stock_code in enumerate(stocks_to_process):
                    while self.paused and not self.stopped:
                        time.sleep(0.5)
                    if self.stopped:
                        break

                    if skip_optimize and self.continuous_skip >= MAX_SKIP_CONTINUOUS:
                        if log_callback:
                            log_callback(f"连续跳过 {MAX_SKIP_CONTINUOUS} 只股票，自动跳过剩余")
                        with self.db_manager.seasonly_connection() as seasonly_conn:
                            remaining_stocks = stocks_to_process[i:]
                            seasonly_conn.execute("BEGIN TRANSACTION")
                            for code in remaining_stocks:
                                seasonly_conn.execute("""
                                    INSERT OR REPLACE INTO seasonly_last_download_time (code, last_update)
                                    VALUES (?, CURRENT_TIMESTAMP)
                                """, [code])
                            seasonly_conn.execute("COMMIT")
                        break

                    self.process_stock(stock_code)

                    elapsed = time.time() - self.start_time
                    speed = self.processed_count / elapsed if elapsed > 0 else 0
                    progress = int((i + 1) / total_stocks * 100)

                    if self.progress_callback:
                        self.progress_callback(progress, self.processed_count, self.skipped_count, speed, f"正在处理: {stock_code}")

                    if i % 20 == 0:
                        gc.collect()

                if log_callback:
                    log_callback(f"季线转换完成: 已处理: {self.processed_count}，跳过: {self.skipped_count}")

        except Exception as e:
            logger.error(f"转换任务失败: {str(e)}")
            raise
        finally:
            if self.progress_callback:
                self.progress_callback(100, self.processed_count, self.skipped_count, 0, "处理完成")


# ==================== 模块接口 ====================
def run_module(use_index=True, skip_optimize=True, progress_callback=None, log_callback=None):
    """被主程序调用的模块入口"""
    if log_callback:
        log_callback("开始季线数据转换...")

    db_manager = DatabaseManager()
    converter = DataConverter(db_manager, progress_callback)
    converter.run_conversion(use_index, skip_optimize, log_callback)
    db_manager.close_all()

    if log_callback:
        log_callback(f"季线转换完成: 处理:{converter.processed_count} 跳过:{converter.skipped_count}")


# ==================== 主程序 ====================
def main():
    import sys
    if sys.version_info < (3, 6):
        print("错误: 需要Python 3.6或更高版本")
        sys.exit(1)
    run_module()


if __name__ == "__main__":
    main()
