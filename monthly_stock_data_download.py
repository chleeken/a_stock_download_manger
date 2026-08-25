"""
日线股票数据转月线数据工具（模块化）
支持全量转换和增量更新，作为模块被 a_stock_download_manger.py 调用
"""

import os
import sys
import logging
import time
import duckdb
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Tuple


# ==================== 日志 ====================
logger = logging.getLogger('stock_monthly')
logger.setLevel(logging.INFO)
logger.handlers.clear()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False


class Config:
    """配置类，管理所有路径和常量"""
    BASE_DIR = Path(__file__).parent
    STOCK_DATA_DIR = BASE_DIR.parent / 'stock_data'
    DAILY_DB_PATH = STOCK_DATA_DIR / 'stock_data.duckdb'
    MONTHLY_DB_PATH = STOCK_DATA_DIR / 'monthly_stock_data.duckdb'
    BATCH_SIZE = 100
    AUTO_SKIP_THRESHOLD = 50
    MAX_RETRIES = 3
    DAILY_TABLE = 'dayly_stock_data'
    MONTHLY_TABLE = 'monthly_data'


class DatabaseManager:
    def __init__(self, daily_db_path: Path, monthly_db_path: Path):
        self.daily_db_path = daily_db_path
        self.monthly_db_path = monthly_db_path
        self.daily_conn: Optional[duckdb.DuckDBPyConnection] = None
        self.monthly_conn: Optional[duckdb.DuckDBPyConnection] = None
        self.code_column_names = ['code', 'stock_code', 'symbol', 'stock_symbol']
        self.date_column_names = ['date', 'trade_date', 'datetime', 'time', 'day']

    def connect(self):
        try:
            self.daily_db_path.parent.mkdir(parents=True, exist_ok=True)
            self.monthly_db_path.parent.mkdir(parents=True, exist_ok=True)
            self.daily_conn = duckdb.connect(str(self.daily_db_path))
            self.monthly_conn = duckdb.connect(str(self.monthly_db_path))
            self._init_monthly_tables()
            logger.info("数据库连接成功")
            return True
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            return False

    def _init_monthly_tables(self):
        if not self.monthly_conn:
            return
        self.monthly_conn.execute("""
            CREATE TABLE IF NOT EXISTS monthly_data (
                code VARCHAR, date DATE, open DOUBLE, high DOUBLE, low DOUBLE,
                close DOUBLE, volume DOUBLE, amount DOUBLE,
                PRIMARY KEY (code, date)
            )
        """)
        self.monthly_conn.execute("""
            CREATE TABLE IF NOT EXISTS monthly_last_download_time (
                code VARCHAR PRIMARY KEY,
                last_download_date DATE,
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.info("月线数据库表结构初始化完成")

    def close(self):
        try:
            if self.daily_conn:
                self.daily_conn.close()
            if self.monthly_conn:
                self.monthly_conn.close()
            logger.info("数据库连接已关闭")
        except Exception as e:
            logger.error(f"关闭数据库连接时出错: {e}")

    def get_daily_table_columns(self) -> Dict[str, str]:
        if not self.daily_conn:
            return {}
        try:
            result = self.daily_conn.execute(f"""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_name = '{Config.DAILY_TABLE}'
            """).fetchall()
            return {col[0].lower(): col[1] for col in result}
        except Exception as e:
            logger.error(f"获取日线表列信息失败: {e}")
            return {}

    def detect_column_name(self, possible_names: List[str], columns: Dict[str, str]) -> Optional[str]:
        for name in possible_names:
            if name in columns:
                return name
            for col in columns.keys():
                if col.lower() == name.lower():
                    return col
        return None

    def get_all_stock_codes(self) -> List[str]:
        if not self.daily_conn:
            return []
        try:
            columns = self.get_daily_table_columns()
            code_col = self.detect_column_name(self.code_column_names, columns)
            if not code_col:
                logger.error("无法识别股票代码列名")
                return []
            result = self.daily_conn.execute(f"""
                SELECT DISTINCT {code_col} FROM {Config.DAILY_TABLE} ORDER BY {code_col}
            """).fetchall()
            return [row[0] for row in result if row[0]]
        except Exception as e:
            logger.error(f"获取股票代码列表失败: {e}")
            return []

    def get_stock_daily_data(self, code: str, start_date: Optional[date] = None) -> List[Dict]:
        if not self.daily_conn:
            return []
        try:
            columns = self.get_daily_table_columns()
            code_col = self.detect_column_name(self.code_column_names, columns)
            date_col = self.detect_column_name(self.date_column_names, columns)
            if not code_col or not date_col:
                logger.error("无法识别必要的列名")
                return []
            query = f"""
                SELECT {code_col} as code, {date_col} as date,
                       open, high, low, close, volume, amount
                FROM {Config.DAILY_TABLE} WHERE {code_col} = ?
            """
            params = [code]
            if start_date:
                query += f" AND {date_col} >= ?"
                params.append(start_date)
            query += f" ORDER BY {date_col}"
            result = self.daily_conn.execute(query, params).fetchall()
            data = []
            for row in result:
                data.append({
                    'code': row[0], 'date': row[1],
                    'open': float(row[2]) if row[2] else 0.0,
                    'high': float(row[3]) if row[3] else 0.0,
                    'low': float(row[4]) if row[4] else 0.0,
                    'close': float(row[5]) if row[5] else 0.0,
                    'volume': float(row[6]) if row[6] else 0.0,
                    'amount': float(row[7]) if row[7] else 0.0
                })
            return data
        except Exception as e:
            logger.error(f"获取股票{code}日线数据失败: {e}")
            return []

    def get_stock_monthly_max_date(self, code: str) -> Optional[date]:
        if not self.monthly_conn:
            return None
        try:
            result = self.monthly_conn.execute("""
                SELECT MAX(date) FROM monthly_data WHERE code = ?
            """, [code]).fetchone()
            return result[0] if result and result[0] else None
        except Exception as e:
            logger.error(f"获取股票{code}月线最大日期失败: {e}")
            return None

    def has_new_daily_in_period(self, code: str, period_start: date) -> bool:
        if not self.daily_conn:
            return False
        try:
            columns = self.get_daily_table_columns()
            code_col = self.detect_column_name(self.code_column_names, columns)
            date_col = self.detect_column_name(self.date_column_names, columns)
            if not code_col or not date_col:
                return False
            result = self.daily_conn.execute(f"""
                SELECT COUNT(*) FROM {Config.DAILY_TABLE}
                WHERE {code_col} = ? AND {date_col} >= ?
            """, [code, period_start]).fetchone()
            return result[0] > 0
        except Exception as e:
            logger.error(f"检查股票{code}周期内新日线数据失败: {e}")
            return False

    def needs_monthly_update(self, code: str) -> bool:
        if not self.daily_conn or not self.monthly_conn:
            return False
        try:
            monthly_max = self.get_stock_monthly_max_date(code)
            columns = self.get_daily_table_columns()
            code_col = self.detect_column_name(self.code_column_names, columns)
            date_col = self.detect_column_name(self.date_column_names, columns)
            if not code_col or not date_col:
                return False
            result = self.daily_conn.execute(f"""
                SELECT MAX({date_col}) FROM {Config.DAILY_TABLE}
                WHERE {code_col} = ?
            """, [code]).fetchone()
            daily_max = result[0] if result else None
            if not daily_max:
                return False
            if not monthly_max:
                return True
            # 比较年-月粒度：将日线日期截断到月初
            daily_month_start = daily_max.replace(day=1)
            monthly_month_start = monthly_max.replace(day=1)
            return daily_month_start > monthly_month_start
        except Exception as e:
            logger.error(f"检查股票{code}是否需要月线更新失败: {e}")
            return False

    def save_monthly_data(self, code: str, monthly_data: List[Dict], is_temporary: bool = False):
        if not self.monthly_conn or not monthly_data:
            return False
        try:
            current_month = monthly_data[0]['date'].replace(day=1)
            next_month = (current_month + timedelta(days=32)).replace(day=1)
            self.monthly_conn.execute("BEGIN TRANSACTION")
            self.monthly_conn.execute("""
                DELETE FROM monthly_data WHERE code = ? AND date >= ? AND date < ?
            """, [code, current_month, next_month])
            for data in monthly_data:
                self.monthly_conn.execute("""
                    INSERT OR REPLACE INTO monthly_data (code, date, open, high, low, close, volume, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [data['code'], data['date'], data['open'], data['high'],
                      data['low'], data['close'], data['volume'], data['amount']])
            last_date = monthly_data[-1]['date']
            self.monthly_conn.execute("""
                INSERT OR REPLACE INTO monthly_last_download_time (code, last_download_date, last_update)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, [code, last_date])
            self.monthly_conn.execute("COMMIT")
            return True
        except Exception as e:
            self.monthly_conn.execute("ROLLBACK")
            logger.error(f"保存股票{code}月线数据失败: {e}")
            return False

    def batch_update_index(self, updates: List[Tuple[str, date]]):
        if not self.monthly_conn or not updates:
            return False
        try:
            self.monthly_conn.execute("BEGIN TRANSACTION")
            for code, last_date in updates:
                self.monthly_conn.execute("""
                    INSERT OR REPLACE INTO monthly_last_download_time (code, last_download_date, last_update)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                """, [code, last_date])
            self.monthly_conn.execute("COMMIT")
            return True
        except Exception as e:
            self.monthly_conn.execute("ROLLBACK")
            logger.error(f"批量更新索引失败: {e}")
            return False

    def convert_daily_to_monthly_sql(self, code: str, period_start: date) -> List[Dict]:
        """使用SQL聚合将日线转为月线，只聚合指定日期之后的数据"""
        if not self.daily_conn or not self.monthly_conn:
            return []
        try:
            columns = self.get_daily_table_columns()
            code_col = self.detect_column_name(self.code_column_names, columns)
            date_col = self.detect_column_name(self.date_column_names, columns)
            if not code_col or not date_col:
                return []

            query = f"""
                SELECT {code_col} as code,
                       DATE_TRUNC('month', {date_col}) as month_start,
                       FIRST(open ORDER BY {date_col}) as open,
                       MAX(high) as high,
                       MIN(low) as low,
                       LAST(close ORDER BY {date_col}) as close,
                       SUM(volume) as volume,
                       SUM(amount) as amount
                FROM {Config.DAILY_TABLE}
                WHERE {code_col} = ? AND {date_col} >= ?
                GROUP BY {code_col}, DATE_TRUNC('month', {date_col})
                ORDER BY month_start
            """
            result = self.daily_conn.execute(query, [code, period_start]).fetchall()
            data = []
            for row in result:
                data.append({
                    'code': row[0],
                    'date': row[1],
                    'open': float(row[2]) if row[2] else 0.0,
                    'high': float(row[3]) if row[3] else 0.0,
                    'low': float(row[4]) if row[4] else 0.0,
                    'close': float(row[5]) if row[5] else 0.0,
                    'volume': float(row[6]) if row[6] else 0.0,
                    'amount': float(row[7]) if row[7] else 0.0
                })
            return data
        except Exception as e:
            logger.error(f"SQL聚合月线失败 {code}: {e}")
            return []


class DataProcessor:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.stats = {
            'total': 0, 'processed': 0, 'skipped': 0, 'failed': 0,
            'start_time': None, 'consecutive_skips': 0
        }
        self.stop_flag = False
        self.pause_flag = False
        self.current_time = datetime.now()

    def check_auto_skip(self) -> bool:
        return False

    def process_stock(self, code: str, start_date: Optional[date] = None,
                      skip_optimization: bool = True) -> Tuple[bool, int]:
        try:
            if not self.db.needs_monthly_update(code):
                return True, 0
            monthly_data = self.db.convert_daily_to_monthly_sql(code, start_date or date(1990, 1, 1))
            if not monthly_data:
                return True, 0
            success = self.db.save_monthly_data(code, monthly_data, is_temporary=False)
            if success:
                return True, len(monthly_data)
            else:
                return False, 0
        except Exception as e:
            logger.error(f"处理股票{code}时出错: {e}")
            return False, 0

    def process_all(self, stock_codes: List[str], skip_optimization: bool = True,
                    progress_callback=None, log_callback=None) -> Dict:
        self.stats['start_time'] = time.time()
        self.stats['total'] = len(stock_codes)
        self.stats['processed'] = 0
        self.stats['skipped'] = 0
        self.stats['failed'] = 0
        self.stats['consecutive_skips'] = 0
        self.stop_flag = False
        self.pause_flag = False

        batch_updates = []

        for i, code in enumerate(stock_codes):
            if self.stop_flag:
                break
            while self.pause_flag:
                time.sleep(0.1)
                if self.stop_flag:
                    break

            if not self.db.needs_monthly_update(code):
                self.stats['consecutive_skips'] += 1
                self.stats['skipped'] += 1
                continue

            self.stats['consecutive_skips'] = 0
            if progress_callback:
                progress_callback(code, i, len(stock_codes))
            success, count = self.process_stock(code, skip_optimization=False)
            if success:
                if count > 0:
                    self.stats['processed'] += 1
                    if log_callback:
                        log_callback(f"✓ {code}: 转换成功，生成{count}条月线记录")
                    batch_updates.append((code, date.today()))
                else:
                    self.stats['skipped'] += 1
            else:
                self.stats['failed'] += 1
                if log_callback:
                    log_callback(f"✗ {code}: 转换失败")

            if len(batch_updates) >= 100:
                self.db.batch_update_index(batch_updates)
                batch_updates = []

        if batch_updates:
            self.db.batch_update_index(batch_updates)

        self.stats['end_time'] = time.time()
        return self.stats

    def stop(self):
        self.stop_flag = True

    def pause(self):
        self.pause_flag = True

    def resume(self):
        self.pause_flag = False


# ==================== 模块接口 ====================
def run_module(skip_optimization=True, progress_callback=None, log_callback=None):
    """被主程序调用的模块入口"""
    db_manager = DatabaseManager(Config.DAILY_DB_PATH, Config.MONTHLY_DB_PATH)
    if not db_manager.connect():
        if log_callback:
            log_callback("数据库连接失败")
        return

    stock_codes = db_manager.get_all_stock_codes()
    if not stock_codes:
        if log_callback:
            log_callback("没有找到股票数据")
        db_manager.close()
        return

    if log_callback:
        log_callback(f"开始月线转换任务，共{len(stock_codes)}只股票")

    processor = DataProcessor(db_manager)
    stats = processor.process_all(
        stock_codes,
        skip_optimization=skip_optimization,
        progress_callback=progress_callback,
        log_callback=log_callback
    )

    elapsed = stats.get('end_time', time.time()) - stats.get('start_time', time.time())
    if log_callback:
        log_callback(f"月线转换完成: 处理:{stats['processed']} 跳过:{stats['skipped']} 失败:{stats['failed']} 耗时:{elapsed:.2f}秒")

    db_manager.close()
    return stats


# ==================== 主程序 ====================
def main():
    app = run_module()
    if app:
        print("月线转换完成")


if __name__ == "__main__":
    main()
