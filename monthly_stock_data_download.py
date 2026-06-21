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

# ==================== 配置模块 ====================
class Config:
    """配置类，管理所有路径和常量"""
    BASE_DIR = Path(__file__).parent
    STOCK_DATA_DIR = BASE_DIR / 'stock_data'
    DAILY_DB_PATH = STOCK_DATA_DIR / 'stock_data.duckdb'
    MONTHLY_DB_PATH = STOCK_DATA_DIR / 'monthly_stock_data.duckdb'
    BATCH_SIZE = 100
    AUTO_SKIP_THRESHOLD = 50
    MAX_RETRIES = 3
    DAILY_TABLE = 'dayly_stock_data'
    DAILY_INDEX_TABLE = 'dayly_last_download_time'
    MONTHLY_TABLE = 'monthly_data'
    MONTHLY_INDEX_TABLE = 'monthly_last_download_time'


# ==================== 日志 ====================
logger = logging.getLogger('stock_monthly')
logger.setLevel(logging.INFO)
logger.handlers.clear()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False


# ==================== 数据库模块 ====================
class DatabaseManager:
    """数据库管理类，处理所有数据库操作"""
    def __init__(self, daily_db_path: Path, monthly_db_path: Path):
        self.daily_db_path = daily_db_path
        self.monthly_db_path = monthly_db_path
        self.daily_conn: Optional[duckdb.DuckDBPyConnection] = None
        self.monthly_conn: Optional[duckdb.DuckDBPyConnection] = None
        self.code_column_names = ['code', 'stock_code', 'symbol', 'stock_symbol']
        self.date_column_names = ['date', 'trade_date', 'datetime', 'time', 'day']

    def connect(self):
        """建立数据库连接"""
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
                logger.error(f"无法识别必要的列名")
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

    def get_last_monthly_date(self, code: str) -> Optional[date]:
        if not self.monthly_conn:
            return None
        try:
            result = self.monthly_conn.execute("""
                SELECT last_download_date FROM monthly_last_download_time WHERE code = ?
            """, [code]).fetchone()
            if result and result[0]:
                return result[0]
            result = self.monthly_conn.execute("""
                SELECT MAX(date) FROM monthly_data WHERE code = ?
            """, [code]).fetchone()
            return result[0] if result and result[0] else None
        except Exception as e:
            logger.error(f"获取股票{code}最后月线日期失败: {e}")
            return None

    def save_monthly_data(self, code: str, monthly_data: List[Dict], is_temporary: bool = False):
        if not self.monthly_conn or not monthly_data:
            return False
        try:
            self.monthly_conn.execute("BEGIN TRANSACTION")
            if is_temporary:
                current_month = monthly_data[0]['date'].replace(day=1)
                next_month = (current_month + timedelta(days=32)).replace(day=1)
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


# ==================== 数据处理模块 ====================
class DataProcessor:
    """数据处理类，负责日线转月线的核心逻辑"""
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.stats = {
            'total': 0, 'processed': 0, 'skipped': 0, 'failed': 0,
            'start_time': None, 'consecutive_skips': 0
        }
        self.stop_flag = False
        self.pause_flag = False

    @staticmethod
    def daily_to_monthly(daily_data: List[Dict]) -> List[Dict]:
        if not daily_data:
            return []
        monthly_map = {}
        for day in daily_data:
            month_key = day['date'].strftime('%Y-%m')
            if month_key not in monthly_map:
                monthly_map[month_key] = {
                    'code': day['code'],
                    'date': day['date'].replace(day=1),
                    'open': day['open'], 'high': day['high'], 'low': day['low'],
                    'close': day['close'], 'volume': day['volume'], 'amount': day['amount'],
                    'days': [day]
                }
            else:
                month_data = monthly_map[month_key]
                month_data['high'] = max(month_data['high'], day['high'])
                month_data['low'] = min(month_data['low'], day['low'])
                month_data['close'] = day['close']
                month_data['volume'] += day['volume']
                month_data['amount'] += day['amount']
                month_data['days'].append(day)
        result = []
        for month_key in sorted(monthly_map.keys()):
            data = monthly_map[month_key]
            result.append({
                'code': data['code'], 'date': data['date'],
                'open': data['open'], 'high': data['high'], 'low': data['low'],
                'close': data['close'], 'volume': data['volume'], 'amount': data['amount']
            })
        return result

    def should_process_stock(self, code: str, skip_optimization: bool) -> Tuple[bool, Optional[date]]:
        try:
            last_date = self.db.get_last_monthly_date(code)
            if not last_date:
                return True, None
            if not skip_optimization:
                return True, last_date
            daily_data = self.db.get_stock_daily_data(code)
            if not daily_data:
                return False, None
            latest_daily_date = daily_data[-1]['date']
            has_new_data = latest_daily_date > last_date
            if has_new_data:
                self.stats['consecutive_skips'] = 0
                return True, last_date
            else:
                self.stats['consecutive_skips'] += 1
                return False, None
        except Exception as e:
            logger.error(f"判断股票{code}处理状态失败: {e}")
            return True, None

    def check_auto_skip(self) -> bool:
        return self.stats['consecutive_skips'] >= Config.AUTO_SKIP_THRESHOLD

    def process_stock(self, code: str, start_date: Optional[date] = None,
                      skip_optimization: bool = True) -> Tuple[bool, int]:
        try:
            daily_data = self.db.get_stock_daily_data(code, start_date)
            if not daily_data:
                return True, 0
            monthly_data = self.daily_to_monthly(daily_data)
            if not monthly_data:
                return True, 0
            today = date.today()
            current_month = today.replace(day=1)
            is_temporary = any(d['date'] >= current_month for d in monthly_data)
            success = self.db.save_monthly_data(code, monthly_data, is_temporary)
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

            if skip_optimization and self.check_auto_skip():
                msg = f"连续{Config.AUTO_SKIP_THRESHOLD}只股票无新数据，自动跳过"
                if log_callback:
                    log_callback(msg)
                self.stats['skipped'] += (len(stock_codes) - i)
                break

            should_process, start_date = self.should_process_stock(code, skip_optimization)
            if should_process:
                if progress_callback:
                    progress_callback(code, i, len(stock_codes))
                success, count = self.process_stock(code, start_date, skip_optimization)
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
            else:
                self.stats['skipped'] += 1

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
