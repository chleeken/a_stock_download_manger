"""
日线转周线数据转换器（模块化，无GUI依赖）
作为模块被 a_stock_download_manger.py 调用
"""
import duckdb
import sys
import time
import re
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Callable
import logging


# ==================== 日志 ====================
logger = logging.getLogger('stock_weekly')
logger.setLevel(logging.INFO)
logger.handlers.clear()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False


class PathManager:
    """路径管理器 - 处理打包后的路径问题"""
    @staticmethod
    def get_program_dir() -> Path:
        if getattr(sys, 'frozen', False):
            return Path(sys.executable).parent
        else:
            return Path(__file__).parent

    @staticmethod
    def is_temp_directory(path: Path) -> bool:
        path_str = str(path).lower()
        temp_indicators = ['temp', 'tmp', 'cache', '_mei', 'appdata\\local\\temp']
        return any(indicator in path_str for indicator in temp_indicators)

    @staticmethod
    def get_work_dir() -> Path:
        cwd = Path.cwd()
        if not PathManager.is_temp_directory(cwd):
            return cwd
        try:
            documents = Path.home() / "Documents"
            if documents.exists():
                return documents
        except:
            pass
        try:
            desktop = Path.home() / "Desktop"
            if desktop.exists():
                return desktop
        except:
            pass
        return Path.home()

    @staticmethod
    def get_config_dir() -> Path:
        config_dir = PathManager.get_work_dir() / "stock_converter_config"
        config_dir.mkdir(parents=True, exist_ok=True)
        return config_dir

    @staticmethod
    def get_last_opened_path() -> Optional[Path]:
        config_file = PathManager.get_config_dir() / "last_path.json"
        if config_file.exists():
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    last_path = Path(data.get('last_path', ''))
                    if last_path.exists():
                        return last_path
            except:
                pass
        return None

    @staticmethod
    def save_last_opened_path(path: Path):
        config_file = PathManager.get_config_dir() / "last_path.json"
        try:
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump({'last_path': str(path)}, f, ensure_ascii=False, indent=2)
        except:
            pass


class WeekConverter:
    """日线转周线转换器 - 优化版本"""
    def __init__(self, day_db_path: Optional[Path] = None,
                 week_db_path: Optional[Path] = None,
                 progress_callback: Optional[Callable] = None,
                 log_callback: Optional[Callable] = None):

        self.progress_callback = progress_callback
        self.log_callback = log_callback

        self.day_db = day_db_path
        self.week_db = week_db_path

        if not self.day_db:
            self.day_db = PathManager.get_work_dir() / "stock_data" / "stock_data.duckdb"
        if not self.week_db:
            self.week_db = self.day_db.parent / "weekly_stock_data.duckdb"

        self.week_db.parent.mkdir(parents=True, exist_ok=True)

        self.main_table = "weekly_data"
        self.current_time = datetime.now()
        self.user_cancelled = False

        self.day_table_name = None
        self.code_column = None
        self.date_column = None
        self.open_column = None
        self.high_column = None
        self.low_column = None
        self.close_column = None
        self.volume_column = None
        self.amount_column = None

        self.stats = {
            'stocks_processed': 0, 'stocks_skipped': 0, 'weekly_rows_added': 0,
            'start_time': None, 'end_time': None, 'total_stocks': 0,
            'incremental_count': 0, 'full_count': 0, 'last_processed_code': None
        }

        self._log("转换器初始化完成")
        self._log(f"日线数据库: {self.day_db}")
        self._log(f"周线数据库: {self.week_db}")

    def _log(self, message: str, level: str = "info"):
        logger.info(message)
        if self.log_callback:
            self.log_callback(message)

    def detect_day_table_structure(self, day_conn):
        self._log("检测日线数据库表结构...")
        try:
            tables = day_conn.execute("SHOW TABLES").fetchall()
            for table in tables:
                table_name = table[0]
                if 'stock' in table_name.lower() or 'day' in table_name.lower():
                    self.day_table_name = table_name
                    break
            if not self.day_table_name:
                self.day_table_name = tables[0][0] if tables else None
            if not self.day_table_name:
                self._log("未找到数据表!", "error")
                return False
            self._log(f"使用表: {self.day_table_name}")

            columns = day_conn.execute(f"DESCRIBE {self.day_table_name}").fetchall()
            column_names = [col[0].lower() for col in columns]

            column_mapping = {
                'code': ['code', 'stock_code', 'ts_code', 'symbol', 'stockcode'],
                'date': ['date', 'trade_date', 'trading_date', 'day'],
                'open': ['open', 'open_price', 'openprice'],
                'high': ['high', 'high_price', 'highprice'],
                'low': ['low', 'low_price', 'lowprice'],
                'close': ['close', 'close_price', 'closeprice'],
                'volume': ['volume', 'vol', 'turnover_volume'],
                'amount': ['amount', 'amt', 'turnover_amount']
            }

            for field, possible_names in column_mapping.items():
                for name in possible_names:
                    if name in column_names:
                        setattr(self, f'{field}_column', name)
                        break

            required = ['code', 'date', 'open', 'high', 'low', 'close']
            missing = [r for r in required if not getattr(self, f'{r}_column')]
            if missing:
                self._log(f"缺少必要列: {missing}", "error")
                return False
            return True
        except Exception as e:
            self._log(f"检测表结构失败: {e}", "error")
            return False

    def get_week_max_date(self, week_conn, stock_code: str) -> Optional[datetime.date]:
        try:
            result = week_conn.execute(f"""
                SELECT MAX(date) FROM {self.main_table} WHERE code = ?
            """, [stock_code]).fetchone()
            return result[0] if result and result[0] else None
        except Exception:
            return None

    def get_day_max_date(self, day_conn, stock_code: str) -> Optional[datetime.date]:
        try:
            result = day_conn.execute(f"""
                SELECT MAX({self.date_column}) FROM {self.day_table_name} WHERE {self.code_column} = ?
            """, [stock_code]).fetchone()
            return result[0] if result and result[0] else None
        except Exception:
            return None

    def create_table_if_not_exists(self, conn):
        try:
            tables = conn.execute("SHOW TABLES").fetchall()
            table_exists = any(self.main_table == t[0] for t in tables)
            if not table_exists:
                conn.execute(f"""
                    CREATE TABLE {self.main_table} (
                        code VARCHAR NOT NULL, date DATE NOT NULL,
                        open DOUBLE, high DOUBLE, low DOUBLE,
                        close DOUBLE, volume BIGINT, amount DOUBLE,
                        PRIMARY KEY (code, date)
                    )
                """)
                conn.execute(f"CREATE INDEX idx_weekly_code ON {self.main_table}(code)")
                conn.execute(f"CREATE INDEX idx_weekly_date ON {self.main_table}(date)")
                self._log("数据库表结构创建完成")
            else:
                self._log("数据库表已存在，将进行增量更新")
            return True
        except Exception as e:
            self._log(f"创建表失败: {e}", "error")
            return False

    def get_stock_codes(self, day_conn) -> List[str]:
        try:
            query = f"""
                SELECT DISTINCT {self.code_column} FROM {self.day_table_name}
                WHERE {self.code_column} IS NOT NULL ORDER BY {self.code_column}
            """
            codes_result = day_conn.execute(query).fetchall()
            all_codes = [str(row[0]) for row in codes_result]
            stock_codes = []
            for code in all_codes:
                match = re.search(r'(\d{6})', code)
                if match:
                    stock_codes.append(match.group(1))
                elif code.isdigit() and len(code) == 6:
                    stock_codes.append(code)
            stock_codes = list(dict.fromkeys(stock_codes))
            self.stats['total_stocks'] = len(stock_codes)
            self._log(f"找到 {len(stock_codes)} 个股票代码")
            return stock_codes
        except Exception as e:
            self._log(f"获取股票代码失败: {e}", "error")
            return []

    def should_process_stock(self, day_conn, week_conn, stock_code: str) -> Tuple[bool, Optional[datetime.date]]:
        week_max_date = self.get_week_max_date(week_conn, stock_code)
        day_max_date = self.get_day_max_date(day_conn, stock_code)
        if not day_max_date:
            return False, None
        if not week_max_date:
            self.stats['full_count'] += 1
            return True, None
        if day_max_date > week_max_date:
            self.stats['incremental_count'] += 1
            return True, week_max_date
        else:
            return False, None

    def process_stock(self, day_conn, week_conn, stock_code: str) -> bool:
        self._log(f"处理股票: {stock_code}")
        try:
            should_process, start_date = self.should_process_stock(day_conn, week_conn, stock_code)
            if not should_process:
                self.stats['stocks_skipped'] += 1
                return False

            conditions = [f"{self.code_column} = '{stock_code}'"]
            if start_date:
                conditions.append(f"{self.date_column} > '{start_date}'")
            where_clause = " WHERE " + " AND ".join(conditions)

            check_query = f"SELECT COUNT(*) FROM {self.day_table_name} {where_clause}"
            count = day_conn.execute(check_query).fetchone()[0]
            if count == 0:
                self.stats['stocks_skipped'] += 1
                return False
            self._log(f"  待处理记录数: {count}")

            volume_select = f"COALESCE({self.volume_column}, 0) as volume" if self.volume_column else "0 as volume"
            amount_select = f"COALESCE({self.amount_column}, 0) as amount" if self.amount_column else "0 as amount"

            weekly_query = f"""
            WITH daily_data AS (
                SELECT {self.date_column} as trade_date, {self.open_column} as open,
                       {self.high_column} as high, {self.low_column} as low,
                       {self.close_column} as close, {volume_select}, {amount_select}
                FROM {self.day_table_name} {where_clause}
            ),
            weekly_group AS (
                SELECT DATE_TRUNC('week', trade_date) + INTERVAL 4 DAY as week_date,
                       FIRST(open ORDER BY trade_date) as week_open,
                       MAX(high) as week_high, MIN(low) as week_low,
                       LAST(close ORDER BY trade_date) as week_close,
                       SUM(volume) as week_volume, SUM(amount) as week_amount
                FROM daily_data WHERE EXTRACT(DOW FROM trade_date) BETWEEN 1 AND 5
                GROUP BY DATE_TRUNC('week', trade_date)
            )
            SELECT '{stock_code}' as code, week_date as date,
                   week_open as open, week_high as high, week_low as low,
                   week_close as close, week_volume as volume, week_amount as amount
            FROM weekly_group ORDER BY week_date
            """

            weekly_result = day_conn.execute(weekly_query).to_arrow_table()
            if weekly_result.num_rows == 0:
                self.stats['stocks_skipped'] += 1
                return False

            weeks_added = weekly_result.num_rows
            self._log(f"  生成 {weeks_added} 周数据")

            try:
                week_conn.register("temp_weekly_view", weekly_result)
                week_conn.execute(f"INSERT OR REPLACE INTO {self.main_table} SELECT * FROM temp_weekly_view")
                week_conn.execute("DROP VIEW IF EXISTS temp_weekly_view")
                self.stats['weekly_rows_added'] += weeks_added
                self.stats['stocks_processed'] += 1
                self._log(f"  成功处理 {stock_code}: 新增/更新 {weeks_added} 周数据")
                return True
            except Exception as e:
                self._log(f"  保存到周线数据库失败: {e}", "error")
                self.stats['stocks_skipped'] += 1
                return False

        except Exception as e:
            self._log(f"  处理失败: {e}", "error")
            self.stats['stocks_skipped'] += 1
            return False

    def process_all_stocks(self):
        day_conn = None
        week_conn = None
        try:
            day_conn = duckdb.connect(str(self.day_db))
            week_conn = duckdb.connect(str(self.week_db))
            day_conn.execute("SET memory_limit='8GB'")
            day_conn.execute("SET threads=8")
            week_conn.execute("SET memory_limit='8GB'")
            week_conn.execute("SET threads=8")

            if not self.detect_day_table_structure(day_conn):
                return
            if not self.create_table_if_not_exists(week_conn):
                return

            self._log("数据库连接成功")
            stock_codes = self.get_stock_codes(day_conn)
            if not stock_codes:
                self._log("未找到任何股票代码!", "error")
                return

            total_stocks = len(stock_codes)
            self._log(f"总共需要处理 {total_stocks} 个股票")

            for idx, stock_code in enumerate(stock_codes, 1):
                if self.user_cancelled:
                    self._log("用户取消操作，停止处理")
                    break
                if self.progress_callback:
                    self.progress_callback(idx, total_stocks, f"处理股票 {stock_code}")
                self.process_stock(day_conn, week_conn, stock_code)
                if idx % 100 == 0:
                    try:
                        week_conn.execute("CHECKPOINT")
                        self._log(f"已处理 {idx}/{total_stocks} 个股票")
                    except Exception as e:
                        self._log(f"检查点执行失败: {e}", "warning")

            self._log(f"数据处理完成: 共处理 {self.stats['stocks_processed']} 个股票")
            self._log(f"全量转换: {self.stats['full_count']} 个, 增量转换: {self.stats['incremental_count']} 个")
            self._log(f"跳过: {self.stats['stocks_skipped']} 个, 新增/更新 {self.stats['weekly_rows_added']:,} 条周线记录")

        except Exception as e:
            self._log(f"处理数据库失败: {e}", "error")
        finally:
            if day_conn:
                day_conn.close()
            if week_conn:
                try:
                    week_conn.execute("CHECKPOINT")
                    week_conn.close()
                except:
                    week_conn.close()

    def run_conversion(self):
        self.stats['start_time'] = datetime.now()
        self._log(f"开始转换: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        if not self.day_db.exists():
            self._log(f"日线数据库不存在: {self.day_db}", "error")
            return
        self.process_all_stocks()
        self.stats['end_time'] = datetime.now()
        self._log(f"转换完成，耗时 {(self.stats['end_time'] - self.stats['start_time']).total_seconds():.1f}秒")


# ==================== 模块接口 ====================
def run_module(day_db_path=None, week_db_path=None, progress_callback=None, log_callback=None):
    """被主程序调用的模块入口"""
    if log_callback:
        log_callback("开始周线数据转换...")
    converter = WeekConverter(
        day_db_path=Path(day_db_path) if day_db_path else None,
        week_db_path=Path(week_db_path) if week_db_path else None,
        progress_callback=progress_callback,
        log_callback=log_callback
    )
    converter.run_conversion()
    if log_callback:
        log_callback(f"周线转换完成")


# ==================== 主程序 ====================
def main():
    import argparse
    parser = argparse.ArgumentParser(description="日线转周线数据转换器")
    parser.add_argument("--day-db", type=str, help="日线数据库路径")
    parser.add_argument("--week-db", type=str, help="周线数据库路径")
    args = parser.parse_args()
    day_db = Path(args.day_db) if args.day_db else None
    week_db = Path(args.week_db) if args.week_db else None
    run_module(day_db_path=day_db, week_db_path=week_db)


if __name__ == "__main__":
    main()
