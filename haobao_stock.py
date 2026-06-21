import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import duckdb
import polars as pl
import csv
import math
import os
import sys
import shutil
import re
from datetime import datetime

# ========== 路径处理模块 ==========
def get_app_path():
    """获取程序所在目录（兼容开发环境和打包环境）"""
    if getattr(sys, 'frozen', False):
        # 打包后的环境
        if hasattr(sys, '_MEIPASS'):
            # PyInstaller 环境
            return sys._MEIPASS
        else:
            # Nuitka 环境
            return os.path.dirname(os.path.abspath(sys.argv[0]))
    else:
        # 开发环境
        return os.path.dirname(os.path.abspath(__file__))

def get_data_path():
    """获取数据文件存储目录（用户数据目录）"""
    if getattr(sys, 'frozen', False):
        # 打包后的环境 - 使用可执行文件所在目录
        return os.path.dirname(os.path.abspath(sys.executable))
    else:
        # 开发环境 - 使用脚本所在目录
        return os.path.dirname(os.path.abspath(__file__))

def ensure_data_dir(path):
    """确保数据目录存在"""
    if not os.path.exists(path):
        try:
            os.makedirs(path)
            print(f"已创建数据目录: {path}")
            return True
        except Exception as e:
            print(f"创建目录失败: {e}")
            return False
    return True

def find_data_file(filename, search_paths=None):
    """查找数据文件 - 优先在用户数据目录查找，找不到则在程序目录查找"""
    app_path = get_app_path()
    data_path = get_data_path()
    
    # 优先在用户数据目录查找（可读写）
    user_paths = [
        os.path.join(data_path, "stock_data", filename),  # stock_data子目录
        os.path.join(data_path, filename),                # 程序目录
    ]
    
    for path in user_paths:
        if os.path.exists(path):
            return path
    
    # 如果在用户数据目录找不到，在程序目录（可能是临时目录）查找
    prog_paths = [
        os.path.join(app_path, "stock_data", filename),   # stock_data子目录
        os.path.join(app_path, filename),                 # 程序目录
        os.path.join(os.path.dirname(app_path), "stock_data", filename),  # 上一级
    ]
    
    for path in prog_paths:
        if os.path.exists(path):
            # 如果找到，复制到用户数据目录
            target_path = os.path.join(data_path, "stock_data", filename)
            try:
                ensure_data_dir(os.path.dirname(target_path))
                shutil.copy2(path, target_path)
                print(f"已复制 {filename} 到数据目录: {target_path}")
                return target_path
            except:
                return path
    
    return None

def copy_default_files_if_needed(data_dir, app_dir):
    """如果需要，复制默认文件到数据目录"""
    default_files = ["zixuan.csv", "zixuangu.csv"]
    
    for filename in default_files:
        target_path = os.path.join(data_dir, filename)
        if not os.path.exists(target_path):
            # 尝试从程序目录复制
            source_path = os.path.join(app_dir, "stock_data", filename)
            if os.path.exists(source_path):
                try:
                    shutil.copy2(source_path, target_path)
                    print(f"已复制 {filename} 到数据目录")
                except Exception as e:
                    print(f"复制 {filename} 失败: {e}")

def debug_paths(app_dir, data_dir, stock_data_dir):
    """调试路径信息"""
    print("=" * 50)
    print("路径调试信息:")
    print(f"程序运行模式: {'打包模式' if getattr(sys, 'frozen', False) else '开发模式'}")
    print(f"sys.executable: {sys.executable}")
    print(f"app_dir (程序临时目录): {app_dir}")
    print(f"data_dir (用户数据目录): {data_dir}")
    print(f"stock_data_dir: {stock_data_dir}")
    print(f"当前工作目录: {os.getcwd()}")
    print(f"TEMP环境变量: {os.environ.get('TEMP', 'Not set')}")
    
    # 检查数据目录是否存在
    if os.path.exists(stock_data_dir):
        print(f"✓ 数据目录存在")
        try:
            files = os.listdir(stock_data_dir)
            print(f"  目录中的文件: {files}")
        except:
            print(f"  无法读取目录内容")
    else:
        print(f"✗ 数据目录不存在")
    
    print("=" * 50)
# ========== 路径处理模块结束 ==========

# ========== 股票代码处理模块 ==========
def normalize_stock_code(code):
    """
    规范化股票代码
    支持格式：
    - 纯数字：600000
    - sh开头：sh600000, SH600000
    - sz开头：sz000001, SZ000001
    - 带点号：600000.SH, 000001.SZ
    """
    if not code or not isinstance(code, str):
        return str(code) if code else ""
    
    # 转换为字符串并去除空格
    code_str = str(code).strip().upper()
    
    # 如果为空，返回空字符串
    if not code_str:
        return ""
    
    # 处理带点号的格式 (600000.SH, 000001.SZ)
    if '.' in code_str:
        parts = code_str.split('.')
        if len(parts) == 2 and parts[1] in ['SH', 'SZ']:
            return parts[0]
    
    # 处理sh/sz开头的格式
    if code_str.startswith('SH') or code_str.startswith('SZ'):
        # 去除前缀，保留数字部分
        return re.sub(r'^[A-Z]+', '', code_str)
    
    # 纯数字格式，直接返回
    return code_str

def is_valid_stock_code(code):
    """
    判断是否为有效的股票代码
    规则：
    - 6位数字（A股）
    - 或去除前缀后为6位数字
    """
    if not code:
        return False
    
    normalized = normalize_stock_code(code)
    return normalized.isdigit() and len(normalized) == 6

def extract_code_from_text(text):
    """
    从文本中提取股票代码
    用于从CSV文件或用户输入中提取代码
    """
    if not text:
        return None
    
    text_str = str(text).strip()
    
    # 尝试直接规范化
    normalized = normalize_stock_code(text_str)
    if is_valid_stock_code(normalized):
        return normalized
    
    # 尝试从文本中提取6位数字
    matches = re.findall(r'\b\d{6}\b', text_str)
    if matches:
        return matches[0]
    
    # 尝试提取sh/sz开头的代码
    matches = re.findall(r'\b(?:SH|SZ)\d{6}\b', text_str.upper())
    if matches:
        return normalize_stock_code(matches[0])
    
    return None

def read_codes_from_csv(file_path):
    """
    从CSV文件读取股票代码，自动跳过非股票代码行
    返回规范化后的股票代码列表
    """
    codes = []
    
    if not os.path.exists(file_path):
        return codes
    
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            for row in reader:
                if not row or not row[0].strip():
                    continue
                
                # 尝试从第一列提取代码
                code = extract_code_from_text(row[0].strip())
                if code:
                    codes.append(code)
                else:
                    # 如果第一列不是有效代码，尝试整行提取
                    code = extract_code_from_text(' '.join(row))
                    if code:
                        codes.append(code)
        
        print(f"从 {file_path} 读取到 {len(codes)} 个有效股票代码")
        return codes
    except Exception as e:
        print(f"读取CSV文件失败: {e}")
        return []
# ========== 股票代码处理模块结束 ==========

class StockViewerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("股票查看工具 · 专业版")
        self.root.state('zoomed')

        self.con = None
        self.stock_info_con = None
        self.db_path = ""
        self.current_table = None
        self.code_col = None
        self.date_col = None
        self.all_codes = pl.Series()
        self.filtered_codes = pl.Series()
        self.current_display_code = None
        self.current_code_index = -1
        self.dark_mode = False
        self.current_market = "all"
        self.current_period = "day"
        self.is_zixuan_mode = False
        self.is_original_zixuan_mode = False
        self.is_zixuan_locked = False
        self.zixuan_codes = []
        self.zixuangu_codes = []
        self.last_normal_period = "day"
        self.initialized = False
        self.base_period = "day"
        self.base_code = None
        self.locked_code = None
        self.rank_mode = None
        self.stock_info_columns = []
        
        # 复权相关
        self.fuquan_type = "none"
        self.has_factor = False
        
        # 弹窗管理
        self.message_window = None
        self.message_timer = None
        
        # K线图缩放相关
        self.candle_offset = 0
        self.total_candles = 0
        self.display_candles = 300
        self.min_candles = 5  # 最小K线数量
        self.max_candles = 500
        
        # K线图自适应显示相关
        self.fixed_candle_spacing = 12  # 固定K线间距（像素）
        self.min_candle_width = 4       # 最小K线宽度
        self.max_candle_width = 12      # 最大K线宽度
        
        # 十字线相关
        self.crosshair_x = None
        self.crosshair_y = None
        self.crosshair_lines = []
        self.crosshair_text = None
        self.current_hover_kline = None
        self.kline_positions = []
        
        # 获取路径
        self.app_dir = get_app_path()      # 程序临时目录（只读）
        self.data_dir = get_data_path()    # 用户数据目录（可读写）
        self.stock_data_dir = os.path.join(self.data_dir, "stock_data")
        
        # 确保数据目录存在
        ensure_data_dir(self.stock_data_dir)
        
        # 复制默认文件
        copy_default_files_if_needed(self.stock_data_dir, self.app_dir)
        
        # 调试路径信息
        debug_paths(self.app_dir, self.data_dir, self.stock_data_dir)
        
        # 查找数据库文件
        self.stock_info_db_path = self._find_db_file("stock_info_data")
        if not self.stock_info_db_path:
            self.stock_info_db_path = os.path.join(self.stock_data_dir, "stock_info_data.duckdb")
            print(f"使用默认路径: {self.stock_info_db_path}")
        
        self.zixuan_csv_path = find_data_file("zixuan.csv") or os.path.join(self.stock_data_dir, "zixuan.csv")
        self.zixuangu_csv_path = find_data_file("zixuangu.csv") or os.path.join(self.stock_data_dir, "zixuangu.csv")
        
        print(f"股票信息数据库: {self.stock_info_db_path}")
        print(f"自选股文件: {self.zixuan_csv_path}")
        print(f"原自选股文件: {self.zixuangu_csv_path}")
        
        # 周期配置
        self.period_config = {
            "day": {
                "name": "日线",
                "db_file": "stock_data",
                "skip_tables": ["time"],
                "target_table": "dayly_stock_data",
                "date_field": "date",
                "button_text": "日"
            },
            "week": {
                "name": "周线",
                "db_file": "weekly_stock_data",
                "skip_tables": ["time"],
                "target_table": "weekly_data",
                "date_field": "date_time",
                "button_text": "周"
            },
            "month": {
                "name": "月线",
                "db_file": "monthly_stock_data",
                "skip_tables": ["time"],
                "target_table": "monthly_data",
                "date_field": "date_time",
                "button_text": "月"
            },
            "season": {  # 新增季线
                "name": "季线",
                "db_file": "seasonly_data",
                "skip_tables": ["time"],
                "target_table": "seasonly_data",
                "date_field": "date_time",
                "button_text": "季"
            },
            "year": {    # 新增年线
                "name": "年线",
                "db_file": "yearly_data",
                "skip_tables": ["time"],
                "target_table": "yearly_data",
                "date_field": "date_time",
                "button_text": "年"
            },
            "60min": {
                "name": "60分钟",
                "db_file": "minute60_stock_data",
                "skip_tables": ["time"],
                "target_table": "minly60_data",
                "date_field": "date_time",
                "button_text": "60分"
            },
            "30min": {
                "name": "30分钟",
                "db_file": "minute30_stock_data",
                "skip_tables": ["time"],
                "target_table": "minly30_data",
                "date_field": "date_time",
                "button_text": "30分"
            },
            "15min": {
                "name": "15分钟",
                "db_file": "minute15_stock_data",
                "skip_tables": ["time"],
                "target_table": "minly15_data",
                "date_field": "date_time",
                "button_text": "15分"
            },
            "5min": {
                "name": "5分钟",
                "db_file": "minute5_stock_data",
                "skip_tables": ["time"],
                "target_table": "minly5_data",
                "date_field": "date_time",
                "button_text": "5分"
            },
            "zixuan": {
                "name": "自选股",
                "db_file": None,
                "skip_tables": [],
                "target_table": None,
                "date_field": None,
                "button_text": "自选"
            }
        }
        
        # K线图相关变量
        self.kline_data = None
        self.kline_canvas = None
        
        # 视图模式
        self.view_mode = "kline"
        
        # 当前选中的指标
        self.current_indicator = "macd"
        
        # 涨幅榜/跌幅榜锁定状态
        self.rank_locked = False
        self.rank_locked_codes = []
        self.rank_locked_type = None

        # 连接股票信息数据库
        self._connect_stock_info_db()
        
        self._create_widgets()
        
        # 绑定键盘事件
        self.root.bind("<Up>", lambda e: self.prev_stock())
        self.root.bind("<Down>", lambda e: self.next_stock())
        self.root.bind("<Prior>", lambda e: self.zoom_in())
        self.root.bind("<Next>", lambda e: self.zoom_out())
        self.root.bind("<Control-plus>", lambda e: self.zoom_in())
        self.root.bind("<Control-minus>", lambda e: self.zoom_out())
        self.root.bind("<Left>", lambda e: self.pan_left())
        self.root.bind("<Right>", lambda e: self.pan_right())
        
        # 加载自选股列表
        self._load_zixuan_codes()
        self._load_zixuangu_codes()
        
        # 程序启动后自动初始化日线
        self.root.after(100, self.auto_initialize)

    def _find_db_file(self, base_name):
        """
        查找数据库文件
        优先查找.duckdb文件，如果不存在则查找.parquet文件
        """
        # 先查找.duckdb文件
        duckdb_path = os.path.join(self.stock_data_dir, f"{base_name}.duckdb")
        if os.path.exists(duckdb_path):
            return duckdb_path
        
        # 再查找.parquet文件
        parquet_path = os.path.join(self.stock_data_dir, f"{base_name}.parquet")
        if os.path.exists(parquet_path):
            print(f"找到Parquet文件: {parquet_path}")
            return parquet_path
        
        # 在程序目录查找
        prog_duckdb = os.path.join(self.app_dir, "stock_data", f"{base_name}.duckdb")
        if os.path.exists(prog_duckdb):
            return prog_duckdb
        
        prog_parquet = os.path.join(self.app_dir, "stock_data", f"{base_name}.parquet")
        if os.path.exists(prog_parquet):
            print(f"在程序目录找到Parquet文件: {prog_parquet}")
            return prog_parquet
        
        return None

    def _connect_db_readonly(self, db_path):
        """
        以只读模式连接数据库
        支持.duckdb和.parquet文件
        """
        if not os.path.exists(db_path):
            return None
        
        try:
            file_ext = os.path.splitext(db_path)[1].lower()
            
            if file_ext == '.parquet':
                # 对于Parquet文件，使用Polars读取，然后注册到DuckDB
                print(f"读取Parquet文件: {db_path}")
                df = pl.read_parquet(db_path)
                
                # 创建内存数据库连接
                conn = duckdb.connect(':memory:')
                # 注册DataFrame
                table_name = os.path.splitext(os.path.basename(db_path))[0]
                conn.register(table_name, df)
                return conn
            else:
                # DuckDB文件，以只读模式连接
                return duckdb.connect(db_path, read_only=True)
        except Exception as e:
            print(f"连接数据库失败: {e}")
            return None

    def _connect_stock_info_db(self):
        """连接股票信息数据库"""
        try:
            ensure_data_dir(self.stock_data_dir)
            
            if os.path.exists(self.stock_info_db_path):
                self.stock_info_con = self._connect_db_readonly(self.stock_info_db_path)
                
                if self.stock_info_con:
                    print(f"已连接股票信息数据库: {self.stock_info_db_path}")
                    
                    try:
                        # 获取表结构
                        tables = self.stock_info_con.execute("SHOW TABLES").fetchall()
                        if tables:
                            # 使用第一个表
                            first_table = tables[0][0]
                            columns = self.stock_info_con.execute(f"PRAGMA table_info({first_table})").fetchall()
                            self.stock_info_columns = [col[1] for col in columns]
                            print(f"股票信息表字段: {self.stock_info_columns}")
                        else:
                            self.stock_info_columns = []
                    except Exception as e:
                        print(f"获取表结构失败: {e}")
                        self.stock_info_columns = []
                else:
                    print(f"连接数据库失败: {self.stock_info_db_path}")
                    self.stock_info_con = None
                    self.stock_info_columns = []
            else:
                print(f"股票信息数据库不存在: {self.stock_info_db_path}")
                self.stock_info_con = None
                self.stock_info_columns = []
        except Exception as e:
            print(f"连接股票信息数据库失败: {e}")
            self.stock_info_con = None
            self.stock_info_columns = []

    # ========== 股票信息获取函数 ==========
    def get_stock_name(self, code):
        """获取股票名称"""
        if not self.stock_info_con or not code:
            return ""
        
        try:
            # 规范化代码
            norm_code = normalize_stock_code(code)
            
            result = self.stock_info_con.execute(
                "SELECT name FROM stock_data WHERE code = ?", 
                [norm_code]
            ).fetchone()
            return result[0] if result and result[0] else ""
        except Exception as e:
            print(f"获取股票名称失败: {e}")
            return ""

    def get_stock_pe(self, code):
        """获取股票市盈率"""
        if not self.stock_info_con or not code or 'pe' not in self.stock_info_columns:
            return None
        
        try:
            norm_code = normalize_stock_code(code)
            result = self.stock_info_con.execute(
                "SELECT pe FROM stock_data WHERE code = ?", 
                [norm_code]
            ).fetchone()
            return result[0] if result else None
        except Exception as e:
            print(f"获取PE失败: {e}")
            return None

    def get_stock_pb(self, code):
        """获取股票市净率"""
        if not self.stock_info_con or not code or 'pb' not in self.stock_info_columns:
            return None
        
        try:
            norm_code = normalize_stock_code(code)
            result = self.stock_info_con.execute(
                "SELECT pb FROM stock_data WHERE code = ?", 
                [norm_code]
            ).fetchone()
            return result[0] if result else None
        except Exception as e:
            print(f"获取PB失败: {e}")
            return None

    def get_stock_market_value(self, code):
        """获取股票市值"""
        if not self.stock_info_con or not code or '市值' not in self.stock_info_columns:
            return None
        
        try:
            norm_code = normalize_stock_code(code)
            result = self.stock_info_con.execute(
                "SELECT \"市值\" FROM stock_data WHERE code = ?", 
                [norm_code]
            ).fetchone()
            return result[0] if result else None
        except Exception as e:
            print(f"获取市值失败: {e}")
            return None

    def get_stock_industry(self, code):
        """获取股票行业"""
        if not self.stock_info_con or not code or '行业' not in self.stock_info_columns:
            return None
        
        try:
            norm_code = normalize_stock_code(code)
            result = self.stock_info_con.execute(
                "SELECT \"行业\" FROM stock_data WHERE code = ?", 
                [norm_code]
            ).fetchone()
            return result[0] if result else None
        except Exception as e:
            print(f"获取行业失败: {e}")
            return None

    def get_stock_dividend(self, code):
        """获取股票每股利润"""
        if not self.stock_info_con or not code or '每股利润' not in self.stock_info_columns:
            return None
        
        try:
            norm_code = normalize_stock_code(code)
            result = self.stock_info_con.execute(
                "SELECT \"每股利润\" FROM stock_data WHERE code = ?", 
                [norm_code]
            ).fetchone()
            return result[0] if result else None
        except Exception as e:
            print(f"获取每股利润失败: {e}")
            return None

    def get_stock_latest_price_and_change(self, code):
        """获取股票最新价和涨跌幅"""
        if not self.con or not code or not self.current_table:
            return None, None, None
        
        try:
            order_field = self.date_col if self.date_col else self.code_col
            sql = f"""
                SELECT close FROM {self.current_table} 
                WHERE {self.code_col} = ? 
                ORDER BY {order_field} DESC 
                LIMIT 2
            """
            result = self.con.execute(sql, [code]).fetchall()
            
            if len(result) >= 2:
                latest_close = result[0][0]
                prev_close = result[1][0]
                
                if prev_close and prev_close > 0:
                    change_pct = (latest_close - prev_close) / prev_close * 100
                    return latest_close, change_pct, prev_close
                else:
                    return latest_close, None, None
            elif len(result) == 1:
                return result[0][0], None, None
            return None, None, None
        except Exception as e:
            print(f"获取最新价和涨跌幅失败: {e}")
            return None, None, None

    # ========== 格式化函数 ==========
    def format_pe(self, pe_value):
        """格式化市盈率显示"""
        if pe_value is None:
            return "0.00"
        try:
            pe_float = float(pe_value)
            if pe_float < 0:
                return "亏损"
            elif pe_float == 0:
                return "0.00"
            else:
                return f"{pe_float:.2f}"
        except:
            return "0.00"

    def format_pb(self, pb_value):
        """格式化市净率显示"""
        if pb_value is None:
            return "0.00"
        try:
            pb_float = float(pb_value)
            if pb_float <= 0:
                return "0.00"
            else:
                return f"{pb_float:.2f}"
        except:
            return "0.00"

    def format_market_value(self, value):
        """格式化市值显示（单位：亿）"""
        if value is None:
            return "0.00"
        try:
            val_float = float(value)
            if val_float <= 0:
                return "0.00"
            else:
                if val_float > 1e8:
                    return f"{val_float/1e8:.2f}"
                else:
                    return f"{val_float:.2f}"
        except:
            return "0.00"

    def format_dividend(self, value):
        """格式化每股利润显示"""
        if value is None:
            return "0.00"
        try:
            val_float = float(value)
            if val_float <= 0:
                return "0.00"
            else:
                return f"{val_float:.2f}"
        except:
            return "0.00"

    def format_change_percent(self, change_pct):
        """格式化涨跌幅显示"""
        if change_pct is None:
            return "0.00%"
        return f"{change_pct:+.2f}%"

    def get_change_color(self, change_pct):
        """获取涨跌幅颜色"""
        if change_pct is None:
            return "black"
        return "red" if change_pct > 0 else ("green" if change_pct < 0 else "black")

    def update_stock_info_display(self):
        """更新所有股票信息显示"""
        if not self.current_display_code:
            return
        
        code = self.current_display_code
        
        # 更新股票名称
        stock_name = self.get_stock_name(code)
        self.stock_name_var.set(stock_name)
        
        # 更新最新价和涨跌幅
        latest_price, change_pct, _ = self.get_stock_latest_price_and_change(code)
        
        if latest_price is not None:
            self.latest_price_var.set(f"{latest_price:.2f}")
            color = self.get_change_color(change_pct)
            self.latest_price_label.config(foreground=color)
        else:
            self.latest_price_var.set("--")
            self.latest_price_label.config(foreground="black")
        
        if change_pct is not None:
            self.change_percent_var.set(self.format_change_percent(change_pct))
            color = self.get_change_color(change_pct)
            self.change_percent_label.config(foreground=color)
        else:
            self.change_percent_var.set("0.00%")
            self.change_percent_label.config(foreground="black")
        
        # PE
        pe_value = self.get_stock_pe(code)
        self.pe_var.set(self.format_pe(pe_value))
        
        # PB
        pb_value = self.get_stock_pb(code)
        self.pb_var.set(self.format_pb(pb_value))
        
        # 市值
        market_value = self.get_stock_market_value(code)
        self.market_value_var.set(self.format_market_value(market_value))
        
        # 行业
        industry = self.get_stock_industry(code)
        self.industry_var.set(industry if industry else "")
        
        # 每股利润
        dividend = self.get_stock_dividend(code)
        self.dividend_var.set(self.format_dividend(dividend))

    # ========== 涨幅榜/跌幅榜功能 ==========
    def calculate_rank(self, rank_type='gain', limit=100):
        """计算涨幅榜或跌幅榜"""
        if not self.con or not self.current_table:
            self.show_message("提示", "请先打开数据库", "warning")
            return []
        
        try:
            codes_result = self.con.execute(
                f"SELECT DISTINCT {self.code_col} FROM {self.current_table}"
            ).fetchall()
            all_codes = [r[0] for r in codes_result]
            
            rank_list = []
            order_field = self.date_col if self.date_col else self.code_col
            
            for code in all_codes:
                sql = f"""
                    SELECT close FROM {self.current_table} 
                    WHERE {self.code_col} = ? 
                    ORDER BY {order_field} DESC 
                    LIMIT 2
                """
                result = self.con.execute(sql, [code]).fetchall()
                
                if len(result) >= 2:
                    latest_close = result[0][0]
                    prev_close = result[1][0]
                    
                    if prev_close and prev_close > 0:
                        change_pct = (latest_close - prev_close) / prev_close * 100
                        rank_list.append((code, change_pct, latest_close))
            
            if rank_type == 'gain':
                rank_list.sort(key=lambda x: x[1], reverse=True)
            else:
                rank_list.sort(key=lambda x: x[1])
            
            return rank_list[:limit]
            
        except Exception as e:
            self.show_message("错误", f"计算{rank_type}榜失败: {str(e)}", "error")
            return []

    def show_gainers(self):
        """显示涨幅榜"""
        if self.is_zixuan_mode or self.is_original_zixuan_mode:
            self._exit_all_special_modes()
        
        self.rank_mode = 'gain'
        self.rank_locked = False
        self.show_message("提示", "正在计算涨幅榜...", "info")
        self.root.update()
        
        gainers = self.calculate_rank('gain', 100)
        
        if not gainers:
            self.show_message("提示", "无涨幅榜数据", "warning")
            return
        
        self.rank_locked_codes = [code for code, _, _ in gainers]
        self.rank_locked_type = 'gain'
        
        self.list_codes.delete(0, tk.END)
        for code, change_pct, latest_price in gainers:
            self.list_codes.insert(tk.END, f"{code}  {change_pct:+.2f}%")
        
        self.label_cnt.config(text=f"📊 涨幅榜 {len(gainers)}只")
        self.position_label.config(text="")
        
        if gainers:
            first_code = gainers[0][0]
            self.current_code_index = 0
            self.load_one_stock(first_code)
            self.highlight_current_code()
        
        self.rank_mode = 'gain'

    def show_losers(self):
        """显示跌幅榜"""
        if self.is_zixuan_mode or self.is_original_zixuan_mode:
            self._exit_all_special_modes()
        
        self.rank_mode = 'loss'
        self.rank_locked = False
        self.show_message("提示", "正在计算跌幅榜...", "info")
        self.root.update()
        
        losers = self.calculate_rank('loss', 100)
        
        if not losers:
            self.show_message("提示", "无跌幅榜数据", "warning")
            return
        
        self.rank_locked_codes = [code for code, _, _ in losers]
        self.rank_locked_type = 'loss'
        
        self.list_codes.delete(0, tk.END)
        for code, change_pct, latest_price in losers:
            self.list_codes.insert(tk.END, f"{code}  {change_pct:+.2f}%")
        
        self.label_cnt.config(text=f"📊 跌幅榜 {len(losers)}只")
        self.position_label.config(text="")
        
        if losers:
            first_code = losers[0][0]
            self.current_code_index = 0
            self.load_one_stock(first_code)
            self.highlight_current_code()
        
        self.rank_mode = 'loss'

    def toggle_rank_lock(self):
        """切换涨幅榜/跌幅榜锁定状态"""
        if not self.rank_mode:
            return
        
        self.rank_locked = not self.rank_locked
        if self.rank_locked:
            self.btn_rank_lock.config(text="🔒锁定")
            self.label_tip.config(text=f"🔒{self.rank_mode}榜已锁定，点击左侧代码查看不同周期")
        else:
            self.btn_rank_lock.config(text="🔓解锁")
            self.label_tip.config(text=f"{self.rank_mode}榜已解锁")

    def _exit_all_special_modes(self):
        """退出所有特殊模式"""
        if self.is_zixuan_mode:
            self._exit_zixuan_mode()
        if self.is_original_zixuan_mode:
            self._exit_original_zixuan_mode()
        self.rank_mode = None
        self.rank_locked = False
        self.rank_locked_codes = []

    # ========== 复权功能 ==========
    def set_fuquan_type(self, fuquan_type):
        """设置复权类型"""
        self.fuquan_type = "none"
        self.fuquan_button.config(text="不复权")
        self.show_message("提示", "当前版本仅支持显示原始数据，复权功能已禁用", "info")
        
        if self.current_display_code:
            self.refresh_kline()
    
    def _check_factor_field(self, df):
        return False
    
    def _apply_fuquan(self, df):
        return df

    # ========== 静默弹窗管理 ==========
    def show_message(self, title, message, type="info"):
        """显示静默弹窗"""
        self.close_message()
        
        self.message_window = tk.Toplevel(self.root)
        self.message_window.title(title)
        self.message_window.attributes('-topmost', True)
        
        window_width = 300
        window_height = 100
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        x = (screen_width - window_width) // 2
        y = (screen_height - window_height) // 2
        self.message_window.geometry(f"{window_width}x{window_height}+{x}+{y}")
        
        self.message_window.overrideredirect(True)
        self.message_window.configure(bg='#333333' if self.dark_mode else '#f0f0f0')
        
        frame = ttk.Frame(self.message_window)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        if type == "info":
            icon = "ℹ️"
        elif type == "warning":
            icon = "⚠️"
        elif type == "error":
            icon = "❌"
        elif type == "success":
            icon = "✅"
        else:
            icon = "📢"
        
        icon_label = ttk.Label(frame, text=icon, font=("Arial", 20))
        icon_label.pack(side=tk.LEFT, padx=5)
        
        text_label = ttk.Label(frame, text=message, wraplength=200, font=("Microsoft YaHei", 10))
        text_label.pack(side=tk.LEFT, padx=5, fill=tk.BOTH, expand=True)
        
        self.message_timer = self.message_window.after(1000, self.close_message)
    
    def close_message(self):
        """关闭弹窗"""
        if self.message_timer:
            self.message_window.after_cancel(self.message_timer)
            self.message_timer = None
        
        if self.message_window:
            try:
                self.message_window.destroy()
            except:
                pass
            self.message_window = None

    def auto_initialize(self):
        """自动初始化日线"""
        try:
            day_config = self.period_config["day"]
            db_path = self._find_db_file(day_config["db_file"])
            
            if db_path and os.path.exists(db_path):
                self.base_period = "day"
                self._switch_normal_period("day")
                self.initialized = True
                print("程序初始化成功：已加载日线数据")
            else:
                print(f"日线数据库不存在")
                self.show_message("提示", "请先打开数据库文件", "info")
        except Exception as e:
            print(f"初始化失败：{e}")

    def _create_kline_view(self):
        """创建K线图视图"""
        for widget in self.frame_right.winfo_children():
            widget.destroy()
        
        chart_toolbar = ttk.Frame(self.frame_right)
        chart_toolbar.pack(fill=tk.X, padx=2, pady=2)
        
        ttk.Label(chart_toolbar, text="日期筛选:").pack(side=tk.LEFT, padx=2)
        ttk.Label(chart_toolbar, text="从").pack(side=tk.LEFT, padx=2)
        self.entry_s = ttk.Entry(chart_toolbar, width=10)
        self.entry_s.pack(side=tk.LEFT, padx=2)
        ttk.Label(chart_toolbar, text="到").pack(side=tk.LEFT, padx=2)
        self.entry_e = ttk.Entry(chart_toolbar, width=10)
        self.entry_e.pack(side=tk.LEFT, padx=2)
        ttk.Button(chart_toolbar, text="📅 筛选", command=self.filter_by_date).pack(side=tk.LEFT, padx=2)
        
        # 指标选择下拉框
        ttk.Label(chart_toolbar, text="指标:").pack(side=tk.LEFT, padx=(20, 2))
        self.indicator_var = tk.StringVar(value="macd")
        self.indicator_combo = ttk.Combobox(chart_toolbar, textvariable=self.indicator_var, 
                                           values=["macd", "布林线", "rsi", "wr", "obv", "kdj"], 
                                           state="readonly", width=8)
        self.indicator_combo.pack(side=tk.LEFT, padx=2)
        self.indicator_combo.bind("<<ComboboxSelected>>", self.on_indicator_change)
        
        self.chart_info = ttk.Label(chart_toolbar, text="")
        self.chart_info.pack(side=tk.RIGHT, padx=5)
        
        self.kline_canvas = tk.Canvas(self.frame_right, bg='white', highlightthickness=0, cursor="crosshair")
        self.kline_canvas.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)
        
        self.kline_canvas.bind("<Configure>", self.on_canvas_resize)
        self.kline_canvas.bind("<Motion>", self.on_mouse_move)
        self.kline_canvas.bind("<Leave>", self.on_mouse_leave)
        self.kline_canvas.bind("<Button-1>", self.on_canvas_click)

    def _create_widgets(self):
        """创建UI组件"""
        # 第一行工具栏
        frame_top = ttk.Frame(self.root)
        frame_top.pack(fill=tk.X, padx=5, pady=2)

        self.label_db = ttk.Label(frame_top, text="未打开数据库")
        self.label_db.pack(side=tk.LEFT, padx=5)

        self.btn_open = ttk.Button(frame_top, text="📂打开", command=self.open_db, width=5)
        self.btn_open.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.btn_open, "打开数据库文件")

        ttk.Label(frame_top, text="表:").pack(side=tk.LEFT, padx=1)
        self.var_table = tk.StringVar()
        self.cb_table = ttk.Combobox(frame_top, textvariable=self.var_table, state="readonly", width=8)
        self.cb_table.pack(side=tk.LEFT, padx=1)
        self.cb_table.bind("<<ComboboxSelected>>", self.on_table_select)

        ttk.Label(frame_top, text="代码:").pack(side=tk.LEFT, padx=1)
        self.var_code = tk.StringVar()
        self.entry_code = ttk.Entry(frame_top, textvariable=self.var_code, width=8)
        self.entry_code.pack(side=tk.LEFT, padx=1)
        self.entry_code.bind("<Return>", self.search_by_code)
        ttk.Button(frame_top, text="🔍搜索", command=self.search_by_code, width=4).pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.entry_code, "输入股票代码后按回车或点击搜索")

        self.btn_reset = ttk.Button(frame_top, text="🔄重置", command=self.clear_all, width=4)
        self.btn_reset.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.btn_reset, "重置所有设置")
        
        self.btn_export = ttk.Button(frame_top, text="💾导出", command=self.export_csv, width=4)
        self.btn_export.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.btn_export, "导出当前数据为CSV文件")
        
        self.btn_theme = ttk.Button(frame_top, text="🌙暗色", command=self.toggle_theme, width=4)
        self.btn_theme.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.btn_theme, "切换深色/浅色主题")
        
        self.btn_prev_stock = ttk.Button(frame_top, text="▲", command=self.prev_stock, width=2)
        self.btn_prev_stock.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.btn_prev_stock, "上一个股票 (↑)")
        
        self.btn_next_stock = ttk.Button(frame_top, text="▼", command=self.next_stock, width=2)
        self.btn_next_stock.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.btn_next_stock, "下一个股票 (↓)")
        
        self.btn_zoom_in = ttk.Button(frame_top, text="➕", command=self.zoom_in, width=2)
        self.btn_zoom_in.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.btn_zoom_in, "放大K线 (Ctrl++)")
        
        self.btn_zoom_out = ttk.Button(frame_top, text="➖", command=self.zoom_out, width=2)
        self.btn_zoom_out.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.btn_zoom_out, "缩小K线 (Ctrl+-)")
        
        self.fuquan_button = ttk.Menubutton(frame_top, text="不复权")
        self.fuquan_button.pack(side=tk.LEFT, padx=1)
        fuquan_menu = tk.Menu(self.fuquan_button, tearoff=0)
        fuquan_menu.add_command(label="不复权", command=lambda: self.set_fuquan_type("none"))
        fuquan_menu.add_command(label="前复权", command=lambda: self.set_fuquan_type("qfq"))
        fuquan_menu.add_command(label="后复权", command=lambda: self.set_fuquan_type("hfq"))
        self.fuquan_button.config(menu=fuquan_menu)
        self._add_tooltip(self.fuquan_button, "选择复权类型")
        
        self.btn_data_view = ttk.Button(frame_top, text="📊数据", command=self.switch_to_data_view, width=4)
        self.btn_data_view.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.btn_data_view, "切换到数据表格视图")
        
        self.btn_kline_view = ttk.Button(frame_top, text="📈K线", command=self.switch_to_kline_view, width=4)
        self.btn_kline_view.pack(side=tk.LEFT, padx=1)
        self.btn_kline_view.state(['disabled'])
        self._add_tooltip(self.btn_kline_view, "切换到K线图视图")
        
        ttk.Separator(frame_top, orient=tk.VERTICAL).pack(side=tk.LEFT, padx=2, fill=tk.Y)
        
        self.period_buttons = {}
        periods_order = ["day", "week", "month", "season", "year", "60min", "30min", "15min", "5min"]
        for period_key in periods_order:
            config = self.period_config[period_key]
            btn = ttk.Button(frame_top, text=config["button_text"], 
                           command=lambda p=period_key: self.switch_period(p), width=3)
            btn.pack(side=tk.LEFT, padx=1)
            self.period_buttons[period_key] = btn
            self._add_tooltip(btn, f"切换到{config['name']}")
        
        self.zixuangu_button = ttk.Button(frame_top, text="自选", command=self.toggle_zixuangu_mode, width=3)
        self.zixuangu_button.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.zixuangu_button, "切换原自选股模式")
        
        self.zixuan_button = ttk.Button(frame_top, text="自选股", command=self.toggle_zixuan_mode, width=4)
        self.zixuan_button.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.zixuan_button, "切换自选股模式")
        
        self.btn_add_zixuan = ttk.Button(frame_top, text="➕", command=self.add_to_zixuan, width=2)
        self.btn_add_zixuan.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.btn_add_zixuan, "添加当前股票到自选股")
        
        self.btn_del_zixuan = ttk.Button(frame_top, text="➖", command=self.remove_from_zixuan, width=2)
        self.btn_del_zixuan.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.btn_del_zixuan, "从自选股中删除当前股票")
        
        ttk.Separator(frame_top, orient=tk.VERTICAL).pack(side=tk.LEFT, padx=2, fill=tk.Y)
        self.btn_pan_left = ttk.Button(frame_top, text="←", command=self.pan_left, width=2)
        self.btn_pan_left.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.btn_pan_left, "向左平移K线 (←)")
        
        self.btn_pan_right = ttk.Button(frame_top, text="→", command=self.pan_right, width=2)
        self.btn_pan_right.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.btn_pan_right, "向右平移K线 (→)")

        # 第二行工具栏
        frame_market = ttk.Frame(self.root)
        frame_market.pack(fill=tk.X, padx=5, pady=2)

        ttk.Label(frame_market, text="市场:").pack(side=tk.LEFT, padx=1)
        self.market_buttons = {}
        market_configs = [
            ("上证", "sh"), ("深证", "sz"), ("中小", "zx"),
            ("创业", "cy"), ("科创", "kc"), ("全部", "all")
        ]
        for text, cmd in market_configs:
            if text == "全部":
                btn = ttk.Button(frame_market, text=text, command=lambda: self.filter_market("all"), width=3)
            else:
                btn = ttk.Button(frame_market, text=text, command=lambda m=cmd: self.filter_market(m), width=3)
            btn.pack(side=tk.LEFT, padx=1)
            self.market_buttons[cmd] = btn
            self._add_tooltip(btn, f"筛选{text}股票")

        ttk.Separator(frame_market, orient=tk.VERTICAL).pack(side=tk.LEFT, padx=2, fill=tk.Y)

        ttk.Label(frame_market, text="K线数:").pack(side=tk.LEFT, padx=1)
        self.candle_count_var = tk.StringVar(value="自适应")
        candle_count_combo = ttk.Combobox(frame_market, textvariable=self.candle_count_var, 
                                         values=["自适应", "全部", "50", "100", "200", "300", "500"], width=6)
        candle_count_combo.pack(side=tk.LEFT, padx=1)
        candle_count_combo.bind("<<ComboboxSelected>>", self.on_candle_count_change)
        self._add_tooltip(candle_count_combo, "选择显示的K线数量，自适应会根据窗口大小自动调整")

        self.btn_refresh = ttk.Button(frame_market, text="📈刷新", command=self.refresh_kline, width=4)
        self.btn_refresh.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.btn_refresh, "刷新K线图")

        ttk.Separator(frame_market, orient=tk.VERTICAL).pack(side=tk.LEFT, padx=2, fill=tk.Y)
        
        self.btn_gainers = ttk.Button(frame_market, text="📈涨幅榜", command=self.show_gainers, width=5)
        self.btn_gainers.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.btn_gainers, "显示涨幅榜")
        
        self.btn_losers = ttk.Button(frame_market, text="📉跌幅榜", command=self.show_losers, width=5)
        self.btn_losers.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.btn_losers, "显示跌幅榜")
        
        self.btn_rank_lock = ttk.Button(frame_market, text="🔓解锁", command=self.toggle_rank_lock, width=4)
        self.btn_rank_lock.pack(side=tk.LEFT, padx=1)
        self._add_tooltip(self.btn_rank_lock, "锁定/解锁涨幅/跌幅榜，锁定后可在左侧点击查看不同周期")
        
        ttk.Separator(frame_market, orient=tk.VERTICAL).pack(side=tk.LEFT, padx=2, fill=tk.Y)
        ttk.Label(frame_market, text="名称:").pack(side=tk.LEFT, padx=1)
        self.stock_name_var = tk.StringVar(value="")
        ttk.Label(frame_market, textvariable=self.stock_name_var, font=("Microsoft YaHei", 9, "bold")).pack(side=tk.LEFT, padx=1)
        
        ttk.Label(frame_market, text="涨跌:").pack(side=tk.LEFT, padx=1)
        self.change_percent_var = tk.StringVar(value="0.00%")
        self.change_percent_label = ttk.Label(frame_market, textvariable=self.change_percent_var, font=("Microsoft YaHei", 9, "bold"))
        self.change_percent_label.pack(side=tk.LEFT, padx=1)
        
        ttk.Label(frame_market, text="最新:").pack(side=tk.LEFT, padx=1)
        self.latest_price_var = tk.StringVar(value="--")
        self.latest_price_label = ttk.Label(frame_market, textvariable=self.latest_price_var, font=("Microsoft YaHei", 9, "bold"))
        self.latest_price_label.pack(side=tk.LEFT, padx=1)
        
        ttk.Label(frame_market, text="PE:").pack(side=tk.LEFT, padx=1)
        self.pe_var = tk.StringVar(value="0.00")
        ttk.Label(frame_market, textvariable=self.pe_var).pack(side=tk.LEFT, padx=1)
        
        ttk.Label(frame_market, text="PB:").pack(side=tk.LEFT, padx=1)
        self.pb_var = tk.StringVar(value="0.00")
        ttk.Label(frame_market, textvariable=self.pb_var).pack(side=tk.LEFT, padx=1)
        
        ttk.Label(frame_market, text="市值(亿):").pack(side=tk.LEFT, padx=1)
        self.market_value_var = tk.StringVar(value="0.00")
        ttk.Label(frame_market, textvariable=self.market_value_var).pack(side=tk.LEFT, padx=1)
        
        ttk.Label(frame_market, text="行业:").pack(side=tk.LEFT, padx=1)
        self.industry_var = tk.StringVar(value="")
        ttk.Label(frame_market, textvariable=self.industry_var).pack(side=tk.LEFT, padx=1)
        
        ttk.Label(frame_market, text="每股利润:").pack(side=tk.LEFT, padx=1)
        self.dividend_var = tk.StringVar(value="0.00")
        ttk.Label(frame_market, textvariable=self.dividend_var).pack(side=tk.LEFT, padx=1)

        self.label_info = ttk.Label(frame_market, text="")
        self.label_info.pack(side=tk.LEFT, padx=5)
        self.label_tip = ttk.Label(frame_market, text="请选择股票", foreground="blue")
        self.label_tip.pack(side=tk.RIGHT, padx=5)

        # 主布局
        main_paned = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        main_paned.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        frame_left = ttk.LabelFrame(main_paned, text="代码")
        main_paned.add(frame_left, weight=6)
        
        stats_frame = ttk.Frame(frame_left)
        stats_frame.pack(fill=tk.X, padx=1, pady=1)
        
        self.label_cnt = ttk.Label(stats_frame, text="0只")
        self.label_cnt.pack(side=tk.LEFT, padx=2)
        self.position_label = ttk.Label(stats_frame, text="")
        self.position_label.pack(side=tk.RIGHT, padx=2)
        
        scroll_l = ttk.Scrollbar(frame_left)
        scroll_l.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.list_codes = tk.Listbox(frame_left, font=("Consolas", 9), yscrollcommand=scroll_l.set, width=8, height=35, selectmode=tk.SINGLE)
        self.list_codes.pack(fill=tk.BOTH, expand=True, padx=1, pady=1)
        scroll_l.config(command=self.list_codes.yview)
        self.list_codes.bind("<<ListboxSelect>>", self.on_code_click)

        self.frame_right = ttk.LabelFrame(main_paned, text="K线图")
        main_paned.add(self.frame_right, weight=94)
        
        self._create_kline_view()

    def _add_tooltip(self, widget, text):
        """为控件添加悬停提示"""
        def show_tooltip(event):
            x, y, _, _ = widget.bbox("insert")
            x += widget.winfo_rootx() + 25
            y += widget.winfo_rooty() + 25
            
            # 创建提示窗口
            self.tooltip = tk.Toplevel(widget)
            self.tooltip.wm_overrideredirect(True)
            self.tooltip.wm_geometry(f"+{x}+{y}")
            
            label = ttk.Label(self.tooltip, text=text, background="#ffffe0", 
                            relief="solid", borderwidth=1, font=("Microsoft YaHei", 9))
            label.pack()
        
        def hide_tooltip(event):
            if hasattr(self, 'tooltip'):
                self.tooltip.destroy()
        
        widget.bind("<Enter>", show_tooltip)
        widget.bind("<Leave>", hide_tooltip)

    def _create_data_view(self):
        """创建数据表视图"""
        self.data_frame = ttk.Frame(self.frame_right)
        
        data_toolbar = ttk.Frame(self.data_frame)
        data_toolbar.pack(fill=tk.X, padx=2, pady=2)
        
        ttk.Label(data_toolbar, text="数据明细 - ").pack(side=tk.LEFT, padx=2)
        self.data_info = ttk.Label(data_toolbar, text="")
        self.data_info.pack(side=tk.LEFT, padx=2)
        
        frame_table = ttk.Frame(self.data_frame)
        frame_table.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)
        
        scroll_x = ttk.Scrollbar(frame_table, orient=tk.HORIZONTAL)
        scroll_y = ttk.Scrollbar(frame_table, orient=tk.VERTICAL)
        
        self.data_tree = ttk.Treeview(frame_table, show="headings", xscrollcommand=scroll_x.set, yscrollcommand=scroll_y.set, height=25)
        
        scroll_x.config(command=self.data_tree.xview)
        scroll_y.config(command=self.data_tree.yview)
        
        scroll_x.pack(side=tk.BOTTOM, fill=tk.X)
        scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        self.data_tree.pack(fill=tk.BOTH, expand=True)

    def on_indicator_change(self, event=None):
        """指标选择改变时触发"""
        self.current_indicator = self.indicator_var.get()
        if self.kline_data is not None and self.view_mode == "kline":
            self.draw_kline()

    def on_candle_count_change(self, event=None):
        """K线数量改变时触发"""
        self.refresh_kline()

    # ========== 平移功能 ==========
    def pan_left(self):
        """向左平移K线"""
        if self.kline_data is None:
            return
        if self.candle_offset < self.total_candles - self.display_candles:
            self.candle_offset += 1
            self._update_pan_buttons()
            self.refresh_kline()
    
    def pan_right(self):
        """向右平移K线"""
        if self.kline_data is None:
            return
        if self.candle_offset > 0:
            self.candle_offset -= 1
            self._update_pan_buttons()
            self.refresh_kline()
    
    def _update_pan_buttons(self):
        """更新平移按钮状态"""
        if self.kline_data is None:
            self.btn_pan_left.state(['disabled'])
            self.btn_pan_right.state(['disabled'])
            return
        
        max_offset = self.total_candles - self.display_candles
        self.btn_pan_left.state(['!disabled'])
        self.btn_pan_right.state(['!disabled'])
        
        if self.candle_offset >= max_offset:
            self.btn_pan_left.state(['disabled'])
        if self.candle_offset <= 0:
            self.btn_pan_right.state(['disabled'])

    # ========== 缩放功能 ==========
    def zoom_in(self):
        """放大K线（减少显示数量）"""
        if self.kline_data is None:
            return
        
        # 如果在自适应模式下，先切换到固定数量模式
        if self.candle_count_var.get() == "自适应":
            self.candle_count_var.set(str(self.display_candles))
        
        # 减少显示数量
        self.display_candles = max(self.min_candles, self.display_candles - 50)
        self.candle_count_var.set(str(self.display_candles))
        
        # 调整偏移量
        self.candle_offset = min(self.candle_offset, self.total_candles - self.display_candles)
        if self.candle_offset < 0:
            self.candle_offset = 0
        
        self._update_pan_buttons()
        self.refresh_kline()
    
    def zoom_out(self):
        """缩小K线（增加显示数量）"""
        if self.kline_data is None:
            return
        
        # 如果在自适应模式下，先切换到固定数量模式
        if self.candle_count_var.get() == "自适应":
            self.candle_count_var.set(str(self.display_candles))
        
        # 增加显示数量
        self.display_candles = min(self.total_candles, self.display_candles + 50)
        self.candle_count_var.set(str(self.display_candles))
        
        # 调整偏移量
        self.candle_offset = min(self.candle_offset, self.total_candles - self.display_candles)
        if self.candle_offset < 0:
            self.candle_offset = 0
        
        self._update_pan_buttons()
        self.refresh_kline()

    # ========== 十字线功能 ==========
    def on_mouse_move(self, event):
        if self.kline_data is None or self.view_mode != "kline":
            return
        self._remove_crosshair()
        self.crosshair_x = event.x
        self.crosshair_y = event.y
        canvas_width = self.kline_canvas.winfo_width()
        canvas_height = self.kline_canvas.winfo_height()
        if canvas_width <= 10 or canvas_height <= 10:
            return
        color = 'white' if self.dark_mode else 'gray'
        line1 = self.kline_canvas.create_line(event.x, 0, event.x, canvas_height, fill=color, width=1, dash=(3, 3))
        line2 = self.kline_canvas.create_line(0, event.y, canvas_width, event.y, fill=color, width=1, dash=(3, 3))
        self.crosshair_lines = [line1, line2]
        self._find_nearest_kline(event.x, event.y)
    
    def on_mouse_leave(self, event):
        self._remove_crosshair()
        if self.crosshair_text:
            for item_id in self.crosshair_text:
                self.kline_canvas.delete(item_id)
            self.crosshair_text = None
    
    def on_canvas_click(self, event):
        pass
    
    def _remove_crosshair(self):
        for line_id in self.crosshair_lines:
            self.kline_canvas.delete(line_id)
        self.crosshair_lines = []
    
    def _find_nearest_kline(self, mouse_x, mouse_y):
        if not self.kline_positions:
            return
        nearest_kline = None
        min_distance = float('inf')
        for pos in self.kline_positions:
            distance = abs(mouse_x - pos['x'])
            if distance < min_distance:
                min_distance = distance
                nearest_kline = pos
        if nearest_kline and min_distance < 30:
            self._show_kline_info(nearest_kline, mouse_y)
    
    def _show_kline_info(self, kline_info, mouse_y):
        if self.crosshair_text:
            for item_id in self.crosshair_text:
                self.kline_canvas.delete(item_id)
        
        canvas_height = self.kline_canvas.winfo_height()
        chart_top = 60
        chart_bottom = canvas_height - 30
        main_chart_ratio = 0.55
        main_chart_height = (chart_bottom - chart_top) * main_chart_ratio
        main_chart_bottom = chart_top + main_chart_height
        price_range = kline_info['max_price'] - kline_info['min_price']
        if mouse_y < chart_top or mouse_y > main_chart_bottom:
            current_price = None
        else:
            price_per_pixel = main_chart_height / price_range if price_range > 0 else 1
            current_price = kline_info['max_price'] - (mouse_y - chart_top) / price_per_pixel
        
        text_color = 'white' if self.dark_mode else 'black'
        
        change_pct = None
        change_color = text_color
        if 'change_pct' in kline_info and kline_info['change_pct'] is not None:
            change_pct = kline_info['change_pct']
            change_color = 'red' if change_pct > 0 else ('green' if change_pct < 0 else text_color)
        
        info_text = [
            f"日期: {kline_info['date']}",
            f"开盘: {kline_info['open']:.2f}",
            f"最高: {kline_info['high']:.2f}",
            f"最低: {kline_info['low']:.2f}",
            f"收盘: {kline_info['close']:.2f}",
            f"成交量: {kline_info['volume']:.0f}"
        ]
        
        if change_pct is not None:
            info_text.append(f"涨跌: {change_pct:+.2f}%")
        
        if current_price is not None:
            info_text.append(f"当前价: {current_price:.2f}")
        
        x_pos = 70
        y_pos = 80
        
        bg_rect = self.kline_canvas.create_rectangle(
            x_pos - 10, y_pos - 10,
            x_pos + 180, y_pos + 20 + len(info_text) * 18,
            fill='#333333' if self.dark_mode else '#f0f0f0',
            outline=text_color,
            width=1
        )
        
        text_items = [bg_rect]
        for i, line in enumerate(info_text):
            if line.startswith("涨跌:"):
                fill_color = change_color
            else:
                fill_color = text_color
            
            text = self.kline_canvas.create_text(
                x_pos, y_pos + i * 18,
                text=line,
                anchor=tk.NW,
                fill=fill_color,
                font=("Consolas", 9)
            )
            text_items.append(text)
        
        self.crosshair_text = text_items

    # ========== 自选股管理 ==========
    def _load_zixuangu_codes(self):
        """加载原自选股列表"""
        if not os.path.exists(self.zixuangu_csv_path):
            self._create_sample_zixuangu_csv()
            return
        
        self.zixuangu_codes = read_codes_from_csv(self.zixuangu_csv_path)
        
        if not self.zixuangu_codes:
            self._create_sample_zixuangu_csv()
    
    def _create_sample_zixuangu_csv(self):
        """创建示例原自选股CSV文件"""
        try:
            ensure_data_dir(self.stock_data_dir)
            sample_codes = ["600000", "600001", "600002", "000001", "000002", "300001"]
            with open(self.zixuangu_csv_path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                for code in sample_codes:
                    writer.writerow([code])
            self.zixuangu_codes = sample_codes
            print(f"已创建示例原自选股文件: {self.zixuangu_csv_path}")
        except Exception as e:
            print(f"创建示例原自选股文件失败: {e}")
            self.zixuangu_codes = []
    
    def toggle_zixuangu_mode(self):
        if self.is_original_zixuan_mode:
            self._exit_original_zixuan_mode()
        else:
            self._enter_original_zixuan_mode()
    
    def _enter_original_zixuan_mode(self):
        if self.is_zixuan_mode:
            self._exit_zixuan_mode()
        self.rank_mode = None
        self.rank_locked = False
        self.is_original_zixuan_mode = True
        if self.current_period != "zixuan":
            self.last_normal_period = self.current_period
        self._load_zixuangu_codes()
        if not self.zixuangu_codes:
            self.show_message("提示", "原自选股列表为空", "warning")
            self.is_original_zixuan_mode = False
            return False
        self._refresh_original_zixuan_list()
        self.zixuangu_button.config(text="自选✓")
        if self.current_display_code and self.current_display_code in self.zixuangu_codes:
            self.locked_code = self.current_display_code
            self.current_code_index = self.zixuangu_codes.index(self.current_display_code)
        else:
            self.locked_code = self.zixuangu_codes[0]
            self.current_code_index = 0
        self._load_stock_in_current_period(self.locked_code)
        self.highlight_current_code()
        self.label_tip.config(text=f"🔒原自选 {self.current_code_index + 1}/{len(self.zixuangu_codes)}: {self.locked_code}")
        return True
    
    def _exit_original_zixuan_mode(self):
        self.is_original_zixuan_mode = False
        self.locked_code = None
        self.zixuangu_button.config(text="自选")
        self.current_period = self.last_normal_period
        if self.con is None:
            self._switch_normal_period(self.last_normal_period)
        else:
            if not self.all_codes.is_empty():
                self.filter_market(self.current_market)
            else:
                self._load_table_data(self.current_table)
    
    def _refresh_original_zixuan_list(self):
        self.list_codes.delete(0, tk.END)
        for code in self.zixuangu_codes:
            self.list_codes.insert(tk.END, f"🔒{code}")
        self.label_cnt.config(text=f"🔒{len(self.zixuangu_codes)}只")
        self.position_label.config(text="")

    def _load_zixuan_codes(self):
        """加载自选股列表"""
        if not os.path.exists(self.zixuan_csv_path):
            self._create_sample_zixuan_csv()
            return
        
        self.zixuan_codes = read_codes_from_csv(self.zixuan_csv_path)
        self._update_del_button_state()
        
        if not self.zixuan_codes:
            self._create_sample_zixuan_csv()
    
    def _create_sample_zixuan_csv(self):
        """创建示例自选股CSV文件"""
        try:
            ensure_data_dir(self.stock_data_dir)
            sample_codes = ["600000", "600001", "600002"]
            with open(self.zixuan_csv_path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                for code in sample_codes:
                    writer.writerow([code])
            self.zixuan_codes = sample_codes
            print(f"已创建示例自选股文件: {self.zixuan_csv_path}")
        except Exception as e:
            print(f"创建示例自选股文件失败: {e}")
            self.zixuan_codes = []
    
    def _save_zixuan_codes(self):
        """保存自选股列表"""
        try:
            with open(self.zixuan_csv_path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                for code in self.zixuan_codes:
                    writer.writerow([code])
        except Exception as e:
            self.show_message("错误", f"保存自选股失败: {str(e)}", "error")
    
    def add_to_zixuan(self):
        """添加当前股票到自选股"""
        if not self.current_display_code:
            self.show_message("提示", "没有当前显示的股票", "warning")
            return
        
        code = self.current_display_code
        if code in self.zixuan_codes:
            self.show_message("提示", f"股票 {code} 已在自选股列表中", "info")
            return
        
        self.zixuan_codes.append(code)
        self._save_zixuan_codes()
        
        if self.is_zixuan_mode:
            self._refresh_zixuan_list()
            if code in self.zixuan_codes:
                self.current_code_index = self.zixuan_codes.index(code)
                self.highlight_current_code()
        
        self._update_del_button_state()
        self.show_message("成功", f"已添加股票 {code} 到自选股", "success")
    
    def remove_from_zixuan(self):
        """从自选股中删除当前股票"""
        if not self.current_display_code:
            self.show_message("提示", "没有当前显示的股票", "warning")
            return
        
        code = self.current_display_code
        if code not in self.zixuan_codes:
            self.show_message("提示", f"股票 {code} 不在自选股列表中", "info")
            return
        
        self.zixuan_codes.remove(code)
        self._save_zixuan_codes()
        
        if self.is_zixuan_mode:
            if len(self.zixuan_codes) > 0:
                if self.current_code_index >= len(self.zixuan_codes):
                    self.current_code_index = len(self.zixuan_codes) - 1
                self._refresh_zixuan_list()
                if self.current_code_index >= 0:
                    new_code = self.zixuan_codes[self.current_code_index]
                    self._load_stock_in_current_period(new_code)
            else:
                self._exit_zixuan_mode()
        
        self._update_del_button_state()
        self.show_message("成功", f"已从自选股中删除股票 {code}", "success")
    
    def _update_del_button_state(self):
        """更新删自选按钮状态"""
        if self.current_display_code and self.current_display_code in self.zixuan_codes:
            self.btn_del_zixuan.state(['!disabled'])
        else:
            self.btn_del_zixuan.state(['disabled'])
    
    def toggle_zixuan_mode(self):
        """切换自选股模式"""
        if self.is_zixuan_mode:
            self._exit_zixuan_mode()
        else:
            self._enter_zixuan_mode()
    
    def _enter_zixuan_mode(self):
        """进入自选股模式"""
        if self.is_original_zixuan_mode:
            self._exit_original_zixuan_mode()
        self.rank_mode = None
        self.rank_locked = False
        self.is_zixuan_mode = True
        self.is_zixuan_locked = True
        if self.current_period != "zixuan":
            self.last_normal_period = self.current_period
        self._load_zixuan_codes()
        if not self.zixuan_codes:
            self.show_message("提示", "自选股列表为空", "warning")
            self.is_zixuan_mode = False
            self.is_zixuan_locked = False
            return False
        self._refresh_zixuan_list()
        self.zixuan_button.config(text="自选股✓")
        if self.current_display_code and self.current_display_code in self.zixuan_codes:
            self.locked_code = self.current_display_code
            self.current_code_index = self.zixuan_codes.index(self.current_display_code)
        else:
            self.locked_code = self.zixuan_codes[0]
            self.current_code_index = 0
        self._load_stock_in_current_period(self.locked_code)
        self.highlight_current_code()
        self.label_tip.config(text=f"⭐自选股 {self.current_code_index + 1}/{len(self.zixuan_codes)}: {self.locked_code}")
        return True
    
    def _exit_zixuan_mode(self):
        """退出自选股模式"""
        self.is_zixuan_mode = False
        self.is_zixuan_locked = False
        self.locked_code = None
        self.zixuan_button.config(text="自选股")
        self.current_period = self.last_normal_period
        if self.con is None:
            self._switch_normal_period(self.last_normal_period)
        else:
            if not self.all_codes.is_empty():
                self.filter_market(self.current_market)
            else:
                self._load_table_data(self.current_table)
    
    def _refresh_zixuan_list(self):
        """刷新自选股列表显示"""
        self.list_codes.delete(0, tk.END)
        for code in self.zixuan_codes:
            self.list_codes.insert(tk.END, f"⭐{code}")
        self.label_cnt.config(text=f"⭐{len(self.zixuan_codes)}只")
        self.position_label.config(text="")
    
    def _load_stock_in_current_period(self, code):
        """在当前周期的数据库中加载指定股票"""
        if not self.con:
            self.show_message("提示", "请先打开周期数据库", "warning")
            return False
        if self.code_col is None or self.code_col == "None":
            self.show_message("提示", "未找到代码列，请重新选择表", "warning")
            return False
        
        # 规范化代码
        norm_code = normalize_stock_code(code)
        
        try:
            sql = f"SELECT COUNT(*) as cnt FROM {self.current_table} WHERE {self.code_col} = ?"
            result = self.con.execute(sql, [norm_code]).fetchone()
            
            if result and result[0] > 0:
                self.load_one_stock(norm_code)
                return True
            else:
                self.show_message("提示", f"在当前周期数据库中未找到股票: {code}", "warning")
                return False
        except Exception as e:
            self.show_message("错误", f"加载股票失败: {str(e)}", "error")
            return False

    # ========== 周期切换功能 ==========
    def switch_period(self, period_key):
        """切换周期 - 使用动态路径"""
        if period_key == self.current_period:
            return
        
        config = self.period_config[period_key]
        
        # 查找数据库文件（支持.duckdb和.parquet）
        db_path = self._find_db_file(config["db_file"])
        
        if not db_path or not os.path.exists(db_path):
            self.show_message("提示", f"数据库文件不存在：{config['db_file']}.duckdb/.parquet", "warning")
            return
        
        try:
            if self.con:
                self.con.close()
            
            # 以只读模式连接数据库
            self.con = self._connect_db_readonly(db_path)
            if not self.con:
                self.show_message("错误", f"无法连接数据库: {db_path}", "error")
                return
            
            self.db_path = db_path
            db_type = "Parquet" if db_path.endswith('.parquet') else "DuckDB"
            self.label_db.config(text=f"📁{config['name']}({db_type})")
            
            # 获取所有表
            all_tables = [t[0] for t in self.con.execute("SHOW TABLES").fetchall()]
            tables_to_skip = config["skip_tables"]
            available_tables = [t for t in all_tables if not any(skip in t.lower() for skip in tables_to_skip)]
            
            if not available_tables:
                self.show_message("提示", f"在{config['name']}数据库中没有找到可用表", "warning")
                return
            
            target_table = config["target_table"]
            if target_table in available_tables:
                selected_table = target_table
            else:
                selected_table = available_tables[0]
                if period_key != "day" or not self.initialized:
                    self.show_message("提示", f"未找到目标表{target_table}，使用{selected_table}", "info")
            
            self.var_table.set(selected_table)
            self.cb_table["values"] = available_tables
            self.current_table = selected_table
            self.current_period = period_key
            
            self._detect_table_columns()
            
            if self.is_zixuan_mode or self.is_original_zixuan_mode:
                target_code = self.locked_code
            elif self.rank_locked and self.rank_locked_codes and self.current_display_code in self.rank_locked_codes:
                target_code = self.current_display_code
            else:
                target_code = self.base_code if self.base_code else (self.all_codes[0] if len(self.all_codes) > 0 else None)
            
            if target_code:
                sql = f"SELECT COUNT(*) as cnt FROM {self.current_table} WHERE {self.code_col} = ?"
                result = self.con.execute(sql, [target_code]).fetchone()
                
                if result and result[0] > 0:
                    self.load_one_stock(target_code)
                else:
                    if len(self.all_codes) > 0:
                        self.load_one_stock(self.all_codes[0])
                        self.show_message("提示", f"股票 {target_code} 在{config['name']}中不存在，已切换到{self.all_codes[0]}", "info")
            else:
                if len(self.all_codes) > 0:
                    self.load_one_stock(self.all_codes[0])
            
            for key, btn in self.period_buttons.items():
                if key == period_key:
                    btn.state(['disabled'])
                else:
                    btn.state(['!disabled'])
            
        except Exception as e:
            self.show_message("错误", f"切换周期失败: {str(e)}", "error")
    
    def _switch_normal_period(self, period_key):
        """正常切换周期"""
        config = self.period_config[period_key]
        db_path = self._find_db_file(config["db_file"])
        
        if not db_path or not os.path.exists(db_path):
            self.show_message("提示", f"数据库文件不存在：{config['db_file']}.duckdb/.parquet", "warning")
            return
        
        try:
            if self.con:
                self.con.close()
            
            self.con = self._connect_db_readonly(db_path)
            if not self.con:
                self.show_message("错误", f"无法连接数据库: {db_path}", "error")
                return
            
            self.db_path = db_path
            db_type = "Parquet" if db_path.endswith('.parquet') else "DuckDB"
            self.label_db.config(text=f"📁{config['name']}({db_type})")
            
            all_tables = [t[0] for t in self.con.execute("SHOW TABLES").fetchall()]
            tables_to_skip = config["skip_tables"]
            available_tables = [t for t in all_tables if not any(skip in t.lower() for skip in tables_to_skip)]
            
            if not available_tables:
                self.show_message("提示", f"在{config['name']}数据库中没有找到可用表", "warning")
                return
            
            target_table = config["target_table"]
            if target_table in available_tables:
                selected_table = target_table
            else:
                selected_table = available_tables[0]
                if period_key != "day" or not self.initialized:
                    self.show_message("提示", f"未找到目标表{target_table}，使用{selected_table}", "info")
            
            self.var_table.set(selected_table)
            self.cb_table["values"] = available_tables
            self.current_table = selected_table
            self.current_period = period_key
            self.last_normal_period = period_key
            
            self._load_table_data(selected_table)
            
            for key, btn in self.period_buttons.items():
                if key == period_key:
                    btn.state(['disabled'])
                else:
                    btn.state(['!disabled'])
            
        except Exception as e:
            self.show_message("错误", f"切换周期失败: {str(e)}", "error")
    
    def _detect_table_columns(self):
        """检测当前表的字段"""
        try:
            result = self.con.execute(f"SELECT * FROM {self.current_table} LIMIT 1")
            row = result.fetchone()
            if row is None:
                print("警告: 表中没有数据")
                return
            columns = [desc[0] for desc in result.description]
            cols_lower = [c.lower() for c in columns]
            code_idx = next((i for i, c in enumerate(cols_lower) if "code" in c), None)
            if code_idx is not None:
                self.code_col = columns[code_idx]
                print(f"找到代码列: {self.code_col}")
            else:
                print("未找到代码列")
                self.code_col = None
            date_idx = None
            for i, c in enumerate(cols_lower):
                if c in ['date', 'date_time', 'time', 'datetime']:
                    date_idx = i
                    break
            if date_idx is not None:
                self.date_col = columns[date_idx]
                print(f"找到日期列: {self.date_col}")
            else:
                self.date_col = None
                print("未找到日期列")
        except Exception as e:
            print(f"检测字段失败: {e}")
            self.code_col = None
            self.date_col = None

    # ========== 上下股票切换 ==========
    def prev_stock(self):
        """上一个股票"""
        if self.is_zixuan_mode:
            if not self.zixuan_codes:
                return
            if self.current_code_index > 0:
                self.current_code_index -= 1
                self.locked_code = self.zixuan_codes[self.current_code_index]
                success = self._load_stock_in_current_period(self.locked_code)
                if success:
                    self.highlight_current_code()
                    self.label_tip.config(text=f"⭐自选股 {self.current_code_index + 1}/{len(self.zixuan_codes)}: {self.locked_code}")
                    self._update_del_button_state()
        elif self.is_original_zixuan_mode:
            if not self.zixuangu_codes:
                return
            if self.current_code_index > 0:
                self.current_code_index -= 1
                self.locked_code = self.zixuangu_codes[self.current_code_index]
                success = self._load_stock_in_current_period(self.locked_code)
                if success:
                    self.highlight_current_code()
                    self.label_tip.config(text=f"🔒原自选 {self.current_code_index + 1}/{len(self.zixuangu_codes)}: {self.locked_code}")
        elif self.rank_mode:
            if self.current_code_index > 0:
                self.current_code_index -= 1
                item_text = self.list_codes.get(self.current_code_index)
                code = item_text.split()[0]
                self.load_one_stock(code)
                self.highlight_current_code()
                self._update_del_button_state()
        else:
            if self.filtered_codes.is_empty():
                return
            if self.current_code_index > 0:
                self.current_code_index -= 1
                code = self.filtered_codes[self.current_code_index]
                self.load_one_stock(code)
                self.highlight_current_code()
                if self.current_period == self.base_period:
                    self.base_code = code
                self._update_del_button_state()
    
    def next_stock(self):
        """下一个股票"""
        if self.is_zixuan_mode:
            if not self.zixuan_codes:
                return
            if self.current_code_index < len(self.zixuan_codes) - 1:
                self.current_code_index += 1
                self.locked_code = self.zixuan_codes[self.current_code_index]
                success = self._load_stock_in_current_period(self.locked_code)
                if success:
                    self.highlight_current_code()
                    self.label_tip.config(text=f"⭐自选股 {self.current_code_index + 1}/{len(self.zixuan_codes)}: {self.locked_code}")
                    self._update_del_button_state()
        elif self.is_original_zixuan_mode:
            if not self.zixuangu_codes:
                return
            if self.current_code_index < len(self.zixuangu_codes) - 1:
                self.current_code_index += 1
                self.locked_code = self.zixuangu_codes[self.current_code_index]
                success = self._load_stock_in_current_period(self.locked_code)
                if success:
                    self.highlight_current_code()
                    self.label_tip.config(text=f"🔒原自选 {self.current_code_index + 1}/{len(self.zixuangu_codes)}: {self.locked_code}")
        elif self.rank_mode:
            if self.current_code_index < self.list_codes.size() - 1:
                self.current_code_index += 1
                item_text = self.list_codes.get(self.current_code_index)
                code = item_text.split()[0]
                self.load_one_stock(code)
                self.highlight_current_code()
                self._update_del_button_state()
        else:
            if self.filtered_codes.is_empty():
                return
            if self.current_code_index < len(self.filtered_codes) - 1:
                self.current_code_index += 1
                code = self.filtered_codes[self.current_code_index]
                self.load_one_stock(code)
                self.highlight_current_code()
                if self.current_period == self.base_period:
                    self.base_code = code
                self._update_del_button_state()
    
    def highlight_current_code(self):
        """高亮当前选中的股票代码"""
        if self.current_code_index >= 0:
            self.list_codes.selection_clear(0, tk.END)
            self.list_codes.selection_set(self.current_code_index)
            self.list_codes.see(self.current_code_index)
            if self.is_zixuan_mode:
                self.position_label.config(text=f"{self.current_code_index + 1}/{len(self.zixuan_codes)}")
            elif self.is_original_zixuan_mode:
                self.position_label.config(text=f"{self.current_code_index + 1}/{len(self.zixuangu_codes)}")
            elif self.rank_mode:
                self.position_label.config(text=f"{self.current_code_index + 1}/{self.list_codes.size()}")
            else:
                self.position_label.config(text=f"{self.current_code_index + 1}/{len(self.filtered_codes)}")

    # ========== 视图切换 ==========
    def switch_to_kline_view(self):
        """切换到K线图视图"""
        if self.view_mode == "kline":
            return
        self.view_mode = "kline"
        if hasattr(self, 'data_frame'):
            self.data_frame.pack_forget()
        self._create_kline_view()
        self.btn_kline_view.state(['disabled'])
        self.btn_data_view.state(['!disabled'])
        if self.current_display_code:
            self.refresh_kline()

    def switch_to_data_view(self):
        """切换到数据表视图"""
        self.view_mode = "data"
        for widget in self.frame_right.winfo_children():
            widget.destroy()
        self._create_data_view()
        self.data_frame.pack(fill=tk.BOTH, expand=True)
        self.btn_data_view.state(['disabled'])
        self.btn_kline_view.state(['!disabled'])
        if self.current_display_code:
            self.refresh_data_table()

    def refresh_data_table(self):
        """刷新数据表"""
        if not self.current_display_code:
            return
        try:
            order_field = self.date_col if self.date_col else self.code_col
            sql = f"""
                SELECT * FROM {self.current_table} 
                WHERE {self.code_col} = ? 
                ORDER BY {order_field}
            """
            result = self.con.execute(sql, [self.current_display_code]).fetchall()
            columns = [desc[0] for desc in self.con.execute(sql, [self.current_display_code]).description]
            df = pl.DataFrame(result, schema=columns, orient="row")
            
            self.data_tree.delete(*self.data_tree.get_children())
            cols = df.columns
            self.data_tree["columns"] = cols
            
            for col in cols:
                self.data_tree.heading(col, text=col)
                if df[col].dtype in [pl.Int64, pl.Float64]:
                    width = 100
                elif "date" in col.lower() or "time" in col.lower():
                    width = 120
                else:
                    width = 120
                self.data_tree.column(col, width=width)
            
            for row in df.tail(100).iter_rows():
                self.data_tree.insert("", tk.END, values=row)
            
            period_name = self.period_config[self.current_period]["name"]
            mode_prefix = "⭐" if self.is_zixuan_mode else ("🔒" if self.is_original_zixuan_mode else "")
            self.data_info.config(text=f"{mode_prefix}{period_name}-{self.current_display_code}{len(df)}条")
        except Exception as e:
            self.show_message("错误", f"加载数据失败: {str(e)}", "error")

    # ========== K线图绘制 ==========
    def refresh_kline(self):
        """刷新K线图"""
        if not self.current_display_code or self.view_mode != "kline":
            return
        try:
            df = self._fetch_stock_data()
            if df.is_empty():
                self.show_message("提示", "无数据", "warning")
                return
            
            required_cols = {"open", "high", "low", "close", "volume"}
            if not required_cols.issubset(set(df.columns)):
                missing = required_cols - set(df.columns)
                self.show_message("错误", f"数据缺少必要字段：{missing}", "error")
                return
            
            # 确保所有数值列为浮点类型，避免类型错误
            for col in ["open", "high", "low", "close", "volume"]:
                if col in df.columns:
                    df = df.with_columns(pl.col(col).cast(pl.Float64))
            
            change_pct_list = [None] * len(df)
            for i in range(1, len(df)):
                if df["close"][i-1] and df["close"][i-1] > 0:
                    change_pct = (df["close"][i] - df["close"][i-1]) / df["close"][i-1] * 100
                    change_pct_list[i] = change_pct
            
            df = df.with_columns(pl.Series("change_pct", change_pct_list).cast(pl.Float64))
            
            # 计算各种指标
            df = self._calculate_kdj(df)
            df = self._calculate_macd(df)
            df = self._calculate_bollinger(df)
            df = self._calculate_rsi(df)
            df = self._calculate_wr(df)
            df = self._calculate_obv(df)
            df = self._calculate_ma(df)
            
            self.kline_data = df
            self.total_candles = len(df)
            
            # 自适应K线数量
            if self.candle_count_var.get() == "自适应":
                self.display_candles = self._calculate_adaptive_candle_count()
            elif self.candle_count_var.get() == "全部":
                self.display_candles = self.total_candles
            else:
                try:
                    self.display_candles = int(self.candle_count_var.get())
                    self.display_candles = min(self.display_candles, self.total_candles)
                except:
                    self.display_candles = min(300, self.total_candles)
            
            # 确保偏移量有效
            self.candle_offset = min(self.candle_offset, max(0, self.total_candles - self.display_candles))
            if self.candle_offset < 0:
                self.candle_offset = 0
            
            self._update_pan_buttons()
            self.draw_kline()
            self.update_stock_info_display()
        except Exception as e:
            import traceback
            traceback.print_exc()
            self.show_message("错误", f"绘图失败: {str(e)}", "error")
    
    def _calculate_adaptive_candle_count(self):
        """计算自适应的K线数量"""
        canvas_width = self.kline_canvas.winfo_width()
        if canvas_width <= 10:
            return self.min_candles
        
        padding = 60
        available_width = canvas_width - 2 * padding
        
        # 根据K线数量自动调整显示数量
        if self.total_candles <= 50:
            # 数量很少时，全部显示
            return self.total_candles
        elif self.total_candles <= 100:
            # 数量较少时，显示大部分
            return min(self.total_candles, 80)
        elif self.total_candles <= 200:
            # 数量适中时，显示150根
            return 150
        elif self.total_candles <= 300:
            # 数量较多时，显示200根
            return 200
        else:
            # 数量很多时，显示300根
            return 300
    
    def _calculate_ma(self, df):
        """计算移动平均线"""
        ma_periods = [5, 10, 30, 60, 125, 250]
        close_prices = df["close"].to_list()
        for period in ma_periods:
            ma_values = []
            for i in range(len(close_prices)):
                if i < period - 1:
                    ma_values.append(None)
                else:
                    period_prices = close_prices[i-period+1:i+1]
                    valid_prices = [p for p in period_prices if p is not None]
                    if len(valid_prices) > 0:
                        ma_values.append(float(sum(valid_prices) / len(valid_prices)))
                    else:
                        ma_values.append(None)
            df = df.with_columns(pl.Series(f"ma{period}", ma_values).cast(pl.Float64))
        return df
    
    def _calculate_bollinger(self, df, period=20, k=2):
        """计算布林线"""
        if len(df) < period:
            boll_upper = [None] * len(df)
            boll_middle = [None] * len(df)
            boll_lower = [None] * len(df)
        else:
            close_prices = df["close"].to_list()
            boll_middle = []
            boll_upper = []
            boll_lower = []
            
            for i in range(len(df)):
                if i < period - 1:
                    boll_middle.append(None)
                    boll_upper.append(None)
                    boll_lower.append(None)
                else:
                    period_prices = close_prices[i-period+1:i+1]
                    ma = sum(period_prices) / period
                    variance = sum((p - ma) ** 2 for p in period_prices) / period
                    std = math.sqrt(variance)
                    
                    boll_middle.append(float(ma))
                    boll_upper.append(float(ma + k * std))
                    boll_lower.append(float(ma - k * std))
        
        df = df.with_columns([
            pl.Series("boll_upper", boll_upper).cast(pl.Float64),
            pl.Series("boll_middle", boll_middle).cast(pl.Float64),
            pl.Series("boll_lower", boll_lower).cast(pl.Float64)
        ])
        return df
    
    def _calculate_rsi(self, df, period=14):
        """计算RSI指标"""
        if len(df) < period + 1:
            rsi_values = [None] * len(df)
        else:
            close_prices = df["close"].to_list()
            rsi_values = [None] * period
            
            gains = []
            losses = []
            
            for i in range(1, len(close_prices)):
                change = close_prices[i] - close_prices[i-1]
                if change > 0:
                    gains.append(change)
                    losses.append(0)
                else:
                    gains.append(0)
                    losses.append(-change)
            
            for i in range(period, len(close_prices)):
                avg_gain = sum(gains[i-period:i]) / period
                avg_loss = sum(losses[i-period:i]) / period
                
                if avg_loss == 0:
                    rsi = 100.0
                else:
                    rs = avg_gain / avg_loss
                    rsi = 100.0 - (100.0 / (1 + rs))
                
                rsi_values.append(float(rsi))
        
        df = df.with_columns(pl.Series("rsi", rsi_values).cast(pl.Float64))
        return df
    
    def _calculate_wr(self, df, period=14):
        """计算WR指标"""
        if len(df) < period:
            wr_values = [None] * len(df)
        else:
            high_prices = df["high"].to_list()
            low_prices = df["low"].to_list()
            close_prices = df["close"].to_list()
            wr_values = [None] * (period - 1)
            
            for i in range(period - 1, len(df)):
                highest_high = max(high_prices[i-period+1:i+1])
                lowest_low = min(low_prices[i-period+1:i+1])
                
                if highest_high - lowest_low != 0:
                    wr = (highest_high - close_prices[i]) / (highest_high - lowest_low) * -100.0
                else:
                    wr = -50.0
                
                wr_values.append(float(wr))
        
        df = df.with_columns(pl.Series("wr", wr_values).cast(pl.Float64))
        return df
    
    def _calculate_obv(self, df):
        """计算OBV指标"""
        if len(df) < 2:
            obv_values = [0.0] * len(df)
        else:
            close_prices = df["close"].to_list()
            volumes = df["volume"].to_list()
            obv_values = [0.0]
            
            for i in range(1, len(df)):
                if close_prices[i] > close_prices[i-1]:
                    obv_values.append(obv_values[-1] + float(volumes[i]))
                elif close_prices[i] < close_prices[i-1]:
                    obv_values.append(obv_values[-1] - float(volumes[i]))
                else:
                    obv_values.append(obv_values[-1])
        
        df = df.with_columns(pl.Series("obv", obv_values).cast(pl.Float64))
        return df
    
    def _calculate_kdj(self, df):
        """计算KDJ指标"""
        n = len(df)
        k_values = []
        d_values = []
        j_values = []
        
        if n < 9:
            k_values = [None] * n
            d_values = [None] * n
            j_values = [None] * n
        else:
            high_list = df["high"].to_list()
            low_list = df["low"].to_list()
            close_list = df["close"].to_list()
            
            # 计算RSV
            rsv_values = []
            for i in range(n):
                if i < 8:
                    rsv_values.append(50.0)
                else:
                    h9 = max(high_list[i-8:i+1])
                    l9 = min(low_list[i-8:i+1])
                    if h9 - l9 != 0:
                        rsv = (close_list[i] - l9) / (h9 - l9) * 100.0
                    else:
                        rsv = 50.0
                    rsv_values.append(float(rsv))
            
            # 计算K值
            k = 50.0
            for i in range(n):
                if i < 8:
                    k_values.append(50.0)
                else:
                    k = 2/3 * k + 1/3 * rsv_values[i]
                    k_values.append(float(k))
            
            # 计算D值
            d = 50.0
            for i in range(n):
                if i < 8:
                    d_values.append(50.0)
                else:
                    d = 2/3 * d + 1/3 * k_values[i]
                    d_values.append(float(d))
            
            # 计算J值
            for i in range(n):
                if i < 8:
                    j_values.append(50.0)
                else:
                    j = 3 * k_values[i] - 2 * d_values[i]
                    j_values.append(float(j))
        
        df = df.with_columns([
            pl.Series("k", k_values).cast(pl.Float64),
            pl.Series("d", d_values).cast(pl.Float64),
            pl.Series("j", j_values).cast(pl.Float64)
        ])
        return df

    def _calculate_macd(self, df):
        """计算MACD指标"""
        n = len(df)
        if n < 26:
            macd_values = [None] * n
            signal_values = [None] * n
            hist_values = [None] * n
        else:
            close_prices = df["close"].to_list()
            
            # 计算EMA12
            ema12 = []
            ema12_val = float(close_prices[0])
            for i, price in enumerate(close_prices):
                if i == 0:
                    ema12.append(float(price))
                else:
                    ema12_val = ema12_val * 11/13 + float(price) * 2/13
                    ema12.append(float(ema12_val))
            
            # 计算EMA26
            ema26 = []
            ema26_val = float(close_prices[0])
            for i, price in enumerate(close_prices):
                if i == 0:
                    ema26.append(float(price))
                else:
                    ema26_val = ema26_val * 25/27 + float(price) * 2/27
                    ema26.append(float(ema26_val))
            
            # 计算MACD
            macd_values = [float(ema12[i] - ema26[i]) for i in range(n)]
            
            # 计算SIGNAL
            signal_values = []
            signal_val = float(macd_values[0])
            for i, macd_val in enumerate(macd_values):
                if i == 0:
                    signal_values.append(float(macd_val))
                else:
                    signal_val = signal_val * 8/10 + float(macd_val) * 2/10
                    signal_values.append(float(signal_val))
            
            # 计算HIST
            hist_values = [float(macd_values[i] - signal_values[i]) for i in range(n)]
        
        df = df.with_columns([
            pl.Series("macd", macd_values).cast(pl.Float64),
            pl.Series("signal", signal_values).cast(pl.Float64),
            pl.Series("hist", hist_values).cast(pl.Float64)
        ])
        return df
    
    def draw_kline(self):
        """绘制K线图"""
        if self.kline_data is None:
            return
        self.kline_canvas.delete("all")
        self.kline_positions = []
        canvas_width = self.kline_canvas.winfo_width()
        canvas_height = self.kline_canvas.winfo_height()
        if canvas_width <= 10 or canvas_height <= 10:
            return
        
        if self.dark_mode:
            bg_color = '#0e1a2b'
            grid_color = '#2a3a4b'
            text_color = 'white'
            up_color = '#e6b422'
            down_color = '#7fc7e0'
            ma_colors = ['#ff6b6b', '#4ecdc4', '#45b7d1', '#96ceb4', '#ffeaa5', '#dfe6e9']
        else:
            bg_color = 'white'
            grid_color = '#e0e0e0'
            text_color = 'black'
            up_color = '#ff4444'
            down_color = '#00aa00'
            ma_colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA5', '#DFE6E9']
        
        self.kline_canvas.config(bg=bg_color)
        
        start_idx = max(0, self.total_candles - self.display_candles - self.candle_offset)
        end_idx = min(self.total_candles, start_idx + self.display_candles)
        df = self.kline_data.slice(start_idx, end_idx - start_idx)
        display_len = len(df)
        
        latest_date = df[self.date_col][-1] if self.date_col and self.date_col in df.columns else ""
        earliest_date = df[self.date_col][0] if self.date_col and self.date_col in df.columns else ""
        period_name = self.period_config[self.current_period]["name"]
        mode_prefix = "⭐" if self.is_zixuan_mode else ("🔒" if self.is_original_zixuan_mode else "")
        self.chart_info.config(text=f"📅{mode_prefix}{period_name}{earliest_date}~{latest_date}|{display_len}根")
        
        padding = 60
        chart_top = 60
        chart_bottom = canvas_height - 30
        
        # 四个子图：价格(55%) + 成交量(15%) + 指标(14%) + MACD(16%)
        main_chart_ratio = 0.55
        volume_ratio = 0.15
        indicator_ratio = 0.14
        macd_ratio = 0.16
        
        main_chart_height = (chart_bottom - chart_top) * main_chart_ratio
        volume_height = (chart_bottom - chart_top) * volume_ratio
        indicator_height = (chart_bottom - chart_top) * indicator_ratio
        macd_height = (chart_bottom - chart_top) * macd_ratio
        
        main_chart_bottom = chart_top + main_chart_height
        volume_top = main_chart_bottom + 5
        volume_bottom = volume_top + volume_height
        indicator_top = volume_bottom + 5
        indicator_bottom = indicator_top + indicator_height
        macd_indicator_top = indicator_bottom + 5
        macd_indicator_bottom = macd_indicator_top + macd_height
        
        high_prices = [h for h in df["high"].to_list() if h is not None and not math.isnan(h)]
        low_prices = [l for l in df["low"].to_list() if l is not None and not math.isnan(l)]
        if not high_prices or not low_prices:
            return
        
        max_price = max(high_prices) * 1.02
        min_price = min(low_prices) * 0.98
        price_range = max_price - min_price
        
        volumes = [v for v in df["volume"].to_list() if v is not None and not math.isnan(v)]
        max_volume = max(volumes) * 1.1 if volumes else 1
        
        # 计算K线间距 - 自适应显示
        if display_len <= 30:
            # K线数量很少时，使用固定间距，靠左显示
            total_width = display_len * self.fixed_candle_spacing
            start_x = padding  # 靠左显示
            candle_spacing = self.fixed_candle_spacing
        else:
            # K线数量较多时，填满整个区域
            start_x = padding
            candle_spacing = (canvas_width - 2 * padding) / display_len
        
        candle_width = max(self.min_candle_width, min(self.max_candle_width, candle_spacing * 0.6))
        candle_half_width = max(1, candle_width * 0.4)
        
        self._draw_grid(canvas_width, chart_top, main_chart_bottom, padding, grid_color)
        self._draw_grid(canvas_width, volume_top, volume_bottom, padding, grid_color)
        self._draw_grid(canvas_width, indicator_top, indicator_bottom, padding, grid_color)
        self._draw_grid(canvas_width, macd_indicator_top, macd_indicator_bottom, padding, grid_color)
        
        for i in range(display_len):
            x = start_x + i * candle_spacing
            open_price = df["open"][i]
            close_price = df["close"][i]
            high_price = df["high"][i]
            low_price = df["low"][i]
            volume = df["volume"][i]
            change_pct = df["change_pct"][i] if "change_pct" in df.columns else None
            
            if None in [open_price, close_price, high_price, low_price, volume] or \
               math.isnan(open_price) or math.isnan(close_price) or \
               math.isnan(high_price) or math.isnan(low_price) or math.isnan(volume):
                continue
                
            is_up = close_price >= open_price
            color = up_color if is_up else down_color
            
            date_str = str(df[self.date_col][i]) if self.date_col else ""
            kline_info = {
                'x': x,
                'date': date_str,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume,
                'max_price': max_price,
                'min_price': min_price,
                'change_pct': change_pct
            }
            self.kline_positions.append(kline_info)
            
            y_high = main_chart_bottom - ((high_price - min_price) / price_range) * main_chart_height
            y_low = main_chart_bottom - ((low_price - min_price) / price_range) * main_chart_height
            self.kline_canvas.create_line(x, y_high, x, y_low, fill=color, width=1)
            
            y_open = main_chart_bottom - ((open_price - min_price) / price_range) * main_chart_height
            y_close = main_chart_bottom - ((close_price - min_price) / price_range) * main_chart_height
            
            if abs(y_open - y_close) < 1:
                y_close = y_open + 1
            
            if is_up:
                self.kline_canvas.create_rectangle(
                    x - candle_half_width, y_close,
                    x + candle_half_width, y_open,
                    fill=color, outline=color
                )
            else:
                self.kline_canvas.create_rectangle(
                    x - candle_half_width, y_open,
                    x + candle_half_width, y_close,
                    fill=color, outline=color
                )
            
            if volume is not None and not math.isnan(volume):
                y_volume_bottom = volume_bottom
                y_volume_top = volume_bottom - (volume / max_volume) * volume_height * 0.8
                self.kline_canvas.create_rectangle(
                    x - candle_half_width, y_volume_top,
                    x + candle_half_width, y_volume_bottom,
                    fill=color, outline=color
                )
        
        # 绘制均线
        ma_periods = [5, 10, 30, 60, 125, 250]
        legend_x = padding + 10
        legend_y = chart_top - 40
        self.kline_canvas.create_text(legend_x, legend_y, text="均线:", anchor=tk.W, fill=text_color, font=("Arial", 9, "bold"))
        
        for idx, period in enumerate(ma_periods[:4]):
            color = ma_colors[idx % len(ma_colors)]
            x_pos = legend_x + 50 + idx * 60
            self.kline_canvas.create_line(x_pos, legend_y, x_pos + 15, legend_y, fill=color, width=2)
            self.kline_canvas.create_text(x_pos + 20, legend_y, text=f"MA{period}", anchor=tk.W, fill=text_color, font=("Arial", 8))
        
        for idx, period in enumerate(ma_periods[4:]):
            color = ma_colors[(idx + 4) % len(ma_colors)]
            x_pos = legend_x + 50 + idx * 60
            y_pos = legend_y + 15
            self.kline_canvas.create_line(x_pos, y_pos, x_pos + 15, y_pos, fill=color, width=2)
            self.kline_canvas.create_text(x_pos + 20, y_pos, text=f"MA{period}", anchor=tk.W, fill=text_color, font=("Arial", 8))
        
        for idx, period in enumerate(ma_periods):
            ma_col = f"ma{period}"
            if ma_col in df.columns:
                points = []
                for i in range(display_len):
                    ma_value = df[ma_col][i]
                    if ma_value is not None and not math.isnan(ma_value):
                        x = start_x + i * candle_spacing
                        y = main_chart_bottom - ((ma_value - min_price) / price_range) * main_chart_height
                        points.append((x, y))
                if len(points) > 1:
                    for j in range(len(points) - 1):
                        self.kline_canvas.create_line(
                            points[j][0], points[j][1],
                            points[j+1][0], points[j+1][1],
                            fill=ma_colors[idx % len(ma_colors)],
                            width=1.5
                        )
        
        # 绘制选中的指标（上部指标区域）
        indicator = self.current_indicator
        if indicator == "macd":
            self._draw_macd_indicator(df, start_x, candle_spacing, indicator_top, indicator_bottom, text_color, up_color, down_color)
        elif indicator == "布林线":
            self._draw_bollinger_indicator(df, start_x, candle_spacing, indicator_top, indicator_bottom, text_color)
        elif indicator == "rsi":
            self._draw_rsi_indicator(df, start_x, candle_spacing, indicator_top, indicator_bottom, text_color)
        elif indicator == "wr":
            self._draw_wr_indicator(df, start_x, candle_spacing, indicator_top, indicator_bottom, text_color)
        elif indicator == "obv":
            self._draw_obv_indicator(df, start_x, candle_spacing, indicator_top, indicator_bottom, text_color)
        elif indicator == "kdj":
            self._draw_kdj_indicator(df, start_x, candle_spacing, indicator_top, indicator_bottom, text_color)
        
        # 底部始终显示MACD指标
        self._draw_macd_indicator(df, start_x, candle_spacing, macd_indicator_top, macd_indicator_bottom, text_color, up_color, down_color)
        
        # 绘制价格标签
        for price in [min_price, max_price, (min_price + max_price) / 2]:
            y = main_chart_bottom - ((price - min_price) / price_range) * main_chart_height
            self.kline_canvas.create_text(
                padding - 5, y,
                text=f"{price:.2f}",
                anchor=tk.E,
                fill=text_color,
                font=("Arial", 8)
            )
        
        # 绘制标题
        latest_date = df[self.date_col][-1] if self.date_col and self.date_col in df.columns else ""
        period_name = self.period_config[self.current_period]["name"]
        mode_prefix = "⭐" if self.is_zixuan_mode else ("🔒" if self.is_original_zixuan_mode else "")
        stock_name = self.get_stock_name(self.current_display_code)
        stock_name_display = f" {stock_name}" if stock_name else ""
        indicator_display = f" | {self.current_indicator.upper()}" if self.current_indicator != "macd" else " | MACD"
        self.kline_canvas.create_text(
            canvas_width // 2, 20,
            text=f"{mode_prefix}{period_name}-{self.current_display_code}{stock_name_display}最新:{latest_date}({display_len}根){indicator_display}",
            fill=text_color,
            font=("Microsoft YaHei", 12, "bold")
        )
        
        self.kline_canvas.create_text(padding, chart_top - 5, text="价格", anchor=tk.W, fill=text_color, font=("Arial", 8))
        self.kline_canvas.create_text(padding, volume_top - 5, text="成交量", anchor=tk.W, fill=text_color, font=("Arial", 8))
        self.kline_canvas.create_text(padding, indicator_top - 5, text=self.current_indicator.upper(), anchor=tk.W, fill=text_color, font=("Arial", 8))
        self.kline_canvas.create_text(padding, macd_indicator_top - 5, text="MACD", anchor=tk.W, fill=text_color, font=("Arial", 8))
    
    def _draw_kdj_indicator(self, df, start_x, candle_spacing, top, bottom, text_color):
        """绘制KDJ指标"""
        k_values = [v for v in df["k"].to_list() if v is not None and not math.isnan(v)]
        d_values = [v for v in df["d"].to_list() if v is not None and not math.isnan(v)]
        j_values = [v for v in df["j"].to_list() if v is not None and not math.isnan(v)]
        
        if k_values or d_values or j_values:
            all_kdj = []
            if k_values:
                all_kdj.extend(k_values)
            if d_values:
                all_kdj.extend(d_values)
            if j_values:
                all_kdj.extend(j_values)
            if all_kdj:
                max_kdj = max(all_kdj) * 1.1
                min_kdj = min(all_kdj) * 0.9
            else:
                max_kdj, min_kdj = 100, 0
            
            indicator_height = bottom - top
            
            k_points, d_points, j_points = [], [], []
            for i in range(len(df)):
                x = start_x + i * candle_spacing
                k_val = df["k"][i]
                d_val = df["d"][i]
                j_val = df["j"][i]
                
                if k_val is not None and not math.isnan(k_val):
                    y = bottom - ((k_val - min_kdj) / (max_kdj - min_kdj)) * indicator_height
                    k_points.append((x, y))
                if d_val is not None and not math.isnan(d_val):
                    y = bottom - ((d_val - min_kdj) / (max_kdj - min_kdj)) * indicator_height
                    d_points.append((x, y))
                if j_val is not None and not math.isnan(j_val):
                    y = bottom - ((j_val - min_kdj) / (max_kdj - min_kdj)) * indicator_height
                    j_points.append((x, y))
            
            if len(k_points) > 1:
                for i in range(len(k_points) - 1):
                    self.kline_canvas.create_line(
                        k_points[i][0], k_points[i][1],
                        k_points[i+1][0], k_points[i+1][1],
                        fill='gold', width=1.5
                    )
            if len(d_points) > 1:
                for i in range(len(d_points) - 1):
                    self.kline_canvas.create_line(
                        d_points[i][0], d_points[i][1],
                        d_points[i+1][0], d_points[i+1][1],
                        fill='royalblue', width=1.5
                    )
            if len(j_points) > 1:
                for i in range(len(j_points) - 1):
                    self.kline_canvas.create_line(
                        j_points[i][0], j_points[i][1],
                        j_points[i+1][0], j_points[i+1][1],
                        fill='tomato', width=1.5
                    )
            
            # 绘制图例
            legend_x = self.kline_canvas.winfo_width() - 150
            legend_y = top + 15
            self.kline_canvas.create_line(legend_x, legend_y, legend_x + 20, legend_y, fill='gold', width=2)
            self.kline_canvas.create_text(legend_x + 25, legend_y, text="K", anchor=tk.W, fill=text_color, font=("Arial", 8))
            self.kline_canvas.create_line(legend_x, legend_y + 15, legend_x + 20, legend_y + 15, fill='royalblue', width=2)
            self.kline_canvas.create_text(legend_x + 25, legend_y + 15, text="D", anchor=tk.W, fill=text_color, font=("Arial", 8))
            self.kline_canvas.create_line(legend_x, legend_y + 30, legend_x + 20, legend_y + 30, fill='tomato', width=2)
            self.kline_canvas.create_text(legend_x + 25, legend_y + 30, text="J", anchor=tk.W, fill=text_color, font=("Arial", 8))
    
    def _draw_macd_indicator(self, df, start_x, candle_spacing, top, bottom, text_color, up_color, down_color):
        """绘制MACD指标"""
        macd_values = [v for v in df["macd"].to_list() if v is not None and not math.isnan(v)]
        signal_values = [v for v in df["signal"].to_list() if v is not None and not math.isnan(v)]
        hist_values = [v for v in df["hist"].to_list() if v is not None and not math.isnan(v)]
        
        if macd_values or signal_values or hist_values:
            all_macd = []
            if macd_values:
                all_macd.extend(macd_values)
            if signal_values:
                all_macd.extend(signal_values)
            if hist_values:
                all_macd.extend(hist_values)
            if all_macd:
                max_macd = max(all_macd) * 1.1
                min_macd = min(all_macd) * 1.1
            else:
                max_macd, min_macd = 1, -1
            
            indicator_height = bottom - top
            
            macd_points, signal_points = [], []
            for i in range(len(df)):
                x = start_x + i * candle_spacing
                macd_val = df["macd"][i]
                signal_val = df["signal"][i]
                hist_val = df["hist"][i]
                
                if macd_val is not None and not math.isnan(macd_val):
                    y = bottom - ((macd_val - min_macd) / (max_macd - min_macd)) * indicator_height
                    macd_points.append((x, y))
                if signal_val is not None and not math.isnan(signal_val):
                    y = bottom - ((signal_val - min_macd) / (max_macd - min_macd)) * indicator_height
                    signal_points.append((x, y))
                if hist_val is not None and not math.isnan(hist_val):
                    y0 = bottom - ((0 - min_macd) / (max_macd - min_macd)) * indicator_height
                    y1 = bottom - ((hist_val - min_macd) / (max_macd - min_macd)) * indicator_height
                    color = up_color if hist_val >= 0 else down_color
                    self.kline_canvas.create_line(x, y0, x, y1, fill=color, width=2)
            
            if len(macd_points) > 1:
                for j in range(len(macd_points) - 1):
                    self.kline_canvas.create_line(
                        macd_points[j][0], macd_points[j][1],
                        macd_points[j+1][0], macd_points[j+1][1],
                        fill='cyan', width=1.5
                    )
            if len(signal_points) > 1:
                for j in range(len(signal_points) - 1):
                    self.kline_canvas.create_line(
                        signal_points[j][0], signal_points[j][1],
                        signal_points[j+1][0], signal_points[j+1][1],
                        fill='magenta', width=1.5
                    )
            
            # 绘制图例
            legend_x = self.kline_canvas.winfo_width() - 150
            legend_y = top + 15
            self.kline_canvas.create_line(legend_x, legend_y, legend_x + 20, legend_y, fill='cyan', width=2)
            self.kline_canvas.create_text(legend_x + 25, legend_y, text="MACD", anchor=tk.W, fill=text_color, font=("Arial", 8))
            self.kline_canvas.create_line(legend_x, legend_y + 15, legend_x + 20, legend_y + 15, fill='magenta', width=2)
            self.kline_canvas.create_text(legend_x + 25, legend_y + 15, text="SIGNAL", anchor=tk.W, fill=text_color, font=("Arial", 8))
    
    def _draw_bollinger_indicator(self, df, start_x, candle_spacing, top, bottom, text_color):
        """绘制布林线指标"""
        boll_upper = [v for v in df["boll_upper"].to_list() if v is not None and not math.isnan(v)]
        boll_middle = [v for v in df["boll_middle"].to_list() if v is not None and not math.isnan(v)]
        boll_lower = [v for v in df["boll_lower"].to_list() if v is not None and not math.isnan(v)]
        
        if boll_upper or boll_middle or boll_lower:
            all_boll = []
            if boll_upper:
                all_boll.extend(boll_upper)
            if boll_middle:
                all_boll.extend(boll_middle)
            if boll_lower:
                all_boll.extend(boll_lower)
            if all_boll:
                max_boll = max(all_boll) * 1.02
                min_boll = min(all_boll) * 0.98
            else:
                max_boll, min_boll = 100, 0
            
            indicator_height = bottom - top
            
            # 绘制布林带
            upper_points = []
            middle_points = []
            lower_points = []
            
            for i in range(len(df)):
                x = start_x + i * candle_spacing
                
                if "boll_upper" in df.columns and df["boll_upper"][i] is not None and not math.isnan(df["boll_upper"][i]):
                    y = bottom - ((df["boll_upper"][i] - min_boll) / (max_boll - min_boll)) * indicator_height
                    upper_points.append((x, y))
                
                if "boll_middle" in df.columns and df["boll_middle"][i] is not None and not math.isnan(df["boll_middle"][i]):
                    y = bottom - ((df["boll_middle"][i] - min_boll) / (max_boll - min_boll)) * indicator_height
                    middle_points.append((x, y))
                
                if "boll_lower" in df.columns and df["boll_lower"][i] is not None and not math.isnan(df["boll_lower"][i]):
                    y = bottom - ((df["boll_lower"][i] - min_boll) / (max_boll - min_boll)) * indicator_height
                    lower_points.append((x, y))
            
            if len(upper_points) > 1:
                for j in range(len(upper_points) - 1):
                    self.kline_canvas.create_line(
                        upper_points[j][0], upper_points[j][1],
                        upper_points[j+1][0], upper_points[j+1][1],
                        fill='orange', width=1.5
                    )
            
            if len(middle_points) > 1:
                for j in range(len(middle_points) - 1):
                    self.kline_canvas.create_line(
                        middle_points[j][0], middle_points[j][1],
                        middle_points[j+1][0], middle_points[j+1][1],
                        fill='yellow', width=1.5
                    )
            
            if len(lower_points) > 1:
                for j in range(len(lower_points) - 1):
                    self.kline_canvas.create_line(
                        lower_points[j][0], lower_points[j][1],
                        lower_points[j+1][0], lower_points[j+1][1],
                        fill='lightgreen', width=1.5
                    )
            
            # 绘制图例
            legend_x = self.kline_canvas.winfo_width() - 150
            legend_y = top + 15
            self.kline_canvas.create_line(legend_x, legend_y, legend_x + 20, legend_y, fill='orange', width=2)
            self.kline_canvas.create_text(legend_x + 25, legend_y, text="UPPER", anchor=tk.W, fill=text_color, font=("Arial", 8))
            self.kline_canvas.create_line(legend_x, legend_y + 15, legend_x + 20, legend_y + 15, fill='yellow', width=2)
            self.kline_canvas.create_text(legend_x + 25, legend_y + 15, text="MIDDLE", anchor=tk.W, fill=text_color, font=("Arial", 8))
            self.kline_canvas.create_line(legend_x, legend_y + 30, legend_x + 20, legend_y + 30, fill='lightgreen', width=2)
            self.kline_canvas.create_text(legend_x + 25, legend_y + 30, text="LOWER", anchor=tk.W, fill=text_color, font=("Arial", 8))
    
    def _draw_rsi_indicator(self, df, start_x, candle_spacing, top, bottom, text_color):
        """绘制RSI指标"""
        rsi_values = [v for v in df["rsi"].to_list() if v is not None and not math.isnan(v)]
        
        if rsi_values:
            indicator_height = bottom - top
            
            # 绘制超买超卖线
            overbought_y = bottom - ((70 - 0) / 100) * indicator_height
            oversold_y = bottom - ((30 - 0) / 100) * indicator_height
            canvas_width = self.kline_canvas.winfo_width()
            
            self.kline_canvas.create_line(padding, overbought_y, canvas_width - padding, overbought_y, 
                                        fill='red', width=1, dash=(2, 2))
            self.kline_canvas.create_text(canvas_width - padding - 10, overbought_y - 10, 
                                        text="70(超买)", fill=text_color, font=("Arial", 8))
            
            self.kline_canvas.create_line(padding, oversold_y, canvas_width - padding, oversold_y, 
                                        fill='green', width=1, dash=(2, 2))
            self.kline_canvas.create_text(canvas_width - padding - 10, oversold_y + 10, 
                                        text="30(超卖)", fill=text_color, font=("Arial", 8))
            
            # 绘制RSI线
            points = []
            for i in range(len(df)):
                if df["rsi"][i] is not None and not math.isnan(df["rsi"][i]):
                    x = start_x + i * candle_spacing
                    y = bottom - (df["rsi"][i] / 100) * indicator_height
                    points.append((x, y))
            
            if len(points) > 1:
                for j in range(len(points) - 1):
                    self.kline_canvas.create_line(
                        points[j][0], points[j][1],
                        points[j+1][0], points[j+1][1],
                        fill='purple', width=2
                    )
    
    def _draw_wr_indicator(self, df, start_x, candle_spacing, top, bottom, text_color):
        """绘制WR指标"""
        wr_values = [v for v in df["wr"].to_list() if v is not None and not math.isnan(v)]
        
        if wr_values:
            indicator_height = bottom - top
            
            # 绘制超买超卖线（WR指标是-100到0，超买在-20以上，超卖在-80以下）
            overbought_y = bottom - ((-20 - (-100)) / 100) * indicator_height
            oversold_y = bottom - ((-80 - (-100)) / 100) * indicator_height
            canvas_width = self.kline_canvas.winfo_width()
            
            self.kline_canvas.create_line(padding, overbought_y, canvas_width - padding, overbought_y, 
                                        fill='red', width=1, dash=(2, 2))
            self.kline_canvas.create_text(canvas_width - padding - 10, overbought_y - 10, 
                                        text="-20(超买)", fill=text_color, font=("Arial", 8))
            
            self.kline_canvas.create_line(padding, oversold_y, canvas_width - padding, oversold_y, 
                                        fill='green', width=1, dash=(2, 2))
            self.kline_canvas.create_text(canvas_width - padding - 10, oversold_y + 10, 
                                        text="-80(超卖)", fill=text_color, font=("Arial", 8))
            
            # 绘制WR线
            points = []
            for i in range(len(df)):
                if df["wr"][i] is not None and not math.isnan(df["wr"][i]):
                    x = start_x + i * candle_spacing
                    y = bottom - ((df["wr"][i] + 100) / 100) * indicator_height
                    points.append((x, y))
            
            if len(points) > 1:
                for j in range(len(points) - 1):
                    self.kline_canvas.create_line(
                        points[j][0], points[j][1],
                        points[j+1][0], points[j+1][1],
                        fill='brown', width=2
                    )
    
    def _draw_obv_indicator(self, df, start_x, candle_spacing, top, bottom, text_color):
        """绘制OBV指标"""
        obv_values = [v for v in df["obv"].to_list() if v is not None and not math.isnan(v)]
        
        if obv_values:
            max_obv = max(obv_values) * 1.02
            min_obv = min(obv_values) * 0.98
            indicator_height = bottom - top
            
            # 绘制OBV线
            points = []
            for i in range(len(df)):
                if df["obv"][i] is not None and not math.isnan(df["obv"][i]):
                    x = start_x + i * candle_spacing
                    y = bottom - ((df["obv"][i] - min_obv) / (max_obv - min_obv)) * indicator_height
                    points.append((x, y))
            
            if len(points) > 1:
                for j in range(len(points) - 1):
                    self.kline_canvas.create_line(
                        points[j][0], points[j][1],
                        points[j+1][0], points[j+1][1],
                        fill='orange', width=2
                    )
    
    def _draw_grid(self, canvas_width, top, bottom, padding, color):
        """绘制网格"""
        for i in range(0, 5):
            y = top + (bottom - top) * i / 4
            self.kline_canvas.create_line(padding, y, canvas_width - padding, y, fill=color, width=0.5, dash=(2, 2))
        for i in range(0, 6):
            x = padding + (canvas_width - 2 * padding) * i / 5
            self.kline_canvas.create_line(x, top, x, bottom, fill=color, width=0.5, dash=(2, 2))

    # ========== 主题切换 ==========
    def toggle_theme(self):
        """切换主题"""
        self.dark_mode = not self.dark_mode
        self.btn_theme.config(text="☀️亮色" if self.dark_mode else "🌙暗色")
        if self.kline_data is not None and self.view_mode == "kline":
            self.draw_kline()

    # ========== 其他功能 ==========
    def on_canvas_resize(self, event):
        """画布大小改变时重绘K线图"""
        if self.kline_data is not None and self.view_mode == "kline":
            self.refresh_kline()

    def filter_market(self, mkt):
        """市场筛选"""
        if self.is_zixuan_mode or self.is_original_zixuan_mode or self.rank_mode:
            self._exit_all_special_modes()
            return
        self.current_market = mkt
        for market, btn in self.market_buttons.items():
            btn.state(['!disabled'])
        if mkt in self.market_buttons:
            self.market_buttons[mkt].state(['disabled'])
        if self.all_codes.is_empty():
            return
        if mkt == "all":
            self.filtered_codes = self.all_codes
        else:
            codes = self.all_codes.cast(pl.Utf8)
            market_rules = {
                "sh": codes.str.starts_with("6") | codes.str.starts_with("9"),
                "sz": codes.str.starts_with("0"),
                "zx": codes.str.starts_with("002"),
                "cy": codes.str.starts_with("300"),
                "kc": codes.str.starts_with("688"),
            }
            cond = market_rules.get(mkt, pl.lit(True))
            self.filtered_codes = self.all_codes.filter(cond)
        self._refresh_code_list()
        if len(self.filtered_codes) > 0:
            self.current_code_index = 0
            self.load_one_stock(self.filtered_codes[0])
            self.highlight_current_code()
    
    def _refresh_code_list(self):
        """刷新代码列表"""
        self.list_codes.delete(0, tk.END)
        for code in self.filtered_codes.to_list():
            self.list_codes.insert(tk.END, f"{code}")
        self.label_cnt.config(text=f"📋{len(self.filtered_codes)}只")
        self.position_label.config(text="")

    def open_db(self):
        """打开数据库"""
        if not os.path.exists(self.stock_data_dir):
            os.makedirs(self.stock_data_dir)
        path = filedialog.askopenfilename(
            title="选择DuckDB/Parquet数据库",
            initialdir=self.stock_data_dir,
            filetypes=[("数据库文件", "*.duckdb *.db *.parquet"), ("DuckDB", "*.duckdb *.db"), ("Parquet", "*.parquet"), ("所有文件", "*.*")]
        )
        if not path:
            return
        try:
            if self.con:
                self.con.close()
            
            self.con = self._connect_db_readonly(path)
            if not self.con:
                self.show_message("错误", f"无法连接数据库: {path}", "error")
                return
            
            self.db_path = path
            db_type = "Parquet" if path.endswith('.parquet') else "DuckDB"
            self.label_db.config(text=f"📁{os.path.basename(path)}({db_type})")
            
            tables = [t[0] for t in self.con.execute("SHOW TABLES").fetchall()]
            self.cb_table["values"] = tables
            
            if not tables:
                self.show_message("提示", "数据库中没有表", "warning")
                return
            
            target_table = None
            for t in tables:
                if "daily" in t.lower() or "stock" in t.lower():
                    target_table = t
                    break
            
            if target_table:
                self.var_table.set(target_table)
            else:
                self.var_table.set(tables[0])
            
            self.on_table_select()
        except Exception as e:
            self.show_message("错误", f"打开失败: {str(e)}", "error")

    def on_table_select(self, event=None):
        """表选择"""
        table = self.var_table.get()
        if not table or not self.con:
            return
        self._load_table_data(table)

    def _load_table_data(self, table):
        """加载表数据"""
        try:
            result = self.con.execute(f"SELECT * FROM {table} LIMIT 1")
            row = result.fetchone()
            if row is None:
                self.show_message("提示", "表中没有数据", "warning")
                return
            columns = [desc[0] for desc in result.description]
            cols_lower = [c.lower() for c in columns]
            code_idx = next((i for i, c in enumerate(cols_lower) if "code" in c), None)
            if code_idx is None:
                self.show_message("提示", "未找到代码列", "warning")
                return
            self.code_col = columns[code_idx]
            date_idx = None
            for i, c in enumerate(cols_lower):
                if c in ['date', 'date_time', 'time', 'datetime']:
                    date_idx = i
                    break
            if date_idx is not None:
                self.date_col = columns[date_idx]
            else:
                self.date_col = None
            self.current_table = table
            code_result = self.con.execute(f"SELECT DISTINCT {self.code_col} FROM {self.current_table} ORDER BY {self.code_col}").fetchall()
            self.all_codes = pl.Series([r[0] for r in code_result])
            self.current_market = "all"
            self.filtered_codes = self.all_codes
            self._refresh_code_list()
            if len(self.all_codes) > 0:
                self.current_code_index = 0
                first_code = self.all_codes[0]
                self.load_one_stock(first_code)
                self.highlight_current_code()
                period_name = self.period_config[self.current_period]["name"]
                self.label_tip.config(text=f"📈{period_name}-{first_code}")
                if self.current_period == self.base_period:
                    self.base_code = first_code
            for market, btn in self.market_buttons.items():
                btn.state(['!disabled'])
            self.market_buttons["all"].state(['disabled'])
            self.label_info.config(text=f"📊{self.period_config[self.current_period]['name']}:{table}")
        except Exception as e:
            self.show_message("错误", f"加载表数据失败: {str(e)}", "error")

    def on_code_click(self, event):
        """点击代码"""
        selection = self.list_codes.curselection()
        if selection:
            index = selection[0]
            if self.is_zixuan_mode:
                if index < len(self.zixuan_codes):
                    self.current_code_index = index
                    self.locked_code = self.zixuan_codes[index].replace("⭐", "")
                    success = self._load_stock_in_current_period(self.locked_code)
                    if success:
                        self.highlight_current_code()
                        self.var_code.set(self.locked_code)
                        self.label_tip.config(text=f"⭐自选股 {index + 1}/{len(self.zixuan_codes)}: {self.locked_code}")
                        self._update_del_button_state()
            elif self.is_original_zixuan_mode:
                if index < len(self.zixuangu_codes):
                    self.current_code_index = index
                    self.locked_code = self.zixuangu_codes[index].replace("🔒", "")
                    success = self._load_stock_in_current_period(self.locked_code)
                    if success:
                        self.highlight_current_code()
                        self.var_code.set(self.locked_code)
                        self.label_tip.config(text=f"🔒原自选 {index + 1}/{len(self.zixuangu_codes)}: {self.locked_code}")
            elif self.rank_mode:
                if self.rank_locked:
                    # 锁定状态下，点击左侧代码时加载当前周期的数据
                    if index < self.list_codes.size():
                        self.current_code_index = index
                        item_text = self.list_codes.get(index)
                        code = item_text.split()[0]
                        self.load_one_stock(code)
                        self.var_code.set(code)
                        self.highlight_current_code()
                        self._update_del_button_state()
                else:
                    if index < self.list_codes.size():
                        self.current_code_index = index
                        item_text = self.list_codes.get(index)
                        code = item_text.split()[0]
                        self.load_one_stock(code)
                        self.var_code.set(code)
                        self.highlight_current_code()
                        self._update_del_button_state()
            else:
                if index < len(self.filtered_codes):
                    self.current_code_index = index
                    code = self.filtered_codes[index]
                    self.load_one_stock(code)
                    self.var_code.set(code)
                    self.highlight_current_code()
                    if self.current_period == self.base_period:
                        self.base_code = code
                    self._update_del_button_state()

    def load_one_stock(self, code):
        """加载股票"""
        try:
            # 规范化代码
            norm_code = normalize_stock_code(code)
            
            order_field = self.date_col if self.date_col else self.code_col
            sql = f"""
                SELECT * FROM {self.current_table} 
                WHERE {self.code_col} = ? 
                ORDER BY {order_field}
            """
            result = self.con.execute(sql, [norm_code]).fetchall()
            columns = [desc[0] for desc in self.con.execute(sql, [norm_code]).description]
            df = pl.DataFrame(result, schema=columns, orient="row")
            
            self.current_display_code = norm_code
            period_name = self.period_config[self.current_period]["name"]
            mode_prefix = "⭐" if self.is_zixuan_mode else ("🔒" if self.is_original_zixuan_mode else "")
            self.label_tip.config(text=f"📈{mode_prefix}{period_name}-{norm_code}{len(df)}条")
            self.candle_offset = 0
            if self.view_mode == "kline":
                self.refresh_kline()
            else:
                self.refresh_data_table()
            self._update_del_button_state()
        except Exception as e:
            self.show_message("错误", f"加载失败: {str(e)}", "error")

    def _fetch_stock_data(self):
        """获取股票数据"""
        order_field = self.date_col if self.date_col else self.code_col
        sql = f"""
            SELECT * FROM {self.current_table} 
            WHERE {self.code_col} = ? 
            ORDER BY {order_field}
        """
        result = self.con.execute(sql, [self.current_display_code]).fetchall()
        columns = [desc[0] for desc in self.con.execute(sql, [self.current_display_code]).description]
        return pl.DataFrame(result, schema=columns, orient="row")

    def search_by_code(self, event=None):
        """搜索代码"""
        input_code = self.var_code.get().strip()
        if not input_code:
            return
        
        # 规范化输入代码
        code = normalize_stock_code(input_code)
        
        if self.is_zixuan_mode:
            clean_codes = [c.replace("⭐", "") for c in self.zixuan_codes]
            if code in clean_codes:
                index = clean_codes.index(code)
                self.current_code_index = index
                self.locked_code = code
                success = self._load_stock_in_current_period(code)
                if success:
                    self.highlight_current_code()
                    self._update_del_button_state()
                return
        elif self.is_original_zixuan_mode:
            clean_codes = [c.replace("🔒", "") for c in self.zixuangu_codes]
            if code in clean_codes:
                index = clean_codes.index(code)
                self.current_code_index = index
                self.locked_code = code
                success = self._load_stock_in_current_period(code)
                if success:
                    self.highlight_current_code()
                return
        elif self.rank_mode:
            for i in range(self.list_codes.size()):
                item_text = self.list_codes.get(i)
                if item_text.startswith(code):
                    self.current_code_index = i
                    self.load_one_stock(code)
                    self.highlight_current_code()
                    self._update_del_button_state()
                    return
        
        codes_list = self.filtered_codes.to_list()
        if code in codes_list:
            index = codes_list.index(code)
            self.current_code_index = index
            self.load_one_stock(code)
            self.highlight_current_code()
            if self.current_period == self.base_period:
                self.base_code = code
            self._update_del_button_state()
        else:
            if not self.filtered_codes.is_empty() and len(self.filtered_codes) < len(self.all_codes):
                all_codes_list = self.all_codes.to_list()
                if code in all_codes_list:
                    self.filter_market("all")
                    index = all_codes_list.index(code)
                    self.current_code_index = index
                    self.load_one_stock(code)
                    self.highlight_current_code()
                    self._update_del_button_state()
                else:
                    self.show_message("提示", f"未找到代码: {input_code}", "warning")
            else:
                self.show_message("提示", f"未找到代码: {input_code}", "warning")

    def filter_by_date(self):
        """日期筛选"""
        if not self.current_display_code or not self.date_col or self.view_mode != "kline":
            return
        start_date = self.entry_s.get().strip()
        end_date = self.entry_e.get().strip()
        try:
            sql = f"SELECT * FROM {self.current_table} WHERE {self.code_col} = ?"
            params = [self.current_display_code]
            if start_date:
                sql += f" AND {self.date_col} >= ?"
                params.append(start_date)
            if end_date:
                sql += f" AND {self.date_col} <= ?"
                params.append(end_date)
            sql += f" ORDER BY {self.date_col}"
            result = self.con.execute(sql, params).fetchall()
            columns = [desc[0] for desc in self.con.execute(sql, params).description]
            df = pl.DataFrame(result, schema=columns, orient="row")
            
            # 确保所有数值列为浮点类型
            for col in ["open", "high", "low", "close", "volume"]:
                if col in df.columns:
                    df = df.with_columns(pl.col(col).cast(pl.Float64))
            
            change_pct_list = [None] * len(df)
            for i in range(1, len(df)):
                if df["close"][i-1] and df["close"][i-1] > 0:
                    change_pct = (df["close"][i] - df["close"][i-1]) / df["close"][i-1] * 100
                    change_pct_list[i] = change_pct
            
            df = df.with_columns(pl.Series("change_pct", change_pct_list).cast(pl.Float64))
            df = self._calculate_kdj(df)
            df = self._calculate_macd(df)
            df = self._calculate_ma(df)
            self.kline_data = df
            self.total_candles = len(df)
            
            # 自适应K线数量
            if self.candle_count_var.get() == "自适应":
                self.display_candles = self._calculate_adaptive_candle_count()
            elif self.candle_count_var.get() == "全部":
                self.display_candles = self.total_candles
            else:
                try:
                    self.display_candles = int(self.candle_count_var.get())
                except:
                    self.display_candles = min(300, self.total_candles)
            
            self.display_candles = min(self.display_candles, self.total_candles)
            self.candle_offset = min(self.candle_offset, self.total_candles - self.display_candles)
            self._update_pan_buttons()
            self.draw_kline()
            self.label_tip.config(text=f"📅 {self.current_display_code}{len(df)}条")
        except Exception as e:
            self.show_message("错误", f"筛选失败: {str(e)}", "error")

    def clear_all(self):
        """重置"""
        self.var_code.set("")
        self.entry_s.delete(0, tk.END)
        self.entry_e.delete(0, tk.END)
        self.label_tip.config(text="请选择股票")
        self.current_display_code = None
        self.current_code_index = -1
        self.kline_data = None
        self.is_zixuan_mode = False
        self.is_original_zixuan_mode = False
        self.is_zixuan_locked = False
        self.rank_mode = None
        self.rank_locked = False
        self.rank_locked_codes = []
        self.locked_code = None
        self.zixuan_button.config(text="自选股")
        self.zixuangu_button.config(text="自选")
        self.btn_rank_lock.config(text="🔓解锁")
        self.candle_offset = 0
        self.fuquan_type = "none"
        self.has_factor = False
        self.fuquan_button.config(text="不复权")
        
        self.stock_name_var.set("")
        self.latest_price_var.set("--")
        self.latest_price_label.config(foreground="black")
        self.change_percent_var.set("0.00%")
        self.change_percent_label.config(foreground="black")
        self.pe_var.set("0.00")
        self.pb_var.set("0.00")
        self.market_value_var.set("0.00")
        self.industry_var.set("")
        self.dividend_var.set("0.00")
        
        for key, btn in self.period_buttons.items():
            btn.state(['!disabled'])
        self.period_buttons[self.last_normal_period].state(['disabled'])
        self.current_period = self.last_normal_period
        
        if self.kline_canvas:
            self.kline_canvas.delete("all")
        
        if not self.all_codes.is_empty():
            self.filtered_codes = self.all_codes
            self._refresh_code_list()
        
        self._update_del_button_state()

    def export_csv(self):
        """导出CSV"""
        if self.kline_data is None and self.view_mode == "kline":
            self.show_message("提示", "没有可导出的数据", "warning")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            initialdir=self.stock_data_dir,
            filetypes=[("CSV文件", "*.csv"), ("所有文件", "*.*")]
        )
        if not path:
            return
        try:
            if self.view_mode == "kline":
                start_idx = max(0, self.total_candles - self.display_candles - self.candle_offset)
                end_idx = min(self.total_candles, start_idx + self.display_candles)
                df = self.kline_data.slice(start_idx, end_idx - start_idx)
            else:
                order_field = self.date_col if self.date_col else self.code_col
                sql = f"""
                    SELECT * FROM {self.current_table} 
                    WHERE {self.code_col} = ? 
                    ORDER BY {order_field}
                """
                result = self.con.execute(sql, [self.current_display_code]).fetchall()
                columns = [desc[0] for desc in self.con.execute(sql, [self.current_display_code]).description]
                df = pl.DataFrame(result, schema=columns, orient="row")
            df.write_csv(path)
            self.show_message("成功", f"已导出 {len(df)} 条记录", "success")
        except Exception as e:
            self.show_message("错误", f"导出失败: {str(e)}", "error")
    
    def __del__(self):
        """析构函数"""
        if self.con:
            self.con.close()
        if self.stock_info_con:
            self.stock_info_con.close()

if __name__ == "__main__":
    root = tk.Tk()
    app = StockViewerApp(root)
    root.mainloop()