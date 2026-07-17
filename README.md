# A股数据下载器

A股全周期行情数据批量下载与管理工具，支持日线/周线/月线/季线/年线及5/15/30/60分钟K线数据。

## 功能特性

- **多周期支持**：日线、周线、月线、季线、年线、60/30/15/5分钟K线
- **智能增量更新**：自动检测数据库已有数据，仅下载缺失/需更新的记录
- **GUI管理界面**：基于Tkinter的图形化操作面板，可视化监控下载进度
- **本地DuckDB存储**：使用DuckDB列式数据库，高效读写，支持WAL模式
- **多线程下载**：支持配置线程数提升下载效率
- **自动重试机制**：网络异常自动重试，保障下载成功率
- **智能跳过**：连续失败自动跳过，避免无效请求
- **自动保存**：定时/定量触发数据持久化，防止意外中断丢失数据
- **数据校验**：内置数据质量检查，确保入库数据完整有效

## 项目结构

```
a_stock_download_manger/
├── a_stock_download_manger.py          # 主程序（GUI界面）
├── run_download.py                      # CLI批量下载脚本（60分钟线示例）
├── minute5_stock_data_download.py       # 5分钟K线下载模块
├── minute15_stock_data_download.py      # 15分钟K线下载模块
├── minute30_stock_data_download.py      # 30分钟K线下载模块
├── minute60_stock_data_download.py      # 60分钟K线下载模块
├── monthly_stock_data_download.py       # 月线下载模块
├── seasonly_stock_data_download.py      # 季线下载模块
├── weekly_stock_data_download.py        # 周线下载模块
├── stock_get_date_config.json           # 下载周期与参数配置
├── Table.csv                            # 股票基础信息表（代码/名称/行业/上市日期等）
├── table.txt                            # 股票代码列表
└── .gitignore                           # Git忽略规则
```

## 环境要求

- Python 3.11+
- Windows / Linux / macOS

## 安装依赖

```bash
pip install baostock duckdb tkinter pyperclip
```

## 使用方法

### 1. GUI方式（推荐）

运行主程序启动图形界面：

```bash
python a_stock_download_manger.py
```

在界面中可：
- 选择需要下载的周期类型（日线/周线/月线等）
- 粘贴或导入股票代码列表
- 设置线程数、批次大小、自动保存间隔等参数
- 查看实时下载进度和统计信息

### 2. CLI批量下载

使用命令行脚本进行自动化下载：

```bash
python run_download.py
```

该脚本读取 `table.txt` 中的股票代码，查询数据库已有数据后批量下载缺失记录。

### 3. 单独调用模块

各周期下载模块可作为独立库使用：

```python
from minute60_stock_data_download import run_module
run_module()
```

## 配置说明

`stock_get_date_config.json` 控制下载行为：

| 字段 | 说明 | 默认值 |
|------|------|--------|
| day | 是否下载日线 | true |
| week | 是否下载周线 | true |
| month | 是否下载月线 | false |
| season | 是否下载季线 | false |
| year | 是否下载年线 | false |
| 60min/30min/15min/5min | 各周期分钟线开关 | false |
| use_skip_mode | 启用智能跳过模式 | false |
| thread_count | 下载线程数 | 1 |
| batch_size | 每批处理股票数 | 1000 |
| auto_save_interval | 自动保存间隔(分钟) | 3 |
| auto_save_stock_count | 触发保存的股票数量 | 100 |
| max_retries | 最大重试次数 | 3 |
| enable_data_validation | 启用数据校验 | true |

## 数据存储

程序运行时自动创建 `stock_data/` 目录，存放所有数据库文件：

| 数据库文件 | 内容 |
|-----------|------|
| stock_data.duckdb | 日线数据 |
| weekly_stock_data.duckdb | 周线数据 |
| monthly_stock_data.duckdb | 月线数据 |
| seasonly_data.duckdb | 季线数据 |
| yearly_data.duckdb | 年线数据 |
| minute60_stock_data.duckdb | 60分钟K线 |
| minute30_stock_data.duckdb | 30分钟K线 |
| minute15_stock_data.duckdb | 15分钟K线 |
| minute5_stock_data.duckdb | 5分钟K线 |

## 数据来源

- **baostock**：A股历史行情数据源
- **Table.csv**：股票基本信息（代码、名称、行业、上市日期、市值等）
