# A股股票数据下载管理器

一站式 A 股数据工具套件：数据下载 → K线浏览 → 量化选股 → 策略回测。

## 项目结构

```
├── a_stock_download_manger.py        # 主程序：多周期数据下载器 (GUI)
├── haobao_stock.py                   # K线查看器：交互式浏览日/周/月/分钟线
├── 量化选股回测工具策略版.py           # 量化选股 + 策略回测引擎
├── git_manger.py                     # Git 版本管理工具
│
├── minute60_stock_data_download.py   # 60分钟K线下载模块
├── weekly_stock_data_download.py     # 日线→周线转换模块
├── monthly_stock_data_download.py    # 日线→月线转换模块
├── seasonly_stock_data_download.py   # 日线→季线转换模块
│
├── stock_data/
│   ├── stock_data.duckdb             # 日线数据库
│   ├── stock_info_data.duckdb        # 股票信息数据库
│   ├── weekly_stock_data.duckdb      # 周线数据库
│   ├── monthly_stock_data.duckdb     # 月线数据库
│   ├── seasonly_data.duckdb          # 季线数据库
│   ├── minute60_stock_data.duckdb    # 60分线数据库
│   ├── celue/MACD金叉策略.py         # MACD金叉选股+回测策略
│   ├── stock_pakeing/config.json     # 选股工具配置文件
│   └── zixuan.csv / zixuangu.csv     # 自选股列表
│
├── table.txt                         # 股票代码列表（下载器用）
├── backtest_reports/                 # 回测报告输出目录
└── stock_get_date_config.json        # 下载器配置文件
```

## 模块功能

### a_stock_download_manger.py — 数据下载器

从 Baostock 下载 A 股历史 K 线数据，支持 9 种周期：日线、周线、月线、季线、年线、60分、30分、15分、5分。

- 多线程批量下载，断点续传
- 智能跳过无数据股票
- Baostock 登录自动保活
- 数据校验、自动保存、失败重试
- 增量更新：只下载缺失日期

```
python a_stock_download_manger.py
```

### haobao_stock.py — K 线查看器

交互式浏览已下载的股票数据，支持多周期切换和多种技术指标。

- K 线图 + 成交量 + MACD 三区联动显示
- 支持 MA5/10/30/60/125/250 均线
- 可切换指标：MACD、布林线、RSI、WR、OBV、KDJ
- MACD 固定在底部区域始终显示
- 十字线查看精确价位
- 鼠标滚轮缩放、方向键平移
- 支持自选股、涨幅榜/跌幅榜
- 暗色/亮色主题切换

```
python haobao_stock.py
```

### 量化选股回测工具策略版.py — 选股 + 回测引擎

嵌入 Python 策略编辑器，执行选股和策略回测。

- 内置代码编辑器，支持粘贴/保存/加载策略
- 策略格式：`select_stock(df)` 选股 + `score_stock(df)` 评分 + `backtest(df)` 回测
- 回测计算年化收益率、总收益率、胜率、最大回撤、交易次数
- 读取 `table.txt` 中的股票代码进行批量回测
- 自动剔除负收益个股，按年化收益率从高到低排列
- 回测报告导出为 CSV
- 速度模式控制（低/中/高）

```
python 量化选股回测工具策略版.py
```

### 数据转换模块

| 模块 | 功能 | 独立运行 |
|------|------|---------|
| `weekly_stock_data_download.py` | 日线→周线聚合 | `python weekly_stock_data_download.py` |
| `monthly_stock_data_download.py` | 日线→月线聚合 | `python monthly_stock_data_download.py` |
| `seasonly_stock_data_download.py` | 日线→季线聚合 | `python seasonly_stock_data_download.py` |
| `minute60_stock_data_download.py` | 60分线下载 | `python minute60_stock_data_download.py --cli` |

## 安装依赖

```bash
pip install baostock duckdb polars matplotlib numpy pyperclip psutil
```

Python 标准库依赖：tkinter, threading, queue, json, re, logging, datetime, pathlib, os, sys, ctypes, io, subprocess（无需额外安装）。

## 快速开始

1. **下载数据**：运行 `a_stock_download_manger.py`，选择周期，点击"更新股票列表"获取代码，然后下载
2. **浏览 K 线**：运行 `haobao_stock.py`，自动连接数据库，浏览股票
3. **选股回测**：运行 `量化选股回测工具策略版.py`，加载策略，选择周期，点击"回测"

## 策略开发

在 `stock_data/celue/` 目录下创建 `.py` 文件，实现以下三个函数即可被选股工具识别：

```python
def select_stock(df) -> bool:
    """选股判断，返回 True 表示选中"""
    # df: Polars DataFrame, 包含 date/open/high/low/close/volume 列

def score_stock(df) -> float:
    """评分 0-100，分数越高越好"""

def backtest(df) -> dict:
    """回测返回: annualized_return(年化), total_return, win_rate, num_trades, max_drawdown"""
```

参考 `stock_data/celue/MACD金叉策略.py`。

## 配置文件

| 文件 | 用途 |
|------|------|
| `stock_get_date_config.json` | 下载器参数 |
| `stock_data/stock_pakeing/config.json` | 选股工具参数（周期勾选、速度模式） |
| `table.txt` | 下载器/回测使用的股票代码列表 |
| `downloaded_stocks.json` | 断点续传记录 |
