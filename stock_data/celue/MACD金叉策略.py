"""
MACD金叉选股+回测策略
策略逻辑：
  选股: MACD金叉(DIF上穿DEA) + 股价在MA60上方 + MACD柱为正
  回测: 金叉买入 → 死叉卖出, 计算年化收益率/胜率/最大回撤
"""

import numpy as np


def select_stock(df):
    """选股函数: 判断当前是否出现MACD金叉买点"""
    if df.height < 60:
        return False

    close = df['close'].to_numpy()
    dif, dea, macd = _calc_macd(close)

    ma60 = _sma(close, 60)
    cur_close = float(close[-1])

    # 金叉条件: DIF上穿DEA + 股价在MA60之上 + MACD柱翻红
    if (dif[-2] <= dea[-2] and dif[-1] > dea[-1]
            and cur_close > ma60[-1]
            and macd[-1] > 0):
        return True
    return False


def score_stock(df):
    """评分函数: 基于MACD形态综合打分(0-100)"""
    if df.height < 60:
        return 0

    close = df['close'].to_numpy()
    dif, dea, macd = _calc_macd(close)
    ma60 = _sma(close, 60)

    score = 50.0

    cur_close = float(close[-1])
    cur_macd = float(macd[-1])
    cur_dif = float(dif[-1])

    # MACD柱强度
    if cur_macd > 0:
        score += 10
    if cur_macd > 0.5:
        score += 10
    if cur_macd > 1.0:
        score += 10

    # DIF位置
    if cur_dif > 0:
        score += 10
    if dif[-1] > dif[-2]:
        score += 5  # DIF向上

    # 趋势强度: 股价在MA60上方越远越好
    if cur_close > ma60[-1]:
        ratio = (cur_close - ma60[-1]) / ma60[-1]
        score += min(15, ratio * 100)

    return min(100, max(0, score))


def backtest(df):
    """
    回测函数: MACD金叉买入→死叉卖出
    返回 dict:
      annualized_return  - 年化收益率
      total_return       - 总收益率
      win_rate           - 胜率
      num_trades         - 交易次数
      max_drawdown       - 最大回撤
    """
    if df.height < 60:
        return _empty_result()

    close = df['close'].to_numpy()
    dif, dea, _ = _calc_macd(close)

    trades = []
    in_pos = False
    entry_price = 0.0
    entry_idx = 0
    equity = [1.0]

    for i in range(1, len(close)):
        if not in_pos and dif[i-1] <= dea[i-1] and dif[i] > dea[i]:
            in_pos = True
            entry_price = float(close[i])
            entry_idx = i

        if in_pos and i > entry_idx:
            daily_ret = (float(close[i]) - float(close[i-1])) / float(close[i-1])
            equity.append(equity[-1] * (1 + daily_ret))

        if in_pos and dif[i-1] >= dea[i-1] and dif[i] < dea[i]:
            exit_price = float(close[i])
            ret = (exit_price - entry_price) / entry_price
            trades.append(ret)
            in_pos = False

    if in_pos:
        exit_price = float(close[-1])
        ret = (exit_price - entry_price) / entry_price
        trades.append(ret)

    if not trades:
        return _empty_result()

    total_ret = equity[-1] - 1
    days = len(close)
    annual_ret = (1 + total_ret) ** (252 / days) - 1 if days > 0 else 0
    win_rate = sum(1 for t in trades if t > 0) / len(trades) if trades else 0

    peak = 1.0
    max_dd = 0.0
    for v in equity:
        if v > peak:
            peak = v
        dd = (peak - v) / peak
        if dd > max_dd:
            max_dd = dd

    return {
        'annualized_return': float(annual_ret),
        'total_return': float(total_ret),
        'win_rate': float(win_rate),
        'num_trades': len(trades),
        'max_drawdown': float(max_dd),
    }


# ---- 内部工具函数 ----

def _calc_macd(close, fast=12, slow=26, signal=9):
    """计算MACD指标, 返回 (dif, dea, macd)"""
    ema_fast = _ema(close, fast)
    ema_slow = _ema(close, slow)
    dif = ema_fast - ema_slow
    dea = _ema(dif, signal)
    macd = 2 * (dif - dea)
    return dif, dea, macd


def _ema(data, period):
    """指数移动平均"""
    result = np.empty_like(data)
    alpha = 2 / (period + 1)
    result[0] = data[0]
    for i in range(1, len(data)):
        result[i] = alpha * data[i] + (1 - alpha) * result[i-1]
    return result


def _sma(data, period):
    """简单移动平均"""
    result = np.full_like(data, np.nan)
    for i in range(period - 1, len(data)):
        result[i] = np.mean(data[i - period + 1:i + 1])
    return result


def _empty_result():
    return {
        'annualized_return': 0,
        'total_return': 0,
        'win_rate': 0,
        'num_trades': 0,
        'max_drawdown': 0,
    }
