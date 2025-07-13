import pandas as pd
import numpy as np
import logging
from src.strategy_development.strategy import StrategyBase

logger = logging.getLogger('quant_trading.macd_strategy')

class MACDStrategy(StrategyBase):
    def __init__(self, params):
        super().__init__("MACD 4小时策略")
        self.set_parameters(**params)
        logger.info(f"MACD策略初始化: 快线周期={params['fast_period']}, 慢线周期={params['slow_period']}, 信号周期={params['signal_period']}")
        
    def calculate_macd(self, data):
        """计算MACD指标"""
        fast_period = self.params['fast_period']
        slow_period = self.params['slow_period']
        signal_period = self.params['signal_period']
        
        # 计算指数移动平均线
        data['ema_fast'] = data['close'].ewm(span=fast_period, adjust=False).mean()
        data['ema_slow'] = data['close'].ewm(span=slow_period, adjust=False).mean()
        
        # 计算MACD线和信号线
        data['macd'] = data['ema_fast'] - data['ema_slow']
        data['signal_line'] = data['macd'].ewm(span=signal_period, adjust=False).mean()
        data['histogram'] = data['macd'] - data['signal_line']
        
        logger.debug("MACD指标计算完成")
        return data

    def generate_signals(self):
        """
        生成交易信号（实现基类抽象方法）
        返回包含信号列的DataFrame
        """
        if self.data is None:
            raise ValueError("数据未设置。请先调用set_data()")
            
        logger.info("开始生成MACD交易信号...")
        
        # 计算MACD指标
        data = self.calculate_macd(self.data.copy())
        
        # 初始化信号列
        data['signal'] = 0
        
        # 生成交易信号
        # MACD上穿信号线（金叉） - 买入信号
        buy_condition = (data['macd'] > data['signal_line']) & (data['macd'].shift(1) <= data['signal_line'].shift(1))
        data.loc[buy_condition, 'signal'] = 1
        logger.info(f"发现 {buy_condition.sum()} 个买入信号")
        
        # MACD下穿信号线（死叉） - 卖出信号
        sell_condition = (data['macd'] < data['signal_line']) & (data['macd'].shift(1) >= data['signal_line'].shift(1))
        data.loc[sell_condition, 'signal'] = -1
        logger.info(f"发现 {sell_condition.sum()} 个卖出信号")
        
        # 更新数据
        self.data = data
        logger.info("交易信号生成完成")
        return data