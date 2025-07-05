import abc
import pandas as pd
import logging

logger = logging.getLogger('quant_trading.strategy_base')

class StrategyBase(abc.ABC):
    """
    策略基类，定义量化策略的统一接口
    所有具体策略都应继承此类并实现抽象方法
    """
    def __init__(self, name):
        self.name = name
        self.params = {}
        self.data = None
        
    def set_parameters(self, **kwargs):
        """设置策略参数"""
        for key, value in kwargs.items():
            self.params[key] = value
    
    def set_data(self, data):
        """设置策略所需数据"""
        self.data = data.copy()
        logger.info(f"策略 {self.name} 数据已设置，行数: {len(self.data)}")
        
    @abc.abstractmethod
    def generate_signals(self):
        """
        抽象方法：生成交易信号
        子类必须实现此方法
        
        返回:
            pd.DataFrame - 包含'signal'列的数据框:
                signal = 1  买入信号
                signal = -1 卖出信号
                signal = 0  无信号
        """
        pass
    
    def calculate_returns(self):
        """计算策略收益率"""
        if self.data is None or 'signal' not in self.data.columns:
            raise ValueError("数据未设置或未生成信号")
            
        # 计算每日收益率
        self.data['returns'] = self.data['Close'].pct_change()
        
        # 计算策略收益率（基于信号）
        self.data['strategy_returns'] = self.data['signal'].shift(1) * self.data['returns']
        
        logger.info("策略收益率计算完成")
        return self.data
    
    def plot_signals(self, save_path=None):
        """绘制价格和交易信号"""
        if self.data is None or 'signal' not in self.data.columns:
            raise ValueError("数据未设置或未生成信号")
            
        import matplotlib.pyplot as plt
        
        plt.figure(figsize=(14, 10))
        
        # 价格曲线
        ax1 = plt.subplot(211)
        ax1.plot(self.data.index, self.data['Close'], label='价格', color='blue')
        
        # 标记买入信号
        buy_signals = self.data[self.data['signal'] == 1]
        ax1.scatter(buy_signals.index, buy_signals['Close'], 
                   marker='^', color='green', s=100, label='买入')
        
        # 标记卖出信号
        sell_signals = self.data[self.data['signal'] == -1]
        ax1.scatter(sell_signals.index, sell_signals['Close'], 
                   marker='v', color='red', s=100, label='卖出')
        
        ax1.set_title(f'{self.name} - 交易信号')
        ax1.legend()
        ax1.grid(True)
        
        # 策略收益率
        if 'strategy_returns' in self.data.columns:
            ax2 = plt.subplot(212)
            cumulative_returns = (1 + self.data['strategy_returns']).cumprod() - 1
            ax2.plot(cumulative_returns.index, cumulative_returns, 
                    label='累计收益', color='purple')
            ax2.set_title('策略累计收益')
            ax2.grid(True)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path)
            logger.info(f"信号图保存至: {save_path}")
        else:
            plt.show()