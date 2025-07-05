import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging

logger = logging.getLogger('quant_trading.performance')

class PerformanceAnalyzer:
    def __init__(self, backtest_data):
        self.data = backtest_data
        # 检查数据是否有效
        self._validate_data()
        
        # 计算收益率
        self.returns = self.calculate_returns()
        
    def _validate_data(self):
        """验证回测数据有效性"""
        if self.data is None:
            logger.error("回测数据为None")
            return
            
        if not isinstance(self.data, pd.DataFrame):
            logger.error(f"回测数据不是DataFrame: {type(self.data)}")
            return
            
        if self.data.empty:
            logger.warning("回测数据为空")
            return
            
        if 'total' not in self.data.columns:
            logger.error("回测数据缺少'total'列")
            # 尝试添加空列防止后续错误
            self.data['total'] = np.nan
        
    def calculate_returns(self):
        """计算每日收益率"""
        # 检查数据是否有效
        if self.data is None or self.data.empty or 'total' not in self.data.columns:
            logger.error("无法计算收益率: 数据无效")
            return pd.Series(dtype=float)
            
        try:
            # 确保'total'列是数值型
            self.data['total'] = pd.to_numeric(self.data['total'], errors='coerce')
            
            # 计算收益率
            self.data['daily_return'] = self.data['total'].pct_change()
            return self.data['daily_return']
        except Exception as e:
            logger.error(f"计算收益率失败: {str(e)}")
            return pd.Series(dtype=float)
    
    def calculate_sharpe_ratio(self, risk_free_rate=0.0):
        """计算夏普比率"""
        if self.returns.empty:
            logger.warning("无法计算夏普比率: 无收益率数据")
            return 0.0
            
        try:
            excess_returns = self.returns - risk_free_rate / 365
            if excess_returns.std() == 0:
                return 0.0
                
            sharpe_ratio = np.sqrt(365) * excess_returns.mean() / excess_returns.std()
            return sharpe_ratio
        except Exception as e:
            logger.error(f"计算夏普比率失败: {str(e)}")
            return 0.0
    
    def calculate_max_drawdown(self):
        """计算最大回撤"""
        if self.returns.empty:
            logger.warning("无法计算最大回撤: 无收益率数据")
            return 0.0
            
        try:
            cumulative_returns = (1 + self.returns).cumprod()
            peak = cumulative_returns.expanding(min_periods=1).max()
            drawdown = (cumulative_returns / peak) - 1
            return drawdown.min()
        except Exception as e:
            logger.error(f"计算最大回撤失败: {str(e)}")
            return 0.0
    
    def generate_performance_report(self):
        """生成绩效报告"""
        report = {}
        
        # 检查基本数据
        if self.data is None or self.data.empty:
            report["错误"] = "回测数据为空"
            return report
            
        if 'total' not in self.data.columns:
            report["错误"] = "回测数据缺少'total'列"
            return report
            
        if self.data['total'].isnull().all():
            report["错误"] = "'total'列全为空值"
            return report
            
        try:
            # 获取关键指标
            start_value = self.data['total'].iloc[0]
            end_value = self.data['total'].iloc[-1]
            
            if pd.isnull(start_value) or pd.isnull(end_value):
                report["错误"] = "净值数据包含空值"
                return report
                
            total_return = (end_value / start_value - 1) * 100
            sharpe = self.calculate_sharpe_ratio()
            max_dd = self.calculate_max_drawdown() * 100
            
            # 计算交易次数
            if 'signal' in self.data.columns:
                trade_signals = self.data['signal'].abs()
                trade_count = trade_signals[trade_signals > 0].count()
            else:
                trade_count = 0
            
            # 构建报告
            report = {
                "起始净值 (USDT)": round(start_value, 2),
                "结束净值 (USDT)": round(end_value, 2),
                "总收益率 (%)": round(total_return, 2),
                "年化夏普比率": round(sharpe, 2),
                "最大回撤 (%)": round(max_dd, 2),
                "总交易次数": trade_count,
                "数据起始日期": self.data.index[0].strftime('%Y-%m-%d') if not self.data.empty else "N/A",
                "数据结束日期": self.data.index[-1].strftime('%Y-%m-%d') if not self.data.empty else "N/A"
            }
            
            return report
        except Exception as e:
            logger.error(f"生成绩效报告失败: {str(e)}")
            return {"错误": f"生成绩效报告失败: {str(e)}"}
    
    def plot_results(self, save_path=None):
        """绘制回测结果图"""
        try:
            if self.data is None or self.data.empty:
                logger.error("无法绘图: 回测数据为空")
                return
                
            plt.figure(figsize=(14, 10))
            
            # 净值曲线 (确保有'total'列)
            if 'total' in self.data.columns:
                ax1 = plt.subplot(211)
                ax1.plot(self.data.index, self.data['total'], label='Portfolio Value', color='blue')
                ax1.set_title('Portfolio Value')
                ax1.set_ylabel('USDT')
                ax1.grid(True)
            
            # 价格曲线
            if 'Close' in self.data.columns:
                ax2 = plt.subplot(212) if 'total' in self.data.columns else plt.subplot(211)
                ax2.plot(self.data.index, self.data['Close'], label='BTC Price', color='purple')
                
                # 标记交易信号
                if 'signal' in self.data.columns:
                    buy_signals = self.data[self.data['signal'] == 1]
                    sell_signals = self.data[self.data['signal'] == -1]
                    
                    if not buy_signals.empty:
                        ax2.scatter(buy_signals.index, buy_signals['Close'], 
                                   marker='^', color='green', s=100, label='Buy')
                    if not sell_signals.empty:
                        ax2.scatter(sell_signals.index, sell_signals['Close'], 
                                   marker='v', color='red', s=100, label='Sell')
                
                ax2.set_title('BTC Price with Trade Signals')
                ax2.set_ylabel('Price (USDT)')
                ax2.legend()
                ax2.grid(True)
            
            plt.tight_layout()
            
            if save_path:
                plt.savefig(save_path)
                logger.info(f"结果图保存至: {save_path}")
            else:
                plt.show()
                
        except Exception as e:
            logger.error(f"绘图失败: {str(e)}")