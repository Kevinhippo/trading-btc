import pandas as pd
import numpy as np
import yaml
from src.utils.date_utils import validate_dates
from src.utils.logging_config import setup_logging
import logging

logger = logging.getLogger('quant_trading.backtest_engine')

class BacktestEngine:
    def __init__(self, data, strategy, config):
        self.data = data
        self.strategy = strategy
        self.config = config
        self.logger = setup_logging()
        self.results = None
        self.trade_count = 0
        self.final_value = 0.0
        
    def run_backtest(self):
        """执行回测"""
        try:
            logger.info("开始回测...")
            
            # 验证配置
            required_config_keys = ['backtest', 'strategy']
            for key in required_config_keys:
                if key not in self.config:
                    raise KeyError(f"配置文件中缺少必需的 '{key}' 部分")
            
            # 验证策略参数
            strategy_params = self.config['strategy'].get('parameters', {})
            if not strategy_params:
                logger.warning("策略参数为空，使用默认设置")
            
            # 设置默认交易量
            trade_amount = strategy_params.get('trade_amount', 0.01)  # 默认0.01 BTC
            
            # 验证日期
            start_date = pd.to_datetime(self.config['backtest']['start_date'])
            end_date = pd.to_datetime(self.config['backtest']['end_date'])
            start_date, end_date = validate_dates(start_date, end_date)
            logger.info(f"回测日期范围: {start_date} 至 {end_date}")
            
            # 筛选数据
            self.data = self.data[(self.data.index >= start_date) & (self.data.index <= end_date)]
            logger.info(f"筛选后数据行数: {len(self.data)}")
            
            # 检查是否有数据
            if self.data.empty:
                logger.error("筛选后数据为空，无法回测")
                # 创建空的结果DataFrame，但包含必要的列
                empty_results = pd.DataFrame(columns=[
                    'position', 'cash', 'holdings', 'total', 'signal'
                ])
                self.results = empty_results
                return empty_results
            
            # 检查必需的数据列
            required_columns = ['Open', 'High', 'Low', 'Close']
            missing_columns = [col for col in required_columns if col not in self.data.columns]
            if missing_columns:
                raise ValueError(f"数据缺少必需列: {missing_columns}")
            
            # 将筛选后的数据设置到策略
            self.strategy.set_data(self.data)
            
            # 生成交易信号
            self.data = self.strategy.generate_signals()
            
            # 检查信号列
            if 'signal' not in self.data.columns:
                raise ValueError("策略未生成'signal'列")
            
            # 初始化仓位和账户列
            self.data['position'] = 0
            initial_capital = float(self.config['backtest']['initial_capital'])
            self.data['cash'] = initial_capital
            self.data['total'] = initial_capital
            self.data['holdings'] = 0
            
            cash = initial_capital
            position = 0
            commission = float(self.config['backtest']['commission'])
            self.trade_count = 0
            
            logger.info(f"初始资金: {cash:.2f} USDT, 手续费率: {commission*100:.2f}%, 交易量: {trade_amount} BTC")
            
            # 初始化最终净值
            self.final_value = cash
            
            # 迭代处理每一行数据
            for i, row in self.data.iterrows():
                current_price = row['Close']
                
                # 执行买入信号
                if row['signal'] == 1 and cash > 0:
                    # 计算可买数量
                    buy_qty = min(trade_amount, cash / current_price)
                    cost = buy_qty * current_price * (1 + commission)
                    
                    if cost <= cash:
                        position += buy_qty
                        cash -= cost
                        self.trade_count += 1
                        logger.debug(f"{i} - 买入 {buy_qty:.6f} BTC @ {current_price:.2f}, 成本: {cost:.2f}")
                
                # 执行卖出信号
                elif row['signal'] == -1 and position > 0:
                    sell_qty = min(position, trade_amount)
                    proceeds = sell_qty * current_price * (1 - commission)
                    
                    position -= sell_qty
                    cash += proceeds
                    self.trade_count += 1
                    logger.debug(f"{i} - 卖出 {sell_qty:.6f} BTC @ {current_price:.2f}, 收入: {proceeds:.2f}")
                
                # 更新账户价值
                holdings_value = position * current_price
                total_value = cash + holdings_value
                self.final_value = total_value
                
                # 更新DataFrame
                self.data.at[i, 'position'] = position
                self.data.at[i, 'cash'] = cash
                self.data.at[i, 'holdings'] = holdings_value
                self.data.at[i, 'total'] = total_value
            
            self.results = self.data
            logger.info(f"回测完成，总交易次数: {self.trade_count}, 最终净值: {self.final_value:.2f} USDT")
            return self.data
            
        except Exception as e:
            logger.error(f"回测过程中发生错误: {str(e)}", exc_info=True)
            # 创建包含'total'列的空DataFrame
            error_df = pd.DataFrame(columns=['total'])
            return error_df
    
    def get_results(self):
        """获取回测结果"""
        return self.results
    
    def get_performance_summary(self):
        """获取回测性能摘要"""
        if self.results is None or self.results.empty:
            return {}
            
        initial_capital = float(self.config['backtest']['initial_capital'])
        final_value = self.final_value
        total_return = (final_value / initial_capital - 1) * 100
        
        return {
            "初始资金 (USDT)": initial_capital,
            "最终净值 (USDT)": final_value,
            "总收益率 (%)": total_return,
            "总交易次数": self.trade_count
        }