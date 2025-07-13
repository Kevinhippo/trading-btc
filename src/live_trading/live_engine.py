import time
import logging
import pandas as pd
import threading
from datetime import datetime, timedelta
from .binance_api import BinanceAPI
from .order_manager import OrderManager
from src.strategy_development.macd_strategy import MACDStrategy

logger = logging.getLogger('quant_trading.live_engine')

class LiveTradingEngine:
    def __init__(self, config, strategy_class=MACDStrategy):
        # 加载配置
        self.symbol = config['symbol']
        self.timeframe = config['timeframe']
        self.testnet = config.get('testnet', True)
        self.api_key = config['api_key']
        self.api_secret = config['api_secret']
        self.strategy_params = config['strategy_params']
        self.trade_amount = config.get('trade_amount', 0.01)
        
        # 初始化API和订单管理器
        self.api = BinanceAPI(self.api_key, self.api_secret, self.testnet)
        self.order_manager = OrderManager(self.api)
        
        # 初始化策略
        self.strategy = strategy_class(self.strategy_params)
        
        # 初始化状态
        self.running = False
        self.last_candle_time = None
        self.position = 0
        self.cash = config.get('initial_capital', 10000)
        self.equity = self.cash
        self.trade_log = []
        self.data_lock = threading.Lock()
        self.trade_lock = threading.Lock()


        logger.info("实盘交易引擎初始化完成")
        
    def fetch_data(self):
        """获取最新的K线数据"""
        try:
            # 获取最新数据
            with self.data_lock:
                df = self.api.get_klines(self.symbol, self.timeframe, limit=100)
            
            # 检查是否有新数据
            if self.last_candle_time is None:
                self.last_candle_time = df.index[-1]
                return df
            
            if df.index[-1] > self.last_candle_time:
                self.last_candle_time = df.index[-1]
                logger.info(f"新K线数据: {self.last_candle_time}")
                return df
            
            return None
        except Exception as e:
            logger.error(f"获取数据失败: {str(e)}")
            return None
        
    def run_strategy(self, data):
        """运行策略生成信号"""
        self.strategy.set_data(data)
        signals = self.strategy.generate_signals()
        
        # 获取最新信号
        last_signal = signals.iloc[-1]['signal']
        return last_signal
    
    def execute_trade(self, signal):
        """根据信号执行交易"""
        with self.trade_lock:
            current_price = self.api.get_ticker_price(self.symbol)
            
            # 买入信号
            if signal == 1 and self.cash > 0:
                # 计算可买数量
                max_qty = self.cash / current_price
                trade_qty = min(self.trade_amount, max_qty)
                
                # 执行订单
                order_result = self.order_manager.execute_order(
                    self.symbol, "BUY", trade_qty
                )
                
                if order_result['status'] == 'FILLED':
                    self.position += trade_qty
                    self.cash -= trade_qty * current_price
                    self.equity = self.cash + self.position * current_price
                    
                    # 记录交易
                    self.trade_log.append({
                        'timestamp': datetime.now(),
                        'type': 'BUY',
                        'quantity': trade_qty,
                        'price': current_price,
                        'equity': self.equity
                    })
                    return True
            
            # 卖出信号
            elif signal == -1 and self.position > 0:
                trade_qty = min(self.position, self.trade_amount)
                
                # 执行订单
                order_result = self.order_manager.execute_order(
                    self.symbol, "SELL", trade_qty
                )
                
                if order_result['status'] == 'FILLED':
                    self.position -= trade_qty
                    self.cash += trade_qty * current_price
                    self.equity = self.cash + self.position * current_price
                    
                    # 记录交易
                    self.trade_log.append({
                        'timestamp': datetime.now(),
                        'type': 'SELL',
                        'quantity': trade_qty,
                        'price': current_price,
                        'equity': self.equity
                    })
                    return True
            
            return False
    
    def monitor_position(self):
        """监控仓位和风险"""
        with self.data_lock:
            # 获取当前价格
            current_price = self.api.get_ticker_price(self.symbol)
            
            # 计算当前市值
            position_value = self.position * current_price
            portfolio_value = self.cash + position_value
            
            # 计算风险指标
            position_percent = (position_value / portfolio_value) * 100
            
            # 记录监控数据
            logger.info(
                f"账户监控: 现金={self.cash:.2f} | "
                f"仓位={self.position:.6f} | "
                f"仓位价值={position_value:.2f} | "
                f"总资产={portfolio_value:.2f} | "
                f"仓位占比={position_percent:.2f}%"
            )
            
            return {
                'timestamp': datetime.now(),
                'cash': self.cash,
                'position': self.position,
                'position_value': position_value,
                'portfolio_value': portfolio_value,
                'position_percent': position_percent
            }
    
    def start(self):
        """启动实盘交易"""
        self.running = True
        logger.info("启动实盘交易...")
        
        # 初始化仓位
        self.monitor_position()
        
        try:
            # 主循环
            while self.running:
                try:
                    # 获取数据
                    data = self.fetch_data()
                    
                    if data is not None:
                        # 运行策略
                        signal = self.run_strategy(data)
                        
                        # 执行交易
                        if signal != 0:
                            self.execute_trade(signal)
                        
                        # 监控仓位
                        self.monitor_position()
                    
                    # 等待下一个K线周期
                    interval_seconds = self.get_interval_seconds()
                    sleep_time = max(5, interval_seconds / 2)  # 每半周期检查一次
                    time.sleep(sleep_time)
                    
                except Exception as e:
                    logger.error(f"交易循环错误: {str(e)}", exc_info=True)
                    time.sleep(60)  # 出错后等待1分钟
        except KeyboardInterrupt:
            self.stop()
    
    def stop(self):
        """停止实盘交易"""
        self.running = False
        logger.info("交易已停止")
        
        # 保存交易日志
        trade_df = pd.DataFrame(self.trade_log)
        if not trade_df.empty:
            trade_df.to_csv("live_trading_log.csv", index=False)
            logger.info("交易日志已保存")
    
    def get_interval_seconds(self):
        """将时间间隔转换为秒数"""
        interval_map = {
            '1m': 60,
            '3m': 180,
            '5m': 300,
            '15m': 900,
            '30m': 1800,
            '1h': 3600,
            '2h': 7200,
            '4h': 14400,
            '6h': 21600,
            '8h': 28800,
            '12h': 43200,
            '1d': 86400,
            '3d': 259200,
            '1w': 604800,
            '1M': 2592000  # 近似值
        }
        return interval_map.get(self.timeframe, 3600)  # 默认1小时