import time
import logging
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import threading
import queue
import numpy as np
from datetime import datetime
import matplotlib as mpl

# 设置支持中文的字体（使用macOS系统自带字体）
#mpl.rcParams['font.family'] = 'Heiti SC'  # 黑体
# 或
mpl.rcParams['font.family'] = 'Songti SC'  # 宋体
# 或
# mpl.rcParams['font.family'] = 'PingFang SC'  # 苹方

# 解决负号显示问题
mpl.rcParams['axes.unicode_minus'] = False

logger = logging.getLogger('quant_trading.monitor')

class RealTimeMonitor:
    def __init__(self, trading_engine):
        self.engine = trading_engine
        self.fig, self.axs = plt.subplots(2, 1, figsize=(14, 10))
        self.price_data = pd.DataFrame(columns=['timestamp', 'price'])
        self.equity_data = pd.DataFrame(columns=['timestamp', 'equity'])
        
        # 使用队列进行线程间通信
        self.data_queue = queue.Queue()
        self.running = True
        self.animation = None
        
        logger.info("实时监控初始化完成")
    
    def update_data(self):
        """更新监控数据 - 使用concat替代append"""
        try:
            # 获取当前价格
            current_price = self.engine.api.get_ticker_price(self.engine.symbol)
            current_time = datetime.now()
            
            # 创建新行DataFrame
            new_price_row = pd.DataFrame({
                'timestamp': [current_time],
                'price': [current_price]
            })
            
            # 更新价格数据 - 使用concat
            self.price_data = pd.concat(
                [self.price_data, new_price_row], 
                ignore_index=True
            ).tail(100)  # 保留最近100个点
            
            # 创建新行DataFrame
            new_equity_row = pd.DataFrame({
                'timestamp': [current_time],
                'equity': [self.engine.equity]
            })
            
            # 更新资产数据 - 使用concat
            self.equity_data = pd.concat(
                [self.equity_data, new_equity_row], 
                ignore_index=True
            ).tail(100)
            
            # 将数据放入队列供主线程使用
            self.data_queue.put({
                'price_data': self.price_data.copy(),
                'equity_data': self.equity_data.copy()
            })
            
            return True
        except Exception as e:
            logger.error(f"更新监控数据失败: {str(e)}")
            return False
    
    def _update_plot(self, frame):
        """更新监控图表 (在主线程中调用)"""
        try:
            # 从队列获取最新数据
            if not self.data_queue.empty():
                data = self.data_queue.get_nowait()
                self.price_data = data['price_data']
                self.equity_data = data['equity_data']
            
            # 清空图表
            for ax in self.axs:
                ax.clear()
            
            # 绘制价格图表
            if not self.price_data.empty and 'timestamp' in self.price_data.columns and 'price' in self.price_data.columns:
                self.axs[0].plot(self.price_data['timestamp'], self.price_data['price'], 'b-')
                self.axs[0].set_title(f'{self.engine.symbol} 价格')
                self.axs[0].set_ylabel('价格 (USDT)')
                self.axs[0].grid(True)
                
                # 标记交易点
                for trade in self.engine.trade_log[-10:]:  # 显示最近10笔交易
                    if trade['type'] == 'BUY':
                        self.axs[0].axvline(trade['timestamp'], color='g', alpha=0.3)
                    elif trade['type'] == 'SELL':
                        self.axs[0].axvline(trade['timestamp'], color='r', alpha=0.3)
            
            # 绘制资产图表
            if not self.equity_data.empty and 'timestamp' in self.equity_data.columns and 'equity' in self.equity_data.columns:
                self.axs[1].plot(self.equity_data['timestamp'], self.equity_data['equity'], 'g-')
                self.axs[1].set_title('账户资产')
                self.axs[1].set_ylabel('资产 (USDT)')
                self.axs[1].grid(True)
            
            plt.tight_layout()
            return self.axs
        
        except Exception as e:
            logger.error(f"更新图表失败: {str(e)}")
            return self.axs
    
    def data_collection_thread(self):
        """数据收集线程"""
        logger.info("启动数据收集线程...")
        while self.running:
            try:
                self.update_data()
                time.sleep(5)  # 每5秒更新一次数据
            except Exception as e:
                logger.error(f"数据收集线程错误: {str(e)}")
                time.sleep(10)
    
    def start(self):
        """启动实时监控"""
        logger.info("启动实时监控...")
        
        # 设置Matplotlib为非交互模式
        plt.ioff()
        
        # 启动数据收集线程
        data_thread = threading.Thread(target=self.data_collection_thread)
        data_thread.daemon = True
        data_thread.start()
        
        # 在主线程中启动动画
        self.animation = FuncAnimation(
            self.fig, self._update_plot, 
            interval=5000,  # 每5秒更新一次
            cache_frame_data=False
        )
        
        # 显示图表（这会阻塞主线程）
        plt.show(block=True)
        
        # 当窗口关闭时停止监控
        self.stop()
    
    def stop(self):
        """停止实时监控"""
        if self.running:
            logger.info("停止实时监控...")
            self.running = False
            if self.animation:
                self.animation.event_source.stop()
            plt.close(self.fig)