import logging
from .binance_api import BinanceAPI

logger = logging.getLogger('quant_trading.order_manager')

class OrderManager:
    def __init__(self, api: BinanceAPI):
        self.api = api
        self.open_orders = {}
        
    def execute_order(self, symbol, side, quantity):
        """执行交易订单"""
        try:
            # 在模拟环境中执行测试订单
            result = self.api.create_test_order(symbol, side, quantity)
            logger.info(f"订单执行: {side} {quantity} {symbol}")
            
            # 记录订单
            order_id = f"sim_{int(time.time() * 1000)}"
            self.open_orders[order_id] = {
                'symbol': symbol,
                'side': side,
                'quantity': quantity,
                'status': 'FILLED',
                'timestamp': datetime.now().isoformat()
            }
            
            return {
                'order_id': order_id,
                'status': 'FILLED',
                'executed_qty': quantity,
                'price': self.api.get_ticker_price(symbol)
            }
        except Exception as e:
            logger.error(f"订单执行失败: {str(e)}")
            return {
                'status': 'REJECTED',
                'error': str(e)
            }
    
    def get_order_status(self, order_id):
        """获取订单状态"""
        return self.open_orders.get(order_id, {'status': 'UNKNOWN'})
    
    def cancel_order(self, symbol, order_id):
        """取消订单"""
        if order_id in self.open_orders:
            self.open_orders[order_id]['status'] = 'CANCELED'
            logger.info(f"订单取消: {order_id}")
            return {'status': 'CANCELED'}
        return {'status': 'NOT_FOUND'}
    
    def get_open_orders(self, symbol=None):
        """获取当前挂单"""
        if symbol:
            return [order for order in self.open_orders.values() if order['symbol'] == symbol]
        return list(self.open_orders.values())