import hmac
import hashlib
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
from urllib.parse import urlencode

logger = logging.getLogger('quant_trading.binance_api')

class BinanceAPI:
    BASE_URL = "https://api.binance.com"
    TESTNET_URL = "https://testnet.binance.vision"
    
    def __init__(self, api_key, api_secret, testnet=False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.base_url = self.TESTNET_URL if testnet else self.BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            "X-MBX-APIKEY": self.api_key
        })
        
    def _sign_request(self, params):
        """对请求进行签名"""
        query_string = urlencode(params)
        signature = hmac.new(
            self.api_secret.encode('utf-8'), 
            query_string.encode('utf-8'), 
            hashlib.sha256
        ).hexdigest()
        params['signature'] = signature
        return params
    
    def _send_request(self, method, endpoint, params=None, signed=False):
        """发送API请求"""
        url = f"{self.base_url}{endpoint}"
        
        if signed and params is None:
            params = {}
        
        if signed:
            params['timestamp'] = int(time.time() * 1000)
            params = self._sign_request(params)
        
        if method == "GET":
            response = self.session.get(url, params=params)
        elif method == "POST":
            response = self.session.post(url, params=params)
        elif method == "DELETE":
            response = self.session.delete(url, params=params)
        else:
            raise ValueError(f"不支持的HTTP方法: {method}")
        
        response.raise_for_status()
        return response.json()
    
    def get_klines(self, symbol, interval, limit=500):
        """获取K线数据"""
        endpoint = "/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        
        data = self._send_request("GET", endpoint, params)
        
        # 转换数据格式
        columns = [
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades', 'taker_buy_asset_volume',
            'taker_buy_quote_volume', 'ignore'
        ]
        
        df = pd.DataFrame(data, columns=columns)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        
        # 转换数据类型
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
        
        return df[['open', 'high', 'low', 'close', 'volume']]
    
    def get_account_info(self):
        """获取账户信息"""
        endpoint = "/api/v3/account"
        return self._send_request("GET", endpoint, signed=True)
    
    def create_test_order(self, symbol, side, quantity, order_type="MARKET"):
        """创建测试订单（模拟交易）"""
        endpoint = "/api/v3/order/test"
        params = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": quantity
        }
        return self._send_request("POST", endpoint, params, signed=True)
    
    def get_open_orders(self, symbol=None):
        """获取当前挂单"""
        endpoint = "/api/v3/openOrders"
        params = {}
        if symbol:
            params['symbol'] = symbol
        return self._send_request("GET", endpoint, params, signed=True)
    
    def get_order_book(self, symbol, limit=10):
        """获取订单簿"""
        endpoint = "/api/v3/depth"
        params = {
            "symbol": symbol,
            "limit": limit
        }
        return self._send_request("GET", endpoint, params)
    
    def get_ticker_price(self, symbol):
        """获取当前价格"""
        endpoint = "/api/v3/ticker/price"
        params = {"symbol": symbol}
        data = self._send_request("GET", endpoint, params)
        return float(data['price'])