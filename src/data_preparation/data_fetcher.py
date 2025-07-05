
import pandas as pd
from binance.client import Client
import os

def fetch_btc_data(file_path):
    """
    从CSV文件获取BTC历史数据
    实际应用中可替换为API调用（如CCXT、Binance API等）
    """
     # 初始化客户端（无需API密钥即可获取公共数据）
    client = Client()

    # 定义参数
    symbol = "BTCUSDT"
    interval = Client.KLINE_INTERVAL_4HOUR  # 4小时K线
    limit = 1000  # 单次最多1000条（按需调整）

    # 获取K线数据
    klines = client.get_klines(
        symbol=symbol,
        interval=interval,
        limit=limit
    )

    # 转换为DataFrame
    columns = [
        "Open Time", "Open", "High", "Low", "Close", "Volume",
        "Close Time", "Quote Volume", "Trades",
        "Taker Buy Base", "Taker Buy Quote", "Ignore"
    ]
    df = pd.DataFrame(klines, columns=columns)

    # 转换时间戳为可读格式
    df["Open Time"] = pd.to_datetime(df["Open Time"], unit="ms")
    df["Close Time"] = pd.to_datetime(df["Close Time"], unit="ms")

    # 保存为CSV
    df.to_csv("data/raw/BTCUSDT_4h.csv", index=False)
    print("数据已保存至 BTCUSDT_4h.csv")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"数据文件不存在: {file_path}")
    
    data = pd.read_csv(file_path, parse_dates=['Open Time'])
    data.set_index('Open Time', inplace=True)
    
    # 确保有必要的列
    required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    if not all(col in data.columns for col in required_columns):
        raise ValueError("数据文件缺少必要的列")
    
    return data