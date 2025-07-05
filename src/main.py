import os
import yaml
import pandas as pd
from src.data_preparation.data_fetcher import fetch_btc_data
from src.data_preparation.data_cleaner import clean_data
from src.strategy_development.macd_strategy import MACDStrategy
from src.backtesting.backtest_engine import BacktestEngine
from src.backtesting.performance import PerformanceAnalyzer
from src.utils.logging_config import setup_logging

def main():
    # 设置日志
    logger = setup_logging()
    logger.info("Starting BTC MACD 4H Strategy Backtest")
    
    # 加载配置
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'macd_config.yaml')
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info("配置文件加载成功")
        
        # 验证配置结构
        if 'strategy' not in config:
            logger.warning("配置缺少'strategy'部分，添加默认结构")
            config['strategy'] = {}
        
        if 'parameters' not in config['strategy']:
            logger.warning("策略参数缺失，添加默认参数")
            config['strategy']['parameters'] = {
                'fast_period': 12,
                'slow_period': 26,
                'signal_period': 9,
                'trade_amount': 0.1
            }
        
        # 确保有trade_amount参数
        if 'trade_amount' not in config['strategy']['parameters']:
            logger.warning("策略参数缺少'trade_amount'，设置默认值0.1")
            config['strategy']['parameters']['trade_amount'] = 0.1
            
        # 验证回测配置
        if 'backtest' not in config:
            logger.warning("配置缺少'backtest'部分，添加默认结构")
            config['backtest'] = {}
            
        # 设置默认回测参数
        defaults = {
            'start_date': '2020-01-01',
            'end_date': '2023-12-31',
            'initial_capital': 10000.0,
            'commission': 0.001
        }
        
        for key, default_value in defaults.items():
            if key not in config['backtest']:
                logger.warning(f"回测配置缺少'{key}'，设置默认值: {default_value}")
                config['backtest'][key] = default_value
                
    except Exception as e:
        logger.error(f"加载配置文件失败: {str(e)}")
        return
    
    # 获取并准备数据
    logger.info("Fetching and preparing data...")
    data_file = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'BTCUSDT_4h.csv')
    
    try:
        raw_data = fetch_btc_data(data_file)
        cleaned_data = clean_data(raw_data)
        logger.info(f"数据准备完成。行数: {len(cleaned_data)}")
        
        # 检查数据是否有效
        if cleaned_data.empty:
            logger.error("清洗后数据为空，无法继续")
            return
            
        required_columns = ['Open', 'High', 'Low', 'Close']
        missing_columns = [col for col in required_columns if col not in cleaned_data.columns]
        if missing_columns:
            logger.error(f"数据缺少必需列: {missing_columns}")
            return
            
        # 记录数据日期范围
        logger.info(f"数据日期范围: {cleaned_data.index.min()} 至 {cleaned_data.index.max()}")
            
    except Exception as e:
        logger.error(f"数据准备失败: {str(e)}")
        return
    
    # 初始化策略
    try:
        strategy_params = config['strategy']['parameters']
        logger.info(f"策略参数: {strategy_params}")
        
        strategy = MACDStrategy(strategy_params)
        strategy.set_data(cleaned_data)
        logger.info("策略初始化完成")
    except Exception as e:
        logger.error(f"策略初始化失败: {str(e)}")
        return
    
    # 运行回测
    logger.info("Running backtest...")
    try:
        backtester = BacktestEngine(cleaned_data, strategy, config)
        results = backtester.run_backtest()
        
        # 检查回测结果
        if results is None:
            logger.error("回测返回None结果")
            return
            
        if results.empty:
            logger.warning("回测结果为空")
        else:
            logger.info(f"回测完成，结果行数: {len(results)}")
            
            # 检查是否有'total'列
            if 'total' not in results.columns:
                logger.error("回测结果缺少'total'列")
            else:
                logger.info(f"回测净值范围: {results['total'].min():.2f} - {results['total'].max():.2f} USDT")
                
                # 检查信号列
                if 'signal' in results.columns:
                    buy_signals = results[results['signal'] == 1]
                    sell_signals = results[results['signal'] == -1]
                    logger.info(f"买入信号数: {len(buy_signals)}, 卖出信号数: {len(sell_signals)}")
    except Exception as e:
        logger.error(f"回测失败: {str(e)}")
        return
    
    # 分析绩效
    logger.info("Analyzing performance...")
    try:
        analyzer = PerformanceAnalyzer(results)
        report = analyzer.generate_performance_report()
        
        # 打印结果
        print("\n===== Backtest Results =====")
        for key, value in report.items():
            print(f"{key}: {value}")
        
        # 保存结果并绘图
        results_dir = os.path.join(os.path.dirname(__file__), '..', 'results')
        os.makedirs(results_dir, exist_ok=True)
        
        results_file = os.path.join(results_dir, 'backtest_results.csv')
        results.to_csv(results_file, index=False)
        logger.info(f"回测结果保存至: {results_file}")
        
        plot_file = os.path.join(results_dir, 'backtest_plot.png')
        analyzer.plot_results(save_path=plot_file)
        logger.info(f"回测图表保存至: {plot_file}")
        
    except Exception as e:
        logger.error(f"绩效分析失败: {str(e)}")

if __name__ == "__main__":
    main()