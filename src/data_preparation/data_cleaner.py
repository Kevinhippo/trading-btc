def clean_data(data):
    """清洗和预处理数据"""
    # 处理缺失值
    data = data.dropna()
    
    # 确保数据按时间排序
    data = data.sort_index()
    
    # 移除异常值（价格不可能为0）
    data = data[data['Close'] > 0]
    
    return data