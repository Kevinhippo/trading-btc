def validate_dates(start_date, end_date):
    """验证日期格式并确保结束日期在开始日期之后"""
    if start_date >= end_date:
        raise ValueError("开始日期必须在结束日期之前")
    return start_date, end_date