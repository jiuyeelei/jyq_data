STK_TYPE_STOCK = 0x2100  # 普通股票
STK_TYPE_SMALL_BOARD = 0x2110  # 中小板
STK_TYPE_GM = 0x2120  # 创业板
STK_TYPE_TypeSTAR = 0x2130  # 科创板
FUND_TYPE_ETF = 0x3110  # ETF


def is_stock(stock_type):
    return (
        stock_type == STK_TYPE_STOCK
        or stock_type == STK_TYPE_SMALL_BOARD
        or stock_type == STK_TYPE_GM
        or stock_type == STK_TYPE_TypeSTAR
    )


def is_fund(stock_type):
    return stock_type == FUND_TYPE_ETF


FC_MARKET_TYPE_SH = 21  # 沪股通对应的市场代码
FC_MARKET_TYPE_SZ = 22  # 深股通对应的市场代码
