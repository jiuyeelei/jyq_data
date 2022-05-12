#!/usr/bin/python3

from crawler.kline_crawler import KlineCrawler
from calculator.kline_cal import KlineCal
from db.kline import KlineModel
from db.stock_list import StockList
from logger.setting import jyqlogger
import sys

if __name__ == "__main__":
    num = len(sys.argv) - 1
    if num < 2:
        exit("参数错误 app.." + sys.argv[2])

    func = sys.argv[2]
    if func == "get_kline":
        code = sys.argv[3]
        kline = KlineModel()
        kline.get_daily_kline("601318")
    elif func == "craw":
        kline_crawler = KlineCrawler()
        kline_crawler.start_craw()
    elif func == "cal":
        kline_cal = KlineCal()
        kline_cal.dataframe_from_mysql("000001", 1)
    elif func == "sync_stocks":
        jyqlogger.info("start to sync stock list")
        stock_list = StockList()
        stock_list.save_stocklist()
