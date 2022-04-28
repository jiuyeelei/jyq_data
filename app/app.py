#!/usr/bin/python3

import config as conf
from crawler.kline_crawler import KlineCrawler

if __name__ == "__main__":
    # kline = Kline()
    # kline.get_daily_kline("601318")

    kline_crawler = KlineCrawler()
    kline_crawler.start_craw()
