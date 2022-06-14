from datetime import datetime, timedelta
from importlib.resources import contents
import time
from sqlalchemy import desc, false
from helper.db_helper import DBHelper
from helper.utils import getNowYYYYMMDD
from config.config import config
from logger.setting import jyqlogger
import json
import asyncio
from pyppeteer import launch


class FCapitalCrawler:
    """Foreign Capital like north financial crawler"""

    def __init__(self) -> None:
        self.db_helper = DBHelper(config)

    def start_crawfc_dailystat(self):
        asyncio.get_event_loop().run_until_complete(self.craw_daily_stat())

    async def craw_daily_stat(self):
        """crawler daily stat of foreign capital from HK exchange site."""
        browser = await launch()

        startDate = await self.getLastFCapitalDate()
        endDate = time.strftime("%Y%m%d", datetime.now().timetuple())
        targetDates = []
        # 大于数据库里最新一条的交易日的日期的数据都加到targetDates这个数组
        while True:
            startDate += timedelta(days=1)
            date = time.strftime("%Y%m%d", startDate.timetuple())

            if int(endDate) <= int(date):

                # 如果当前的时间>=15.30 则把当天的日期也加入到targetDates
                nowDateStr = time.strftime("%M%H%s", datetime.now().timetuple())
                if endDate == date and int(nowDateStr) >= 153000:
                    jyqlogger.info(
                        "current is later after 15:30:00, add current date into target date array."
                    )
                    targetDates.append(endDate)

                break
            else:
                targetDates.append(date)

        for idx, d in enumerate(targetDates):
            page = await browser.newPage()
            url = f"https://www.hkex.com.hk/chi/csm/DailyStat/data_tab_daily_{d}c.js"
            jyqlogger.info("the target data is {}, the target url is {}", d, url)
            resp = await page.goto(url)
            if resp.status == 200 and f"data_tab_daily_{d}c.js" in resp.url:
                body = await resp.text()
                contents = body[10:]  # 页面有效的内容
                datas = json.loads(contents.replace("\\", "\\\\"))

                insert_datas = []

                for d in datas:
                    tradeDate = d["date"]
                    mkt = d["market"]
                    values = d["content"][0]["table"]["tr"]
                    turnover = float(
                        values[0]["td"][0][0].replace(",", "").replace("-", "0")
                    )
                    buyTrades = float(
                        values[1]["td"][0][0].replace(",", "").replace("-", "0")
                    )
                    sellTrades = float(
                        values[2]["td"][0][0].replace(",", "").replace("-", "0")
                    )
                    sumBSAmt = int(
                        values[3]["td"][0][0].replace(",", "").replace("-", "0")
                    )
                    buyAmt = int(
                        values[4]["td"][0][0].replace(",", "").replace("-", "0")
                    )
                    sellAmt = int(
                        values[5]["td"][0][0].replace(",", "").replace("-", "0")
                    )
                    dailyQuotaBalance = (
                        0
                        if not len(values) >= 7
                        else float(
                            values[6]["td"][0][0].replace(",", "").replace("-", "0")
                        )
                    )
                    dailyQuotaBalanceR = (
                        0
                        if not len(values) >= 8
                        else float(
                            values[7]["td"][0][0].replace(",", "").replace("-", "0")
                        )
                    )

                    insert_datas.append(
                        [
                            mkt,
                            tradeDate,
                            turnover,
                            buyTrades,
                            sellTrades,
                            sumBSAmt,
                            buyAmt,
                            sellAmt,
                            dailyQuotaBalance,
                            dailyQuotaBalanceR,
                        ]
                    )
                jyqlogger.info("the insert data is {}", insert_datas)
                if len(insert_datas) > 0:
                    await self.insert_datas(insert_datas)
            else:
                jyqlogger.warning("data: {} not exists", d)

            if not page.isClosed():
                await page.close()
                jyqlogger.info("close current page.")

            if idx == len(targetDates) - 1:
                await browser.close()
                jyqlogger.info("the browser is closed.")

    async def insert_datas(self, insert_datas):
        db_conn = self.db_helper.get_conn()
        cursor = db_conn.cursor()
        insert_sql = "INSERT INTO invd.r_daily_fcapital_stat(mkt, trade_date, buy_trades, sell_trades, turnover, buy_amt, sell_amt, sum_buysell_amt, daily_quota_balance, daily_quota_balance_percet) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.executemany(insert_sql, insert_datas)
        db_conn.commit()
        db_conn.close()
        jyqlogger.info("数据插入完成!")

    async def getLastFCapitalDate(self):
        """从数据库获取最新的一条的统计数据的日期，
        即数据从最新一条数据之后的日期开始
        """
        db_conn = self.db_helper.get_conn()
        cursor = db_conn.cursor()
        startDate = getNowYYYYMMDD()
        try:
            cursor.execute(
                f"SELECT trade_date  FROM r_daily_fcapital_stat  ORDER BY trade_date DESC limit 1"
            )
            rows = cursor.fetchall()
            print(len(rows))
            for r in rows:
                startDate = r[0]

            jyqlogger.info("the start date is:{}", startDate)

        except Exception as e:
            jyqlogger.error("Error: unable to fetch data. -->{}", e)

        db_conn.close()
        return startDate
