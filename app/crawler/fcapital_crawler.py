from typing import final

from async_timeout import timeout
import common.consts as const
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
        """每日统计爬取
        获取每日北向资金流入流出数据
        """
        asyncio.get_event_loop().run_until_complete(self.craw_daily_stat())

    def get_target_dates(self, start_date, format="%Y%m%d"):
        """_summary_ 传入start_date 获取 start_date 到今天的数据

        Args:
            start_date (string): 开始日期

        Returns:
            数组: 返回目标日期的列表
        """
        targetDates = []
        endDate = time.strftime(format, datetime.now().timetuple())
        # 大于数据库里最新一条的交易日的日期的数据都加到targetDates这个数组
        while True:
            date = time.strftime(format, start_date.timetuple())
            # 如果当前日期跟数据库里面最后一条日期一样的话，则不返回日期，即不进行数据爬取
            if int(endDate.replace("/", "")) == int(date.replace("/", "")):
                break

            start_date += timedelta(days=1)
            date = time.strftime(format, start_date.timetuple())
            if int(endDate.replace("/", "")) <= int(date.replace("/", "")):

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

        return targetDates

    async def craw_daily_stat(self):
        """crawler daily stat of foreign capital from HK exchange site."""
        browser = await launch()

        start_date = await self.get_lastfc_stat_date()
        targetDates = self.get_target_dates(start_date=start_date)
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
                    await self.insert_stat_datas(insert_datas)
            else:
                jyqlogger.warning("data: {} not exists", d)

            if not page.isClosed():
                await page.close()
                jyqlogger.info("close current page.")

            if idx == len(targetDates) - 1:
                await browser.close()
                jyqlogger.info("the browser is closed.")

    async def insert_stat_datas(self, insert_datas):
        db_conn = self.db_helper.get_conn()
        cursor = db_conn.cursor()
        insert_sql = "INSERT INTO invd.r_daily_fcapital_stat(mkt, trade_date, turnover, buy_trades, sell_trades, sum_buysell_amt, buy_amt, sell_amt, daily_quota_balance, daily_quota_balance_percet) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        try:
            cursor.executemany(insert_sql, insert_datas)
            db_conn.commit()
        except Exception as e:
            jyqlogger.error(e)
        finally:
            cursor.close()
            db_conn.close()
        jyqlogger.info("数据插入完成!")

    async def get_lastfc_stat_date(self):
        """从r_daily_fcapital_stat表里获取最新的一条的统计数据的日期，
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

        finally:
            cursor.close()
            db_conn.close()
        return startDate

    def start_crawfc_holding_data(self):
        """获取北向资金每日持仓数据"""
        asyncio.get_event_loop().run_until_complete(self.craw_all_mkts_holdingdata())

    async def craw_all_mkts_holdingdata(self):
        await asyncio.gather(
            self.craw_daily_holding(const.FC_MARKET_TYPE_SH),
            self.craw_daily_holding(const.FC_MARKET_TYPE_SZ),
        )

    async def craw_daily_holding(self, market):
        browser = await launch({"headless": True})

        start_date = await self.get_last_hoding_date(market)
        targetDates = self.get_target_dates(start_date=start_date, format="%Y/%m/%d")

        for idx, d in enumerate(targetDates):
            page = await browser.newPage()
            url = f"https://www.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t={market}"
            jyqlogger.info("the target data is {}, the target url is {}", d, url)
            await page.goto(url, {"timeout": 0})
            search_btn_selector = await page.querySelector("#btnSearch")

            await page.querySelectorEval(
                "#txtShareholdingDate", f"el => el.value = '{d}'"
            ),

            await search_btn_selector.click()

            await page.waitForNavigation()

            result = await page.evaluate(
                """
                document.getElementById('pnlResult').children[0].children[0].innerText
                """
            )
            jyqlogger.info("the result is {}", result)

            real_date = await page.querySelectorEval(
                "#txtShareholdingDate", f"el => el.value"
            )
            jyqlogger.info("the real date is {}", real_date)

            values = await page.evaluate(
                """() => [...document.querySelectorAll('#mutualmarket-result > tbody > tr')]
                   .map(element => element.innerText)
                """
            )

            if d != real_date:
                jyqlogger.warning(
                    "the real date is {}, but the target date is {}, just ignore!! loop continued.",
                    real_date,
                    d,
                )
                continue

            insert_datas = []
            for v in values:
                dataArr = v.replace("\n", "").split("\t")
                security_ccass_code = dataArr[0]
                security_name = dataArr[1]
                holding_amt = float(dataArr[2].replace(",", ""))
                holding_amt_rate = dataArr[3]

                insert_datas.append(
                    [
                        market,
                        d,
                        security_ccass_code,
                        security_name,
                        holding_amt,
                        holding_amt_rate,
                    ]
                )
            await self.insert_hoding_data(insert_datas)
            jyqlogger.info("{} data insert success!", d)

            ####
            if not page.isClosed():
                await page.close()

            if idx == len(targetDates) - 1:
                await browser.close()
                jyqlogger.info("the browser is closed.")

    async def get_last_hoding_date(self, mkt):
        """从r_north_holding表里获取最新的一条的统计数据的日期，
        即数据从最新一条数据之后的日期开始
        """
        db_conn = self.db_helper.get_conn()
        cursor = db_conn.cursor()
        startDate = getNowYYYYMMDD()
        try:
            cursor.execute(
                f"SELECT trade_date  FROM r_north_holding where security_mkt = {mkt} ORDER BY trade_date DESC limit 1"
            )
            rows = cursor.fetchall()
            print(len(rows))
            for r in rows:
                startDate = r[0]

            jyqlogger.info("the start date is:{}", startDate)

        except Exception as e:
            jyqlogger.error("Error: unable to fetch data. -->{}", e)

        finally:
            cursor.close()
            db_conn.close()
        return startDate

    async def insert_hoding_data(self, insert_datas):
        """将持仓数据插入到数据库中

        Args:
            insert_datas (array): 插入数据库的数据
        """
        db_conn = self.db_helper.get_conn()
        cursor = db_conn.cursor()
        try:
            insert_sql = "INSERT INTO invd.r_north_holding(security_mkt, trade_date, security_ccass_code, security_name, holding_amt, holding_amt_rate) VALUES (%s, %s, %s, %s, %s, %s)"
            cursor.executemany(insert_sql, insert_datas)
            db_conn.commit()
        except Exception as e:
            jyqlogger.error(e)
        finally:
            cursor.close()
            db_conn.close()
        jyqlogger.info("数据插入完成!")
