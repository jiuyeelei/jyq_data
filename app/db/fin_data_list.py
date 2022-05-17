from numpy import append
import common.consts as const
from helper.db_helper import DBHelper
from config.config import config
from logger.setting import jyqlogger as logger
import asyncio


class StockFinDataList:
    def __init__(self) -> None:
        self.db_helper = DBHelper(config)
        self.target_stk_list = self.load_targe_stklist()

    async def start_to_load(self):
        target_stk_list = self.load_targe_stklist()
        for code in target_stk_list:
            await asyncio.gather(
                self.load_income_statement(code),
                self.load_cashflow(code),
                self.load_balance_sheet(code),
            )

    def load_targe_stklist(self):
        db_conn = self.db_helper.get_conn()
        target_stk_list = []
        try:
            cur = db_conn.cursor()
            cur.execute(
                f"select code, mkt from jyq.r_stock_list where stype in ({const.STK_TYPE_STOCK}, {const.STK_TYPE_SMALL_BOARD}, {const.STK_TYPE_GM}, {const.STK_TYPE_TypeSTAR}, {const.FUND_TYPE_ETF}) and stat = 1 and mkt in (1, 2)"
            )
            results = cur.fetchall()
            logger.info(f"total target stock list size {len(results)}")
            for r in results:
                append(target_stk_list, r[0])
            return results

        except Exception as e:
            print(e)
            print("Error:unable to fetch data.")
        finally:
            db_conn.close()

    async def load_income_statement(self, code):
        logger.info(f"start to load income statement of {code}")

    async def load_cashflow(self, code):
        logger.info(f"start to load cash flow of {code}")

    async def load_balance_sheet(self, code):
        logger.info(f"start to load balance sheet of {code}")
