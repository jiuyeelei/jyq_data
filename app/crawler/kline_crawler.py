import asyncio
import aiohttp
import time
import json
from aiohttp.client import ClientSession

from db.kline import KlineModel


class KlineCrawler:
    def __init__(self):
        self.kline_model = KlineModel()
        self.begin_date = self.kline_model.get_lastest_tdate()
        print(self.begin_date)
        datas = []
        with open("data/stock_base_info_stocks.json", "r") as stk_base_info:
            stock_datas = json.load(stk_base_info)
            datas = datas + stock_datas
            # self.stocks = [{"market_type": 1, "stock_id": "601318"}]
        with open("data/stock_base_info_index.json", "r") as index_base_info:
            index_datas = json.load(index_base_info)
            datas = datas + index_datas
        self.stocks = datas
        # self.stocks = [
        #     {"stock_id": "399001", "market_type": 2},
        #     {"stock_id": "000001", "market_type": 1},
        # ]

    async def craw_all_kline(self):
        my_conn = aiohttp.TCPConnector(limit=10)
        async with aiohttp.ClientSession(connector=my_conn) as session:
            tasks = []
            print(f"共计需要处理股票数量为:{len(self.stocks)}")
            for stk in self.stocks:
                stk_id = stk["stock_id"]
                if (
                    stk_id.startswith("6")
                    or stk_id.startswith("3")
                    or stk_id.startswith("0")
                ):
                    mkt = 0 if int(stk["market_type"]) == 2 else 1
                    await self.craw_target_kline(f"{mkt}.{stk_id}", session=session)

    async def craw_target_kline(self, stock_id, session: ClientSession):
        """get target stock kline from eastmoney site.
        currently, we get only pre-kline only.
        Args:
            stock_id (string): mkt_type.stock_id, such as 1.601318
            shang mkt: 1,
            shenzheng mkt: 0
        """
        print(f"开始处理 {stock_id}, {self.begin_date}...")
        target_url = f"http://push2his.eastmoney.com/api/qt/stock/kline/get?fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&beg={self.begin_date}&end=20500101&ut=fa5fd1943c7b386f172d6893dbfba10b&rtntype=6&secid={stock_id}&klt=101&fqt=1"
        try:
            async with session.get(target_url) as response:
                result = await response.text()
                stk_data = json.loads(result)["data"]
                code = stk_data["code"]
                market = stk_data["market"]
                klines = stk_data["klines"]
                # print(klines[2594])
                await self.kline_model.save_kline(code, market, klines)
        except Exception as e:
            print(f"出现异常了：{stock_id}")
            print(e)

    def start_craw(self):
        start = time.time()
        asyncio.run(self.craw_all_kline())
        print(f"spent {time.time() - start} seconds")
