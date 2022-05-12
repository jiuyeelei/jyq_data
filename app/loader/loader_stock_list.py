from helper.db_helper import DBHelper
from config.config import config


class StockListLoader:
    def __init__(self):
        self.db_helper = DBHelper(config)

    def sync_stocklist(self):
        stk_bsifo_coll = self.db_helper.get_mongo_collection("stock_base_info")
        filter = {"flag": True, "stock_stat": 0, "market_type": {"$in": [1, 2, 18]}}
        stock_list = stk_bsifo_coll.find(filter)
        return stock_list
