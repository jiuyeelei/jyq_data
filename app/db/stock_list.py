from helper.db_helper import DBHelper
from config.config import config
from loader.loader_stock_list import StockListLoader


class StockList:
    def __init__(self):
        self.db_helper = DBHelper(config)

    def del_all_stocks(self):
        db_conn = self.db_helper.get_conn()
        cur = db_conn.cursor()
        cur.execute("update jyq.r_stock_list set stat = 2 where stat = 1")
        db_conn.commit()
        cur.close()
        db_conn.close()

    def save_stocklist(self):
        stock_list = StockListLoader().sync_stocklist()
        db_conn = self.db_helper.get_conn()
        insert_datas = []
        for stock in stock_list:
            code = stock["stock_id"]
            mkt = stock["market_type"]
            name = stock["cn_name"]
            stype = stock["stock_type"]

            insert_datas.append(
                [
                    code,
                    int(mkt),
                    name,
                    int(stype),
                ]
            )
        insert_sql = "INSERT INTO jyq.r_stock_list(code, mkt, cname, stype) values (%s, %s, %s, %s)"
        cur = db_conn.cursor()
        if len(insert_datas) > 0:
            self.del_all_stocks()
        cur.executemany(insert_sql, insert_datas)
        db_conn.commit()
        cur.close()
        db_conn.close()
        print("同步股票列表完成")
