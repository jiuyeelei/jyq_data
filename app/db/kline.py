from sys import flags
from typing import final
from helper.db_helper import DBHelper
from config.config import config
from datetime import datetime, timedelta
import pandas as pd
from logger.setting import jyqlogger


class KlineModel:
    def __init__(self):
        self.db_helper = DBHelper(config)

    async def save_kline(self, code, market, klines):
        db_conn = self.db_helper.get_conn()
        insert_datas = []
        for kline_str in klines:
            datas = kline_str.split(",")
            trade_date = datas[0]
            open_price = float(datas[1])
            close_price = float(datas[2])
            high_price = float(datas[3])
            low_price = float(datas[4])
            trade_amt = int(datas[5])
            trade_val = float(datas[6])
            # amplitude = float(datas[7])
            updown_rate = float(datas[9])
            updown_ratio = float(datas[8])
            exchange_ratio = float(datas[10])
            insert_datas.append(
                [
                    code,
                    market,
                    trade_date,
                    open_price,
                    close_price,
                    high_price,
                    low_price,
                    trade_amt,
                    trade_val,
                    # amplitude,
                    updown_rate,
                    updown_ratio,
                    exchange_ratio,
                ]
            )

        insert_sql = "INSERT INTO invd.r_stock_kline(code, market, trade_date, open_price, close_price, high_price, low_price, trade_amt, trade_val, updown_rate, updown_ratio, exchg_ratio) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cur = db_conn.cursor()
        try:
            cur.executemany(insert_sql, insert_datas)
            db_conn.commit()
        except Exception as e:
            jyqlogger.error(e)
        finally:
            cur.close()
            db_conn.close()
        print("处理完成:" + code)

    def get_daily_kline(self, stock_id):
        """_summary_

        Args:
            stock_id (_type_): _description_
        """
        db_conn = self.db_helper.get_conn()
        cursor = db_conn.cursor()
        try:
            cursor.execute(
                f"SELECT Code, TradingDate, OpenPrice, ClosePrice, HighPrice, LowPrice FROM QT_StockPerformance WHERE Code = {stock_id}"
            )
            results = cursor.fetchall()
            for row in results:
                code = row[0]
                trading_date = row[1]
                open_price = row[2]
                close_price = row[3]
                high_price = row[4]
                low_price = row[5]
                print(
                    f"code:{code}, trade date: {trading_date}, open_price: {open_price}, close_price: {close_price}, high_price: {high_price}, low_price: {low_price}"
                )
        except Exception as e:
            jyqlogger.error(e)
        finally:
            cursor.close()
            db_conn.close()

    def get_lastest_tdate(self):
        """获取k线里面最后一根K线的日期"""
        db_conn = self.db_helper.get_conn()
        cursor = db_conn.cursor()

        try:
            select_sql = f"SELECT trade_date  FROM invd.r_stock_kline order by trade_date desc limit 1"
            cursor.execute(select_sql)

            rows = cursor.fetchall()
            print(len(rows))
            for r in rows:
                startDate = r[0]

            jyqlogger.info("the start date is:{}", startDate)

            targe_date = startDate + timedelta(days=1)
            return str(targe_date.strftime("%Y%m%d"))
        except Exception as e:
            jyqlogger.error(e)
            return 0
        finally:
            jyqlogger.warning("execute connection close()!")
            cursor.close()
            db_conn.close()

    def get_pandas_dataframe(self, code, mkt):
        db_engine = self.db_helper.get_pandas_engine()
        df = pd.read_sql(
            f"SELECT code, trade_date, open_price, close_price, high_price, low_price, trade_amt, trade_val, updown_rate, updown_ratio, exchg_ratio FROM r_stock_kline where code = '{code}' and market = {mkt}",
            db_engine,
        )

        return df
