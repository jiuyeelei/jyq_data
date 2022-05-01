from db.kline import KlineModel


class KlineCal:
    def __init__(self) -> None:
        self.kline_model = KlineModel()

    def dataframe_from_mysql(self, code, mkt):
        df = self.kline_model.get_pandas_dataframe(code, mkt)
        df1 = df[df["updown_ratio"] > 3]
        print(df1)
