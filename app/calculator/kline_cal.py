from db.kline import KlineModel


class KlineCal:
    def __init__(self) -> None:
        self.kline_model = KlineModel()

    def dataframe_from_mysql(self):
        df = self.kline_model.get_pandas_dataframe("399001", 0)
        df1 = df[df["updown_ratio"] > 3]
        print(df1)
