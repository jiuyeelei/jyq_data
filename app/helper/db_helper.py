import pymysql
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from logger.setting import jyqlogger
import pymongo


class DBHelper:
    def __init__(self, config) -> None:
        self.config = config
        self.init_mongo_conn()

    def init_mongo_conn(self):
        """初始化Mongo连接"""
        self.mongo_client = pymongo.MongoClient(self.config.mongo_uri)

    def get_mysql_conn(self):
        """初始化mysql连接"""
        mysql_conn = pymysql.connect(
            host=self.config.db_host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.passwd,
            database="invd",
        )
        return mysql_conn

    def get_conn(self):
        """get mysql database connection

        Returns:
            Connection: database connection.
        """
        return self.get_mysql_conn()

    def get_mongo_collection(self, collection):
        """Get mongo collection"""
        return self.mongo_client["quota"][collection]

    def get_pandas_engine(self):
        pd.set_option("display.float_format", lambda x: "%.2f" % x)
        # 显示Dateframe所有列(参数设置为None代表显示所有行，也可以自行设置数字)
        pd.set_option("display.max_columns", None)
        # 显示Dateframe所有行
        pd.set_option("display.max_rows", None)
        # 设置Dataframe数据的显示长度，默认为50
        pd.set_option("max_colwidth", 200)
        # 禁止Dateframe自动换行(设置为Flase不自动换行，True反之)
        pd.set_option("expand_frame_repr", False)

        engine = create_engine(
            f"mysql+pymysql://{self.config.user}:{self.config.passwd}@{self.config.db_host}:{self.config.port}/jyq"
        )
        return engine
