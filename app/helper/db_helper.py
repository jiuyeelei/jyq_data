import pymysql


class DBHelper:
    def __init__(self, config) -> None:
        self.config = config

    def get_conn(self):
        """get database connection

        Returns:
            Connection: database connection.
        """
        db = pymysql.connect(
            host=self.config.db_host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.passwd,
            database="jyq",
        )
        return db
