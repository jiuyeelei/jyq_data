import time


def getNowYYYYMMDD() -> str:
    return time.strftime("%Y%m%d", time.localtime())
