import yaml
import sys


def get_config(env):
    with open(f"./config/config-{env}.yaml", "r") as stream:
        config = yaml.safe_load(stream)
        return config


class Config(object):
    DEBUG = True

    def __getitem__(self, key):
        return self.__getattribute__(key)


class ProdConfig(Config):
    prod_config = get_config("prod")
    dailyprice_config = prod_config["dailyprice_conn"]
    db_host = dailyprice_config["host"]
    port = dailyprice_config["port"]
    user = dailyprice_config["user"]
    passwd = dailyprice_config["passwd"]


class DevConfig(Config):
    dev_config = get_config("dev")
    dailyprice_config = dev_config["dailyprice_conn"]
    db_host = dailyprice_config["host"]
    port = dailyprice_config["port"]
    user = dailyprice_config["user"]
    passwd = dailyprice_config["passwd"]


num = len(sys.argv) - 1
if num < 1 or num > 1:
    exit("参数错误..")

env = sys.argv[1]
mapping = {"dev": DevConfig, "prod": ProdConfig}

APP_ENV = env.lower()
config = mapping[APP_ENV]()
