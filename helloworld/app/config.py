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
    db_host = prod_config["database"]["host"]
    user = prod_config["database"]["user"]
    passwd = prod_config["database"]["passwd"]


class DevConfig(Config):
    dev_config = get_config("dev")
    db_host = dev_config["database"]["host"]
    user = dev_config["database"]["user"]
    passwd = dev_config["database"]["passwd"]


num = len(sys.argv) - 1
if num < 1 or num > 1:
    exit("参数错误..")

env = sys.argv[1]
mapping = {"dev": DevConfig, "prod": ProdConfig}

APP_ENV = env.lower()
config = mapping[APP_ENV]()
