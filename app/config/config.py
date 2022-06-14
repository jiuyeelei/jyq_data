import yaml
import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_config(env):
    with open(f"{BASE_DIR}/config/config-{env}.yaml", "r") as stream:
        config = yaml.safe_load(stream)
        return config


class Config(object):
    DEBUG = True

    def __getitem__(self, key):
        return self.__getattribute__(key)


class ProdConfig(Config):
    # Mysql config
    prod_config = get_config("prod")
    mysql_conn = prod_config["mysql_conn"]
    db_host = mysql_conn["host"]
    port = mysql_conn["port"]
    user = mysql_conn["user"]
    passwd = mysql_conn["passwd"]

    # mongo config
    mongo_config = prod_config["mongo_conn"]
    mongo_uri = mongo_config["uri"]


class DevConfig(Config):
    dev_config = get_config("dev")
    mysql_conn = dev_config["mysql_conn"]
    db_host = mysql_conn["host"]
    port = mysql_conn["port"]
    user = mysql_conn["user"]
    passwd = mysql_conn["passwd"]

    # mongo config
    mongo_config = dev_config["mongo_conn"]
    mongo_uri = mongo_config["uri"]


num = len(sys.argv) - 1
if num < 1:
    exit("参数错误 config..")

print("初始化config...")
env = sys.argv[1]
mapping = {"dev": DevConfig, "prod": ProdConfig}

APP_ENV = env.lower()
config = mapping[APP_ENV]()
