from loguru import logger
from config.config import BASE_DIR
import os

logger.add(
    os.path.join(BASE_DIR, "logs/jyq_data.log"),
    format="{time} {level} {message}",
    filter="",
    level="DEBUG",
    rotation="500 MB",
    compression="zip",
)
jyqlogger = logger
