import logging
import time
from logging.handlers import RotatingFileHandler


def myLog(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    f_format = logging.Formatter(
        '[%(created)f]-[%(levelname)s ]-[%(filename)s]-[%(name)s]-[%(lineno)d]-[%(funcName)s]-[%(message)s]'
    )
    # add a rotating handler
    handler = RotatingFileHandler("test.log", maxBytes=0, backupCount=5)
    handler.setFormatter(f_format)
    logger.addHandler(handler)
    return logger


# logger.warning('This will get logged to a file')
# logger.info("Admin had logged in ...")