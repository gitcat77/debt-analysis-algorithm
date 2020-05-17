# -*- encoding:utf-8 -*-
import logging
from logging import handlers
from configures.configures import configures_sys_log_path

# create logger
logger_name = "debt-analysis-algorithm"
logger = logging.getLogger(logger_name)
logger.setLevel(logging.DEBUG)

# create file handler
# fh = logging.FileHandler(configures_sys_log_path())
fh = handlers.RotatingFileHandler(filename=configures_sys_log_path(), maxBytes=52428800, backupCount=1000,
                                  encoding="utf-8")
fh.setLevel(logging.INFO)

# create formatter
fmt = "%(asctime)-15s.%(msecs)03d %(levelname)s %(filename)s %(lineno)d %(process)d %(message)s"
datefmt = "%a %d %b %Y %H:%M:%S"
formatter = logging.Formatter(fmt, datefmt)

# add handler and formatter to logger
fh.setFormatter(formatter)
logger.addHandler(fh)


# 定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象#
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)
