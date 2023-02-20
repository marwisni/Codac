import logging
from logging.handlers import RotatingFileHandler
import config

def logger_init(level, path, max_bytes, backup_count):
    path.mkdir(exist_ok=True)
    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    logger_formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")
    logger_file_handler = RotatingFileHandler(path.joinpath('status.log'), maxBytes=max_bytes, backupCount=backup_count, encoding='utf8')
    logger_file_handler.setFormatter(logger_formatter)
    logger.addHandler(logger_file_handler)
    return logger


logger = logger_init(level=config.LOGS['level'],
                     path=config.LOGS['path'],
                     max_bytes=config.LOGS['maxBytes'],
                     backup_count=config.LOGS['backupCount'])

logger.info('Data joiner package has been initialized')