import logging
from logging.handlers import RotatingFileHandler
from argparse import ArgumentParser
from data_joiner import config

def logger_init(level, path, max_bytes, backup_count):
    path.mkdir(exist_ok=True)
    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    logger_formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")
    logger_file_handler = RotatingFileHandler(path.joinpath('status.log'), maxBytes=max_bytes, backupCount=backup_count, encoding='utf8')
    logger_file_handler.setFormatter(logger_formatter)
    logger.addHandler(logger_file_handler)
    return logger


def get_args():
    parser = ArgumentParser()
    parser.add_argument('source',
                        nargs='*',
                        default=[config.SOURCES['first'], config.SOURCES['second']],
                        help='Needs two sources .csv files. First is for personal data and second for financial data.')
    parser.add_argument('-c', '--country',
                        default=config.SOURCES['countries'],
                        help='Countries that should be included in output files. Empty list return all available countries')
    return parser.parse_args()


logger = logger_init(level=config.LOGS['level'],
                     path=config.LOGS['path'],
                     max_bytes=config.LOGS['maxBytes'],
                     backup_count=config.LOGS['backupCount'])

logger.info('Data joiner package has been initialized')

args = get_args()
