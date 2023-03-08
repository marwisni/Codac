"""
Module including functions used in main module.

Functions:
    - logger_init(int, str, int, int) -> Logger
    - get_args() -> List(str)
    - column_rename(DataFrame, dict) -> DataFrame
    - country_filter(DataFrame, str) -> DataFrame
"""
from sys import stdout
import logging
import pathlib
from logging.handlers import RotatingFileHandler
from argparse import ArgumentParser
from pyspark.sql import SparkSession
import data_joiner.config as config

__docformat__ = 'restructuredtext'

def logger_init(level: str, path: str, max_bytes: int, backup_count: int):
    """Logging initialization."""
    pathlib.Path(path).mkdir(exist_ok=True)
    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    logger_formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")
    logger_file_handler = RotatingFileHandler(pathlib.Path(path).joinpath('status.log'),
                                              maxBytes=max_bytes,
                                              backupCount=backup_count,
                                              encoding='utf8')
    logger_file_handler.setFormatter(logger_formatter)
    logger_console_handler = logging.StreamHandler(stdout)
    logger_console_handler.setFormatter(logger_formatter)
    logger.addHandler(logger_file_handler)
    logger.addHandler(logger_console_handler)
    logger.info('Logging has been initialized.')
    return logger

def if_is_csv_file(path: str):
    """Checking if provided path leads to .csv file"""
    return pathlib.Path(path).is_file() and path[-3:] == 'csv'


def get_args(logger: logging.Logger):
    """Return parsed arguments for application. Provide --help option.

    Returns:
    --------
        List[str]: List of 3 arguments:
        - path to source personal data .csv file
        - path to source financial data .csv file
        - list of countries to filter (comma separated string)
        If any of this arguments was not provided then default from config.py file is used.
    """
    parser = ArgumentParser()
    parser.add_argument('-p', '--personal',
                        default=config.SOURCES['first'],
                        help='Path to .csv file which contains personal clients data.')
    parser.add_argument('-f', '--financial',
                        default=config.SOURCES['second'],
                        help='Path to .csv file which contains financial clients data.')
    parser.add_argument('-c', '--country',
                        default=config.SOURCES['countries'],
                        help='Countries that should be included in output files. Empty list return all available countries')
    args = parser.parse_args()
    if not if_is_csv_file(args.personal):
        logger.info('Provided path for .csv file with personal data is not valid. Default file will be used instead.')
        args.personal=config.SOURCES['first']
    if not if_is_csv_file(args.financial):
        logger.info('Provided path for .csv file with financial data is not valid. Default file will be used instead.')
        args.financial=config.SOURCES['second']
    logger.info('Arguments has been parsed successfully.')
    return args


def spark_init(name: str, logger: logging.Logger):
    """Spark session initialization."""
    session = SparkSession.builder.appName(name).getOrCreate()
    logger.info('Spark session has started')
    return session


def log_level_parser(level: str):
    """Parsing custom logging levels for logging module."""
    parser = {
        'CRITICAL': logging.CRITICAL,
        'ERROR': logging.ERROR,
        'WARN': logging.WARN,
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG
    }
    try:
        return parser[level]
    except KeyError:
        return logging.NOTSET
