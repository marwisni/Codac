"""
Module including functions used in main module.

Functions:
    - logger_init(int, str, int, int) -> Logger
    - get_args() -> List(str)
    - column_rename(DataFrame, dict) -> DataFrame
    - country_filter(DataFrame, str) -> DataFrame
"""
from logging import getLogger, Formatter
from logging.handlers import RotatingFileHandler
from argparse import ArgumentParser
from pyspark.sql.dataframe import DataFrame
import data_joiner.config as config

__docformat__ = 'restructuredtext'

def logger_init(level, path, max_bytes, backup_count):
    """Logging initialization."""
    path.mkdir(exist_ok=True)
    logger = getLogger(__name__)
    logger.setLevel(level)
    logger_formatter = Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")
    logger_file_handler = RotatingFileHandler(path.joinpath('status.log'),
                                              maxBytes=max_bytes,
                                              backupCount=backup_count,
                                              encoding='utf8')
    logger_file_handler.setFormatter(logger_formatter)
    logger.addHandler(logger_file_handler)
    return logger


def get_args():
    """Return parsed arguments for application. Provide --help option.

    Returns:
        List(str): List of 3 arguments:
        - path to source personal data .csv file
        - path to source financial data .csv file
        - list of countries to filter (comma separated string)
        If any of this arguments was not provided then default from config.py file is used.
    """
    parser = ArgumentParser()
    parser.add_argument('source',
                        nargs='*',
                        default=[config.SOURCES['first'], config.SOURCES['second']],
                        help='Needs two sources .csv files. First is for personal data and second for financial data.')
    parser.add_argument('-c', '--country',
                        default=config.SOURCES['countries'],
                        help='Countries that should be included in output files. Empty list return all available countries')
    return parser.parse_args()


def column_rename(dataframe: DataFrame, change: dict):
    """Rename particular column names.
    
    Args:    
        - dataframe (DataFrame): DataFrame for which columns names should be changed.        
        - change (dict): Dictionary of changes that should happen in format "old_name": "new_name".

    Returns:
        DataFrame: Dataframe with renamed columns.
    """
    changes_list = []
    for column in dataframe.columns:
        if column in change:
            changes_list.append(f"{column} as {change[column]}")
        else:
            changes_list.append(column)
    return dataframe.selectExpr(changes_list)


def country_filter(dataframe: DataFrame, countries_str: str):
    """Filter data from dataframe including only particular countries.

    Args:
        dataframe (DataFrame): Input DataFrame which should be filtered by country.
        countries_str (str): Countries as comma separated string which should be included after filtering.

    Returns:
        DataFrame: Dataframe with data only from countries included in the countries_str.
    """
    if countries_str == '':
        return dataframe
    countries = [country.strip() for country in countries_str.split(',')]
    return dataframe.filter(dataframe.country.isin(countries))
