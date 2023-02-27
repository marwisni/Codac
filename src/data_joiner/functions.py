"""
Module including functions used in main module.

Functions:
    - logger_init(int, str, int, int) -> Logger
    - get_args() -> List(str)
    - column_rename(DataFrame, dict) -> DataFrame
    - country_filter(DataFrame, str) -> DataFrame
"""
from sys import stdout
from logging import getLogger, Formatter, StreamHandler
from logging.handlers import RotatingFileHandler
from argparse import ArgumentParser
from pyspark.sql import SparkSession
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
    logger_console_handler = StreamHandler(stdout)
    logger_console_handler.setFormatter(logger_formatter)
    logger.addHandler(logger_file_handler)
    logger.addHandler(logger_console_handler)
    logger.info('Logging has been initialized.')
    return logger


def get_args(logger):
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
    logger.info('Arguments has been parsed successfully.')
    return parser.parse_args()


def spark_init(name, logger):
    """Spark session initialization."""
    session = SparkSession.builder.appName(name).getOrCreate()
    logger.info('Spark session has started')
    return session


def dataframe_import(spark_session, path, header, logger):
    """Reading data from.csv file.

    Args:
        - spark_session (SparkSession): Spark session used to read data from .csv file.
        - path (str): Path to the file which should be imported to the DataFrame.
        - header (bool): Information if data should be imported with headers or not.
        - logger (Logger): Logger used to logging if function end with success.

    Returns:
        DataFrame: DataFrame with imported data.
    """
    dataframe = spark_session.read.csv(path, header=header)
    logger.info(f'Data from {path} has been imported successfully.')
    return dataframe


def columns_select(dataframe, select, logger):
    """Selecting particular columns from the dataframe.

    Args:
        dataframe (Dataframe): Dataframe from which columns selection should be made.
        select (list(str)): List of columns names that should be selected.
        logger (Logger): Logger used to logging if function end with success.
    Returns:
        DataFrame: DataFrame with selected columns.
    """
    dataframe = dataframe.select(*select)
    logger.info(f"Only columns: {', '.join(select)} have been selected from dataframe.")
    return dataframe


def columns_drop(dataframe, drop, logger):
    """Removing particular columns from the dataframe.

    Args:
        dataframe (Dataframe): Dataframe from which columns should be removed.
        drop (list(str)): List of columns names that should be removed.
        logger (Logger): Logger used to logging if function end with success.

    Returns:
        Dataframe: DataFrame without dropped columns.
    """
    dataframe = dataframe.drop(*drop)
    logger.info(f"Removed {', '.join(drop)} columns from the dataframe")
    return dataframe

def dataframe_join(dataframe1, dataframe2, join, logger):
    """Joining two dataframes.

    Args:
        dataframe1 (Dataframe): First dataframe which should be joined.
        dataframe2 (Dataframe): Second dataframe which should be joined.
        join (list(str)): List of columns names that dataframes should be joined by.
        logger (Logger): Logger used to logging if function end with success.

    Returns:
        Dataframe: DataFrame including data from both sources dataframes.
    """
    dataframe = dataframe1.join(dataframe2, join)
    logger.info('Dataframes has been joined together successfully.')
    return dataframe

def columns_rename(dataframe, rename, logger):
    """Rename particular column names.
    
    Args:    
        - dataframe (DataFrame): DataFrame for which columns names should be changed.        
        - rename (dict): Dictionary of changes that should happen in format "old_name": "new_name".
        - logger (Logger): Logger used to logging if function end with success.

    Returns:
        DataFrame: Dataframe with renamed columns.
    """
    changes_list = []
    for column in dataframe.columns:
        if column in rename:
            changes_list.append(f"{column} as {rename[column]}")
        else:
            changes_list.append(column)
    logger.info("Columns' names have been changed successfully.")
    return dataframe.selectExpr(changes_list)


def country_filter(dataframe, countries_str, logger):
    """Filter data from dataframe including only particular countries.

    Args:
        - dataframe (DataFrame): Input DataFrame which should be filtered by country.
        - countries_str (str): Countries as comma separated string which should be included after filtering.
        - logger (Logger): Logger used to logging if function end with success.

    Returns:
        DataFrame: Dataframe with data only from countries included in the countries_str.
    """
    if countries_str == '':
        return dataframe
    countries_list = [country.strip() for country in countries_str.split(',')]
    logger.info('Data has been filtered by country successfully.')
    return dataframe.filter(dataframe.country.isin(countries_list))

def dataframe_save(dataframe, path, header, logger):
    """Saving results to .csv file.

    Args:
        - dataframe (Dataframe): Dataframe with results which should be saved.
        - path (str): Path to the location where data should be saved.
        - header (bool): Information if data should be saved with headers or not.
        - logger (Logger): Logger used to logging if function end with success.
    """
    dataframe.write.csv(path, header=header, mode='overwrite')
    logger.info("Output file has been saved successfully.")
    return