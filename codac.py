"""Docstring for module"""
import sys
import logging
from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession


logger = logging.getLogger('codac.py')
logger.setLevel(logging.INFO)
logger_formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")
logger_file_handler = RotatingFileHandler(
    "status.log", maxBytes=1024, backupCount=3, encoding='utf8')
logger_file_handler.setFormatter(logger_formatter)
logger.addHandler(logger_file_handler)

spark = SparkSession.builder.appName('codac').getOrCreate()

logger.info('Spark session has started')
if len(sys.argv) > 1:
    personal_data = spark.read.csv(sys.argv[1], header=True)
else:
    personal_data = spark.read.csv(
        './source_data/dataset_one.csv', header=True)
logger.info('Personal data has been imported successfully.')
personal_data = personal_data.select('id', 'email', 'country')
logger.info('Personal data has been filtered successfully.')

if len(sys.argv) > 2:
    financial_data = spark.read.csv(sys.argv[2], header=True)
else:
    financial_data = spark.read.csv(
        './source_data/dataset_two.csv', header=True)
logger.info('Financial data has been imported successfully.')
financial_data = financial_data.drop('cc_n')
logger.info('Financial data has been filtered successfully.')

joined_data = personal_data.join(financial_data, ['id'])
logger.info('Personal and financial data has been joined together successfully.')


def country_filter(dataframe, countries_str: str):
    """TODO Docstring for function"""
    countries = [country.strip() for country in countries_str.split(',')]
    logger.info('Data has been filtered by country successfully.')
    return dataframe.filter(dataframe.country.isin(countries))


if len(sys.argv) > 3:
    joined_data = country_filter(joined_data, sys.argv[3])
else:
    joined_data = country_filter(joined_data, 'United Kingdom, Netherlands')

changes = {
    'id': 'client_identifier',
    'btc_a': 'bitcoin_address',
    'cc_t':  'credit_card_type'
}


def column_rename(dataframe, change: dict):
    """TODO docstring for function, TODO Maybe should iterate by change not by colmns?"""
    changes_list = []
    for column in dataframe.columns:
        if column in change.keys():
            changes_list.append(f"{column} as {change[column]}")
        else:
            changes_list.append(column)
    logger.info("Columns' names have been changed successfully.")
    return dataframe.selectExpr(changes_list)


joined_data = column_rename(joined_data, changes)
joined_data.write.csv('./client_data', header=True, mode='overwrite')
logger.info("Output file has been saved successfully.")
spark.stop()
