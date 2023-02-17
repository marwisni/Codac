import logging
import pathlib
from argparse import ArgumentParser
from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession


def logger_init(level, path, max_bytes, backup_count):
    pathlib.Path(__file__).parents[1].joinpath('logs').mkdir(exist_ok=True)
    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    logger_formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")
    logger_file_handler = RotatingFileHandler(path, maxBytes=max_bytes, backupCount=backup_count, encoding='utf8')
    logger_file_handler.setFormatter(logger_formatter)
    logger.addHandler(logger_file_handler)
    return logger


logger = logger_init(level=logging.INFO,
                     path=pathlib.Path(__file__).parents[2].joinpath('logs/status.log'),
                     max_bytes=1024,
                     backup_count=3)

def get_args():
    parser = ArgumentParser()
    parser.add_argument('source',
                        nargs='*',
                        default=['src/source_data/dataset_one.csv', 'src/source_data/dataset_two.csv'],
                        help='Needs two sources .csv files. First is for personal data and second for financial data.')
    parser.add_argument('-c', '--country',
                        default='United Kingdom, Netherlands',
                        help='Countries that should be included in output files. Empty list return all available countries')
    return parser.parse_args()

args = get_args()

spark = SparkSession.builder.appName('codac').getOrCreate()
logger.info('Spark session has started')

personal_data = spark.read.csv(args.source[0], header=True)
logger.info('Personal data has been imported successfully.')
financial_data = spark.read.csv(args.source[1], header=True)
logger.info('Financial data has been imported successfully.')

personal_data = personal_data.select('id', 'email', 'country')
logger.info('Personal data has been filtered successfully.')

financial_data = financial_data.drop('cc_n')
logger.info('Financial data has been filtered successfully.')

joined_data = personal_data.join(financial_data, ['id'])
logger.info('Personal and financial data has been joined together successfully.')


def country_filter(dataframe, countries_str: str):
    if countries_str == '':
        return dataframe
    else:
        countries = [country.strip() for country in countries_str.split(',')]
        return dataframe.filter(dataframe.country.isin(countries))


joined_data = country_filter(joined_data, args.country)
logger.info('Data has been filtered by country successfully.')

changes = {
    'id': 'client_identifier',
    'btc_a': 'bitcoin_address',
    'cc_t':  'credit_card_type'
}


def column_rename(dataframe, change: dict):
    # TODO Maybe should iterate by change not by colmns?
    changes_list = []
    for column in dataframe.columns:
        if column in change.keys():
            changes_list.append(f"{column} as {change[column]}")
        else:
            changes_list.append(column)
    logger.info("Columns' names have been changed successfully.")
    return dataframe.selectExpr(changes_list)


joined_data = column_rename(joined_data, changes)
joined_data.write.csv('src/client_data', header=True, mode='overwrite')
logger.info("Output file has been saved successfully.")
spark.stop()
