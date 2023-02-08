"""Docstring for module"""
import sys
import logging
# from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession


logging.basicConfig(level=logging.INFO)

spark = SparkSession.builder.appName('codac').getOrCreate()

logging.info('Spark session has started')
personal_data = spark.read.csv(sys.argv[1], header=True)
# '/mnt/c/Users/marwisni/Downloads/codac_assignment_2023/dataset_one.csv', header=True)
logging.info('Personal data has been imported successfully.')
personal_data = personal_data.select('id', 'email', 'country')
logging.info('Personal data has been filtered successfully.')

financial_data = spark.read.csv(sys.argv[2], header=True)
# '/mnt/c/Users/marwisni/Downloads/codac_assignment_2023/dataset_two.csv', header=True)
logging.info('Financial data has been imported successfully.')
financial_data = financial_data.drop('cc_n')
logging.info('Financial data has been filtered successfully.')

joined_data = personal_data.join(financial_data, ['id'])
logging.info('Personal and financial data has been joined together successfully.')


def country_filter(dataframe, countries_str: str):
    """TODO Docstring for function"""
    countries = [country.strip() for country in countries_str.split(',')]
    return dataframe.filter(dataframe.country.isin(countries))


joined_data = country_filter(joined_data, sys.argv[3])
logging.info('Data has been filtered by country successfully.')

changes = {
    'id': 'client_identifier',
    'btc_a': 'bitcoin_address',
    'cc_t':  'credit_card_type'
}
def column_rename(dataframe, change: dict):
    """TODO docstring for function"""
    changes_list = []
    print(dataframe.columns)
    for column in dataframe.columns:
        print(column)
        if column in change.keys():
            changes_list.append(f"{column} as {change[column]}")
        else:
            changes_list.append(column)
    return changes_list


joined_data = joined_data.selectExpr(column_rename(joined_data, changes))
logging.info("Columns' names have been changed successfully.")
joined_data.write.csv('./client_data', header=True, mode='overwrite')
logging.info("Output file has been saved successfully.")
