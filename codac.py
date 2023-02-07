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
logging.info(
    'Personal and financial data has been joined together successfully.')


def country_filter(df, countries_str: str):
    """TODO Docstring for function"""
    countries = [country.strip() for country in countries_str.split(',')]
    return df.filter(df.country.isin(countries))


joined_data = country_filter(joined_data, sys.argv[3])
logging.info('Data has been filtered by country successfully.')


def column_rename(df, old_columns: list[str], new_columns: list[str]):
    """TODO docstring for function"""
    if (len(old_columns) == len(new_columns)):
        changes = {}
        for i in range(len(old_columns)):
            changes.update({old_columns[i]: f'{old_columns[i]} as {new_columns[i]}'})
        changes_list = []
        for column in df.columns:
            if column in changes.keys():
                changes_list.append(changes[column])
            else:
                changes_list.append(column)
        return (changes_list)
    else:
        logging.error(
            'Old columns name list and new columns name list have different length')
        return None

print(column_rename(joined_data, ['id', 'btc_a', 'cc_t'], ['client_identifier', 'bitcoin_address', 'credit_card_type']))
changing_expression = ["id as client_identifier", "email",
                       "country", "btc_a as bitcoin_address", "cc_t as credit_card_type"]
joined_data = joined_data.selectExpr(changing_expression)
logging.info("Columns' names have been changed successfully.")
joined_data.write.csv('./client_data', header=True, mode='overwrite')
logging.info("Output file has been saved successfully.")
