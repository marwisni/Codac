"""Docstring"""
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
countries = [country.strip() for country in sys.argv[3].split(',')]
joined_data = joined_data.filter(joined_data.country.isin(countries))
logging.info('Data has been filtered by country successfully.')
changing_expression = ["id as client_identifier", "email",
                       "country", "btc_a as bitcoin_address", "cc_t as credit_card_type"]
joined_data = joined_data.selectExpr(changing_expression)
logging.info("Columns' names have been changed successfully.")
joined_data.write.csv('./client_data', header=True, mode='overwrite')
logging.info("Output file has been saved successfully.")
