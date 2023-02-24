"""Codac PySpark assignment. Main application module"""

from pyspark.sql import SparkSession
from data_joiner import config, tools, main_logger, args


# Starting sparksession
spark = SparkSession.builder.appName('codac').getOrCreate()
main_logger.info('Spark session has started')

# Importing data from .csv files
personal_data = spark.read.csv(str(args.source[0]), header=True)
main_logger.info('Personal data has been imported successfully.')
financial_data = spark.read.csv(args.source[1], header=True)
main_logger.info('Financial data has been imported successfully.')

# Removing personal identifiable information from the first dataset, excluding emails.
personal_data = personal_data.select('id', 'email', 'country')
main_logger.info('Removed personal identifiable information from personal data successfully.')

# Removing credit card number from second dataset.
financial_data = financial_data.drop('cc_n')
main_logger.info('Removed credit card number from financial data successfully.')

# Joining personal and financial data together.
joined_data = personal_data.join(financial_data, ['id'])
main_logger.info('Personal and financial data has been joined together successfully.')

# Filtering results by country.
joined_data = tools.country_filter(joined_data, args.country)
main_logger.info('Data has been filtered by country successfully.')

# Renaming columns in results.
joined_data = tools.column_rename(joined_data, config.CHANGES)
main_logger.info("Columns' names have been changed successfully.")

# Saving results to .csv file.
joined_data.write.csv(config.OUTPUT, header=True, mode='overwrite')
main_logger.info("Output file has been saved successfully.")

# Stopping spark session.
spark.stop()
