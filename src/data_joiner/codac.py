"""Codac PySpark assignment. Main application module"""

from pyspark.sql import SparkSession
from data_joiner import config, functions#, main_logger, args


# Initializing logging.
logger = functions.logger_init(level=config.LOGS['level'],
                                    path=config.LOGS['path'],
                                    max_bytes=config.LOGS['maxBytes'],
                                    backup_count=config.LOGS['backupCount'])
logger.info('Data joiner package has been initialized')

# Getting arguments from command line and parsing it
args = functions.get_args()

# Starting sparksession
spark = SparkSession.builder.appName('codac').getOrCreate()
logger.info('Spark session has started')

# Importing data from .csv files
personal_data = spark.read.csv(str(args.source[0]), header=True)
logger.info('Personal data has been imported successfully.')
financial_data = spark.read.csv(args.source[1], header=True)
logger.info('Financial data has been imported successfully.')

# Removing personal identifiable information from the first dataset, excluding emails.
personal_data = personal_data.select('id', 'email', 'country')
logger.info('Removed personal identifiable information from personal data successfully.')

# Removing credit card number from second dataset.
financial_data = financial_data.drop('cc_n')
logger.info('Removed credit card number from financial data successfully.')

# Joining personal and financial data together.
joined_data = personal_data.join(financial_data, ['id'])
logger.info('Personal and financial data has been joined together successfully.')

# Filtering results by country.
joined_data = functions.country_filter(joined_data, args.country)
logger.info('Data has been filtered by country successfully.')

# Renaming columns in results.
joined_data = functions.column_rename(joined_data, config.CHANGES)
logger.info("Columns' names have been changed successfully.")

# Saving results to .csv file.
joined_data.write.csv(config.OUTPUT, header=True, mode='overwrite')
logger.info("Output file has been saved successfully.")

# Stopping spark session.
spark.stop()
