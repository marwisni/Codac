"""Codac PySpark assignment. Main application module"""

from data_joiner import config, functions#, main_logger, args


# Initializing logging.
logger = functions.logger_init(level=config.LOGS['level'],
                                    path=config.LOGS['path'],
                                    max_bytes=config.LOGS['maxBytes'],
                                    backup_count=config.LOGS['backupCount'])
# logger.info('Data joiner package has been initialized')

# Getting arguments from command line and parsing it
args = functions.get_args(logger)

# Starting sparksession
spark = functions.spark_init('codac', logger)

# Importing data from .csv files
personal_data = functions.dataframe_import(spark, args.source[0], True, logger)
financial_data = functions.dataframe_import(spark, args.source[1], True, logger)

# Removing personal identifiable information from the first dataset, excluding emails.
personal_data_trimmed = functions.columns_select(personal_data, config.SELECT, logger)

# Removing credit card number from second dataset.
financial_data_trimmed = functions.columns_drop(financial_data, config.DROP, logger)

# Joining personal and financial data together.
joined_data = functions.dataframe_join(personal_data_trimmed,
                                       financial_data_trimmed,
                                       config.JOIN,
                                       logger)

# Filtering results by country.
joined_data_filtered = functions.country_filter(joined_data, args.country, logger)

# Renaming columns in results.
joined_data_renamed = functions.columns_rename(joined_data_filtered, config.RENAME, logger)

# Saving results to .csv file.
functions.dataframe_save(joined_data_renamed, config.OUTPUT, True, logger)

# Stopping spark session.
spark.stop()
