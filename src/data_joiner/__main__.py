"""Codac PySpark assignment. Main application module"""

from data_joiner import config, functions
from data_joiner.df import DF


def main():
    """Main function of the application. Things that will be done in the order:
    - Initializing logging
    - Parsing arguments from command line
    - Starting Spark session
    - Importing data from .csv files to Dataframes
    - Transforming and joining Dataframes
    - Saving results to output .csv file.
    """
    # Initializing logging.
    logger = functions.logger_init(level=functions.log_level_parser(config.LOGS['level']),
                                        path=config.LOGS['path'],
                                        max_bytes=config.LOGS['maxBytes'],
                                        backup_count=config.LOGS['backupCount'])

    # Getting arguments from command line and parsing it
    args = functions.get_args(logger)

    # Starting sparksession
    spark = functions.spark_init('codac', logger)

    # Importing data from .csv files
    #personal_data = functions.dataframe_import(spark, args.personal, True, logger)
    client_data = DF(spark, 'personal_data', args.personal, logger)
    #financial_data = functions.dataframe_import(spark, args.financial, True, logger)
    financial_data = DF(spark, 'financial_data', args.financial, logger)

    # Removing personal identifiable information from the first dataset, excluding emails.
    #personal_data_trimmed = functions.columns_select(personal_data, config.SELECT, logger)
    client_data.columns_select(config.SELECT)

    # Removing credit card number from second dataset.
    #financial_data_trimmed = functions.columns_drop(financial_data, config.DROP, logger)
    financial_data.columns_drop(config.DROP)

    # Joining personal and financial data together.
    client_data.join(financial_data, config.JOIN)

    # Filtering results by country.
    client_data.country_filter(args.country)

    # Renaming columns in results.
    client_data.columns_rename(config.RENAME)

    # Saving results to .csv file.
    client_data.save(config.OUTPUT)

    # Stopping spark session.
    spark.stop()
