"""Codac PySpark assignment. Main application module"""

from data_joiner import config, functions
from data_joiner.df import DF


def main() -> None:
    """Main function of the application. Things that will be done in the order:
    - Initializing logging
    - Parsing arguments from command line
    - Starting Spark session
    - Importing data from .csv files to Dataframes
    - Transforming and joining Dataframes
    - Saving results to output .csv file.
    """
    # Initializing logging.
    logger = functions.init_logger(level=functions.parser_log_levels(config.LOGS['level']),
                                        path=config.LOGS['path'],
                                        max_bytes=config.LOGS['maxBytes'],
                                        backup_count=config.LOGS['backupCount'])

    # Getting arguments from command line and parsing it
    args = functions.get_args(logger)

    # Starting sparksession
    spark = functions.init_spark('codac', logger)

    # Importing data from .csv files
    client_data = DF(spark, 'client_data', args.personal, logger)
    financial_data = DF(spark, 'financial_data', args.financial, logger)

    # Removing personal identifiable information from the first dataset, excluding emails.
    client_data.select_columns(config.SELECT)

    # Removing credit card number from second dataset.
    financial_data.drop_columns(config.DROP)

    # Joining personal and financial data together.
    client_data.join(financial_data, config.JOIN)

    # Filtering results by country.
    client_data.filter_countries(args.country)

    # Renaming columns in results.
    client_data.rename_columns(config.RENAME)

    # Saving results to .csv file.
    client_data.save(config.OUTPUT)

    # Stopping spark session.
    spark.stop()
