from argparse import ArgumentParser
from pyspark.sql import SparkSession
from data_joiner import config, logger, args


spark = SparkSession.builder.appName('codac').getOrCreate()
logger.info('Spark session has started')

personal_data = spark.read.csv(str(args.source[0]), header=True)
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

joined_data = column_rename(joined_data, config.CHANGES)
joined_data.write.csv(config.OUTPUT, header=True, mode='overwrite')
logger.info("Output file has been saved successfully.")
spark.stop()
