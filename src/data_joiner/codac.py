from pyspark.sql import SparkSession
from data_joiner import config, tools, logger, args


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

joined_data = tools.country_filter(joined_data, args.country)
logger.info('Data has been filtered by country successfully.')

joined_data = tools.column_rename(joined_data, config.CHANGES)
logger.info("Columns' names have been changed successfully.")
joined_data.write.csv(config.OUTPUT, header=True, mode='overwrite')
logger.info("Output file has been saved successfully.")
spark.stop()
