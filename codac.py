"""Docstring"""
from pyspark.sql import SparkSession


COUNTRIES = ['United Kingdom', 'Netherlands']
spark = SparkSession.builder.appName('codac').getOrCreate()

personal_data = spark.read.csv(
    '/mnt/c/Users/marwisni/Downloads/codac_assignment_2023/dataset_one.csv', header=True)
personal_data = personal_data.select('id','email','country')

financial_data = spark.read.csv(
    '/mnt/c/Users/marwisni/Downloads/codac_assignment_2023/dataset_two.csv', header=True)
financial_data = financial_data.drop('cc_n')

joined_data = personal_data.join(financial_data, ['id'])
joined_data = joined_data.filter(joined_data.country.isin(COUNTRIES))
changing_expression = ["id as client_identifier", "email", "country", "btc_a as bitcoin_address", "cc_t as credit_card_type"]
joined_data = joined_data.selectExpr(changing_expression)
joined_data.show(30)
joined_data.write.csv('./client_data', header=True, mode='overwrite')    
