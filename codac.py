"""Docstring for this module"""
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('My first spark experience').getOrCreate()

personal_data = spark.read.csv(
    '/mnt/c/Users/marwisni/Downloads/codac_assignment_2023/dataset_one.csv', header=True)
print(type(personal_data))
personal_data.show(5)

financial_data = spark.read.csv(
    '/mnt/c/Users/marwisni/Downloads/codac_assignment_2023/dataset_two.csv', header=True)
print(type(financial_data))
financial_data.show(5)

joined_data = personal_data.join(financial_data, ['id'])
for column in joined_data.columns:
    print(column)
joined_data.show(30)
joined_data.write.csv(
    '/mnt/c/Users/marwisni/Downloads/codac_assignment_2023/joined_data.csv', header=True)
