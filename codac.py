from pyspark.sql import SparkSession

with open('/mnt/c/Users/marwisni/Downloads/codac_assignment_2023/dataset_one.csv',
          mode='r', encoding='utf-8') as f:
    print(f.readline())

spark = SparkSession.builder.appName('My first spark experience').getOrCreate()

print(spark.version)
df = spark.read.csv(
    '/mnt/c/Users/marwisni/Downloads/codac_assignment_2023/dataset_one.csv', header=True)
type(df)
df.show(5)

print('Hello World')
