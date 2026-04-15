from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("Lab4_BigData") \
        .getOrCreate()
