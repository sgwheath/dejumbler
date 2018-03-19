from pyspark.sql import SparkSession

from functools import lru_cache


# This method provides the local SparkSession object for reading the frequency dictionary .csv file.
@lru_cache(maxsize=None)
def get_spark():
    return (SparkSession.builder
            .master("local")
            .appName("word-dejumbler")
            .getOrCreate())
