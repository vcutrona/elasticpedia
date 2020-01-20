from pyspark.sql import SparkSession
from functools import lru_cache

from elasticpedia.config.spark_conf import SparkConfig


@lru_cache(maxsize=None)
def get_spark():
    config = SparkConfig()
    return (SparkSession.builder
            .master(config.master)
            .appName(config.app_name)
            .getOrCreate())
