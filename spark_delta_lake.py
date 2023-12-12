from delta import *
from pyspark.sql import SparkSession
from mlrun import get_or_create_ctx

if __name__ == '__main__':
    ctx = get_or_create_ctx("spark-function")

    builder = SparkSession.builder.appName("sparky") \
        .config("spark.sql.extension", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()
    data = spark.range(0, 5)
    data.write.format("delta").mode("overwrite").save("v3io://projects/spark-delta/deltab")
    spark.stop()