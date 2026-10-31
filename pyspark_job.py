from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def clean_data(df):
    df = df.filter(F.col("name").isNotNull())
    df = df.filter(F.col("amount") > 0)
    df = df.withColumn("amount_with_tax", F.col("amount") * 1.20)
    return df


if __name__ == "__main__":
    spark = SparkSession.builder.appName("clean_data_job").getOrCreate()

    data = [
        (1, "Ahmed", 100.0),
        (2, "Sara", 0.0),
        (3, None, 50.0),
        (4, "Omar", -20.0),
        (5, "Mona", 250.0),
    ]
    df = spark.createDataFrame(data, ["id", "name", "amount"])

    clean_data(df).show()
    spark.stop()
