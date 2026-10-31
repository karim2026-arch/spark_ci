import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

from pyspark_job import clean_data


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_clean_data")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("amount", DoubleType(), True),
])


def make_df(spark, rows):
    return spark.createDataFrame(rows, schema)


def test_valid_records_are_kept(spark):
    df = make_df(spark, [(1, "Ahmed", 100.0), (2, "Sara", 0.01), (3, "Omar", 75.5)])
    result = clean_data(df)

    ids = sorted(r["id"] for r in result.collect())
    assert ids == [1, 2, 3]
    assert result.count() == 3


def test_zero_and_negative_amounts_are_removed(spark):
    df = make_df(spark, [(1, "Ahmed", 100.0), (2, "Sara", 0.0), (3, "Omar", -20.0)])
    result = clean_data(df)

    ids = [r["id"] for r in result.collect()]
    assert ids == [1]


def test_null_names_are_removed(spark):
    df = make_df(spark, [(1, "Ahmed", 100.0), (2, None, 50.0)])
    result = clean_data(df)

    ids = [r["id"] for r in result.collect()]
    assert ids == [1]
    assert result.filter(result["name"].isNull()).count() == 0


def test_amount_with_tax(spark):
    df = make_df(spark, [(1, "Ahmed", 100.0), (2, "Sara", 19.99), (3, "Omar", 250.0)])
    result = clean_data(df)

    assert "amount_with_tax" in result.columns
    for row in result.collect():
        # compare floats with a tolerance
        assert row["amount_with_tax"] == pytest.approx(row["amount"] * 1.20)

    taxes = {r["id"]: round(r["amount_with_tax"], 2) for r in result.collect()}
    assert taxes == {1: 120.0, 2: 23.99, 3: 300.0}


def test_mixed_batch(spark):
    df = make_df(spark, [
        (1, "Ahmed", 100.0),
        (2, None, 0.0),
        (3, None, 40.0),
        (4, "Omar", -5.0),
        (5, "Mona", 10.0),
    ])
    result = clean_data(df)

    assert sorted(r["id"] for r in result.collect()) == [1, 5]
    assert result.columns == ["id", "name", "amount", "amount_with_tax"]

def test_empty_dataframe(spark):
    df = make_df(spark, [])
    assert clean_data(df).count() == 0
