import pytest
from pyspark.sql import SparkSession
from utils.data_loader import read_csv
from utils.schema_definitions import SALES_DATA_SCHEMA, CUSTOMERS_SCHEMA

@pytest.fixture(scope="session")
def spark():
    """Creates a SparkSession for testing."""
    spark = SparkSession.builder \
        .appName("Data Testing Project") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="module")
def sales_df(spark):
    """Load sales data as PySpark DataFrame with a predefined schema."""
    return read_csv(spark, "data/sales_data.csv", schema=SALES_DATA_SCHEMA)

@pytest.fixture(scope="module")
def customers_df(spark):
    """Load customers data as PySpark DataFrame with a predefined schema."""
    return read_csv(spark, "data/customers.csv", schema=CUSTOMERS_SCHEMA)

