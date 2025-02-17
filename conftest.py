import pytest
from pyspark.sql import SparkSession
from utils.data_loader import api_transactions_df, sales_df, customers_df, api_transactions_view_df

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
def sales_data_df():
    """Fixture to provide Sales Data DataFrame."""
    return sales_df

@pytest.fixture(scope="module")
def customers_data_df():
    """Fixture to provide Customers Data DataFrame."""
    return customers_df

@pytest.fixture(scope="module")
def api_transactions_data_df():
    """Fixture to provide API response DataFrame."""
    return api_transactions_df

@pytest.fixture(scope="module")
def api_transactions_view_data_df():
    """Fixture to provide API response DataFrame."""
    return api_transactions_view_df
