import pytest
from utils.schema_definitions import SALES_DATA_SCHEMA, CUSTOMERS_SCHEMA, API_TRANSACTIONS_SCHEMA


@pytest.mark.schema
def test_sales_schema_column_names(sales_data_df):
    """Test that the sales data has expected column names."""
    actual_columns = sales_data_df.columns
    expected_columns = [field.name for field in SALES_DATA_SCHEMA.fields]

    assert actual_columns == expected_columns, (
        f"Column names mismatch:\nExpected: {expected_columns}\nGot: {actual_columns}"
    )


@pytest.mark.schema
def test_sales_schema_column_types(sales_data_df):
    """Test that the sales data has expected column types."""
    actual_types = [type(field.dataType) for field in sales_data_df.schema.fields]
    expected_types = [type(field.dataType) for field in SALES_DATA_SCHEMA.fields]

    assert actual_types == expected_types, (
        f"Column types mismatch:\nExpected: {expected_types}\nGot: {actual_types}"
    )

@pytest.mark.schema
def test_customers_schema_column_names(customers_data_df):
    """Test that the customers data has expected column names."""
    actual_columns = customers_data_df.columns
    expected_columns = [field.name for field in CUSTOMERS_SCHEMA.fields]

    assert actual_columns == expected_columns, (
        f"Column names mismatch:\nExpected: {expected_columns}\nGot: {actual_columns}"
    )


@pytest.mark.schema
def test_customers_schema_column_types(customers_data_df):
    """Test that the customers data has expected column types."""
    actual_types = [type(field.dataType) for field in customers_data_df.schema.fields]
    expected_types = [type(field.dataType) for field in CUSTOMERS_SCHEMA.fields]

    assert actual_types == expected_types, (
        f"Column types mismatch:\nExpected: {expected_types}\nGot: {actual_types}"
    )

@pytest.mark.schema
@pytest.mark.raw_data
def test_api_transactions_schema_column_names(api_transactions_view_data_df, api_transactions_data_df):
    """Test that the raw API transactions view has the expected column names."""
    actual_columns = api_transactions_view_data_df.columns
    expected_columns = api_transactions_data_df.columns  # Source DataFrame columns

    assert actual_columns == expected_columns, (
        f"Column names mismatch:\nExpected: {expected_columns}\nGot: {actual_columns}"
    )


@pytest.mark.schema
@pytest.mark.raw_data
def test_api_transactions_schema_column_types(api_transactions_view_data_df, api_transactions_data_df):
    """Test that the raw API transactions view has the expected column types."""
    actual_types = [type(field.dataType) for field in api_transactions_view_data_df.schema.fields]
    expected_types = [type(field.dataType) for field in api_transactions_data_df.schema.fields]

    assert actual_types == expected_types, (
        f"Column types mismatch:\nExpected: {expected_types}\nGot: {actual_types}"
    )
