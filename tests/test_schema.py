import pytest
from utils.schema_definitions import SALES_DATA_SCHEMA, CUSTOMERS_SCHEMA


@pytest.mark.schema
def test_sales_schema_column_names(sales_df):
    """Test that the sales data has expected column names."""
    actual_columns = sales_df.columns
    expected_columns = [field.name for field in SALES_DATA_SCHEMA.fields]

    assert actual_columns == expected_columns, (
        f"Column names mismatch:\nExpected: {expected_columns}\nGot: {actual_columns}"
    )


@pytest.mark.schema
def test_sales_schema_column_types(sales_df):
    """Test that the sales data has expected column types."""
    actual_types = [type(field.dataType) for field in sales_df.schema.fields]
    expected_types = [type(field.dataType) for field in SALES_DATA_SCHEMA.fields]

    assert actual_types == expected_types, (
        f"Column types mismatch:\nExpected: {expected_types}\nGot: {actual_types}"
    )

@pytest.mark.schema
def test_customers_schema_column_names(customers_df):
    """Test that the customers data has expected column names."""
    actual_columns = customers_df.columns
    expected_columns = [field.name for field in CUSTOMERS_SCHEMA.fields]

    assert actual_columns == expected_columns, (
        f"Column names mismatch:\nExpected: {expected_columns}\nGot: {actual_columns}"
    )


@pytest.mark.schema
def test_customers_schema_column_types(customers_df):
    """Test that the customers data has expected column types."""
    actual_types = [type(field.dataType) for field in customers_df.schema.fields]
    expected_types = [type(field.dataType) for field in CUSTOMERS_SCHEMA.fields]

    assert actual_types == expected_types, (
        f"Column types mismatch:\nExpected: {expected_types}\nGot: {actual_types}"
    )