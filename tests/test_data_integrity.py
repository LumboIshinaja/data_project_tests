import pytest
from utils.schema_definitions import SALES_DATA_SCHEMA
from utils.data_validators import count_duplicates, count_nulls, count_non_positive_values, count_incorrect_totals, count_negative_values, get_distinct_values


@pytest.mark.integrity
def test_transaction_id_uniqueness(sales_data_df):
    """Test that 'transaction_id' is unique in the sales data."""
    duplicate_count = count_duplicates(sales_data_df, "transaction_id")
    assert duplicate_count == 0, f"Found {duplicate_count} duplicate transaction_id(s)"


@pytest.mark.integrity
def test_required_fields_no_null_values(sales_data_df):
    """Test that non-nullable fields contain no NULL values in the actual data."""
    non_nullable_columns = [field.name for field in SALES_DATA_SCHEMA.fields if not field.nullable]

    for column in non_nullable_columns:
        null_count = count_nulls(sales_data_df, column)
        assert null_count == 0, f"Column '{column}' contains NULL values, but it is marked as non-nullable!"


@pytest.mark.integrity
@pytest.mark.business_logic
def test_quantity_positive(sales_data_df):
    """Test that 'quantity' column has positive values."""
    invalid_quantity_count = count_non_positive_values(sales_data_df, "quantity")
    assert invalid_quantity_count == 0, f"Found {invalid_quantity_count} rows with non-positive 'quantity'."


@pytest.mark.integrity
@pytest.mark.business_logic
def test_price_positive(sales_data_df):
    """Test that 'price' column has positive values."""
    invalid_price_count = count_non_positive_values(sales_data_df, "price")
    assert invalid_price_count == 0, f"Found {invalid_price_count} rows with non-positive 'price'."


@pytest.mark.integrity
@pytest.mark.business_logic
def test_discount_non_negative(sales_data_df):
    """Test that 'discount' column has non-negative values."""
    invalid_discount_count = count_negative_values(sales_data_df, "discount")

    assert invalid_discount_count == 0, f"Found {invalid_discount_count} rows with negative 'discount'."


@pytest.mark.integrity
@pytest.mark.business_logic
def test_total_calculation(sales_data_df):
    """Test that 'total' column is correctly calculated as (price * quantity - discount)."""
    incorrect_total_count = count_incorrect_totals(sales_data_df)

    assert incorrect_total_count == 0, f"Found {incorrect_total_count} rows with incorrect 'total' values."

@pytest.mark.integrity
def test_id_exists_in_customers(sales_data_df, customers_data_df):
    """Test if every customer_id in sales data exists in customers data (foreign key check)."""
    sales_customer_ids = get_distinct_values(sales_data_df, 'customer_id')
    customers_ids = get_distinct_values(customers_data_df, 'customer_id')

    assert sales_customer_ids.issubset(customers_ids), (
        f"Missing customer_ids in customers data: {sales_customer_ids - customers_ids}"
    )
