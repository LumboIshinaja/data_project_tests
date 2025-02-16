def count_duplicates(df, column_name):
    """Returns the number of duplicate values in a column."""
    return df.groupBy(column_name).count().filter("count > 1").count()

def count_nulls(df, column_name):
    """Returns the number of null values in a column."""
    return df.filter(df[column_name].isNull()).count()

def count_non_positive_values(df, column_name):
    """Count rows where a given column has non-positive values (<= 0)."""
    return df.filter(df[column_name] <= 0).count()

def count_negative_values(df, column_name):
    """Count rows where a given column has negative values (< 0)."""
    return df.filter(df[column_name] < 0).count()

def count_incorrect_totals(df):
    """Count rows where 'total' is not equal to (price * quantity - discount)."""
    return df.filter(
        df["total"] != (df["price"] * df["quantity"] - df["discount"])
    ).count()

def get_distinct_values(df, column_name):
    """Get distinct values from a column as a set."""
    return {row[column_name] for row in df.select(column_name).distinct().collect()}
