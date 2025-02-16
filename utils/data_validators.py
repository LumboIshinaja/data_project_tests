import time


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

def measure_execution_time(action):
    """
    Measure execution time of a PySpark DataFrame action.
    Args:
        action (function): Function representing a PySpark action (e.g., df.collect, df.count).
    Returns:
        tuple(float, Any): Execution time in seconds and the result of the action.
    """
    start_time = time.perf_counter()
    result = action()
    end_time = time.perf_counter()

    execution_time = end_time - start_time
    return execution_time, result
