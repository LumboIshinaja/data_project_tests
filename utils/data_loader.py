def read_csv(spark, file_path, schema=None):
    """Read CSV file into PySpark DataFrame with an optional schema."""
    return spark.read.csv(file_path, header=True, schema=schema)
