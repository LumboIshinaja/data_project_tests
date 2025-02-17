from pyspark.sql import SparkSession
from utils.schema_definitions import API_TRANSACTIONS_SCHEMA, SALES_DATA_SCHEMA, CUSTOMERS_SCHEMA

def read_csv(spark, file_path, schema=None):
    """Read CSV file into PySpark DataFrame with an optional schema."""
    return spark.read.csv(file_path, header=True, schema=schema)

def load_dataframe(spark, file_path, schema):
    """Generic function to load any dataset with a given schema."""
    return read_csv(spark, file_path, schema)

def read_json(spark, file_path, schema=None):
    """Read JSON file into PySpark DataFrame with an optional schema."""
    return spark.read.json(file_path, schema=schema)

def load_json_dataframe(spark, file_path, schema):
    """Generic function to load JSON dataset with a given schema."""
    return read_json(spark, file_path, schema)

# Initialize Spark Session
spark = SparkSession.builder.appName("DataLayerSimulation").getOrCreate()

# File paths
api_transactions_path  = "data/api_transactions.json"
sales_data_path = "data/sales_data.csv"
customers_data_path = "data/customers.csv"

# Load data into DataFrames
sales_df = load_dataframe(spark, sales_data_path, SALES_DATA_SCHEMA)
customers_df = load_dataframe(spark, customers_data_path, CUSTOMERS_SCHEMA)
api_transactions_df = spark.read.option("multiline", "true").json(api_transactions_path, schema=API_TRANSACTIONS_SCHEMA)


# Create temporary views to simulate raw and structured data Convert view back into a DataFrame
# This is an attempt to simulate the real world flow since we dont have DataWarehouse nearby :)
api_transactions_df.createOrReplaceTempView("raw_api_transactions")
api_transactions_view_df = spark.sql("SELECT * FROM raw_api_transactions")


print("Temporary view created successfully.")

# spark.sql("SELECT * FROM raw_api_transactions LIMIT 100").show()