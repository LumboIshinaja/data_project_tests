from pyspark.sql import SparkSession
from utils.schema_definitions import API_RESPONSE_SCHEMA, SALES_DATA_SCHEMA, CUSTOMERS_SCHEMA

def read_csv(spark, file_path, schema=None):
    """Read CSV file into PySpark DataFrame with an optional schema."""
    return spark.read.csv(file_path, header=True, schema=schema)

def load_dataframe(spark, file_path, schema):
    """Generic function to load any dataset with a given schema."""
    return read_csv(spark, file_path, schema)

# Initialize Spark Session
spark = SparkSession.builder.appName("DataLayerSimulation").getOrCreate()

# File paths
api_response_path = "data/api_response.csv"
sales_data_path = "data/sales_data.csv"
customers_data_path = "data/customers.csv"

# Load data using the generic function
api_df = load_dataframe(spark, api_response_path, API_RESPONSE_SCHEMA)
sales_df = load_dataframe(spark, sales_data_path, SALES_DATA_SCHEMA)
customers_df = load_dataframe(spark, customers_data_path, CUSTOMERS_SCHEMA)

# Create temporary views to simulate raw and structured data
api_df.createOrReplaceTempView("api_response")

print("Temporary views created successfully.")
