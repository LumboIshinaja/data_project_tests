from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

SALES_DATA_SCHEMA = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product", StringType(), False),
    StructField("category", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DoubleType(), False),      
    StructField("discount", DoubleType(), False),   
    StructField("total", DoubleType(), False),      
    StructField("purchase_date", TimestampType(), False),
    StructField("country", StringType(), False),
])
