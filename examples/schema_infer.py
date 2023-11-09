from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Create a Spark session
spark = SparkSession.builder.appName("SchemaInferenceExample").getOrCreate()

# Define the infer_schema decorator
def infer_schema(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        inferred_schema = result.schema
        print(f"Inferred Schema: {inferred_schema}")
        return result
    return wrapper

# Define a Spark function decorated with infer_schema
@infer_schema
def my_spark_function():
    # Simulate Spark processing logic with a DataFrame creation
    data = [("1", "Alice", "25"), ("2", "Bob", "30")]
    columns = ["id", "name", "age"]
    df = spark.createDataFrame(data, columns)
    return df

# Call the decorated function
result_df = my_spark_function()

# Stop the Spark session
spark.stop()
