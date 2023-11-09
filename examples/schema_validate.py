from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Create a Spark session
spark = SparkSession.builder.appName("SchemaValidationExample").getOrCreate()

# Define an example schema
expected_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("age", StringType(), True)
])


# Define the validate_schema decorator
def validate_schema(expected_schema):
    def decorator(func):
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            actual_schema = result.schema
            if expected_schema != actual_schema:
                raise ValueError(f"Schema mismatch. Expected: {expected_schema}, Actual: {actual_schema}")
            return result
        return wrapper
    return decorator

# Define a Spark function decorated with validate_schema
@validate_schema(expected_schema)
def my_spark_function():
    # Simulate Spark processing logic with a DataFrame creation
    data = [("1", "Alice", "25"), ("2", "Bob", "30")]
    columns = ["id", "name", "age"]
    df = spark.createDataFrame(data, columns)
    return df

# Call the decorated function
result_df = my_spark_function()
print("Schema validation passed successfully!")
spark.stop()
