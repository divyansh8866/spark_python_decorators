from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Create a Spark session
spark = SparkSession.builder.appName("SampleDataExample").getOrCreate()

# Define the sample_data decorator
def sample_data(fraction=0.1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            sampled_result = result.sample(fraction)
            return sampled_result
        return wrapper
    return decorator

# Define a Spark function decorated with sample_data
# Here fraction is required_records/total_records . 0.5 for 50% of records and 1.0 for 100% of data.
@sample_data(fraction=0.5)
def my_spark_function():
    # Simulate Spark processing logic with a DataFrame creation
    data = [("1", "Alice", "25"), ("2", "Bob", "30")]
    columns = ["id", "name", "age"]
    df = spark.createDataFrame(data, columns)
    return df

# Call the decorated function
sampled_result_df = my_spark_function()

# Show the sampled result DataFrame
sampled_result_df.show()

# Stop the Spark session
spark.stop()
