from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Create a Spark session
spark = SparkSession.builder.appName("DynamicPartitionsExample").getOrCreate()

# Define the dynamic_partitions decorator
def dynamic_partitions(func):
    def wrapper(*args, **kwargs):
        # Execute the Spark function
        result = func(*args, **kwargs)

        # Get the current number of partitions
        num_partitions = result.rdd.getNumPartitions()

        # Calculate the desired number of dynamic partitions
        dynamic_partitions = max(1, min(num_partitions * 2, 200))

        # Coalesce the result DataFrame to the dynamic number of partitions
        result = result.coalesce(dynamic_partitions)

        return result

    return wrapper

# Define a Spark function decorated with dynamic_partitions
@dynamic_partitions
def my_spark_function():
    # Simulate Spark processing logic with a DataFrame creation
    data = [("1", "Alice", "25"), ("2", "Bob", "30")]
    columns = ["id", "name", "age"]
    df = spark.createDataFrame(data, columns)
    return df

# Call the decorated function
result_df = my_spark_function()

# Show the result DataFrame
result_df.show()

# Stop the Spark session
spark.stop()
