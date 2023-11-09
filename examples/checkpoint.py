from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Create a Spark session
spark = SparkSession.builder.appName("CheckpointResultExample").getOrCreate()

# Define the checkpoint_result decorator
def checkpoint_result(checkpoint_dir):
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Execute the Spark function
            result = func(*args, **kwargs)

            # Write the result DataFrame to the checkpoint location
            result.write.mode("overwrite").option("checkpointLocation", checkpoint_dir).save()

            return result

        return wrapper

    return decorator

# Define a Spark function decorated with checkpoint_result
@checkpoint_result(checkpoint_dir="path/to/checkpoint")
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
