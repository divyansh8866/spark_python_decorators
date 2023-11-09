# Enhancing Spark Functions with Python Decorators

**Visit Me @** : [Divyansh Patel](https://divyanshpatel.com/)
Get full code with example here : Github Repo

Apache Spark, the powerful distributed computing framework, offers extensive capabilities for processing large-scale data. While working with Spark, it's crucial to ensure that your data processing functions are not only efficient but also robust. In this blog post, we'll explore how Python decorators can be employed to enhance Spark functions, addressing issues related to schema validation, schema inference, data sampling, memory usage monitoring, and dynamic partitions.

## 1. Schema Validation Decorator

```python
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

# usage 
@validate_schema(expected_schema)
def my_spark_function(*args, **kwargs):
    # Your Spark processing logic here
    return result

```

This decorator ensures that the output schema of your Spark function matches the expected schema. It acts as a safety net, preventing potential issues caused by schema changes that might otherwise go unnoticed.

## 2. Schema Inference Decorator

```python
def infer_schema(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        inferred_schema = result.schema
        print(f"Inferred Schema: {inferred_schema}")
        return result
    return wrapper

# Usage
@infer_schema
def my_spark_function(*args, **kwargs):
    # Your Spark processing logic here
    return result
```

During development, it's crucial to quickly inspect the structure of the result. The `infer_schema` decorator prints the inferred schema of the output DataFrame, providing developers with valuable insights into the data.

## 3. Sampling Decorator

```python
def sample_data(fraction=0.1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            sampled_result = result.sample(fraction)
            return sampled_result
        return wrapper
    return decorator
# Usage
@sample_data(fraction=0.1)
def my_spark_function(*args, **kwargs):
    # Your Spark processing logic here
    return result
```

For a rapid overview of the data, the `sample_data` decorator allows you to obtain a fraction of the output DataFrame. This is particularly handy for quick data inspection and debugging.

## 4. Dynamic Partitions Decorator

```python
def dynamic_partitions(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        num_partitions = result.rdd.getNumPartitions()
        dynamic_partitions = max(1, min(num_partitions * 2, 200))
        result = result.coalesce(dynamic_partitions)
        return result
    return wrapper
# Usage
@dynamic_partitions
def my_spark_function(*args, **kwargs):
    # Your Spark processing logic here
    return result

```

Optimizing the balance between parallelism and resource utilization is essential. The `dynamic_partitions` decorator dynamically adjusts the number of partitions based on the current number of partitions in the result DataFrame, enhancing performance in various scenarios.

## 5. **Checkpoint Decorator**

```python
def checkpoint_result(checkpoint_dir):
    def decorator(func):
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            result.write.mode("overwrite").option("checkpointLocation", checkpoint_dir).save()
            return result
        return wrapper
    return decorator
# Usage
# Define a Spark function decorated with checkpoint_result
@checkpoint_result(checkpoint_dir="path/to/checkpoint")
def my_spark_function():
    # Simulate Spark processing logic with a DataFrame creation
    data = [("1", "Alice", "25"), ("2", "Bob", "30")]
    columns = ["id", "name", "age"]
    df = spark.createDataFrame(data, columns)
    return df
```

This decorator creates a checkpoint of the result DataFrame. Checkpoints help in optimizing the execution plan and can be particularly useful in iterative algorithms.

**Conclusion**
In this blog, we explored Python decorators to augment Apache Spark functions, covering schema validation, inference, data sampling, dynamic partitioning, and result checkpointing. Stay tuned for more decorators on my website, offering enhanced capabilities for efficient and reliable large-scale data processing in Spark.
