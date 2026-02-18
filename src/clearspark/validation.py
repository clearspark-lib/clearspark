from pyspark.sql import SparkSession
from pyspark.sql.column import Column

__all__ = ["_validate_with_buckets_params", "_validate_load_data_params"]

def _validate_with_buckets_params(spark_df, value_column_name: str, bucket_column_name: str, buckets: list):
    """
    Validates the parameters for the `with_buckets` function.

    Args:
        spark_df (DataFrame): Input PySpark DataFrame.
        value_column_name (str): Name of the numeric column to classify.
        bucket_column_name (str): Name of the new bucket column to be created.
        buckets (list): List of numeric boundary values.

    Raises:
        TypeError: If any parameter has an unexpected type.
        ValueError: If `buckets` is empty, contains duplicates, or `value_column_name`
                    does not exist in the DataFrame.
    """
    if not isinstance(value_column_name, str):
        raise TypeError(f"'value_column_name' must be a string, got {type(value_column_name).__name__}.")

    if not isinstance(bucket_column_name, str):
        raise TypeError(f"'bucket_column_name' must be a string, got {type(bucket_column_name).__name__}.")

    if not isinstance(buckets, list):
        raise TypeError(f"'buckets' must be a list, got {type(buckets).__name__}.")

    if len(buckets) == 0:
        raise ValueError("'buckets' must contain at least one boundary value.")

    if not all(isinstance(b, (int, float)) for b in buckets):
        raise TypeError("All values in 'buckets' must be numeric (int or float).")

    if len(buckets) != len(set(buckets)):
        raise ValueError("'buckets' must not contain duplicate values.")

    if value_column_name not in spark_df.columns:
        raise ValueError(f"Column '{value_column_name}' not found in DataFrame. Available columns: {spark_df.columns}.")

def _validate_load_data_params(data_path, spark_session, select_columns, filter_spec, data_format):
    """
    Validates the parameters for the `load_data` function, ensuring robust checks 
    for Spark Session, Column types, and native Python types.
    """
    def _is_column(obj):
        return hasattr(obj, "_jc") or hasattr(obj, "alias")
    
    if not isinstance(spark_session, SparkSession):
        raise TypeError(f"'spark_session' must be a SparkSession, got {type(spark_session).__name__}.")

    if not isinstance(data_path, str) or not data_path.strip():
        raise ValueError(f"'data_path' must be a non-empty string, got {type(data_path).__name__}.")

    if select_columns is not None:
        if not isinstance(select_columns, list):
            raise TypeError(f"'select_columns' must be a list, got {type(select_columns).__name__}.")
        
        for i, c in enumerate(select_columns):
            if not isinstance(c, str) and not _is_column(c):
                raise TypeError(
                    f"Element at index {i} of 'select_columns' must be a string or Column object, "
                    f"got {type(c).__name__}."
                )

    if filter_spec is not None:
        if not isinstance(filter_spec, str) and not _is_column(filter_spec):
            raise TypeError(
                f"'filter_spec' must be a string or a Spark Column expression, "
                f"got {type(filter_spec).__name__}."
            )

    if not isinstance(data_format, str):
        raise TypeError(f"'data_format' must be a string, got {type(data_format).__name__}.")
