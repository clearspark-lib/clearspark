
__all__ = ["_validate_with_buckets_params"]

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