
from clearspark.utils.validation_util import \
    _is_column, \
    _is_spark_session

from pyspark.sql.dataframe import \
    DataFrame

__all__ = ["_validate_with_buckets_params", "_validate_load_data_params", "_validate_with_categories_params", "_validate_save_data_params"]

def _validate_with_buckets_params(spark_df, value_column_name: str, bucket_column_name: str, buckets: list):
    """
    Validates the parameters for the `with_buckets` function.

    Args:
        spark_df (DataFrame): Input PySpark DataFrame.
        value_column_name (str): Name of the numeric column to classify.
        bucket_column_name (str): Name of the new bucket column to be created.
        buckets (list): List of numeri ac boundary values.

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

def _validate_with_categories_params(spark_df, value_column_name, category_column_name, mapping):
    """
    Validates the parameters for the `with_categories` function.

    Args:
        spark_df (DataFrame): Input PySpark DataFrame.
        value_column_name (str): Name of the column to be categorized.
        category_column_name (str): Name of the new category column.
        mapping (dict): Dictionary mapping categories to lists of values.

    Raises:
        TypeError: If parameters are not of the expected types or if mapping 
                   values are not lists.
        ValueError: If mapping is empty or if the input column is missing.
    """
    if not isinstance(value_column_name, str):
        raise TypeError(f"'value_column_name' must be a string, got {type(value_column_name).__name__}.")

    if not isinstance(category_column_name, str):
        raise TypeError(f"'category_column_name' must be a string, got {type(category_column_name).__name__}.")

    if not isinstance(mapping, dict):
        raise TypeError(f"'mapping' must be a dictionary, got {type(mapping).__name__}.")

    if not mapping:
        raise ValueError("'mapping' dictionary cannot be empty.")

    for category, subcategories in mapping.items():
        if not isinstance(category, str):
            raise TypeError(f"Mapping keys (categories) must be strings, got {type(category).__name__} for '{category}'.")
        
        if not isinstance(subcategories, list):
            raise TypeError(
                f"Mapping values must be lists of subcategories, "
                f"got {type(subcategories).__name__} for category '{category}'."
            )

    if value_column_name not in spark_df.columns:
        raise ValueError(
            f"Column '{value_column_name}' not found in DataFrame. "
            f"Available columns: {spark_df.columns}."
        )

def _validate_load_data_params(data_path, spark_session, select_columns, filter_spec, data_format):
    """
    Validates the parameters for the `load_data` function, ensuring robust checks 
    for Spark Session, Column types, and native Python types.
    """

    if not _is_spark_session(spark_session):
        raise TypeError(f"'spark_session' must be a SparkSession-like object, got {type(spark_session).__name__}.")

    if not isinstance(data_path, str) or not data_path.strip():
        raise ValueError(f"'data_path' must be a non-empty string, got {type(data_path).__name__}.")

    if select_columns is not None:
        if not isinstance(select_columns, list):
            raise TypeError(f"'select_columns' must be a list, got {type(select_columns).__name__}.")
        
        for index, element in enumerate(select_columns):
            if not isinstance(element, str) and not _is_column(element):
                raise TypeError(
                    f"Element at index {index} of 'select_columns' must be a string or Column object, "
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
    

VALID_WRITE_MODES = {"overwrite", "append", "ignore", "error"}

def _validate_save_data_params(df, data_path, data_format, mode, partition_by):
    if not isinstance(df, DataFrame):
        raise TypeError(f"'df' must be a PySpark DataFrame, got {type(df)}.")

    if not isinstance(data_path, str) or not data_path.strip():
        raise ValueError("'data_path' must be a non-empty string.")

    if not isinstance(data_format, str) or not data_format.strip():
        raise TypeError(f"'data_format' must be a non-empty string, got {type(data_format)}.")

    if mode not in VALID_WRITE_MODES:
        raise ValueError(f"'mode' must be one of {VALID_WRITE_MODES}, got '{mode}'.")

    if partition_by is not None:
        if not isinstance(partition_by, list):
            raise TypeError(f"'partition_by' must be a list, got {type(partition_by)}.")
        for item in partition_by:
            if not isinstance(item, str):
                raise TypeError(f"All items in 'partition_by' must be strings, got {type(item)}.")