from pyspark.sql.functions import\
     when, \
     col, \
     lit

from pyspark.sql.dataframe import \
    DataFrame

from clearspark.validation import\
     _validate_with_buckets_params, \
     _validate_with_categories_params, \
     _validate_load_data_params

from clearspark.utils.functions_util import \
     _is_catalog_path, \
     _get_reader

__all__ = ["with_buckets", "load_data", "with_categories"]

def with_buckets(spark_df: DataFrame, value_column_name: str, bucket_column_name: str, buckets: list) -> DataFrame:
    """
    Adds a categorical column to a PySpark DataFrame that classifies numeric values into labeled ranges (buckets).

    The resulting labels are prefixed with a zero-padded index to preserve natural sort order,
    making them suitable for ordering in charts and reports.

    Args:
        spark_df (DataFrame): Input PySpark DataFrame.
        value_column_name (str): Name of the numeric column to classify.
        bucket_column_name (str): Name of the new column to be added with the bucket labels.
        buckets (list): List of numeric boundary values that define the bucket edges.
                        Does not need to be sorted. Must contain at least one value.

    Returns:
        DataFrame: A new DataFrame with an additional string column containing the bucket label
                   for each row.

    Raises:
        IndexError: If `buckets` is an empty list.

    Label data_format:
        "00. no info"       → null values
        "01. <{min}"        → values below the smallest boundary
        "0N. {low}-{high}"  → values within a boundary range (inclusive)
        "0N. >{max}"        → values above the largest boundary

    Example:
        >>> with_buckets(df, "revenue", "revenue_range", [100, 500, 1000])

        Resulting labels in "revenue_range":
            "00. no info"    → null
            "01. <100"       → revenue < 100
            "02. 100-500"    → 100 <= revenue <= 500
            "03. 500-1000"   → 500 <= revenue <= 1000
            "04. >1000"      → revenue > 1000
    """
    _validate_with_buckets_params(spark_df, value_column_name, bucket_column_name, buckets)

    value_col = col(value_column_name)
    sorted_buckets = sorted(buckets) 
    n = len(sorted_buckets)

    #
    expr = when(
        value_col.isNull(), 
        lit("00. no info")
    )
    
    expr = expr.when(
        value_col < sorted_buckets[0], 
        lit(f"01. <{sorted_buckets[0]}")
    )

    for i in range(len(sorted_buckets) - 1):
        low, high = sorted_buckets[i], sorted_buckets[i + 1]

        label = f"{(i + 2):02d}. {low}-{high}"

        expr = expr\
            .when(
                (value_col >= low) & (value_col <= high), 
                lit(label)
            )

    expr = expr\
        .otherwise(
            lit(f"{(n + 1):02d}. >{sorted_buckets[-1]}")
        )

    return spark_df.withColumn(bucket_column_name, expr)

def with_categories(spark_df: DataFrame, value_column_name: str, category_column_name: str, mapping: dict) -> DataFrame:
    """
    Categorizes string or numeric values into broader groups based on a mapping dictionary.

    This function creates a new column where each row is assigned a category label if its 
    value exists in the provided mapping lists. It handles nulls explicitly and provides 
    a fallback for values not found in the dictionary.

    Args:
        spark_df (DataFrame): Input PySpark DataFrame.
        value_column_name (str): Name of the existing column to be categorized.
        category_column_name (str): Name of the new column to be added with the category labels.
        categories (dict): A dictionary where keys are category labels (str) and 
                           values are lists of elements that belong to that category.
                           Example: {"Fruit": ["Apple", "Banana"], "Vegetable": ["Carrot"]}.

    Returns:
        DataFrame: A new DataFrame with the additional categorical column.

    Label data_format:
        "no info"       → Applied to null values in the source column.
        "{key}"         → Applied if the value is found in the corresponding dictionary list.
        "uncategorized" → Applied to any value not present in the mapping dictionary.

    Example:
        >>> mapping = {
        ...     "Finance": ["bank", "fintech", "insurance"],
        ...     "Tech": ["saas", "cloud", "hardware"]
        ... }
        >>> with_categories(df, "industry", "sector", mapping)

        If "industry" is "bank", "sector" becomes "Finance".
        If "industry" is "agriculture", "sector" becomes "uncategorized".
        If "industry" is null, "sector" becomes "no info".
    """
    _validate_with_categories_params(spark_df, value_column_name, category_column_name, mapping)

    value_col = col(value_column_name)

    expr = when(
        value_col.isNull(),
        lit('no info')
    )

    for category, subcategory in mapping.items():

        expr = expr.when(
            value_col.isin(subcategory),
            lit(category)
        )

    expr = expr.otherwise(lit('uncategorized'))

    return spark_df.withColumn(category_column_name, expr)

def load_data(data_path: str, spark_session, select_columns: list = None, filter_spec = None, data_format: str = "delta") -> DataFrame:
    """
    Loads data into a PySpark DataFrame from a catalog table or a file path, 
    with optional column selection and row filtering.

    This function abstracts the data source type. If the path does not contain a "/", 
    it treats the input as a Spark Catalog table; otherwise, it treats it as a file path.

    Args:
        data_path (str): The table name (e.g., 'database.table') or the file path 
                         (e.g., 's3://bucket/path').
        spark_session (SparkSession): The active Spark session. (in DataBricks is the variable 'spark')
        select_columns (list, optional): List of column names (str) or Spark Column objects 
                                         to select. Defaults to None.
        filter_spec (str or Column, optional): A SQL-like filter expression (str) or a 
                                               Spark Column boolean expression. 
                                               Defaults to None.
        data_format (str, optional): The format of the data (e.g., "delta", "parquet"). 
                                     Defaults to "delta".

    Returns:
        DataFrame: A PySpark DataFrame with the applied selections and filters.

    Raises:
        TypeError: If any parameter has an unexpected type or if `select_columns` 
                   contains invalid types.
        ValueError: If `data_path` is empty or invalid.

    Example:
        >>> # Using strings and SQL filter
        >>> load_data("gold.sales", spark, select_columns=["id", "amount"], filter_spec="amount > 100")
        
        >>> # Using Column objects and Column expressions
        >>> from pyspark.sql.functions import col
        >>> load_data("/mnt/data/logs", spark, select_columns=[col("id"), col("status")], filter_spec=col("status").isNotNull())
    """
    _validate_load_data_params(data_path, spark_session, select_columns, filter_spec, data_format)

    reader = _get_reader(spark_session, data_format)

    if _is_catalog_path(data_path):
        df = reader.table(data_path)
    else:
        df = reader.load(data_path)

    if select_columns is not None:
        df = df.select(*select_columns)

    if filter_spec is not None:
        df = df.filter(filter_spec)

    return df