from pyspark.sql.functions import\
     when, \
     col, \
     lit

from clearspark.validation import\
     _validate_with_buckets_params

__all__ = ["with_buckets"]

def with_buckets(spark_df, value_column_name: str, bucket_column_name: str, buckets: list):
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

    Label format:
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