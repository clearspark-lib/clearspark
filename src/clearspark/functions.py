from pyspark.sql.functions import\
     when, \
     col, \
     lit

from pyspark.sql.dataframe import \
    DataFrame

from clearspark.validation import\
     _validate_with_buckets_params, \
     _validate_with_categories_params, \
     _validate_load_data_params, \
     _validate_save_data_params

from clearspark.utils.functions_util import \
     _is_catalog_path, \
     _get_reader, \
     _get_writer

from clearspark.docstrings import \
    load_data_doc, \
    save_data_doc, \
    with_buckets_doc, \
    with_categories_doc

__all__ = [
    "load_data",
    "save_data",
    "with_buckets",
    "with_categories"
]

# 'data' sufix functions

def load_data(spark_session, data_path: str, select_columns: list = None, filter_specs = None, data_format: str = "delta") -> DataFrame:

    _validate_load_data_params(data_path, spark_session, select_columns, filter_specs, data_format)

    reader = _get_reader(spark_session, data_format)

    if _is_catalog_path(data_path):
        df = reader.table(data_path)
    else:
        df = reader.load(data_path)

    if select_columns is not None:
        df = df.select(*select_columns)

    if filter_specs is not None:
        df = df.filter(filter_specs)

    return df

def save_data(df: DataFrame, data_path: str, data_format: str = "delta", mode: str = "overwrite", partition_by: list = None) -> None:

    _validate_save_data_params(df, data_path, data_format, mode, partition_by)

    writer = _get_writer(df, data_format, mode, partition_by)

    if _is_catalog_path(data_path):
        writer.saveAsTable(data_path)
    else:
        writer.save(data_path)

# 'with' prefix functions

def with_buckets(spark_df: DataFrame, value_column_name: str, bucket_column_name: str, buckets: list) -> DataFrame:

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

# add docstrings
load_data.__doc__ = load_data_doc
save_data.__doc__ = save_data_doc
with_buckets.__doc__ = with_buckets_doc
with_categories.__doc__ = with_buckets_doc