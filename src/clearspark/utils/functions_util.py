
__all__ = ["_is_catalog_path", "_get_reader", "_get_writer"]

def _is_catalog_path(data_path):
    return "/" not in data_path

def _get_reader(spark_session, data_format):
    return spark_session.read.format(data_format)

def _get_writer(df, data_format: str, mode: str, partition_by: list):
    writer = df.write.format(data_format).mode(mode)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    return writer