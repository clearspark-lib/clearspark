

__all__ = ["_is_catalog_path", "_get_reader"]

def _is_catalog_path(data_path):
    return "/" not in data_path

def _get_reader(spark_session, data_format):
    return spark_session.read.format(data_format)