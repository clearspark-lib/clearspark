
__all__ = ['_is_column', '_is_spark_session']

def _is_column(obj):
    return hasattr(obj, '_jc') or hasattr(obj, 'alias')

def _is_spark_session(obj):
    return hasattr(obj, "read") and hasattr(obj, "sql")