
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

@udf(returnType=FloatType())
def int_to_usd(int):
    """convert int to usd (1 usd=88.72rs)"""
    return int/88.72


