from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    """
    Cria e retorna uma SparkSession configurada para Delta Lake.

    :param app_name: O nome da aplicação Spark.
    :return: Uma instância de SparkSession.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()