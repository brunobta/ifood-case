import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """
    Fixture para criar uma SparkSession para os testes, com tear up e tear down.
    """
    session = SparkSession.builder \
        .master("local[2]") \
        .appName("PytestSparkSession") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    yield session
    
    session.stop()

