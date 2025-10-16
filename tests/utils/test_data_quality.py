import pytest
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, TimestampType
from datetime import datetime, timedelta

from src.utils.data_quality import apply_data_quality_checks


def test_apply_data_quality_checks(spark_session):
    """Testa a função apply_data_quality_checks com vários cenários."""
    schema = StructType([
        StructField("total_amount", DoubleType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
    ])

    now = datetime.now()
    data = [
        # Caso Válido
        (10.0, 1, now, now + timedelta(minutes=15)),
        # Casos Inválidos
        (-5.0, 1, now, now + timedelta(minutes=15)),  # invalid_total_amount
        (10.0, 0, now, now + timedelta(minutes=15)),   # invalid_passenger_count
        (10.0, 1, None, now + timedelta(minutes=15)), # null_pickup_datetime
        (10.0, 1, now, None),                         # null_dropoff_datetime
        (10.0, 1, now, now - timedelta(minutes=5)),   # invalid_trip_duration
        (10.0, 1, now, now + timedelta(days=2)),      # trip_duration_too_long
    ]

    df = spark_session.createDataFrame(data, schema)

    # Aplica a função de DQ
    dq_df = apply_data_quality_checks(df)

    # Coleta os resultados para verificação
    results = dq_df.collect()

    # Verificações
    # 1. Caso Válido
    assert results[0]["dq_status"] == "PASS"
    assert len([f for f in results[0]["dq_failures"] if f is not None]) == 0

    # 2. invalid_total_amount
    assert results[1]["dq_status"] == "FAIL"
    assert "invalid_total_amount" in results[1]["dq_failures"]

    # 3. invalid_passenger_count
    assert results[2]["dq_status"] == "FAIL"
    assert "invalid_passenger_count" in results[2]["dq_failures"]

    # 4. null_pickup_datetime
    assert results[3]["dq_status"] == "FAIL"
    assert "null_pickup_datetime" in results[3]["dq_failures"]

    # 5. null_dropoff_datetime
    assert results[4]["dq_status"] == "FAIL"
    assert "null_dropoff_datetime" in results[4]["dq_failures"]

    # 6. invalid_trip_duration
    assert results[5]["dq_status"] == "FAIL"
    assert "invalid_trip_duration" in results[5]["dq_failures"]

    # 7. trip_duration_too_long
    assert results[6]["dq_status"] == "FAIL"
    assert "trip_duration_too_long" in results[6]["dq_failures"]
