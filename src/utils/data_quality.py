from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, array, size, unix_timestamp, expr


def apply_data_quality_checks(df: DataFrame) -> DataFrame:
    """
    Aplica um conjunto de regras de qualidade de dados e rotula cada registro.

    :param df: DataFrame a ser verificado.
    :return: DataFrame com colunas de status de DQ ('dq_status', 'dq_failures').
    """
    # Define as regras de qualidade e a mensagem de erro para cada uma
    dq_rules = [
        (col("total_amount") <= 0, "invalid_total_amount"),
        (col("passenger_count") <= 0, "invalid_passenger_count"),
        (col("pickup_datetime").isNull(), "null_pickup_datetime"),
        (col("dropoff_datetime").isNull(), "null_dropoff_datetime"),
        # Regra mais avançada: a corrida não pode durar mais de 24 horas
        (col("dropoff_datetime") <= col("pickup_datetime"), "invalid_trip_duration"),
        (
            (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) > 24 * 3600,
            "trip_duration_too_long"
        )
    ]

    # Cria uma coluna de array contendo todas as falhas para um registro
    failure_reasons_col = array([when(rule, lit(reason)) for rule, reason in dq_rules])

    # Adiciona a coluna de falhas (com nulos) e depois filtra
    df_with_failures = df.withColumn("dq_failures_with_nulls", failure_reasons_col)
    
    df_with_filtered_failures = df_with_failures.withColumn(
        "dq_failures",
        expr("filter(dq_failures_with_nulls, x -> x is not null)")
    )

    return df_with_filtered_failures.withColumn(
        "dq_status",
        when(size(col("dq_failures")) == 0, lit("PASS")).otherwise(lit("FAIL"))
    ).drop("dq_failures_with_nulls")