from __future__ import annotations

import pandas as pd


def write_batch_to_delta(
    spark,
    pdf: pd.DataFrame,
    table_name: str,
) -> None:
    sdf = spark.createDataFrame(pdf)
    sdf.write.format("delta").mode("append").saveAsTable(table_name)


def full_table_name(catalog: str, schema: str, table_prefix: str, layer: str) -> str:
    return f"{catalog}.{schema}.{table_prefix}{layer}"
