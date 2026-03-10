from __future__ import annotations

import pandas as pd
from pyspark.sql.types import StructType


def write_batch_to_delta(
    spark,
    pdf: pd.DataFrame,
    table_name: str,
    schema: StructType | None = None,
) -> None:
    if schema is not None:
        sdf = spark.createDataFrame(pdf, schema=schema)
    else:
        sdf = spark.createDataFrame(pdf)
    sdf.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        table_name
    )


def full_table_name(catalog: str, schema: str, table_prefix: str, layer: str) -> str:
    return f"{catalog}.{schema}.{table_prefix}{layer}"
