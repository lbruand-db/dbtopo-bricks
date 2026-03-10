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


def set_table_geo_metadata(spark, table_name: str, crs: str = "EPSG:4326") -> None:
    """Set CRS table property and geometry column comment."""
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('crs' = '{crs}')")
    spark.sql(
        f"ALTER TABLE {table_name} "
        f"ALTER COLUMN geometry COMMENT 'WKT geometry ({crs})'"
    )


def full_table_name(catalog: str, schema: str, table_prefix: str, layer: str) -> str:
    return f"{catalog}.{schema}.{table_prefix}{layer}"
