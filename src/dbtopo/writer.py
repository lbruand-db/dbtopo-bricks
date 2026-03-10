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


def set_table_geo_metadata(
    spark,
    table_name: str,
    crs: str = "EPSG:4326",
    source_schema: str = "",
    version: str = "",
    version_date: str = "",
) -> None:
    """Set CRS, source info as table properties and comments."""
    props = f"'crs' = '{crs}'"
    comment_parts = [f"IGN BD TOPO {version}"]
    if source_schema:
        props += f", 'source_schema' = '{source_schema}'"
    if version:
        props += f", 'bdtopo_version' = '{version}'"
    if version_date:
        props += f", 'bdtopo_version_date' = '{version_date}'"
        comment_parts.append(f"date={version_date}")
    comment_parts.append(f"geometry={crs}")
    comment = ", ".join(comment_parts)

    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ({props})")
    spark.sql(f"COMMENT ON TABLE {table_name} IS '{comment}'")
    spark.sql(
        f"ALTER TABLE {table_name} ALTER COLUMN geometry COMMENT 'WKT geometry ({crs})'"
    )


def full_table_name(catalog: str, schema: str, table_prefix: str, layer: str) -> str:
    return f"{catalog}.{schema}.{table_prefix}{layer}"
