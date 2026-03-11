from __future__ import annotations

import datetime
import logging

import pandas as pd
from pyspark.sql.types import DateType, StructType, TimestampType

from dbtopo.metadata import get_column_descriptions, get_table_description

logger = logging.getLogger(__name__)


def delete_department_rows(spark, table_name: str, dept: str) -> None:
    """Delete existing rows for a department. No-op if table doesn't exist."""
    try:
        spark.sql(f"DELETE FROM {table_name} WHERE dept = '{dept}'")
    except Exception:
        pass  # Table doesn't exist yet on first run


def _escape_comment(text: str) -> str:
    """Escape single quotes for use in SQL COMMENT strings."""
    return text.replace("'", "\\'")


def ensure_table_with_metadata(
    spark,
    table_name: str,
    schema: StructType,
    layer: str,
    version: str = "3-5",
    version_date: str = "",
    crs: str = "EPSG:4326",
    lang: str = "en",
) -> None:
    """Create the table with column and table comments if it doesn't exist.

    Uses CREATE TABLE IF NOT EXISTS ... USING DELTA with per-column COMMENT
    clauses and a table-level COMMENT, so metadata is set at creation time.
    Also sets table properties for CRS and version info.

    Parameters
    ----------
    lang : str
        Language for metadata comments ("en" or "fr"). Defaults to "en".
    """
    col_descs = get_column_descriptions(layer, lang=lang)
    table_desc = get_table_description(
        layer, version=version, version_date=version_date, lang=lang
    )

    # Build column definitions from the Spark schema.
    # The geometry column is STRING in the StructType (WKT from pyogrio),
    # but must be declared as native GEOMETRY(srid) in the table so that
    # write_batch_to_delta's ST_GeomFromWKT output is schema-compatible
    # and the entire table is constrained to a single SRID.
    srid = crs.split(":")[1] if ":" in crs else "4326"
    col_defs = []
    for field in schema.fields:
        if field.name == "geometry":
            col_type = f"GEOMETRY({srid})"
        else:
            col_type = field.dataType.simpleString()
        col_sql = f"`{field.name}` {col_type}"
        if field.name in col_descs:
            col_sql += f" COMMENT '{_escape_comment(col_descs[field.name])}'"
        else:
            logger.warning(
                "No metadata for column '%s' in layer '%s'",
                field.name,
                layer,
            )
        col_defs.append(col_sql)

    cols_str = ",\n  ".join(col_defs)
    create_sql = (
        f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        f"  {cols_str}\n"
        f") USING DELTA\n"
        f"COMMENT '{_escape_comment(table_desc)}'"
    )
    spark.sql(create_sql)

    # Set table properties for programmatic access.
    props = f"'crs' = '{crs}'"
    if version:
        props += f", 'bdtopo_version' = '{version}'"
    if version_date:
        props += f", 'bdtopo_version_date' = '{version_date}'"
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ({props})")


def _safe_parse_date(val) -> datetime.date | None:
    """Parse a string to datetime.date, bypassing pandas Timestamp limits."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, datetime.date):
        return val
    try:
        return datetime.date.fromisoformat(str(val)[:10])
    except (ValueError, TypeError):
        return None


def _safe_parse_datetime(val) -> datetime.datetime | None:
    """Parse a string to datetime.datetime, bypassing pandas Timestamp limits."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, datetime.datetime):
        return val
    try:
        return datetime.datetime.fromisoformat(str(val))
    except (ValueError, TypeError):
        return None


def write_batch_to_delta(
    spark,
    pdf: pd.DataFrame,
    table_name: str,
    schema: StructType | None = None,
    source_srid: int = 0,
    target_srid: int = 4326,
) -> None:
    if schema is not None:
        # Coerce pandas object columns to proper types so Arrow serialization
        # succeeds.  pyogrio returns dates as strings (and historical dates
        # like "1612-01-01" overflow pandas' nanosecond Timestamp range).
        # We parse directly to Python datetime.date / datetime.datetime
        # objects which have no such limitation.
        for field in schema.fields:
            if field.name not in pdf.columns:
                continue
            if isinstance(field.dataType, DateType):
                pdf[field.name] = pdf[field.name].apply(_safe_parse_date)
            elif isinstance(field.dataType, TimestampType):
                pdf[field.name] = pdf[field.name].apply(_safe_parse_datetime)
        sdf = spark.createDataFrame(pdf, schema=schema)
    else:
        sdf = spark.createDataFrame(pdf)

    # Convert WKT string geometry to native GEOMETRY type, with optional
    # server-side reprojection via ST_Transform.
    if "geometry" in sdf.columns:
        other_cols = [c for c in sdf.columns if c != "geometry"]
        geom_expr = f"ST_GeomFromWKT(geometry, {source_srid})"
        if source_srid != 0 and source_srid != target_srid:
            geom_expr = f"ST_Transform({geom_expr}, {target_srid})"
        select_exprs = other_cols + [f"{geom_expr} AS geometry"]
        sdf = sdf.selectExpr(*select_exprs)

    sdf.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        table_name
    )


def full_table_name(catalog: str, schema: str, table_prefix: str, layer: str) -> str:
    return f"{catalog}.{schema}.{table_prefix}{layer}"
