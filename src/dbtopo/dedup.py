"""Deduplicate Delta tables by a key column."""

from __future__ import annotations


def get_table_metadata(
    spark, table_name: str
) -> tuple[dict[str, str], str, dict[str, str]]:
    """Extract table properties, comment, and column comments from a table.

    Returns (tbl_props, table_comment, col_comments).
    """
    props_rows = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
    tbl_props = {
        row.key: row.value
        for row in props_rows
        if not row.key.startswith("delta.") and not row.key.startswith("option.")
    }

    comment_rows = spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}").collect()
    comment_row = next((row for row in comment_rows if row.col_name == "Comment"), None)
    table_comment = (comment_row.data_type or "") if comment_row else ""

    col_comments: dict[str, str] = {}
    for row in comment_rows:
        if row.comment and row.col_name and row.col_name != "Comment":
            col_comments[row.col_name] = row.comment

    return tbl_props, table_comment, col_comments


def copy_table_metadata(
    spark,
    table_name: str,
    tbl_props: dict[str, str],
    table_comment: str,
    col_comments: dict[str, str],
) -> None:
    """Apply table properties, comment, and column comments to a table."""
    if tbl_props:
        props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in tbl_props.items())
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ({props_sql})")

    if table_comment:
        spark.sql(f"COMMENT ON TABLE {table_name} IS '{table_comment}'")

    for col, comment in col_comments.items():
        spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {col} COMMENT '{comment}'")


def dedup_table(
    spark,
    src: str,
    dst: str,
    dedup_key: str = "cleabs",
) -> dict[str, int]:
    """Deduplicate a single table into a new table.

    Returns {"before": N, "after": M, "removed": N-M}.
    """
    tbl_props, table_comment, col_comments = get_table_metadata(spark, src)

    before = spark.sql(f"SELECT COUNT(*) as cnt FROM {src}").collect()[0].cnt

    spark.sql(f"""
        CREATE OR REPLACE TABLE {dst} AS
        SELECT * EXCEPT(rn)
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY {dedup_key} ORDER BY dept
            ) as rn
            FROM {src}
        ) WHERE rn = 1
    """)

    after = spark.sql(f"SELECT COUNT(*) as cnt FROM {dst}").collect()[0].cnt

    copy_table_metadata(spark, dst, tbl_props, table_comment, col_comments)

    return {"before": before, "after": after, "removed": before - after}


def list_source_tables(
    spark,
    catalog: str,
    schema: str,
    table_prefix: str,
    dedup_suffix: str,
) -> list[str]:
    """List tables matching prefix, excluding already-deduped ones."""
    return [
        row.tableName
        for row in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
        if row.tableName.startswith(table_prefix)
        and not row.tableName.endswith(dedup_suffix)
    ]


def dedup_tables(
    spark,
    catalog: str,
    schema: str,
    table_prefix: str = "ign_bdtopo_",
    dedup_key: str = "cleabs",
    dedup_suffix: str = "_dedup",
) -> dict[str, dict[str, int]]:
    """Deduplicate all tables matching prefix.

    Returns a dict of {table_name: {"before", "after", "removed"}}.
    """
    tables = list_source_tables(spark, catalog, schema, table_prefix, dedup_suffix)
    stats: dict[str, dict[str, int]] = {}

    for table_name in sorted(tables):
        src = f"{catalog}.{schema}.{table_name}"
        dst = f"{catalog}.{schema}.{table_name}{dedup_suffix}"
        print(f"Deduplicating {src} -> {dst}")

        result = dedup_table(spark, src, dst, dedup_key)
        stats[table_name] = result
        print(
            f"  {result['before']:,} -> {result['after']:,} rows "
            f"({result['removed']:,} duplicates removed)"
        )

    return stats
