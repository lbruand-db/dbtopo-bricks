"""Validate loaded Delta tables: row counts, native GEOMETRY, SRID, coordinates."""

from __future__ import annotations


def validate_tables(
    spark,
    catalog: str,
    schema: str,
    table_prefix: str = "ign_bdtopo_",
) -> list[str]:
    """Run all validation checks on tables matching the prefix.

    Returns a list of failure descriptions (empty if all passed).
    """
    fqn = f"{catalog}.{schema}"
    tables = [
        row.tableName
        for row in spark.sql(f"SHOW TABLES IN {fqn}").collect()
        if row.tableName.startswith(table_prefix)
    ]

    if not tables:
        return [f"No tables found with prefix '{table_prefix}' in {fqn}"]

    failures: list[str] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {name}: {detail}")
        if not ok:
            failures.append(f"{name}: {detail}")

    _check_row_counts(spark, fqn, tables, check)
    geo_tables = _check_geometry_type(spark, fqn, tables, check)
    _check_srid(spark, fqn, geo_tables, check)
    _check_coordinate_range(spark, fqn, geo_tables, check)
    _check_st_functions(spark, fqn, geo_tables, check)
    _check_dedup(spark, fqn, tables, check)

    return failures


def _check_row_counts(spark, fqn, tables, check):
    print("=== Row counts ===")
    for t in sorted(tables):
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {fqn}.{t}").collect()[0].cnt
        check(f"Row count {t}", count > 0, f"{count:,} rows")


def _check_geometry_type(spark, fqn, tables, check):
    """Returns list of tables that have a geometry column."""
    print("=== Native GEOMETRY column type ===")
    geo_tables = []
    for t in sorted(tables):
        cols = spark.sql(f"DESCRIBE TABLE {fqn}.{t}").collect()
        geo_col = [r for r in cols if r.col_name == "geometry"]
        if geo_col:
            dtype = geo_col[0].data_type
            check(
                f"GEOMETRY type {t}",
                dtype.startswith("geometry"),
                f"type={dtype}",
            )
            geo_tables.append(t)
        else:
            check(f"GEOMETRY column {t}", False, "not found")
    return geo_tables


def _check_srid(spark, fqn, geo_tables, check):
    print("=== SRID validation (expect 4326) ===")
    for t in geo_tables:
        srid_row = spark.sql(f"""
            SELECT ST_SRID(geometry) as srid FROM {fqn}.{t}
            WHERE geometry IS NOT NULL LIMIT 1
        """).collect()
        if srid_row:
            srid = srid_row[0].srid
            check(f"SRID {t}", srid == 4326, f"srid={srid}")
        else:
            check(f"SRID {t}", False, "no non-null geometry rows")


def _check_coordinate_range(spark, fqn, geo_tables, check):
    print("=== Coordinate range (WGS84 France bounds) ===")
    for t in geo_tables:
        bounds = spark.sql(f"""
            SELECT
                MIN(ST_XMin(geometry)) as min_lon,
                MAX(ST_XMax(geometry)) as max_lon,
                MIN(ST_YMin(geometry)) as min_lat,
                MAX(ST_YMax(geometry)) as max_lat
            FROM {fqn}.{t} WHERE geometry IS NOT NULL
        """).collect()[0]
        if bounds.min_lon is not None:
            lon_ok = -6 < bounds.min_lon and bounds.max_lon < 11
            lat_ok = 40 < bounds.min_lat and bounds.max_lat < 53
            detail = (
                f"lon=[{bounds.min_lon:.4f}, {bounds.max_lon:.4f}], "
                f"lat=[{bounds.min_lat:.4f}, {bounds.max_lat:.4f}]"
            )
            check(f"Lon range {t}", lon_ok, detail)
            check(f"Lat range {t}", lat_ok, detail)
        else:
            check(f"Bounds {t}", False, "all geometries null")


def _check_st_functions(spark, fqn, geo_tables, check):
    if not geo_tables:
        return
    print("=== ST_* function compatibility ===")
    t = geo_tables[0]
    row = spark.sql(f"""
        SELECT
            ST_GeometryType(geometry) AS geom_type,
            ST_NPoints(geometry) AS n_points,
            ST_Area(geometry) AS area,
            ST_AsText(ST_Centroid(geometry)) AS centroid_wkt,
            ST_IsValid(geometry) AS is_valid,
            ST_AsGeoJSON(geometry) AS geojson
        FROM {fqn}.{t} WHERE geometry IS NOT NULL LIMIT 1
    """).collect()[0]
    check("ST_GeometryType", row.geom_type is not None, row.geom_type)
    check(
        "ST_NPoints",
        row.n_points is not None and row.n_points > 0,
        str(row.n_points),
    )
    check("ST_Area", row.area is not None, str(row.area))
    check(
        "ST_Centroid",
        row.centroid_wkt is not None,
        row.centroid_wkt[:80],
    )
    check("ST_IsValid", row.is_valid is True, str(row.is_valid))
    check(
        "ST_AsGeoJSON",
        row.geojson is not None,
        f"{row.geojson[:80]}...",
    )


def _check_dedup(spark, fqn, tables, check):
    dedup_tbls = [t for t in tables if t.endswith("_dedup")]
    if not dedup_tbls:
        return
    print("=== Dedup table validation ===")
    cnt_sql = "SELECT COUNT(*) as cnt FROM"
    for dt in dedup_tbls:
        base = dt.replace("_dedup", "")
        if base in tables:
            src_cnt = spark.sql(f"{cnt_sql} {fqn}.{base}").collect()[0].cnt
            dst_cnt = spark.sql(f"{cnt_sql} {fqn}.{dt}").collect()[0].cnt
            check(
                f"Dedup {dt}",
                dst_cnt <= src_cnt,
                f"{src_cnt} -> {dst_cnt}",
            )
