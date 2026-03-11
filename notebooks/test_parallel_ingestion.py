# Databricks notebook source
# MAGIC %md
# MAGIC # Parallel GPKG Ingestion via Spark `mapInPandas`
# MAGIC
# MAGIC Benchmarks sequential vs Spark-parallel batch reading of GPKG layers.
# MAGIC Uses an already-downloaded 7z archive on the Volume.

# COMMAND ----------

# MAGIC %pip install geopandas pyogrio shapely "numpy<2" tqdm /Workspace/Users/timo.roest@databricks.com/.bundle/dbtopo-bricks/e2e_test/artifacts/.internal/dbtopo-0.1.0-py3-none-any.whl
# MAGIC %restart_python

# COMMAND ----------

import time
from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Configuration — adjust to match your environment
CATALOG = "timo_roest_test"
SCHEMA = "ign_bdtopo"
VOLUME = "bronze_volume"
DEPT = "D001"
VERSION = "3-5"
PROJECTION = "LAMB93"
VERSION_DATE = "2025-09-15"
LAYER = "batiment"
BATCH_SIZE = 50_000

volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
base_name = f"BDTOPO_{VERSION}_TOUSTHEMES_GPKG_{PROJECTION}_{DEPT}_{VERSION_DATE}"
archive_path = f"{volume_path}/{base_name}.7z"

print(f"Archive: {archive_path}")
print(f"Exists: {Path(archive_path).exists()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract GPKG to Volume (shared filesystem)

# COMMAND ----------

from dbtopo.extractor import extract_gpkg

extract_dir = f"{volume_path}/extracted"
gpkg_path = extract_gpkg(archive_path, output_dir=extract_dir)
print(f"GPKG: {gpkg_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Layer info

# COMMAND ----------

from dbtopo.gpkg_reader import batch_ranges, layer_crs_epsg, list_layers

layers = list_layers(gpkg_path)
print(f"Layers: {len(layers)}")
print(f"Target: {LAYER}")

total, ranges = batch_ranges(gpkg_path, LAYER, BATCH_SIZE)
epsg = layer_crs_epsg(gpkg_path, LAYER)
print(f"Total features: {total:,}")
print(f"Batches: {len(ranges)} (batch_size={BATCH_SIZE:,})")
print(f"Source CRS: EPSG:{epsg}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Benchmark: Sequential (driver-side)

# COMMAND ----------

from dbtopo.gpkg_reader import read_layer_batched
from dbtopo.transformer import transform_batch

seq_table = f"{CATALOG}.{SCHEMA}.bench_seq_{LAYER}"
spark.sql(f"DROP TABLE IF EXISTS {seq_table}")

from dbtopo.schema import spark_schema_from_gpkg
from dbtopo.writer import write_batch_to_delta
from pyspark.sql.types import StringType

layer_schema = spark_schema_from_gpkg(
    gpkg_path, LAYER, extra_columns={"dept": StringType(), "layer": StringType()}
)

t0 = time.time()
seq_rows = 0
for batch_idx, processed, total_feat, gdf in read_layer_batched(
    gpkg_path, LAYER, BATCH_SIZE
):
    gdf, source_srid = transform_batch(gdf, dept=DEPT, layer=LAYER)
    write_batch_to_delta(
        spark, gdf, seq_table, schema=layer_schema,
        source_srid=source_srid, target_srid=4326,
    )
    seq_rows += len(gdf)
    print(f"  Batch {batch_idx}: {len(gdf)} rows ({seq_rows}/{total_feat})")

seq_time = time.time() - t0
print(f"\nSequential: {seq_rows:,} rows in {seq_time:.1f}s ({seq_rows/seq_time:.0f} rows/sec)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Benchmark: Spark-parallel (`mapInPandas`)

# COMMAND ----------

from dbtopo.writer import _ingestion_schema

par_table = f"{CATALOG}.{SCHEMA}.bench_par_{LAYER}"
spark.sql(f"DROP TABLE IF EXISTS {par_table}")

ingest_schema, cast_exprs = _ingestion_schema(layer_schema)
date_ts_cols = list(cast_exprs.keys())

gpkg_str = str(gpkg_path)
range_rows = [(gpkg_str, LAYER, DEPT, offset, size) for _, offset, size in ranges]
range_df = spark.createDataFrame(
    range_rows,
    schema="gpkg_path string, layer string, dept string, offset int, batch_size int",
)
range_df = range_df.repartition(len(ranges))


def read_and_transform(iterator, _dt_cols=date_ts_cols):
    import geopandas as gpd
    from dbtopo.transformer import transform_batch

    for pdf in iterator:
        for _, row in pdf.iterrows():
            gdf = gpd.read_file(
                row["gpkg_path"],
                layer=row["layer"],
                engine="pyogrio",
                skip_features=int(row["offset"]),
                max_features=int(row["batch_size"]),
            )
            if len(gdf) == 0:
                continue
            gdf, _ = transform_batch(gdf, dept=row["dept"], layer=row["layer"])
            for c in _dt_cols:
                if c in gdf.columns:
                    mask = gdf[c].isna()
                gdf[c] = gdf[c].astype(str)
                import numpy as np
                gdf.loc[mask, c] = np.nan
            yield gdf


t0 = time.time()

result_df = range_df.mapInPandas(read_and_transform, schema=ingest_schema)

# Build selectExpr for geometry + date casts
select_exprs = []
for field in layer_schema.fields:
    if field.name == "geometry":
        geom_expr = f"ST_GeomFromWKT(geometry, {epsg})"
        if epsg != 0 and epsg != 4326:
            geom_expr = f"ST_Transform({geom_expr}, 4326)"
        select_exprs.append(f"{geom_expr} AS geometry")
    elif field.name in cast_exprs:
        select_exprs.append(cast_exprs[field.name])
    else:
        select_exprs.append(f"`{field.name}`")

result_df = result_df.selectExpr(*select_exprs)
result_df.write.format("delta").mode("append").option(
    "mergeSchema", "true"
).saveAsTable(par_table)

par_time = time.time() - t0
par_rows = spark.table(par_table).count()
print(f"\nParallel: {par_rows:,} rows in {par_time:.1f}s ({par_rows/par_time:.0f} rows/sec)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison

# COMMAND ----------

print(f"Sequential: {seq_rows:,} rows in {seq_time:.1f}s ({seq_rows/seq_time:.0f} rows/sec)")
print(f"Parallel:   {par_rows:,} rows in {par_time:.1f}s ({par_rows/par_time:.0f} rows/sec)")
print(f"Speedup:    {seq_time/par_time:.1f}x")
print(f"Row count match: {seq_rows == par_rows}")

# Validate row counts match
assert seq_rows == par_rows, f"Row count mismatch: seq={seq_rows} vs par={par_rows}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {seq_table}")
spark.sql(f"DROP TABLE IF EXISTS {par_table}")
print("Benchmark tables dropped.")
