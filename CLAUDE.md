# dbtopo-bricks

Load IGN BD TOPO (French national topographic dataset) into Databricks Delta tables.

## Project structure

- `src/dbtopo/` -- Python package (wheel), deployed via Databricks Asset Bundles
- `tests/` -- pytest tests using mocked Spark sessions (no Java/Spark runtime needed)
- `notebooks/` -- setup and validation notebooks run as job tasks
- `databricks.yml` -- DAB job definition with `for_each_task` parallelism

## Key modules

| Module | Purpose |
|---|---|
| `cli.py` | Click CLI with entry points: `download`, `load`, `dedup`, `validate` |
| `downloader.py` | Downloads .7z archives from IGN servers to a Databricks Volume |
| `extractor.py` | Extracts GPKG files from 7z archives using `py7zr.extract()` |
| `gpkg_reader.py` | Reads GPKG layers in batches via pyogrio |
| `schema.py` | Builds explicit Spark StructType from GPKG metadata (OGR types) |
| `transformer.py` | Extracts source SRID, converts geometry to WKT, adds dept/layer columns |
| `writer.py` | Pre-creates tables with metadata, writes batches to Delta with ST_GeomFromWKT + ST_Transform |
| `metadata.py` | Bilingual (EN/FR) BD TOPO v3.5 column/table descriptions for 60 layers |
| `dedup.py` | Deduplicates cross-department overlapping features by `cleabs` |
| `task_values.py` | Helper for `dbutils.jobs.taskValues.set()` |
| `config.py` | Pydantic config models, department codes list |

## Build & test

```bash
uv sync                    # install deps
uv run pytest              # run tests (72 tests, all use mocked Spark, no Java needed)
uvx ruff check src tests   # lint
uvx ruff format src tests  # format
```

## Deploy & run

```bash
databricks bundle deploy --profile DEFAULT
databricks bundle run bdtopo_load --profile DEFAULT
```

The job resource key is `bdtopo_load` (underscore). The job name is `bdtopo-load-${target}`.

## Pipeline (5 tasks)

1. **setup_catalog** -- creates catalog/schema/volume if needed
2. **download** -- `for_each_task` over departments (concurrency 10), downloads .7z archives
3. **extract_and_load** -- `for_each_task` over departments (concurrency 10), extracts GPKG, transforms, pre-creates tables with column/table comments, loads to Delta with native GEOMETRY. Idempotent: deletes existing dept rows before append.
4. **dedup** -- deduplicates all loaded tables by `cleabs` into `*_dedup` tables
5. **validate** -- validates row counts, native GEOMETRY(4326), SRID, coordinate ranges, ST_* functions

## Important conventions

- **Runtime requirements**: Native GEOMETRY type needs **DBR 17.3 LTS+** on classic compute, or **serverless client version 4+**.
- **pyspark is dev-only**: it's pre-installed on Databricks runtime. Installing it via pip on serverless breaks the runtime. Keep it in `[dependency-groups] dev` only.
- **py7zr**: use `.extract()` not `.read()` -- `.read()` is unavailable on Databricks serverless py7zr version.
- **Formatting**: run `uvx ruff format src tests` before committing. CI enforces ruff.
- **Spark schema**: always provide explicit StructType via `spark_schema_from_gpkg()` when writing batches -- type inference fails on sparse data.
- **Idempotent loads**: `delete_department_rows()` is called before loading each department to prevent data duplication on reruns.
- **Table metadata**: Tables are pre-created with `CREATE TABLE IF NOT EXISTS ... USING DELTA` with column and table COMMENTs via `ensure_table_with_metadata()`. Descriptions are bilingual (English default, French via `--lang fr`).
- **PySpark GEOMETRY limitation**: `.collect()` cannot deserialize native GEOMETRY. Wrap in `ST_AsText()` or `ST_AsBinary()` before collecting.
- **No local reprojection**: CRS reprojection is done server-side via `ST_Transform(ST_GeomFromWKT(wkt, source_srid), 4326)`. No pyproj dependency.

## Targets

| Target | Catalog | Departments | Notes |
|---|---|---|---|
| dev | lucasbruand_catalog | all 96 (configurable) | Default target |
| staging | staging_catalog | 001, 075, 092 | |
| prod | prod_catalog | all 96 | Production mode, service principal |
