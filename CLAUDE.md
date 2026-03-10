# dbtopo-bricks

Load IGN BD TOPO (French national topographic dataset) into Databricks Delta tables.

## Project structure

- `src/dbtopo/` — Python package (wheel), deployed via Databricks Asset Bundles
- `tests/` — pytest tests using mocked Spark sessions (no Java/Spark runtime needed)
- `notebooks/` — setup notebooks run as job tasks
- `databricks.yml` — DAB job definition with `for_each_task` parallelism

## Key modules

| Module | Purpose |
|---|---|
| `cli.py` | Click CLI with entry points: `download`, `load`, `dedup`, `validate` |
| `downloader.py` | Downloads .7z archives from IGN servers to a Databricks Volume |
| `extractor.py` | Extracts GPKG files from 7z archives using `py7zr.extract()` |
| `gpkg_reader.py` | Reads GPKG layers in batches via pyogrio |
| `schema.py` | Builds explicit Spark StructType from GPKG metadata (OGR types) |
| `transformer.py` | Reprojects geometry to EPSG:4326, adds dept/layer columns |
| `writer.py` | Writes batches to Delta, manages table metadata (CRS, version) |
| `dedup.py` | Deduplicates cross-department overlapping features by `cleabs` |
| `task_values.py` | Helper for `dbutils.jobs.taskValues.set()` |
| `config.py` | Pydantic config models, department codes list |

## Build & test

```bash
uv sync                    # install deps
uv run pytest              # run tests (all use mocked Spark, no Java needed)
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

1. **setup_catalog** — creates catalog/schema/volume if needed
2. **download** — `for_each_task` over departments (concurrency 10), downloads .7z archives
3. **extract_and_load** — `for_each_task` over departments (concurrency 10), extracts GPKG, transforms, loads to Delta. Idempotent: deletes existing dept rows before append.
4. **dedup** — deduplicates all loaded tables by `cleabs` into `*_dedup` tables, preserving table metadata
5. **validate** — checks all tables have non-zero row counts

## Important conventions

- **pyspark is dev-only**: it's pre-installed on Databricks runtime. Installing it via pip on serverless breaks the runtime. Keep it in `[dependency-groups] dev` only.
- **py7zr**: use `.extract()` not `.read()` — `.read()` is unavailable on Databricks serverless py7zr version.
- **Formatting**: run `uvx ruff format src tests` before committing. CI enforces ruff.
- **Spark schema**: always provide explicit StructType via `spark_schema_from_gpkg()` when writing batches — type inference fails on sparse data.
- **Idempotent loads**: `delete_department_rows()` is called before loading each department to prevent data duplication on reruns.
- **Table metadata**: CRS, version, source schema stored as TBLPROPERTIES + table/column comments via `set_table_geo_metadata()`.

## Targets

| Target | Catalog | Departments |
|---|---|---|
| dev | lucasbruand_catalog | all 96 (configurable) |
| staging | staging_catalog | 001, 075, 092 |
| prod | prod_catalog | all 96 |
