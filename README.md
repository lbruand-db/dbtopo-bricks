# dbtopo-bricks

Load the [IGN BD TOPO](https://geoservices.ign.fr/bdtopo) database (French national topographic dataset) into Databricks Delta tables with geometry support.

## What it does

Downloads department-level GeoPackage (GPKG) files from IGN's GeoServices, extracts them, reprojects geometries to WGS84, and writes them into Unity Catalog Delta tables — orchestrated as a Databricks Job via Asset Bundles.

## Pipeline

```
Download (.7z from IGN) → Extract (py7zr) → Read (fiona, batched) → Transform (reproject to WGS84, WKT) → Write (Delta)
```

## Quick start

### Prerequisites

- [uv](https://docs.astral.sh/uv/) for Python package management
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) authenticated to your workspace

### Local development

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest -v

# Build wheel
uv build --wheel --out-dir dist
```

### Deploy to Databricks

```bash
# Validate bundle
databricks bundle validate

# Deploy (builds wheel automatically via uv)
databricks bundle deploy

# Run the job (downloads + loads department 001 by default)
databricks bundle run bdtopo_load

# Override departments
databricks bundle run bdtopo_load --params departments=075,092
```

### Targets

| Target | Catalog | Departments | Description |
| ------ | ------- | ----------- | ----------- |
| dev | lucasbruand_catalog | 001 | Single department for testing |
| staging | staging_catalog | 001,075,092 | A few departments |
| prod | prod_catalog | all | All 96+ departments |

```bash
databricks bundle deploy -t prod
databricks bundle run bdtopo_load -t prod
```

## Job tasks

The `bdtopo_load` job runs on serverless compute with 4 sequential tasks:

1. **setup_catalog** — Creates Unity Catalog schema and volume
2. **download** — Downloads .7z archives from `data.geopf.fr` to a UC volume
3. **extract_and_load** — Extracts GPKG, reads layers in batches, reprojects to WGS84, writes to Delta
4. **validate** — Checks that all tables have data

## Data source

- **BD TOPO v3.5** from [IGN GeoServices](https://geoservices.ign.fr/bdtopo)
- Downloaded per department from `data.geopf.fr`
- 58 layers across 8 themes (administrative, buildings, hydrography, transport, etc.)
- Source CRS: Lambert 93 (EPSG:2154), reprojected to WGS84 (EPSG:4326)

## Project structure

```
dbtopo-bricks/
├── databricks.yml              # DAB bundle definition
├── pyproject.toml              # Python package (uv/hatch)
├── notebooks/
│   └── 00_setup_catalog.py     # UC resource creation
├── src/dbtopo/
│   ├── cli.py                  # Click CLI + Databricks entry points
│   ├── config.py               # Pydantic configuration
│   ├── downloader.py           # IGN download with retry
│   ├── extractor.py            # 7z extraction
│   ├── gpkg_reader.py          # Batched fiona/geopandas reader
│   ├── transformer.py          # Reproject + WKT conversion
│   └── writer.py               # Delta table writer
├── tests/
│   ├── fixtures/
│   │   └── test_D001_batiment.gpkg  # 10k features for testing
│   ├── test_config.py
│   ├── test_downloader.py
│   ├── test_extractor.py
│   ├── test_gpkg_reader.py
│   ├── test_transformer.py
│   └── test_writer.py
└── SPECS/
    └── SPEC.md                 # Detailed specification
```

## Tech stack

| Component | Library |
| --------- | ------- |
| Download | requests (with retry) |
| Archive extraction | py7zr |
| GPKG reading | fiona, geopandas |
| Geometry ops | shapely, pyproj |
| Spark / Delta | pyspark (Databricks Runtime) |
| Package manager | uv |
| Deployment | Databricks Asset Bundles |
| Orchestration | Databricks Jobs (serverless) |
