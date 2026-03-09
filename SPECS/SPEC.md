# BD TOPO to Databricks Delta Table Loader

## Goal

Load the IGN BD TOPO database (French national topographic dataset) into Databricks Delta tables with full geometry support. The tool downloads department-level GeoPackage (GPKG) files from IGN's GeoServices, extracts and transforms them, and writes them into Unity Catalog Delta tables.

## Data Source

**BD TOPO** is a comprehensive 2D/3D vector description of French territory maintained by IGN, with metric precision, usable at scales from 1:2,000 to 1:50,000. Current version: 3.5 (quarterly releases since 2019).

### Download

- **Source**: https://geoservices.ign.fr/bdtopo#telechargementgpkgdep
- **Actual download host**: `data.geopf.fr`
- **Format**: GeoPackage (GPKG) — SQLite-based single-file geospatial format
- **Granularity**: By département (96 metropolitan + overseas territories)
- **Archive format**: `.7z` (7-Zip)
- **Coordinate system**: Lambert 93 (EPSG:2154) for metropolitan France

#### Confirmed URL Pattern

```
https://data.geopf.fr/telechargement/download/BDTOPO/BDTOPO_{version}_TOUSTHEMES_{package}_{projection}_{dept}_{version_date}/BDTOPO_{version}_TOUSTHEMES_{package}_{projection}_{dept}_{version_date}.7z
```

Example:
```
https://data.geopf.fr/telechargement/download/BDTOPO/BDTOPO_3-5_TOUSTHEMES_GPKG_LAMB93_D001_2025-09-15/BDTOPO_3-5_TOUSTHEMES_GPKG_LAMB93_D001_2025-09-15.7z
```

Parameters:
- `version`: BD TOPO version, e.g. `3-5`
- `package`: `GPKG`
- `projection`: `LAMB93` (metropolitan France)
- `dept`: department code prefixed with `D`, e.g. `D001`, `D075`
- `version_date`: release date, e.g. `2025-09-15`

An MD5 checksum file is available at the same URL with `.md5` extension instead of `.7z`.

### Themes (layers in GPKG)

BD TOPO organizes data into 8 INSPIRE-aligned themes:

| Theme | Description | Example layers |
|-------|-------------|----------------|
| Administratif | Administrative boundaries | commune, arrondissement, département |
| Bâtiment | Buildings/constructions | batiment, construction_ponctuelle |
| Hydrographie | Water features | cours_d_eau, plan_d_eau, troncon_hydrographique |
| Lieux nommés | Named places / toponyms | lieu_dit_habite, zone_d_habitation |
| Occupation du sol | Land use / vegetation | zone_de_vegetation, haie |
| Services et activités | Services / infrastructure | equipement_de_transport, poste_de_transformation |
| Transport | Road/rail/air networks | troncon_de_route, voie_ferree, aerodrome |
| Zones réglementées | Regulated zones | parc_ou_reserve, zone_d_activite_ou_d_interet |

## Architecture

### Pipeline Steps

```
1. Download       → Fetch .7z per department from IGN
2. Extract        → Decompress to .gpkg files
3. Read           → Load GPKG layers using GeoPandas/Fiona
4. Transform      → Convert geometries to WKT/WKB, reproject if needed
5. Upload/Write   → Write to Databricks Delta tables via Spark
```

### Step 1: Download

- Accept a list of department codes (e.g., `["01", "75", "92"]`) or `"all"` for all departments
- Build download URL using the confirmed pattern from `data.geopf.fr` (see above)
- Download `.7z` archive to a **Databricks Volume** used as cache: `/Volumes/{catalog}/{schema}/{volume}/`
- Use `requests` with retry logic (`HTTPAdapter` + `urllib3.Retry`) for resilience against 5xx errors
- Stream downloads in 1MB chunks with progress bar (`tqdm`)
- Cache already-downloaded files in the volume to avoid re-downloading
- Optionally verify integrity via MD5 checksum (`.md5` file available alongside each archive)

### Step 2: Extract

- Extract `.7z` archive in-memory or to a temp directory using `py7zr`
- Locate the `.gpkg` file inside the archive (filter `archive.getnames()` for `.gpkg` extension)
- The `.gpkg` is nested inside the archive directory structure — use path joining to resolve

### Step 3: Read GPKG

- Use `fiona` to open layers from the extracted `.gpkg`
- Read in **batches** (e.g., 10,000 features at a time) using `itertools.islice` on the fiona iterator to control memory
- Convert each batch to a `GeoDataFrame` via `gpd.GeoDataFrame.from_features(batch)`
- Allow filtering by layer name (e.g., `batiment`, `troncon_de_route`)

### Step 4: Transform

- Convert geometry column to **WKT string** via `batch['geometry'].astype(str)`
- Add metadata columns: `dept` (department code, e.g. `D001`), `layer` (source layer name)
- Handle schema variations across departments (defensive column merging)
- Reproject from Lambert 93 (EPSG:2154) to WGS84 (EPSG:4326)

### Step 5: Write to Delta

- **Pre-create the Unity Catalog schema and volume**:
  ```sql
  CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};
  CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume};
  ```
- **Table naming**: one Delta table per layer, prefixed with `ign_bdtopo_` (e.g., `{catalog}.{schema}.ign_bdtopo_batiment`)
- **Table creation**: pre-create tables with explicit schema using `CREATE TABLE IF NOT EXISTS ... USING DELTA`
- Convert each GeoPandas batch to a Spark DataFrame via `spark.createDataFrame(batch)`
- Write mode: `append` — accumulate departments into the same table
- Each batch is written independently, enabling progress tracking

#### Example Table Schema (batiment layer)

```sql
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.ign_bdtopo_batiment (
  id STRING,
  dept STRING,
  layer STRING,
  cleabs STRING,
  nature STRING,
  usage_1 STRING,
  usage_2 STRING,
  construction_legere BOOLEAN,
  etat_de_l_objet STRING,
  date_creation STRING,
  date_modification STRING,
  date_d_apparition STRING,
  date_de_confirmation STRING,
  sources STRING,
  identifiants_sources STRING,
  methode_d_acquisition_planimetrique STRING,
  methode_d_acquisition_altimetrique STRING,
  precision_planimetrique DOUBLE,
  precision_altimetrique DOUBLE,
  nombre_de_logements DOUBLE,
  nombre_d_etages DOUBLE,
  materiaux_des_murs STRING,
  materiaux_de_la_toiture STRING,
  hauteur DOUBLE,
  altitude_minimale_sol DOUBLE,
  altitude_minimale_toit DOUBLE,
  altitude_maximale_toit DOUBLE,
  altitude_maximale_sol DOUBLE,
  origine_du_batiment STRING,
  appariement_fichiers_fonciers STRING,
  identifiants_rnb STRING,
  geometry STRING
) USING DELTA
```

Note: schemas vary per layer. The table schema should be inferred from the GPKG layer metadata or from the first batch.

### Geometry in Delta

- Store geometry as **WKT string** column (`geometry`) for maximum compatibility
- Users can leverage `ST_*` functions (via Sedona / Databricks built-in) on the WKT column
- Future improvement: use native `GEOMETRY` type once Databricks support matures

## Deployment: Databricks Asset Bundles (DABs)

The project is packaged and deployed as a **Databricks Asset Bundle**, enabling reproducible multi-environment deployments via `databricks bundle deploy`.

### Bundle Structure

The bundle defines:
- A **Python wheel** built from `src/dbtopo/` containing all pipeline logic
- **Databricks Jobs** for orchestrating the pipeline steps
- **Unity Catalog resources** (schema, volume) as bundle resources
- **Environment-specific targets** (dev, staging, prod)

### databricks.yml

```yaml
bundle:
  name: dbtopo-bricks

variables:
  catalog:
    default: "dev_catalog"
  departments:
    default: "001"
  version_date:
    default: "2025-09-15"

artifacts:
  dbtopo_wheel:
    type: whl
    path: .
    build: uv build --wheel --out-dir dist

resources:
  jobs:
    bdtopo_load:
      name: "bdtopo-load-${bundle.target}"
      tasks:
        - task_key: setup_catalog
          notebook_task:
            notebook_path: notebooks/00_setup_catalog.py
          environment_key: default
        - task_key: download
          depends_on:
            - task_key: setup_catalog
          python_wheel_task:
            package_name: dbtopo
            entry_point: download
            parameters:
              - "--departments"
              - "${var.departments}"
              - "--catalog"
              - "${var.catalog}"
              - "--version-date"
              - "${var.version_date}"
          libraries:
            - whl: ./dist/*.whl
          environment_key: default
        - task_key: extract_and_load
          depends_on:
            - task_key: download
          python_wheel_task:
            package_name: dbtopo
            entry_point: load
            parameters:
              - "--departments"
              - "${var.departments}"
              - "--catalog"
              - "${var.catalog}"
          libraries:
            - whl: ./dist/*.whl
            - pypi:
                package: py7zr
            - pypi:
                package: geopandas
            - pypi:
                package: fiona
          environment_key: default
        - task_key: validate
          depends_on:
            - task_key: extract_and_load
          python_wheel_task:
            package_name: dbtopo
            entry_point: validate
            parameters:
              - "--catalog"
              - "${var.catalog}"
              - "--departments"
              - "${var.departments}"
          libraries:
            - whl: ./dist/*.whl
          environment_key: default

      environments:
        - environment_key: default
          spec:
            client: "1"

targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: "dev_catalog"
      departments: "001"
  staging:
    variables:
      catalog: "staging_catalog"
      departments: "001,075,092"
  prod:
    mode: production
    variables:
      catalog: "prod_catalog"
      departments: "all"
    run_as:
      service_principal_name: "bdtopo-service-principal"
```

### Job Tasks

| Task | Entry point | Description |
| ---- | ----------- | ----------- |
| `setup_catalog` | Notebook `00_setup_catalog.py` | Creates catalog, schema, and volume if not exist |
| `download` | `dbtopo:download` (wheel) | Downloads .7z archives to the volume |
| `extract_and_load` | `dbtopo:load` (wheel) | Extracts GPKG, reads layers, transforms, writes to Delta |
| `validate` | `dbtopo:validate` (wheel) | Runs spot-check assertions on Delta tables |

### Entry Points (pyproject.toml)

```toml
[project.scripts]
dbtopo = "dbtopo.cli:main"

[project.entry-points."databricks"]
download = "dbtopo.cli:download"
load = "dbtopo.cli:load"
validate = "dbtopo.cli:validate"
```

### Local Development with uv

```bash
# Initialize project (already done)
uv init

# Add dependencies
uv add requests tqdm py7zr geopandas fiona shapely pyproj pydantic pyyaml

# Add dev dependencies
uv add --dev pytest pytest-cov

# Run tests locally
uv run pytest

# Build wheel (used by DABs artifact)
uv build --wheel --out-dir dist

# Run CLI locally
uv run dbtopo load --departments 001 --catalog dev_catalog
```

### Deployment Workflow

```bash
# Validate bundle configuration
databricks bundle validate

# Deploy to dev (default target) — builds wheel via uv automatically
databricks bundle deploy

# Run the job
databricks bundle run bdtopo_load

# Deploy to production
databricks bundle deploy -t prod
databricks bundle run bdtopo_load -t prod

# Override departments for a one-off run
databricks bundle run bdtopo_load --params departments=075,092
```

### Notebooks

A setup notebook handles Unity Catalog resource creation (cannot be done from a wheel task):

```
notebooks/
└── 00_setup_catalog.py   # CREATE SCHEMA IF NOT EXISTS, CREATE VOLUME IF NOT EXISTS
```

## Configuration

```yaml
# config.yaml
ign:
  version: "3-5"
  version_date: "2025-09-15"
  projection: "LAMB93"
  departments: ["001", "075", "092"]  # or "all"
  layers: []  # empty = all layers (e.g., ["batiment", "troncon_de_route"])

databricks:
  catalog: "my_catalog"
  schema: "ign_bdtopo"
  volume: "bronze_volume"            # Volume for caching downloaded archives
  table_prefix: "ign_bdtopo_"        # Table name = prefix + layer name
  write_mode: "append"
  batch_size: 10000                  # Features per batch for streaming writes

transform:
  target_crs: "EPSG:4326"            # Reproject from Lambert 93 to WGS84
```

## Tech Stack

| Component            | Library                          |
| -------------------- | -------------------------------- |
| Download             | `requests` (with retry adapter)  |
| Progress             | `tqdm`                           |
| Archive extraction   | `py7zr`                          |
| GPKG reading         | `fiona`, `geopandas`             |
| Geometry ops         | `shapely`, `pyproj`              |
| Spark / Delta write  | `pyspark` (Databricks Runtime)   |
| Config               | `pydantic` + YAML                |
| Package manager      | `uv`                             |
| Deployment           | Databricks Asset Bundles (DABs)  |
| Orchestration        | Databricks Jobs (multi-task)     |

## Project Structure

```
dbtopo-bricks/
├── SPECS/
│   └── SPEC.md
├── databricks.yml               # DAB bundle definition
├── pyproject.toml               # Python package + entry points
├── notebooks/
│   └── 00_setup_catalog.py      # UC resource creation (schema, volume)
├── src/
│   └── dbtopo/
│       ├── __init__.py
│       ├── cli.py               # Entry points: download, load, validate
│       ├── config.py            # Pydantic config model
│       ├── downloader.py        # IGN download logic
│       ├── extractor.py         # 7z extraction
│       ├── gpkg_reader.py       # GPKG/Fiona layer reading
│       ├── transformer.py       # Geometry conversion, CRS
│       └── writer.py            # Delta table writer
├── tests/
│   ├── fixtures/
│   │   └── expected_features.yaml
│   ├── test_downloader.py
│   ├── test_extractor.py
│   ├── test_gpkg_reader.py
│   ├── test_transformer.py
│   ├── test_writer.py
│   ├── test_gpkg_spotcheck.py
│   ├── test_delta_spotcheck.py
│   └── conftest.py
└── README.md
```

## Testing Strategy

### Unit Tests (offline, no Databricks/IGN dependency)

Standard unit tests for each module using mocks for external dependencies (requests, spark, fiona).

### Integration Tests: GPKG Spot-Check Validation

After loading data into Delta, validate correctness by spot-checking specific known features from the source GPKG. This ensures the full pipeline (download → extract → read → transform → write) preserves data faithfully.

#### Approach

1. **Reference fixtures**: For a small test department (e.g., `D001`), maintain a set of expected feature values in `tests/fixtures/expected_features.yaml`:

```yaml
# tests/fixtures/expected_features.yaml
# Known features extracted manually from the GPKG for validation
department: "D001"
features:
  - layer: "batiment"
    cleabs: "BATIMENT0000000123456789"
    checks:
      nature: "Indifférenciée"
      hauteur: 12.5
      nombre_d_etages: 3.0
      geometry_wkt_starts_with: "POLYGON ((5.123"
  - layer: "troncon_de_route"
    cleabs: "TRONROUT0000000987654321"
    checks:
      nature: "Route à 2 chaussées"
      nombre_de_voies: 2.0
      geometry_wkt_starts_with: "LINESTRING (5.456"
  - layer: "commune"
    cleabs: "COMMUNE0000000001600001"
    checks:
      nom_officiel: "L'Abergement-Clémenciat"
      code_insee: "01001"
      geometry_wkt_starts_with: "MULTIPOLYGON (((4.9"
```

2. **Spot-check at GPKG read level**: After reading a batch from fiona, look up the fixture features by `cleabs` and assert field values match. This validates the read + transform steps independently of Databricks.

```python
# tests/test_gpkg_spotcheck.py
def test_feature_values_match_fixture(gpkg_path, fixtures):
    """Read the real GPKG and verify known features have expected values."""
    for expected in fixtures:
        gdf = read_layer(gpkg_path, layer=expected["layer"])
        row = gdf[gdf["cleabs"] == expected["cleabs"]]
        assert len(row) == 1, f"Feature {expected['cleabs']} not found"
        for field, value in expected["checks"].items():
            if field == "geometry_wkt_starts_with":
                assert row.iloc[0].geometry.wkt.startswith(value)
            else:
                assert row.iloc[0][field] == value
```

3. **Spot-check at Delta level**: After writing to Delta, query the table and verify the same fixture features are present and correct. This validates the full end-to-end pipeline including Spark serialization.

```python
# tests/test_delta_spotcheck.py
def test_delta_contains_expected_features(spark, catalog, schema, fixtures):
    """Query Delta table and verify known features survived the pipeline."""
    for expected in fixtures:
        table = f"{catalog}.{schema}.ign_bdtopo_{expected['layer']}"
        row = spark.sql(
            f"SELECT * FROM {table} WHERE cleabs = '{expected['cleabs']}'"
        ).collect()
        assert len(row) == 1
        for field, value in expected["checks"].items():
            if field == "geometry_wkt_starts_with":
                assert row[0]["geometry"].startswith(value)
            else:
                assert row[0][field] == value
```

#### Generating Fixtures

A helper script extracts fixture data from a downloaded GPKG so fixtures stay in sync with the source data:

```bash
# Extract fixture values for specific cleabs IDs from a GPKG
dbtopo extract-fixtures --department 001 --layer batiment --cleabs BATIMENT0000000123456789 --output tests/fixtures/
```

#### Test Levels

| Level | What it validates | Requires |
| ----- | ----------------- | -------- |
| Unit | Module logic (URL building, schema parsing, batching) | Nothing (mocked) |
| GPKG spot-check | Read + transform correctness against real data | Downloaded GPKG (cached in CI or volume) |
| Delta spot-check | Full pipeline end-to-end including Spark write | Databricks cluster/connect |
| Row count check | Feature count per layer per dept matches fiona `len(src)` | Databricks cluster/connect |

### Project Structure (tests)

```
tests/
├── fixtures/
│   └── expected_features.yaml
├── test_downloader.py
├── test_extractor.py
├── test_gpkg_reader.py
├── test_transformer.py
├── test_writer.py
├── test_gpkg_spotcheck.py       # GPKG-level spot-check
├── test_delta_spotcheck.py      # Delta-level spot-check
└── conftest.py                  # Shared fixtures, gpkg cache path
```

## Prior Art

This project is based on existing notebook prototypes at:
https://github.com/lbruand-db/various-notebooks/tree/main/ign

- `01_download_from_ign.ipynb` — download logic with retry, volume caching
- `02_extract_load_gpkg.ipynb` — extraction, batched reading, schema creation, Delta writes

The goal of this project is to productionize these notebooks into a reusable, configurable Python package.

## Open Questions

1. **Schema per layer**: Each GPKG layer has a different schema. Should we auto-infer from fiona metadata or maintain explicit schema definitions?
2. **Schema evolution**: How to handle schema changes between BD TOPO versions across quarters?
3. **Incremental loads**: Should we support BD TOPO Express (weekly) or differential products?
4. **Overseas territories**: Different CRS per territory (not LAMB93) — handle automatically based on department code?
5. **Idempotency**: Current approach is `append` — need a dedup/merge strategy to re-run safely (e.g., delete-then-insert by dept+layer, or MERGE by `cleabs`)
6. **Version date discovery**: Currently hardcoded — should we auto-detect the latest available version date from IGN?
