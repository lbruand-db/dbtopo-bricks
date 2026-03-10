from __future__ import annotations

import sys

import click
from tqdm import tqdm

from dbtopo.config import ALL_DEPARTMENTS
from dbtopo.downloader import download_department
from dbtopo.extractor import extract_gpkg
from dbtopo.gpkg_reader import list_layers, read_layer_batched
from dbtopo.schema import spark_schema_from_gpkg
from dbtopo.transformer import transform_batch
from dbtopo.writer import full_table_name, write_batch_to_delta


def _parse_departments(departments: str) -> list[str]:
    if departments.strip().lower() == "all":
        return ALL_DEPARTMENTS
    return [d.strip() for d in departments.split(",")]


@click.group()
def main():
    """dbtopo - Load IGN BD TOPO into Databricks Delta tables."""
    pass


@main.command()
@click.option(
    "--departments", required=True, help="Comma-separated dept codes or 'all'"
)
@click.option("--catalog", required=True)
@click.option("--schema", default="ign_bdtopo")
@click.option("--volume", default="bronze_volume")
@click.option("--version", default="3-5")
@click.option("--projection", default="LAMB93")
@click.option("--version-date", default="2025-09-15")
def download_cmd(
    departments, catalog, schema, volume, version, projection, version_date
):
    """Download BD TOPO .7z archives to a Databricks Volume."""
    dept_list = _parse_departments(departments)
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}"

    for dept in dept_list:
        print(f"Downloading department {dept}...")
        download_department(
            dept=dept,
            volume_path=volume_path,
            version=version,
            projection=projection,
            version_date=version_date,
        )

    print(f"Download complete: {len(dept_list)} department(s).")


@main.command()
@click.option(
    "--departments", required=True, help="Comma-separated dept codes or 'all'"
)
@click.option("--catalog", required=True)
@click.option("--schema", default="ign_bdtopo")
@click.option("--volume", default="bronze_volume")
@click.option("--version", default="3-5")
@click.option("--projection", default="LAMB93")
@click.option("--version-date", default="2025-09-15")
@click.option(
    "--layers", default="", help="Comma-separated layer names, or empty for all"
)
@click.option("--batch-size", default=10000, type=int)
@click.option("--table-prefix", default="ign_bdtopo_")
def load_cmd(
    departments,
    catalog,
    schema,
    volume,
    version,
    projection,
    version_date,
    layers,
    batch_size,
    table_prefix,
):
    """Extract GPKG from archives, transform, and load into Delta tables."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    dept_list = _parse_departments(departments)
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
    layer_filter = [x.strip() for x in layers.split(",") if x.strip()] if layers else []

    for dept in dept_list:
        dept_code = dept if dept.startswith("D") else f"D{dept}"
        base_name = (
            f"BDTOPO_{version}_TOUSTHEMES_GPKG_{projection}_{dept_code}_{version_date}"
        )
        archive_path = f"{volume_path}/{base_name}.7z"

        print(f"Extracting {archive_path}...")
        gpkg_path = extract_gpkg(archive_path)

        available_layers = list_layers(gpkg_path)
        target_layers = layer_filter if layer_filter else available_layers

        for layer_name in target_layers:
            if layer_name not in available_layers:
                print(f"  Skipping unknown layer: {layer_name}")
                continue

            table = full_table_name(catalog, schema, table_prefix, layer_name)
            print(f"  Loading layer {layer_name} -> {table}")

            from pyspark.sql.types import StringType

            layer_schema = spark_schema_from_gpkg(
                gpkg_path,
                layer_name,
                extra_columns={"dept": StringType(), "layer": StringType()},
            )

            pbar = None
            for batch_idx, processed, total, gdf in read_layer_batched(
                gpkg_path, layer_name, batch_size
            ):
                if batch_idx == 0:
                    pbar = tqdm(total=total, desc=f"    {layer_name}")

                gdf = transform_batch(gdf, dept=dept_code, layer=layer_name)
                write_batch_to_delta(spark, gdf, table, schema=layer_schema)

                if pbar:
                    pbar.update(len(gdf))

            if pbar:
                pbar.close()

    print("Load complete.")


@main.command()
@click.option("--catalog", required=True)
@click.option("--schema", default="ign_bdtopo")
@click.option("--table-prefix", default="ign_bdtopo_")
def validate_cmd(catalog, schema, table_prefix):
    """Validate loaded data by checking row counts."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    tables = [
        row.tableName
        for row in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
        if row.tableName.startswith(table_prefix)
    ]

    if not tables:
        print(f"No tables found with prefix '{table_prefix}' in {catalog}.{schema}")
        sys.exit(1)

    all_ok = True
    for table_name in sorted(tables):
        fqn = f"{catalog}.{schema}.{table_name}"
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {fqn}").collect()[0].cnt
        if count == 0:
            print(f"  FAIL: {fqn} is empty")
            all_ok = False
        else:
            print(f"  OK: {fqn} has {count:,} rows")

    if not all_ok:
        print("Validation failed: some tables are empty.")
        sys.exit(1)

    print("Validation passed.")


# Databricks python_wheel_task entry points.
# These call click commands with standalone_mode=False to avoid sys.exit(0).
def download():
    download_cmd(standalone_mode=False)


def load():
    load_cmd(standalone_mode=False)


def validate():
    validate_cmd(standalone_mode=False)


if __name__ == "__main__":
    main()
