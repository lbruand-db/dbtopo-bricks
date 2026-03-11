from __future__ import annotations

import warnings

import geopandas as gpd


def get_source_srid(gdf: gpd.GeoDataFrame) -> int:
    """Extract the EPSG SRID from a GeoDataFrame's CRS.

    Returns 0 if the CRS is missing or has no EPSG code.
    """
    if gdf.crs is None:
        return 0
    epsg = gdf.crs.to_epsg()
    return epsg if epsg is not None else 0


def geometry_to_wkt(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        gdf = gdf.copy()
        gdf["geometry"] = gdf["geometry"].astype(str)
    return gdf


def add_metadata(gdf: gpd.GeoDataFrame, dept: str, layer: str) -> gpd.GeoDataFrame:
    gdf = gdf.copy()
    gdf["dept"] = dept
    gdf["layer"] = layer
    return gdf


def transform_batch(
    gdf: gpd.GeoDataFrame,
    dept: str,
    layer: str,
) -> tuple[gpd.GeoDataFrame, int]:
    """Transform a batch for Delta ingestion.

    Returns (transformed_gdf, source_srid).  Reprojection and datetime
    handling are deferred to Databricks (ST_Transform and native
    TimestampType respectively).
    """
    source_srid = get_source_srid(gdf)
    gdf = geometry_to_wkt(gdf)
    gdf = add_metadata(gdf, dept, layer)
    return gdf, source_srid
