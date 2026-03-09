from __future__ import annotations

import warnings

import geopandas as gpd


def reproject(gdf: gpd.GeoDataFrame, target_crs: str = "EPSG:4326") -> gpd.GeoDataFrame:
    if gdf.crs is None or str(gdf.crs) == target_crs:
        return gdf
    return gdf.to_crs(target_crs)


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


def normalize_datetimes(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Convert all datetime columns to ISO strings for consistent Delta schema."""
    import numpy as np

    gdf = gdf.copy()
    for col in gdf.columns:
        if hasattr(gdf[col], "dt") and np.issubdtype(gdf[col].dtype, np.datetime64):
            gdf[col] = gdf[col].dt.strftime("%Y-%m-%dT%H:%M:%S").replace("NaT", None)
    return gdf


def transform_batch(
    gdf: gpd.GeoDataFrame,
    dept: str,
    layer: str,
    target_crs: str = "EPSG:4326",
) -> gpd.GeoDataFrame:
    gdf = reproject(gdf, target_crs)
    gdf = normalize_datetimes(gdf)
    gdf = geometry_to_wkt(gdf)
    gdf = add_metadata(gdf, dept, layer)
    return gdf
