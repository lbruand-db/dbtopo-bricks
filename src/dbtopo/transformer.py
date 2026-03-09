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


def transform_batch(
    gdf: gpd.GeoDataFrame,
    dept: str,
    layer: str,
    target_crs: str = "EPSG:4326",
) -> gpd.GeoDataFrame:
    gdf = reproject(gdf, target_crs)
    gdf = geometry_to_wkt(gdf)
    gdf = add_metadata(gdf, dept, layer)
    return gdf
