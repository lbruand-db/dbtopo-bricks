from __future__ import annotations

import re
from pathlib import Path
from typing import Iterator

import geopandas as gpd
import pyogrio


def list_layers(gpkg_path: str | Path) -> list[str]:
    info = pyogrio.list_layers(str(gpkg_path))
    return [name for name, _ in info]


def _count_features(gpkg_path: str | Path, layer: str) -> int:
    info = pyogrio.read_info(str(gpkg_path), layer=layer)
    return info["features"]


def batch_ranges(
    gpkg_path: str | Path,
    layer: str,
    batch_size: int = 10000,
) -> tuple[int, list[tuple[int, int, int]]]:
    """Compute all (idx, offset, size) ranges without reading data.

    Returns (total_features, [(idx, offset, size), ...]).
    """
    total = _count_features(str(gpkg_path), layer)
    ranges: list[tuple[int, int, int]] = []
    idx = 0
    offset = 0
    while offset < total:
        size = min(batch_size, total - offset)
        ranges.append((idx, offset, size))
        offset += size
        idx += 1
    return total, ranges


def layer_crs_epsg(gpkg_path: str | Path, layer: str) -> int:
    """Extract EPSG code from layer CRS metadata via pyogrio.read_info().

    Handles both short form (``EPSG:2154``) and WKT
    (``AUTHORITY["EPSG","2154"]``).  No pyproj dependency needed.
    Returns 0 if the CRS is missing or has no EPSG code.
    """
    info = pyogrio.read_info(str(gpkg_path), layer=layer)
    crs = info.get("crs", "")
    if not crs:
        return 0
    # Short form: "EPSG:2154"
    short = re.match(r"^EPSG:(\d+)$", crs)
    if short:
        return int(short.group(1))
    # WKT form: AUTHORITY["EPSG","2154"] or ID["EPSG",2154]
    wkt = re.search(r'(?:AUTHORITY|ID)\["EPSG",\s*"?(\d+)"?\]', crs)
    return int(wkt.group(1)) if wkt else 0


def read_layer_batched(
    gpkg_path: str | Path,
    layer: str,
    batch_size: int = 10000,
) -> Iterator[tuple[int, int, int, gpd.GeoDataFrame]]:
    """Yield (batch_index, features_processed, total_features, gdf) tuples."""
    path = str(gpkg_path)
    total = _count_features(path, layer)
    batch_idx = 0
    offset = 0
    while offset < total:
        gdf = gpd.read_file(
            path,
            layer=layer,
            engine="pyogrio",
            skip_features=offset,
            max_features=batch_size,
        )
        if len(gdf) == 0:
            break
        yield batch_idx, offset, total, gdf
        offset += len(gdf)
        batch_idx += 1


def read_layer(
    gpkg_path: str | Path,
    layer: str,
) -> gpd.GeoDataFrame:
    return gpd.read_file(str(gpkg_path), layer=layer, engine="pyogrio")
