from __future__ import annotations

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
