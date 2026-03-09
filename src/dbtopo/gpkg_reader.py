from __future__ import annotations

import itertools
from pathlib import Path
from typing import Iterator

import fiona
import geopandas as gpd


def list_layers(gpkg_path: str | Path) -> list[str]:
    return fiona.listlayers(str(gpkg_path))


def read_layer_batched(
    gpkg_path: str | Path,
    layer: str,
    batch_size: int = 10000,
) -> Iterator[tuple[int, int, int, gpd.GeoDataFrame]]:
    """Yield (batch_index, features_processed, total_features, gdf) tuples."""
    with fiona.open(str(gpkg_path), layer=layer) as src:
        total = len(src)
        crs = src.crs
        features_iter = iter(src)
        batch_idx = 0
        while True:
            batch = list(itertools.islice(features_iter, batch_size))
            if not batch:
                break
            gdf = gpd.GeoDataFrame.from_features(batch, crs=crs)
            yield batch_idx, batch_idx * batch_size, total, gdf
            batch_idx += 1


def read_layer(
    gpkg_path: str | Path,
    layer: str,
) -> gpd.GeoDataFrame:
    return gpd.read_file(str(gpkg_path), layer=layer)
