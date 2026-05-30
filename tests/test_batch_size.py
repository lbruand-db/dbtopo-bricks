"""Unit tests for the per-layer batch-size heuristic.

We exercise `estimate_batch_size` against tiny in-memory GPKG-shaped SQLite
databases — no real GPKG fixtures needed because the function only reads
the gpkg_geometry_columns metadata table and the layer's geometry BLOB
column lengths.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from dbtopo.batch_size import (
    DEFAULT_MEM_LIMIT_MB,
    HUGE_SINGLE_ROW_BYTES,
    WKT_INFLATION_FACTOR,
    estimate_batch_size,
)


def _make_gpkg_like(
    path: Path,
    layer: str,
    blob_sizes_bytes: list[int],
    *,
    ogr_feature_count: int | None = None,
) -> None:
    """Build a minimal GPKG-shaped SQLite file: gpkg_geometry_columns + a
    layer table with a `geom` BLOB column populated to the given sizes.

    If `ogr_feature_count` is given, also creates the `gpkg_ogr_contents`
    extension table with that count — exercises the row-count fast path.
    """
    con = sqlite3.connect(path)
    try:
        con.execute(
            "CREATE TABLE gpkg_geometry_columns "
            "(table_name TEXT, column_name TEXT, geometry_type_name TEXT, "
            "srs_id INTEGER, z INTEGER, m INTEGER)"
        )
        con.execute(
            "INSERT INTO gpkg_geometry_columns VALUES "
            "(?, 'geom', 'POLYGON', 4326, 0, 0)",
            (layer,),
        )
        con.execute(f'CREATE TABLE "{layer}" (id INTEGER PRIMARY KEY, geom BLOB)')
        con.executemany(
            f'INSERT INTO "{layer}" (geom) VALUES (?)',
            [(b"\x00" * size,) for size in blob_sizes_bytes],
        )
        if ogr_feature_count is not None:
            con.execute(
                "CREATE TABLE gpkg_ogr_contents "
                "(table_name TEXT PRIMARY KEY, feature_count INTEGER)"
            )
            con.execute(
                "INSERT INTO gpkg_ogr_contents VALUES (?, ?)",
                (layer, ogr_feature_count),
            )
        con.commit()
    finally:
        con.close()


def test_typical_layer_sizes_batch_to_fit_memory(tmp_path: Path) -> None:
    # 100 rows, each 1 KB blob → 3 KB WKT each. 512 MB / 3 KB ~ 175k > default.
    path = tmp_path / "typical.gpkg"
    _make_gpkg_like(path, "commune", [1024] * 100)

    stats = estimate_batch_size(path, "commune", default_batch_size=5000)
    assert stats.n_rows == 100
    assert stats.geom_col == "geom"
    assert stats.recommended_batch_size == 5000  # capped at default


def test_large_geometry_layer_drops_batch_size(tmp_path: Path) -> None:
    # One ~2 MB row mixed with small ones. WKT inflated to 6 MB.
    # Budget 512 MB / 6 MB ~= 85 batches.
    path = tmp_path / "large.gpkg"
    _make_gpkg_like(path, "cours_d_eau", [1024] * 99 + [2 * 1024 * 1024])

    stats = estimate_batch_size(path, "cours_d_eau", default_batch_size=5000)
    expected_target = (DEFAULT_MEM_LIMIT_MB * 1024 * 1024) // (
        2 * 1024 * 1024 * WKT_INFLATION_FACTOR
    )
    assert stats.recommended_batch_size == expected_target


def test_huge_single_row_forces_batch_size_one(tmp_path: Path) -> None:
    # 30 MB blob → 90 MB WKT, above the 50 MB single-row threshold.
    path = tmp_path / "huge.gpkg"
    _make_gpkg_like(path, "zone_de_vegetation", [100, 30 * 1024 * 1024])

    stats = estimate_batch_size(path, "zone_de_vegetation", default_batch_size=5000)
    assert stats.recommended_batch_size == 1
    assert stats.max_blob_bytes * WKT_INFLATION_FACTOR > HUGE_SINGLE_ROW_BYTES


def test_missing_layer_falls_back_to_default(tmp_path: Path) -> None:
    path = tmp_path / "nope.gpkg"
    _make_gpkg_like(path, "real_layer", [100])

    stats = estimate_batch_size(path, "ghost_layer", default_batch_size=5000)
    assert stats.recommended_batch_size == 5000
    assert stats.n_rows == 0


def test_empty_layer_falls_back_to_default(tmp_path: Path) -> None:
    path = tmp_path / "empty.gpkg"
    _make_gpkg_like(path, "empty_layer", [])

    stats = estimate_batch_size(path, "empty_layer", default_batch_size=2000)
    assert stats.recommended_batch_size == 2000
    assert stats.n_rows == 0


def test_uses_gpkg_ogr_contents_when_present(tmp_path: Path) -> None:
    # Fast path: gpkg_ogr_contents holds the count, no COUNT(*) needed.
    # We seed an *inconsistent* feature_count (7) vs. actual rows (3) so we
    # can prove the fast path was taken — if we'd fallen back to COUNT(*),
    # n_rows would be 3.
    path = tmp_path / "ogr.gpkg"
    _make_gpkg_like(path, "lyr", [100, 200, 300], ogr_feature_count=7)
    stats = estimate_batch_size(path, "lyr", default_batch_size=5000)
    assert stats.n_rows == 7


@pytest.mark.parametrize("blob_size,expected", [(1024, 5000), (1 * 1024 * 1024, 170)])
def test_recommendation_scales_with_max_blob(
    tmp_path: Path, blob_size: int, expected: int
) -> None:
    path = tmp_path / f"scale_{blob_size}.gpkg"
    _make_gpkg_like(path, "lyr", [blob_size])
    stats = estimate_batch_size(path, "lyr", default_batch_size=5000)
    assert stats.recommended_batch_size == expected
