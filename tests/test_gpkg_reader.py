"""Tests for gpkg_reader require a real GPKG file. Skipped if not available."""

from pathlib import Path

import pytest

GPKG_TEST_PATH = Path(__file__).parent / "fixtures" / "test_D001_batiment.gpkg"
BAD_DATETIME_GPKG = Path(__file__).parent / "fixtures" / "test_bad_datetime.gpkg"


@pytest.fixture
def gpkg_path():
    if not GPKG_TEST_PATH.exists():
        pytest.skip(f"Test GPKG not found at {GPKG_TEST_PATH}")
    return GPKG_TEST_PATH


def test_list_layers(gpkg_path):
    from dbtopo.gpkg_reader import list_layers

    layers = list_layers(gpkg_path)
    assert isinstance(layers, list)
    assert len(layers) > 0


def test_read_layer_batched(gpkg_path):
    from dbtopo.gpkg_reader import list_layers, read_layer_batched

    layers = list_layers(gpkg_path)
    layer = layers[0]
    batches = list(read_layer_batched(gpkg_path, layer, batch_size=100))
    assert len(batches) > 0
    batch_idx, processed, total, gdf = batches[0]
    assert batch_idx == 0
    assert len(gdf) > 0
    assert len(gdf) <= 100


def test_read_bad_datetime_gpkg():
    """Regression test: pyogrio reads malformed datetime (seconds=60) without error."""
    if not BAD_DATETIME_GPKG.exists():
        pytest.skip(f"Bad datetime GPKG not found at {BAD_DATETIME_GPKG}")

    from dbtopo.gpkg_reader import read_layer
    from dbtopo.transformer import transform_batch

    gdf = read_layer(BAD_DATETIME_GPKG, "batiment")
    assert len(gdf) == 1

    # Transform should produce consistent types for all columns
    result, source_srid = transform_batch(gdf, dept="D001", layer="batiment")
    assert len(result) == 1
    assert "dept" in result.columns

    # date_modification should be readable (the malformed field with seconds=60)
    assert "date_modification" in result.columns
