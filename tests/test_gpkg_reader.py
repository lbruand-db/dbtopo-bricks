"""Tests for gpkg_reader require a real GPKG file. Skipped if not available."""

import pytest
from pathlib import Path

# Set this env var or fixture path to run integration tests
GPKG_TEST_PATH = Path("/tmp/bdtopo_test/test.gpkg")


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
