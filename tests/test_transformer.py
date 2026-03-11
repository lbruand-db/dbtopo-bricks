import geopandas as gpd
from shapely.geometry import Point

from dbtopo.transformer import (
    add_metadata,
    geometry_to_wkt,
    get_source_srid,
    transform_batch,
)


def _make_gdf(crs="EPSG:2154"):
    return gpd.GeoDataFrame(
        {"name": ["A"]},
        geometry=[Point(600000, 6600000)],
        crs=crs,
    )


def test_get_source_srid():
    gdf = _make_gdf("EPSG:2154")
    assert get_source_srid(gdf) == 2154


def test_get_source_srid_wgs84():
    gdf = _make_gdf("EPSG:4326")
    assert get_source_srid(gdf) == 4326


def test_get_source_srid_none():
    gdf = _make_gdf("EPSG:4326")
    gdf.crs = None
    assert get_source_srid(gdf) == 0


def test_geometry_to_wkt():
    gdf = _make_gdf("EPSG:4326")
    result = geometry_to_wkt(gdf)
    assert isinstance(result["geometry"].iloc[0], str)
    assert result["geometry"].iloc[0].startswith("POINT")


def test_add_metadata():
    gdf = _make_gdf()
    result = add_metadata(gdf, "D001", "batiment")
    assert result["dept"].iloc[0] == "D001"
    assert result["layer"].iloc[0] == "batiment"


def test_transform_batch():
    gdf = _make_gdf("EPSG:2154")
    result, source_srid = transform_batch(gdf, "D001", "batiment")
    assert "dept" in result.columns
    assert isinstance(result["geometry"].iloc[0], str)
    assert source_srid == 2154


def test_transform_batch_preserves_source_coordinates():
    """Geometry should NOT be reprojected locally — coordinates stay in source CRS."""
    gdf = _make_gdf("EPSG:2154")
    result, _ = transform_batch(gdf, "D001", "batiment")
    wkt = result["geometry"].iloc[0]
    # Lambert 93 coordinates are large numbers (e.g. 600000, 6600000)
    assert "600000" in wkt
