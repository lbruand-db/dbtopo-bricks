import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from dbtopo.transformer import reproject, geometry_to_wkt, add_metadata, normalize_datetimes, transform_batch


def _make_gdf(crs="EPSG:2154"):
    return gpd.GeoDataFrame(
        {"name": ["A"]},
        geometry=[Point(600000, 6600000)],
        crs=crs,
    )


def test_reproject_to_wgs84():
    gdf = _make_gdf("EPSG:2154")
    result = reproject(gdf, "EPSG:4326")
    assert result.crs.to_epsg() == 4326
    point = result.geometry.iloc[0]
    # Should be roughly in metropolitan France
    assert 0 < point.x < 10
    assert 40 < point.y < 52


def test_reproject_noop_when_same_crs():
    gdf = _make_gdf("EPSG:4326")
    result = reproject(gdf, "EPSG:4326")
    assert result is gdf


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


def test_normalize_datetimes():
    gdf = gpd.GeoDataFrame(
        {
            "name": ["A"],
            "date_creation": pd.to_datetime(["2021-12-15T17:30:00"]),
            "some_int": [42],
        },
        geometry=[Point(0, 0)],
        crs="EPSG:4326",
    )
    result = normalize_datetimes(gdf)
    assert result["date_creation"].iloc[0] == "2021-12-15T17:30:00"
    assert not pd.api.types.is_datetime64_any_dtype(result["date_creation"])
    assert result["some_int"].iloc[0] == 42  # non-datetime untouched


def test_normalize_datetimes_with_nat():
    gdf = gpd.GeoDataFrame(
        {
            "date_creation": pd.to_datetime(["2021-12-15T17:30:00", pd.NaT]),
        },
        geometry=[Point(0, 0), Point(1, 1)],
        crs="EPSG:4326",
    )
    result = normalize_datetimes(gdf)
    assert result["date_creation"].iloc[0] == "2021-12-15T17:30:00"
    assert pd.isna(result["date_creation"].iloc[1])


def test_transform_batch():
    gdf = _make_gdf("EPSG:2154")
    result = transform_batch(gdf, "D001", "batiment", "EPSG:4326")
    assert "dept" in result.columns
    assert isinstance(result["geometry"].iloc[0], str)
