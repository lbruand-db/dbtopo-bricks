from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructType,
)

from dbtopo.schema import spark_schema_from_gpkg

FIXTURE = "tests/fixtures/test_D001_batiment.gpkg"


def test_schema_from_gpkg_returns_structtype():
    schema = spark_schema_from_gpkg(FIXTURE, "batiment")
    assert isinstance(schema, StructType)


def test_schema_has_geometry_column():
    schema = spark_schema_from_gpkg(FIXTURE, "batiment")
    names = [f.name for f in schema.fields]
    assert "geometry" in names
    geo = schema["geometry"]
    assert geo.dataType == StringType()


def test_schema_ogr_types_mapped_correctly():
    schema = spark_schema_from_gpkg(FIXTURE, "batiment")
    # OFTString -> StringType
    assert schema["cleabs"].dataType == StringType()
    # OFTReal -> DoubleType
    assert schema["hauteur"].dataType == DoubleType()
    # OFSTBoolean subtype -> BooleanType
    assert schema["construction_legere"].dataType == BooleanType()


def test_schema_extra_columns():
    schema = spark_schema_from_gpkg(
        FIXTURE,
        "batiment",
        extra_columns={"dept": StringType(), "layer": StringType()},
    )
    names = [f.name for f in schema.fields]
    assert "dept" in names
    assert "layer" in names
    assert schema["dept"].dataType == StringType()
