"""Build a Spark StructType from GPKG layer metadata (pyogrio)."""

from __future__ import annotations

from pathlib import Path

import pyogrio
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Mapping from OGR type names to Spark types.
_OGR_TO_SPARK = {
    "OFTString": StringType(),
    "OFTInteger": IntegerType(),
    "OFTReal": DoubleType(),
    "OFTInteger64": LongType(),
    "OFTBinary": StringType(),
    "OFTDate": DateType(),
    "OFTDateTime": TimestampType(),
    "OFTTime": StringType(),
}

# OGR subtypes that override the base type.
_OGR_SUBTYPE_OVERRIDE = {
    "OFSTBoolean": BooleanType(),
    "OFSTFloat32": DoubleType(),
    "OFSTInt16": IntegerType(),
}


def spark_schema_from_gpkg(
    gpkg_path: str | Path,
    layer: str,
    extra_columns: dict[str, DataType] | None = None,
) -> StructType:
    """Derive a Spark StructType from the GPKG layer definition.

    Parameters
    ----------
    gpkg_path:
        Path to the GeoPackage file.
    layer:
        Layer name inside the GeoPackage.
    extra_columns:
        Additional columns to append
        (e.g. ``{"dept": StringType(), "layer": StringType()}``).
        These are added as nullable fields after the GPKG columns.

    Returns a StructType with one field per GPKG column plus
    ``geometry`` (string) and any *extra_columns*.
    """
    info = pyogrio.read_info(str(gpkg_path), layer=layer)
    fields: list[StructField] = []

    for name, ogr_type, ogr_subtype in zip(
        info["fields"], info["ogr_types"], info["ogr_subtypes"]
    ):
        if ogr_subtype in _OGR_SUBTYPE_OVERRIDE:
            spark_type = _OGR_SUBTYPE_OVERRIDE[ogr_subtype]
        else:
            spark_type = _OGR_TO_SPARK.get(ogr_type, StringType())
        fields.append(StructField(name, spark_type, nullable=True))

    # geometry is converted to WKT string by the transformer
    fields.append(StructField("geometry", StringType(), nullable=True))

    if extra_columns:
        for col_name, col_type in extra_columns.items():
            fields.append(StructField(col_name, col_type, nullable=True))

    return StructType(fields)
