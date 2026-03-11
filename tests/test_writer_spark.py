"""Tests for writer functions that require a Spark session (mocked)."""

from unittest.mock import MagicMock

import pandas as pd
from pyspark.sql.types import StringType, StructField, StructType

from dbtopo.writer import (
    delete_department_rows,
    ensure_table_with_metadata,
    write_batch_to_delta,
)


class TestDeleteDepartmentRows:
    def test_deletes_by_dept(self):
        spark = MagicMock()
        delete_department_rows(spark, "cat.sch.tbl", "D001")
        spark.sql.assert_called_once_with("DELETE FROM cat.sch.tbl WHERE dept = 'D001'")

    def test_noop_when_table_missing(self):
        spark = MagicMock()
        spark.sql.side_effect = Exception("Table not found")
        # Should not raise
        delete_department_rows(spark, "cat.sch.tbl", "D001")


class TestWriteBatchToDelta:
    def test_uses_explicit_schema_when_provided(self):
        spark = MagicMock()
        pdf = pd.DataFrame({"a": [1]})
        schema = MagicMock()

        write_batch_to_delta(spark, pdf, "my_table", schema=schema)

        spark.createDataFrame.assert_called_once()
        _, kwargs = spark.createDataFrame.call_args
        assert kwargs["schema"] is schema

    def test_infers_schema_when_none(self):
        spark = MagicMock()
        pdf = pd.DataFrame({"a": [1]})

        write_batch_to_delta(spark, pdf, "my_table")

        spark.createDataFrame.assert_called_once()
        args, kwargs = spark.createDataFrame.call_args
        assert "schema" not in kwargs or kwargs.get("schema") is None

    def test_writes_to_correct_table(self):
        spark = MagicMock()
        sdf = spark.createDataFrame.return_value
        # selectExpr is not called when there's no geometry column
        sdf.columns = ["a"]
        writer = sdf.write.format.return_value.mode.return_value.option.return_value
        pdf = pd.DataFrame({"a": [1]})

        write_batch_to_delta(spark, pdf, "cat.sch.tbl")

        sdf.write.format.assert_called_with("delta")
        writer.saveAsTable.assert_called_with("cat.sch.tbl")

    def test_converts_geometry_to_native_type(self):
        spark = MagicMock()
        sdf = spark.createDataFrame.return_value
        sdf.columns = ["name", "geometry"]
        tsdf = sdf.selectExpr.return_value
        chain = tsdf.write.format.return_value.mode.return_value
        writer = chain.option.return_value
        pdf = pd.DataFrame({"name": ["A"], "geometry": ["POINT (1 2)"]})

        write_batch_to_delta(spark, pdf, "cat.sch.tbl", source_srid=4326)

        sdf.selectExpr.assert_called_once_with(
            "name", "ST_GeomFromWKT(geometry, 4326) AS geometry"
        )
        writer.saveAsTable.assert_called_with("cat.sch.tbl")

    def test_reprojects_geometry_server_side(self):
        spark = MagicMock()
        sdf = spark.createDataFrame.return_value
        sdf.columns = ["name", "geometry"]
        sdf.selectExpr.return_value  # transformed_sdf
        pdf = pd.DataFrame({"name": ["A"], "geometry": ["POINT (600000 6600000)"]})

        write_batch_to_delta(
            spark, pdf, "cat.sch.tbl", source_srid=2154, target_srid=4326
        )

        sdf.selectExpr.assert_called_once_with(
            "name",
            "ST_Transform(ST_GeomFromWKT(geometry, 2154), 4326) AS geometry",
        )

    def test_skips_transform_when_same_srid(self):
        spark = MagicMock()
        sdf = spark.createDataFrame.return_value
        sdf.columns = ["name", "geometry"]
        pdf = pd.DataFrame({"name": ["A"], "geometry": ["POINT (2.3 48.8)"]})

        write_batch_to_delta(
            spark, pdf, "cat.sch.tbl", source_srid=4326, target_srid=4326
        )

        sdf.selectExpr.assert_called_once_with(
            "name", "ST_GeomFromWKT(geometry, 4326) AS geometry"
        )


class TestEnsureTableWithMetadata:
    def _make_schema(self, *col_names):
        return StructType([StructField(n, StringType(), True) for n in col_names])

    def test_creates_table_with_column_comments(self):
        spark = MagicMock()
        schema = self._make_schema("cleabs", "geometry", "dept")

        ensure_table_with_metadata(spark, "cat.sch.tbl", schema, layer="arrondissement")

        calls = [c[0][0] for c in spark.sql.call_args_list]
        create_call = [c for c in calls if "CREATE TABLE" in c][0]
        assert "IF NOT EXISTS" in create_call
        assert "USING DELTA" in create_call
        assert "COMMENT" in create_call
        # Column-level comments should be present for known columns
        assert "cleabs" in create_call
        assert "geometry" in create_call

    def test_sets_table_properties(self):
        spark = MagicMock()
        schema = self._make_schema("cleabs")

        ensure_table_with_metadata(
            spark,
            "t",
            schema,
            layer="batiment",
            version="3-5",
            version_date="2025-09-15",
        )

        calls = [c[0][0] for c in spark.sql.call_args_list]
        props_call = [c for c in calls if "TBLPROPERTIES" in c][0]
        assert "crs" in props_call
        assert "bdtopo_version" in props_call
        assert "bdtopo_version_date" in props_call

    def test_french_language(self):
        spark = MagicMock()
        schema = self._make_schema("cleabs", "geometry")

        ensure_table_with_metadata(
            spark,
            "t",
            schema,
            layer="arrondissement",
            lang="fr",
        )

        calls = [c[0][0] for c in spark.sql.call_args_list]
        create_call = [c for c in calls if "CREATE TABLE" in c][0]
        # French description for cleabs
        assert "Identifiant unique" in create_call

    def test_english_language_default(self):
        spark = MagicMock()
        schema = self._make_schema("cleabs", "geometry")

        ensure_table_with_metadata(
            spark,
            "t",
            schema,
            layer="arrondissement",
        )

        calls = [c[0][0] for c in spark.sql.call_args_list]
        create_call = [c for c in calls if "CREATE TABLE" in c][0]
        # English description for cleabs
        assert "Unique object identifier" in create_call

    def test_unknown_layer_still_creates_table(self):
        spark = MagicMock()
        schema = self._make_schema("col_a", "col_b")

        ensure_table_with_metadata(
            spark,
            "t",
            schema,
            layer="unknown_layer_xyz",
        )

        calls = [c[0][0] for c in spark.sql.call_args_list]
        create_call = [c for c in calls if "CREATE TABLE" in c][0]
        assert "IF NOT EXISTS" in create_call
        # No column comments for unknown layer, but table still created
        assert "col_a" in create_call

    def test_geometry_column_uses_native_type(self):
        spark = MagicMock()
        schema = self._make_schema("cleabs", "geometry", "dept")

        ensure_table_with_metadata(spark, "t", schema, layer="arrondissement")

        calls = [c[0][0] for c in spark.sql.call_args_list]
        create_call = [c for c in calls if "CREATE TABLE" in c][0]
        # geometry column should be GEOMETRY(4326), not string
        assert "`geometry` GEOMETRY(4326)" in create_call
        # other columns should remain their original types
        assert "`cleabs` string" in create_call

    def test_table_description_includes_version(self):
        spark = MagicMock()
        schema = self._make_schema("cleabs")

        ensure_table_with_metadata(
            spark,
            "t",
            schema,
            layer="batiment",
            version="3-5",
            version_date="2025-09-15",
        )

        calls = [c[0][0] for c in spark.sql.call_args_list]
        create_call = [c for c in calls if "CREATE TABLE" in c][0]
        assert "v3-5" in create_call
        assert "2025-09-15" in create_call
