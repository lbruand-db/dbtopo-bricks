"""Tests for writer functions that require a Spark session (mocked)."""

from unittest.mock import MagicMock

import pandas as pd

from dbtopo.writer import (
    delete_department_rows,
    set_table_geo_metadata,
    write_batch_to_delta,
)


class TestDeleteDepartmentRows:
    def test_deletes_by_dept(self):
        spark = MagicMock()
        delete_department_rows(spark, "cat.sch.tbl", "D001")
        spark.sql.assert_called_once_with(
            "DELETE FROM cat.sch.tbl WHERE dept = 'D001'"
        )

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
        writer = sdf.write.format.return_value.mode.return_value.option.return_value
        pdf = pd.DataFrame({"a": [1]})

        write_batch_to_delta(spark, pdf, "cat.sch.tbl")

        sdf.write.format.assert_called_with("delta")
        writer.saveAsTable.assert_called_with("cat.sch.tbl")


class TestSetTableGeoMetadata:
    def test_sets_crs_property_and_column_comment(self):
        spark = MagicMock()

        set_table_geo_metadata(spark, "my_table")

        calls = [c[0][0] for c in spark.sql.call_args_list]
        assert any("crs" in c and "TBLPROPERTIES" in c for c in calls)
        assert any("COMMENT ON TABLE" in c for c in calls)
        assert any(
            "ALTER COLUMN geometry COMMENT" in c and "EPSG:4326" in c for c in calls
        )

    def test_includes_version_info(self):
        spark = MagicMock()

        set_table_geo_metadata(
            spark,
            "t",
            source_schema="ign_bdtopo",
            version="3-5",
            version_date="2025-09-15",
        )

        calls = [c[0][0] for c in spark.sql.call_args_list]
        props_call = [c for c in calls if "TBLPROPERTIES" in c][0]
        assert "bdtopo_version" in props_call
        assert "bdtopo_version_date" in props_call
        assert "source_schema" in props_call

        comment_call = [c for c in calls if "COMMENT ON TABLE" in c][0]
        assert "3-5" in comment_call
        assert "2025-09-15" in comment_call

    def test_minimal_metadata(self):
        spark = MagicMock()

        set_table_geo_metadata(spark, "t", crs="EPSG:2154")

        calls = [c[0][0] for c in spark.sql.call_args_list]
        props_call = [c for c in calls if "TBLPROPERTIES" in c][0]
        assert "EPSG:2154" in props_call
        assert "source_schema" not in props_call
