"""Tests for dbtopo.dedup using a mock Spark session."""

from unittest.mock import MagicMock

from dbtopo.dedup import (
    copy_table_metadata,
    dedup_table,
    dedup_tables,
    get_table_metadata,
    list_source_tables,
)

# ---------------------------------------------------------------------------
# Helpers to build mock Spark rows
# ---------------------------------------------------------------------------


def _row(**kwargs):
    """Create a mock Row with attribute access."""
    m = MagicMock()
    for k, v in kwargs.items():
        setattr(m, k, v)
    return m


def _mock_spark_for_metadata(props_rows, describe_rows):
    """Build a mock spark where sql() returns different results per query."""
    spark = MagicMock()

    def sql_side_effect(query):
        result = MagicMock()
        if "SHOW TBLPROPERTIES" in query:
            result.collect.return_value = props_rows
        elif "DESCRIBE TABLE EXTENDED" in query:
            result.collect.return_value = describe_rows
        return result

    spark.sql.side_effect = sql_side_effect
    return spark


# ---------------------------------------------------------------------------
# get_table_metadata
# ---------------------------------------------------------------------------


class TestGetTableMetadata:
    def test_extracts_custom_props_only(self):
        props = [
            _row(key="crs", value="EPSG:4326"),
            _row(key="delta.minReaderVersion", value="1"),
            _row(key="option.mergeSchema", value="true"),
            _row(key="bdtopo_version", value="3-5"),
        ]
        describe = [
            _row(col_name="geometry", data_type="string", comment="WKT"),
            _row(col_name="dept", data_type="string", comment=""),
        ]
        spark = _mock_spark_for_metadata(props, describe)

        tbl_props, comment, col_comments = get_table_metadata(spark, "t")

        assert tbl_props == {"crs": "EPSG:4326", "bdtopo_version": "3-5"}

    def test_extracts_table_comment(self):
        props = []
        describe = [
            _row(col_name="Comment", data_type="my table comment", comment=""),
        ]
        spark = _mock_spark_for_metadata(props, describe)

        _, comment, _ = get_table_metadata(spark, "t")
        assert comment == "my table comment"

    def test_extracts_column_comments(self):
        props = []
        describe = [
            _row(col_name="geometry", data_type="string", comment="WKT geo"),
            _row(col_name="dept", data_type="string", comment=""),
            _row(col_name="cleabs", data_type="string", comment=None),
        ]
        spark = _mock_spark_for_metadata(props, describe)

        _, _, col_comments = get_table_metadata(spark, "t")
        assert col_comments == {"geometry": "WKT geo"}

    def test_empty_table(self):
        spark = _mock_spark_for_metadata([], [])
        tbl_props, comment, col_comments = get_table_metadata(spark, "t")
        assert tbl_props == {}
        assert comment == ""
        assert col_comments == {}


# ---------------------------------------------------------------------------
# copy_table_metadata
# ---------------------------------------------------------------------------


class TestCopyTableMetadata:
    def test_copies_all_metadata(self):
        spark = MagicMock()
        copy_table_metadata(
            spark,
            "dst",
            tbl_props={"crs": "EPSG:4326"},
            table_comment="my comment",
            col_comments={"geometry": "WKT"},
        )

        calls = [c[0][0] for c in spark.sql.call_args_list]
        assert any("SET TBLPROPERTIES" in c for c in calls)
        assert any("COMMENT ON TABLE" in c for c in calls)
        assert any("ALTER COLUMN geometry COMMENT" in c for c in calls)

    def test_skips_empty_props(self):
        spark = MagicMock()
        copy_table_metadata(spark, "dst", {}, "", {})
        spark.sql.assert_not_called()

    def test_skips_empty_comment_but_copies_props(self):
        spark = MagicMock()
        copy_table_metadata(
            spark,
            "dst",
            tbl_props={"crs": "EPSG:4326"},
            table_comment="",
            col_comments={},
        )
        assert spark.sql.call_count == 1
        assert "TBLPROPERTIES" in spark.sql.call_args_list[0][0][0]


# ---------------------------------------------------------------------------
# dedup_table
# ---------------------------------------------------------------------------


class TestDedupTable:
    def test_dedup_returns_stats(self):
        spark = MagicMock()
        call_idx = {"n": 0}

        def sql_side_effect(query):
            result = MagicMock()
            if "SHOW TBLPROPERTIES" in query:
                result.collect.return_value = []
            elif "DESCRIBE TABLE EXTENDED" in query:
                result.collect.return_value = []
            elif "COUNT(*)" in query:
                # First call = before (10), second call = after (8)
                row = _row(cnt=10 if call_idx["n"] == 0 else 8)
                call_idx["n"] += 1
                result.collect.return_value = [row]
            return result

        spark.sql.side_effect = sql_side_effect

        stats = dedup_table(spark, "src_tbl", "dst_tbl", "cleabs")

        assert stats == {"before": 10, "after": 8, "removed": 2}

    def test_dedup_executes_ctas(self):
        spark = MagicMock()

        def sql_side_effect(query):
            result = MagicMock()
            if "COUNT(*)" in query:
                result.collect.return_value = [_row(cnt=5)]
            else:
                result.collect.return_value = []
            return result

        spark.sql.side_effect = sql_side_effect

        dedup_table(spark, "src", "dst", "my_key")

        sql_calls = [c[0][0] for c in spark.sql.call_args_list]
        ctas = [c for c in sql_calls if "CREATE OR REPLACE TABLE" in c]
        assert len(ctas) == 1
        assert "PARTITION BY my_key" in ctas[0]
        assert "dst" in ctas[0]
        assert "EXCEPT(rn)" in ctas[0]


# ---------------------------------------------------------------------------
# list_source_tables
# ---------------------------------------------------------------------------


class TestListSourceTables:
    def test_filters_by_prefix_and_excludes_dedup(self):
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [
            _row(tableName="ign_bdtopo_batiment"),
            _row(tableName="ign_bdtopo_batiment_dedup"),
            _row(tableName="ign_bdtopo_route"),
            _row(tableName="other_table"),
        ]

        result = list_source_tables(spark, "cat", "sch", "ign_bdtopo_", "_dedup")

        assert result == ["ign_bdtopo_batiment", "ign_bdtopo_route"]

    def test_empty_schema(self):
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = []

        result = list_source_tables(spark, "cat", "sch", "ign_bdtopo_", "_dedup")
        assert result == []


# ---------------------------------------------------------------------------
# dedup_tables (integration-style with mocks)
# ---------------------------------------------------------------------------


class TestDedupTables:
    def test_returns_empty_when_no_tables(self):
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = []

        stats = dedup_tables(spark, "cat", "sch")
        assert stats == {}

    def test_dedup_all_tables(self):
        spark = MagicMock()
        call_count = {"n": 0}

        def sql_side_effect(query):
            result = MagicMock()
            if "SHOW TABLES" in query:
                result.collect.return_value = [
                    _row(tableName="ign_bdtopo_batiment"),
                ]
            elif "SHOW TBLPROPERTIES" in query:
                result.collect.return_value = []
            elif "DESCRIBE TABLE EXTENDED" in query:
                result.collect.return_value = []
            elif "COUNT(*)" in query:
                row = _row(cnt=100 if call_count["n"] % 2 == 0 else 95)
                call_count["n"] += 1
                result.collect.return_value = [row]
            return result

        spark.sql.side_effect = sql_side_effect

        stats = dedup_tables(spark, "cat", "sch")

        assert "ign_bdtopo_batiment" in stats
        assert stats["ign_bdtopo_batiment"]["before"] == 100
        assert stats["ign_bdtopo_batiment"]["after"] == 95
        assert stats["ign_bdtopo_batiment"]["removed"] == 5
