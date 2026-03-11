from dbtopo.writer import build_select_exprs, full_table_name


def test_full_table_name():
    result = full_table_name("my_cat", "my_schema", "ign_bdtopo_", "batiment")
    assert result == "my_cat.my_schema.ign_bdtopo_batiment"


def test_full_table_name_custom_prefix():
    result = full_table_name("c", "s", "bdtopo_", "commune")
    assert result == "c.s.bdtopo_commune"


class TestBuildSelectExprs:
    def test_plain_columns(self):
        result = build_select_exprs(["id", "name"])
        assert result == ["`id`", "`name`"]

    def test_geometry_srid_zero(self):
        result = build_select_exprs(["id", "geometry"])
        assert result == ["`id`", "ST_GeomFromWKT(geometry, 0) AS geometry"]

    def test_geometry_same_srid_no_transform(self):
        result = build_select_exprs(["geometry"], source_srid=4326, target_srid=4326)
        assert result == ["ST_GeomFromWKT(geometry, 4326) AS geometry"]

    def test_geometry_different_srid_transforms(self):
        result = build_select_exprs(["geometry"], source_srid=2154, target_srid=4326)
        assert result == [
            "ST_Transform(ST_GeomFromWKT(geometry, 2154), 4326) AS geometry"
        ]

    def test_cast_exprs_applied(self):
        casts = {"date_col": "TRY_CAST(`date_col` AS DATE) AS `date_col`"}
        result = build_select_exprs(["id", "date_col"], cast_exprs=casts)
        assert result == ["`id`", "TRY_CAST(`date_col` AS DATE) AS `date_col`"]

    def test_all_combined(self):
        casts = {"updated_at": "TRY_CAST(`updated_at` AS TIMESTAMP) AS `updated_at`"}
        result = build_select_exprs(
            ["id", "geometry", "updated_at", "name"],
            cast_exprs=casts,
            source_srid=2154,
            target_srid=4326,
        )
        assert result == [
            "`id`",
            "ST_Transform(ST_GeomFromWKT(geometry, 2154), 4326) AS geometry",
            "TRY_CAST(`updated_at` AS TIMESTAMP) AS `updated_at`",
            "`name`",
        ]

    def test_none_cast_exprs(self):
        result = build_select_exprs(["id"], cast_exprs=None)
        assert result == ["`id`"]
