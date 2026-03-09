from dbtopo.writer import full_table_name


def test_full_table_name():
    result = full_table_name("my_cat", "my_schema", "ign_bdtopo_", "batiment")
    assert result == "my_cat.my_schema.ign_bdtopo_batiment"


def test_full_table_name_custom_prefix():
    result = full_table_name("c", "s", "bdtopo_", "commune")
    assert result == "c.s.bdtopo_commune"
