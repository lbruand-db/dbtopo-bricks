from dbtopo.config import ALL_DEPARTMENTS, AppConfig


def test_default_config():
    cfg = AppConfig()
    assert cfg.ign.version == "3-5"
    assert cfg.transform.target_crs == "EPSG:4326"
    assert cfg.databricks.catalog == "dev_catalog"


def test_volume_path():
    cfg = AppConfig()
    assert cfg.volume_path() == "/Volumes/dev_catalog/ign_bdtopo/bronze_volume"


def test_resolve_departments_explicit():
    cfg = AppConfig(ign={"departments": ["001", "075"]})
    assert cfg.resolve_departments() == ["001", "075"]


def test_resolve_departments_all():
    cfg = AppConfig(ign={"departments": ["all"]})
    depts = cfg.resolve_departments()
    assert len(depts) == len(ALL_DEPARTMENTS)
    assert "001" in depts
    assert "02A" in depts
