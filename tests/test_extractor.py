import pytest

from dbtopo.extractor import find_gpkg_in_archive


def test_find_gpkg_in_archive_not_found(tmp_path):
    """Test that a missing archive raises an error."""
    fake_archive = tmp_path / "fake.7z"
    fake_archive.write_bytes(b"not a real archive")
    with pytest.raises(Exception):
        find_gpkg_in_archive(fake_archive)
