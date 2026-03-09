import hashlib
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from dbtopo.downloader import (
    _compute_file_md5,
    build_download_url,
    build_md5_url,
    download_file,
)


def test_build_download_url_basic():
    url = build_download_url("3-5", "LAMB93", "001", "2025-09-15")
    assert url == (
        "https://data.geopf.fr/telechargement/download/BDTOPO/"
        "BDTOPO_3-5_TOUSTHEMES_GPKG_LAMB93_D001_2025-09-15/"
        "BDTOPO_3-5_TOUSTHEMES_GPKG_LAMB93_D001_2025-09-15.7z"
    )


def test_build_download_url_with_d_prefix():
    url = build_download_url("3-5", "LAMB93", "D075", "2025-09-15")
    assert "D075" in url
    assert "DD075" not in url


def test_build_download_url_different_version():
    url = build_download_url("3-4", "LAMB93", "092", "2025-06-15")
    assert "BDTOPO_3-4_" in url
    assert "D092" in url
    assert "2025-06-15" in url


def test_build_md5_url():
    archive_url = (
        "https://data.geopf.fr/telechargement/download/BDTOPO/"
        "BDTOPO_3-5_TOUSTHEMES_GPKG_LAMB93_D001_2025-09-15/"
        "BDTOPO_3-5_TOUSTHEMES_GPKG_LAMB93_D001_2025-09-15.7z"
    )
    md5_url = build_md5_url(archive_url)
    assert md5_url.endswith(".md5")
    assert ".7z" not in md5_url


def test_compute_file_md5():
    content = b"hello world"
    expected = hashlib.md5(content).hexdigest()
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(content)
        f.flush()
        assert _compute_file_md5(Path(f.name)) == expected


def test_download_file_md5_verification(tmp_path):
    content = b"fake archive content"
    expected_md5 = hashlib.md5(content).hexdigest()
    dest = tmp_path / "test.7z"

    mock_md5_resp = MagicMock()
    mock_md5_resp.text = f"{expected_md5}  LIENS/test.7z"
    mock_md5_resp.raise_for_status = MagicMock()

    mock_dl_resp = MagicMock()
    mock_dl_resp.headers = {"content-length": str(len(content))}
    mock_dl_resp.iter_content = MagicMock(return_value=[content])
    mock_dl_resp.raise_for_status = MagicMock()
    mock_dl_resp.__enter__ = MagicMock(return_value=mock_dl_resp)
    mock_dl_resp.__exit__ = MagicMock(return_value=False)

    with patch("dbtopo.downloader._make_session") as mock_session:
        session = MagicMock()
        mock_session.return_value = session

        def side_effect(url, **kwargs):
            if url.endswith(".md5"):
                return mock_md5_resp
            return mock_dl_resp

        session.get = MagicMock(side_effect=side_effect)

        result = download_file("https://example.com/test.7z", dest)
        assert result == dest
        assert dest.exists()
        assert _compute_file_md5(dest) == expected_md5


def test_download_file_md5_mismatch(tmp_path):
    content = b"fake archive content"
    dest = tmp_path / "test.7z"

    mock_md5_resp = MagicMock()
    mock_md5_resp.text = "0000000000000000000000000000dead  LIENS/test.7z"
    mock_md5_resp.raise_for_status = MagicMock()

    mock_dl_resp = MagicMock()
    mock_dl_resp.headers = {"content-length": str(len(content))}
    mock_dl_resp.iter_content = MagicMock(return_value=[content])
    mock_dl_resp.raise_for_status = MagicMock()
    mock_dl_resp.__enter__ = MagicMock(return_value=mock_dl_resp)
    mock_dl_resp.__exit__ = MagicMock(return_value=False)

    with patch("dbtopo.downloader._make_session") as mock_session:
        session = MagicMock()
        mock_session.return_value = session

        def side_effect(url, **kwargs):
            if url.endswith(".md5"):
                return mock_md5_resp
            return mock_dl_resp

        session.get = MagicMock(side_effect=side_effect)

        import pytest

        with pytest.raises(ValueError, match="MD5 mismatch"):
            download_file("https://example.com/test.7z", dest)
