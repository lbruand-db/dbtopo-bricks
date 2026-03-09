from dbtopo.downloader import build_download_url


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
