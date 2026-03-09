from __future__ import annotations

import os
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry


def build_download_url(
    version: str,
    projection: str,
    dept: str,
    version_date: str,
) -> str:
    dept_code = dept if dept.startswith("D") else f"D{dept}"
    base_name = (
        f"BDTOPO_{version}_TOUSTHEMES_GPKG_{projection}_{dept_code}_{version_date}"
    )
    return f"https://data.geopf.fr/telechargement/download/BDTOPO/{base_name}/{base_name}.7z"


def _make_session(
    max_retries: int = 5, backoff_factor: float = 1.0
) -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def download_file(
    url: str,
    dest_path: str | Path,
    chunk_size: int = 1024 * 1024,
    skip_existing: bool = True,
) -> Path:
    dest_path = Path(dest_path)
    if skip_existing and dest_path.exists() and dest_path.stat().st_size > 0:
        print(f"Already cached: {dest_path}")
        return dest_path

    dest_path.parent.mkdir(parents=True, exist_ok=True)

    session = _make_session()
    with session.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        total_size = int(r.headers.get("content-length", 0))
        with (
            open(dest_path, "wb") as f,
            tqdm(
                total=total_size, unit="B", unit_scale=True, desc=dest_path.name
            ) as pbar,
        ):
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))

    print(f"Downloaded: {dest_path}")
    return dest_path


def download_department(
    dept: str,
    volume_path: str,
    version: str = "3-5",
    projection: str = "LAMB93",
    version_date: str = "2025-09-15",
) -> Path:
    url = build_download_url(version, projection, dept, version_date)
    filename = os.path.basename(url)
    dest = Path(volume_path) / filename
    return download_file(url, dest)
