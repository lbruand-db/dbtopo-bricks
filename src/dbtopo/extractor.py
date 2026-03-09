from __future__ import annotations

import tempfile
from pathlib import Path

import py7zr


def list_archive_contents(archive_path: str | Path) -> list[str]:
    with py7zr.SevenZipFile(str(archive_path), mode="r") as archive:
        return archive.getnames()


def find_gpkg_in_archive(archive_path: str | Path) -> str:
    names = list_archive_contents(archive_path)
    gpkg_files = [n for n in names if n.endswith(".gpkg")]
    if not gpkg_files:
        raise FileNotFoundError(f"No .gpkg file found in {archive_path}")
    return gpkg_files[0]


def extract_gpkg(
    archive_path: str | Path, output_dir: str | Path | None = None
) -> Path:
    archive_path = Path(archive_path)
    gpkg_name = find_gpkg_in_archive(archive_path)

    if output_dir is None:
        output_dir = Path(tempfile.mkdtemp(prefix="dbtopo_"))
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    with py7zr.SevenZipFile(str(archive_path), mode="r") as archive:
        archive.extract(targets=[gpkg_name], path=str(output_dir))

    gpkg_path = output_dir / gpkg_name
    if not gpkg_path.exists():
        raise FileNotFoundError(f"Extracted file not found at {gpkg_path}")

    print(f"Extracted: {gpkg_path}")
    return gpkg_path
