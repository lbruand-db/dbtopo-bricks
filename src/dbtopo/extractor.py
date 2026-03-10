from __future__ import annotations

import tempfile
from pathlib import Path

import py7zr
from tqdm import tqdm

WRITE_CHUNK = 1024 * 1024  # 1 MB


def list_archive_contents(archive_path: str | Path) -> list[str]:
    with py7zr.SevenZipFile(str(archive_path), mode="r") as archive:
        return archive.getnames()


def _gpkg_uncompressed_size(archive_path: str | Path, gpkg_name: str) -> int:
    """Return the uncompressed size of a file inside a 7z archive."""
    with py7zr.SevenZipFile(str(archive_path), mode="r") as archive:
        for entry in archive.list():
            if entry.filename == gpkg_name:
                return entry.uncompressed
    return 0


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

    total = _gpkg_uncompressed_size(archive_path, gpkg_name)

    with py7zr.SevenZipFile(str(archive_path), mode="r") as archive:
        bio_dict = archive.read(targets=[gpkg_name])

    bio = bio_dict[gpkg_name]
    gpkg_path = output_dir / gpkg_name
    gpkg_path.parent.mkdir(parents=True, exist_ok=True)

    with (
        tqdm(total=total, unit="B", unit_scale=True, desc="Extracting") as pbar,
        open(gpkg_path, "wb") as out,
    ):
        while True:
            chunk = bio.read(WRITE_CHUNK)
            if not chunk:
                break
            out.write(chunk)
            pbar.update(len(chunk))

    print(f"Extracted: {gpkg_path}")
    return gpkg_path
