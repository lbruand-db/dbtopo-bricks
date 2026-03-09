"""Find rows in a GPKG layer with malformed datetime fields (seconds > 59)
and write them to a small test GPKG for regression testing."""

from __future__ import annotations

import sys
from pathlib import Path

import fiona
import geopandas as gpd


def find_bad_datetime_rows(gpkg_path: str, layer: str = "batiment") -> list[dict]:
    """Iterate with fiona and catch DateTimeField parse errors."""
    bad_rows = []
    last_good_idx = -1

    with fiona.open(gpkg_path, layer=layer) as src:
        total = len(src)
        print(f"Scanning {total} features in layer '{layer}'...")

        for idx, feature in enumerate(src):
            last_good_idx = idx
            if idx % 50000 == 0:
                print(f"  {idx}/{total}...")

    # If we get here without error, all rows are fine with this fiona version.
    # Try the alternate approach: read with pyogrio and compare datetime columns.
    print(f"Fiona read all {total} rows without error. Trying pyogrio comparison...")
    return _find_bad_via_pyogrio(gpkg_path, layer)


def _find_bad_via_pyogrio(gpkg_path: str, layer: str) -> list[dict]:
    """Read with pyogrio (lenient) and find rows where datetime seconds >= 60."""
    import pyogrio

    gdf = gpd.read_file(gpkg_path, layer=layer, engine="pyogrio")
    print(f"  Loaded {len(gdf)} rows via pyogrio")

    # Find datetime columns
    dt_cols = [c for c in gdf.columns if gdf[c].dtype == "datetime64[ns]" or "date" in c.lower()]
    print(f"  Datetime-like columns: {dt_cols}")

    # Read raw values via pyogrio to check for anomalous seconds
    raw_df = pyogrio.read_dataframe(gpkg_path, layer=layer, read_geometry=False)

    bad_indices = set()
    for col in dt_cols:
        if col not in raw_df.columns:
            continue
        series = raw_df[col]
        if series.dtype == object:  # string datetimes
            for i, val in series.items():
                if val and isinstance(val, str) and ":" in val:
                    parts = val.split(":")
                    if len(parts) >= 3:
                        try:
                            secs = float(parts[2].rstrip("Z"))
                            if secs >= 60 or secs < 0:
                                print(f"  BAD: row {i}, col '{col}', value '{val}'")
                                bad_indices.add(i)
                        except ValueError:
                            pass

    return list(bad_indices)


def main():
    if len(sys.argv) < 2:
        print("Usage: python extract_bad_datetime_row.py <gpkg_path> [layer] [output_path]")
        print("  Defaults: layer=batiment, output=tests/fixtures/test_bad_datetime.gpkg")
        sys.exit(1)

    gpkg_path = sys.argv[1]
    layer = sys.argv[2] if len(sys.argv) > 2 else "batiment"
    output = sys.argv[3] if len(sys.argv) > 3 else "tests/fixtures/test_bad_datetime.gpkg"

    # First try: iterate with fiona and catch the error to find the failing index
    bad_idx = None
    try:
        with fiona.open(gpkg_path, layer=layer) as src:
            total = len(src)
            print(f"Scanning {total} features in layer '{layer}' with fiona...")
            for idx, feature in enumerate(src):
                if idx % 50000 == 0:
                    print(f"  {idx}/{total}...")
        print("Fiona read all rows without error.")
    except ValueError as e:
        if "second must be" in str(e):
            bad_idx = idx + 1  # fiona failed on the next one after last successful
            print(f"Fiona failed at feature index ~{bad_idx}: {e}")
        else:
            raise

    if bad_idx is not None:
        # Read the bad row with pyogrio (which handles it)
        print(f"Reading row {bad_idx} with pyogrio...")
        gdf = gpd.read_file(
            gpkg_path,
            layer=layer,
            engine="pyogrio",
            skip_features=bad_idx,
            max_features=1,
        )
        print(f"  Got {len(gdf)} row(s)")
        print(f"  Columns: {list(gdf.columns)}")
        print(f"  Row data:")
        for col in gdf.columns:
            val = gdf[col].iloc[0]
            if val is not None and str(val) != "None" and str(val) != "NaT":
                print(f"    {col}: {val}")

        Path(output).parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(output, layer=layer, driver="GPKG")
        print(f"\nWrote bad row to: {output}")
        return

    # Fallback: scan with pyogrio for string datetime anomalies
    print("Scanning for bad datetimes via pyogrio raw read...")
    bad_indices = _find_bad_via_pyogrio(gpkg_path, layer)

    if not bad_indices:
        print("No bad datetime rows found.")
        sys.exit(0)

    print(f"\nFound {len(bad_indices)} bad row(s). Extracting...")
    # Read those rows with pyogrio
    gdf_all = gpd.read_file(gpkg_path, layer=layer, engine="pyogrio")
    gdf_bad = gdf_all.iloc[list(bad_indices)]
    print(f"  Extracted {len(gdf_bad)} row(s)")

    Path(output).parent.mkdir(parents=True, exist_ok=True)
    gdf_bad.to_file(output, layer=layer, driver="GPKG")
    print(f"Wrote {len(gdf_bad)} bad row(s) to: {output}")


if __name__ == "__main__":
    main()
