"""Per-layer batch-size heuristic.

GPKG files are SQLite databases; geometry is stored as a BLOB. We can size
each layer's Spark write batch from the actual blob sizes *before* reading any
data, by combining three cheap probes:

1. **Row count** — `COUNT(*)` walks the SQLite B-tree leaves only, no payload
   load (sub-second even for ~1M rows).
2. **Typical row size** — a `LIMIT 1000` sample gives `AVG(LENGTH(geom))` and a
   sense of the distribution.
3. **Worst-case row size** — a full-table `MAX(LENGTH(geom))` scan. `LENGTH()`
   on a BLOB reads only the byte-count from the row header (SQLite stores it
   alongside the row metadata); the blob payload itself never enters memory.
   At BD TOPO scale this still completes in under a second per layer.

We size the batch against the worst case (OOM is driven by the largest single
row in a batch, not the average) and inflate WKB → WKT by a configurable
factor (~3 in practice).
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

# Heuristic constants.
WKT_INFLATION_FACTOR = 3  # WKT serialised text is ~3× the WKB blob.
DEFAULT_MEM_LIMIT_MB = 512  # Per-executor memory budget (1 GB nominal,
# leave headroom for Arrow + Python).
HUGE_SINGLE_ROW_BYTES = 50 * 1024 * 1024  # 50 MB WKT → force batch_size=1.
SAMPLE_ROWS = 1000


@dataclass(frozen=True)
class LayerSizeStats:
    """Diagnostics for one layer's geometry-size distribution."""

    layer: str
    n_rows: int
    geom_col: str
    avg_blob_bytes: int
    max_blob_bytes: int
    recommended_batch_size: int


def _geometry_column(con: sqlite3.Connection, layer: str) -> str | None:
    """Look up the geometry column name from GPKG metadata."""
    row = con.execute(
        "SELECT column_name FROM gpkg_geometry_columns WHERE table_name = ?",
        (layer,),
    ).fetchone()
    return row[0] if row else None


def _row_count(con: sqlite3.Connection, layer: str) -> int:
    """Try the OGR feature-count extension first (zero-scan if populated)."""
    try:
        row = con.execute(
            "SELECT feature_count FROM gpkg_ogr_contents WHERE table_name = ?",
            (layer,),
        ).fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except sqlite3.OperationalError:
        pass  # gpkg_ogr_contents not present
    return con.execute(f'SELECT COUNT(*) FROM "{layer}"').fetchone()[0]


def estimate_batch_size(
    gpkg_path: str | Path,
    layer: str,
    default_batch_size: int,
    *,
    mem_limit_mb: int = DEFAULT_MEM_LIMIT_MB,
    wkt_inflation: int = WKT_INFLATION_FACTOR,
) -> LayerSizeStats:
    """Pick a per-layer batch size that fits the worst-case row in memory.

    Returns a `LayerSizeStats` with the recommended batch size *and* the
    diagnostic stats that drove the decision — caller logs them so future
    OOMs can be debugged from the printed plan.

    If the layer or its geometry column can't be probed, falls back to the
    caller-provided `default_batch_size`.
    """
    path = str(gpkg_path)
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as con:
        geom_col = _geometry_column(con, layer)
        if not geom_col:
            return LayerSizeStats(layer, 0, "", 0, 0, default_batch_size)

        n_rows = _row_count(con, layer)
        if n_rows == 0:
            return LayerSizeStats(layer, 0, geom_col, 0, 0, default_batch_size)

        # Sample for typical/avg — cheap, bounded.
        sample = con.execute(
            f'SELECT LENGTH("{geom_col}") FROM "{layer}" LIMIT ?',
            (SAMPLE_ROWS,),
        ).fetchall()
        sizes = [s[0] for s in sample if s[0] is not None]
        avg_blob = int(sum(sizes) / len(sizes)) if sizes else 0

        # Full-table MAX for worst case (row-header scan only).
        max_blob = (
            con.execute(f'SELECT MAX(LENGTH("{geom_col}")) FROM "{layer}"').fetchone()[
                0
            ]
            or 0
        )

    max_wkt = max_blob * wkt_inflation

    if max_wkt > HUGE_SINGLE_ROW_BYTES:
        recommended = 1
    elif max_wkt <= 0:
        recommended = default_batch_size
    else:
        budget = mem_limit_mb * 1024 * 1024
        target = budget // max_wkt
        recommended = max(1, min(default_batch_size, int(target)))

    return LayerSizeStats(
        layer=layer,
        n_rows=n_rows,
        geom_col=geom_col,
        avg_blob_bytes=avg_blob,
        max_blob_bytes=max_blob,
        recommended_batch_size=recommended,
    )
