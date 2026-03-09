"""
Standalone script: process one year of daily WBGT into a mean annual
productivity loss raster.

Designed to be called as a single array task on a cluster (one job per year).
Streams WBGT files directly via vsicurl — no raw files written to disk.

Usage
-----
    python process_wbgt.py --year 2000 --out-dir /path/to/annual/ --config /path/to/config.yaml

    # Or override URL template directly:
    python process_wbgt.py --year 2000 --out-dir ./annual/
"""

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import rasterio
import yaml


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exposure-response function
# ---------------------------------------------------------------------------

# ERF values here
WBGT_THRESHOLDS = [26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39] # list of WBGT breakpoints (°C)
PRODUCTIVITY_VALUES = [1.0, 0.923, 0.846, 0.769, 0.692, 0.615, 0.539, 0.462, 0.385, 0.308, 0.231, 0.154, 0.077, 0.0]   # corresponding productivity fractions


def wbgt_to_productivity_loss(wbgt_array: np.ndarray) -> np.ndarray:
    if WBGT_THRESHOLDS is None or PRODUCTIVITY_VALUES is None:
        raise ValueError("ERF not defined. Set WBGT_THRESHOLDS and PRODUCTIVITY_VALUES in this script.")
    thresholds   = np.array(WBGT_THRESHOLDS,    dtype=float)
    productivity = np.array(PRODUCTIVITY_VALUES, dtype=float)
    result = np.interp(wbgt_array, thresholds, productivity,
                       left=productivity[0], right=productivity[-1])
    result = np.where(np.isnan(wbgt_array), np.nan, result)
    return 1.0 - result   # return loss fraction


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

def iter_dates(year: int):
    d = date(year, 1, 1)
    while d.year == year:
        yield d
        d += timedelta(days=1)


def process_year(year: int, url_template: str, out_path: Path) -> bool:
    """
    Stream all daily WBGT for `year`, compute mean annual productivity loss,
    and write to `out_path`. Returns True on success.
    """
    accumulator = None
    valid_count  = None
    ref_meta     = None
    nodata_val   = None
    n_days       = 0
    skipped      = 0

    days = list(iter_dates(year))
    log.info(f"Year {year}: processing {len(days)} days.")

    for d in days:
        url = "/vsicurl/" + url_template.format(year=d.year, month=d.month, day=d.day)
        try:
            with rasterio.open(url) as src:
                if ref_meta is None:
                    ref_meta   = src.meta.copy()
                    ref_meta.update(dtype="float32", nodata=np.nan)
                    shape      = (src.height, src.width)
                    nodata_val = src.nodata
                    accumulator = np.zeros(shape, dtype=np.float64)
                    valid_count = np.zeros(shape, dtype=np.int32)
                wbgt = src.read(1).astype(np.float32)
        except Exception as e:
            skipped += 1
            if skipped <= 5:   # only log first few to avoid flooding
                log.warning(f"  Could not open {url}: {e}")
            continue

        if nodata_val is not None:
            wbgt[wbgt == nodata_val] = np.nan

        loss  = wbgt_to_productivity_loss(wbgt)
        valid = ~np.isnan(loss)
        accumulator[valid] += loss[valid]
        valid_count[valid] += 1
        n_days += 1

        if n_days % 30 == 0:
            log.info(f"  {year}: {n_days} days processed, {skipped} skipped.")

    if accumulator is None or valid_count.max() == 0:
        log.error(f"Year {year}: no valid data found.")
        return False

    log.info(f"Year {year}: {n_days} days processed, {skipped} skipped.")

    mean_loss = np.where(valid_count > 0, accumulator / valid_count, np.nan).astype(np.float32)
    log.info(f"Year {year}: loss range {np.nanmin(mean_loss):.4f}–{np.nanmax(mean_loss):.4f}, "
             f"mean {np.nanmean(mean_loss):.4f}.")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(out_path, "w", **ref_meta) as dst:
        dst.write(mean_loss, 1)
    log.info(f"Year {year}: saved to {out_path}.")
    return True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Process one year of WBGT → productivity loss raster.")
    parser.add_argument("--year",     type=int, required=True, help="Year to process.")
    parser.add_argument("--out-dir",  type=str, default="data/processed/annual",
                        help="Output directory for annual rasters.")
    parser.add_argument("--config",   type=str, default=None,
                        help="Path to config/config.yaml (to read URL template). "
                             "If omitted, uses the default CHIRTS-ERA5 URL.")
    parser.add_argument("--skip-existing", action="store_true", default=True,
                        help="Skip year if output raster already exists (default: True).")
    parser.add_argument("--force",    action="store_true",
                        help="Overwrite existing output raster.")
    return parser.parse_args()


def main():
    args = parse_args()

    # Resolve URL template
    url_template = ("https://data.chc.ucsb.edu/experimental/CHIRTS-ERA5/"
                    "wbgt/tifs/daily/{year}/WBGT.{year}.{month:02d}.{day:02d}.tif")
    if args.config:
        with open(args.config) as f:
            cfg = yaml.safe_load(f)
        url_template = cfg["wbgt"]["url_template"]

    out_dir  = Path(args.out_dir)
    out_path = out_dir / f"productivity_loss_{args.year}.tif"

    if out_path.exists() and not args.force:
        log.info(f"Output already exists, skipping: {out_path}")
        sys.exit(0)

    success = process_year(args.year, url_template, out_path)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
