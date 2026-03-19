"""
Standalone script: process one year of daily WBGT into a mean annual
productivity loss raster.

Designed to be called as a single array task on a cluster (one job per year).
Streams WBGT files directly via vsicurl — no raw files written to disk.

Supported datasets (set via --dataset):
  wbgt            CHIRTS-ERA5 WBGT (original)
  wbgt_baseline   CHC-CMIP6 observed WBGTmax baseline
  wbgt_future     CHC-CMIP6 future WBGTmax — requires --future-epoch and --scenario

Usage
-----
    # Historical
    python process_wbgt.py --year 2000 --dataset wbgt --config config/config.yaml

    # Baseline (wbgt_max)
    python process_wbgt.py --year 2000 --dataset wbgt_baseline --config config/config.yaml

    # Future
    python process_wbgt.py --year 2000 --dataset wbgt_future --future-epoch 2030 --scenario ssp245 --config config/config.yaml
"""

import argparse
import logging
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import requests
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


_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    )
}


def download_day(url: str, dest: Path, retries: int = 3, backoff: float = 5.0) -> Path | None:
    """Download one daily file with retries. Returns dest on success, None on failure."""
    import time
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=180, headers=_HEADERS)
            if r.status_code == 200:
                dest.write_bytes(r.content)
                return dest
            elif r.status_code == 404:
                return None  # file genuinely missing — no point retrying
        except Exception:
            pass
        if attempt < retries - 1:
            time.sleep(backoff * (attempt + 1))
    return None


def process_year(year: int, url_template: str, out_path: Path,
                 n_workers: int = 16) -> bool:
    """
    1. Download all daily WBGT files for `year` in parallel.
    2. Process each file from disk (apply ERF, accumulate).
    3. Delete each file immediately after reading.
    Returns True on success.
    """
    days = list(iter_dates(year))
    log.info(f"Year {year}: downloading {len(days)} days with {n_workers} workers.")

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        tasks = {
            d: (url_template.format(year=d.year, month=d.month, day=d.day),
                tmp_dir / f"WBGT.{d.year}.{d.month:02d}.{d.day:02d}.tif")
            for d in days
        }

        # --- Parallel download ---
        downloaded = {}
        with ThreadPoolExecutor(max_workers=n_workers) as ex:
            futures = {ex.submit(download_day, url, path): d
                       for d, (url, path) in tasks.items()}
            for i, fut in enumerate(as_completed(futures), 1):
                d = futures[fut]
                path = fut.result()
                if path:
                    downloaded[d] = path
                if i % 30 == 0:
                    log.info(f"  {year}: {i}/{len(days)} downloads complete.")

        n_failed = len(days) - len(downloaded)
        if n_failed:
            log.warning(f"  {year}: {n_failed} days unavailable (not on server).")

        if not downloaded:
            log.error(f"Year {year}: no data downloaded.")
            return False

        # --- Process from disk, delete immediately after reading ---
        accumulator = None
        valid_count  = None
        ref_meta     = None
        nodata_val   = None
        n_days       = 0
        skipped      = 0

        for d in sorted(downloaded):
            fpath = downloaded[d]
            try:
                with rasterio.open(fpath) as src:
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
                if skipped <= 5:
                    log.warning(f"  Could not read {fpath.name}: {e}")
                continue
            finally:
                fpath.unlink(missing_ok=True)

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

VALID_DATASETS = ("wbgt", "wbgt_baseline", "wbgt_future")
VALID_EPOCHS   = ("2030", "2050")
VALID_SCENARIOS = ("ssp245", "ssp585")


def parse_args():
    parser = argparse.ArgumentParser(description="Process one year of WBGT → productivity loss raster.")
    parser.add_argument("--year",         type=int, required=True,
                        help="Year to process.")
    parser.add_argument("--dataset",      type=str, default="wbgt",
                        choices=VALID_DATASETS,
                        help="Which WBGT dataset to use (default: wbgt).")
    parser.add_argument("--future-epoch", type=str, default=None,
                        choices=VALID_EPOCHS,
                        help="Future epoch [2030|2050] — required when --dataset wbgt_future.")
    parser.add_argument("--scenario",     type=str, default=None,
                        choices=VALID_SCENARIOS,
                        help="SSP scenario [ssp245|ssp585] — required when --dataset wbgt_future.")
    parser.add_argument("--out-dir",      type=str, default=None,
                        help="Output directory. Defaults to data/processed/annual/<dataset_label>/")
    parser.add_argument("--config",       type=str, default=None,
                        help="Path to config/config.yaml.")
    parser.add_argument("--force",        action="store_true",
                        help="Overwrite existing output raster.")
    parser.add_argument("--workers",      type=int, default=16,
                        help="Parallel download workers (default: 16).")
    return parser.parse_args()


def resolve_url_template(cfg: dict, dataset: str, future_epoch: str | None, scenario: str | None) -> str:
    """
    Return the URL template for daily files with {year}, {month}, {day} placeholders.
    For wbgt_future, {epoch} and {scenario} are pre-substituted here.
    """
    if dataset == "wbgt_future":
        if future_epoch is None or scenario is None:
            raise ValueError("--future-epoch and --scenario are required for dataset wbgt_future.")
        raw = cfg["wbgt_future"]["url_template"]
        return raw.format(epoch=future_epoch, scenario=scenario,
                          year="{year}", month="{month:02d}", day="{day:02d}")
    return cfg[dataset]["url_template"]


def dataset_label(dataset: str, future_epoch: str | None, scenario: str | None) -> str:
    """Human-readable label used for output subdirectory and filenames."""
    if dataset == "wbgt_future":
        return f"wbgt_future_{future_epoch}_{scenario}"
    return dataset


def main():
    args = parse_args()

    # Load config
    cfg = {}
    if args.config:
        with open(args.config) as f:
            cfg = yaml.safe_load(f)
    else:
        # Minimal fallback defaults (no config file needed for wbgt)
        cfg = {
            "wbgt": {"url_template": (
                "https://data.chc.ucsb.edu/experimental/CHIRTS-ERA5/"
                "wbgt/tifs/daily/{year}/WBGT.{year}.{month:02d}.{day:02d}.tif"
            )},
        }

    label        = dataset_label(args.dataset, args.future_epoch, args.scenario)
    url_template = resolve_url_template(cfg, args.dataset, args.future_epoch, args.scenario)

    # Output directory: explicit arg, or auto-derived from dataset label
    if args.out_dir:
        out_dir = Path(args.out_dir)
    else:
        out_dir = Path("data/processed/annual") / label

    out_path = out_dir / f"productivity_loss_{args.year}.tif"

    log.info(f"Dataset: {label}  |  Year: {args.year}  |  Output: {out_path}")

    if out_path.exists() and not args.force:
        log.info(f"Output already exists, skipping.")
        sys.exit(0)

    success = process_year(args.year, url_template, out_path, n_workers=args.workers)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
