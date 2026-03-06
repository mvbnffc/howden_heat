# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment

Conda environment name: `howden_heat`

```bash
conda env create -f environment.yml
conda activate howden_heat
jupyter lab
```

To add packages, update `environment.yml` and run `conda env update -f environment.yml --prune`.

## Project Purpose

Climate risk research project analysing heatwave-driven labour productivity loss and wealth inequality of that exposure across 92 LMICs. Core inputs: daily WBGT GeoTIFFs (CHIRTS-ERA5, 5 km), population raster (1 km), Meta Relative Wealth Index (2.4 km).

## Data

All data lives in `data/` (not tracked in git).

| File/Dir | Description |
|---|---|
| `data/wbgt/` | Daily WBGT GeoTIFFs — `WBGT.{YYYY}.{MM}.{DD}.tif` |
| `data/population/` | Global population raster (1 km) |
| `data/rwi/` | RWI global GeoTIFF + per-country CSVs |
| `data/boundaries/` | Natural Earth 1:10m country boundaries |
| `data/processed/` | Intermediate outputs (productivity loss rasters) |

WBGT download URL template: `https://data.chc.ucsb.edu/experimental/CHIRTS-ERA5/wbgt/tifs/daily/{year}/WBGT.{year}.{month:02d}.{day:02d}.tif`

## Code Patterns

- Notebooks live in `notebooks/` and use `Path.cwd().parent` to find the project root.
- Reusable functions live in `scripts/` and are imported by notebooks.
- Config (ISO codes, data paths, epochs) is in `config/config.yaml`.
- Raster alignment always uses the population raster as the reference grid.
- RWI nodata is `-999` — always mask before analysis.

## Workflow Order

1. `01_data_exploration` — validate inputs
2. `02_wbgt_processing` — daily WBGT → mean annual productivity loss raster
3. `03_inequality_analysis` — CI + concentration curves per country
4. `04_figures` — final figures
