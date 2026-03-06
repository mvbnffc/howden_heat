# Howden Heat — Heatwave Risk & Inequality Analysis

Analysis of population exposure to heatwave-driven labour productivity loss and the inequality of those impacts across 92 low- and middle-income countries (LMICs).

## Overview

Using daily WBGT data (CHIRTS-ERA5, 5 km), global population (1 km), and the Meta Relative Wealth Index (2.4 km), this project quantifies:
- Mean annual labour productivity loss from heat stress
- The distributional inequality of that loss (concentration index, concentration curves)

Analysis is at the national scale across 92 LMICs.

## Setup

```bash
conda env create -f environment.yml
conda activate howden_heat
jupyter lab
```

## Data

All data lives in `data/` (not tracked in git).

| Dataset | Source | Path |
|---|---|---|
| WBGT daily GeoTIFFs | [CHIRTS-ERA5](https://data.chc.ucsb.edu/experimental/CHIRTS-ERA5/wbgt/tifs/daily/) | `data/wbgt/` |
| Population (1 km, 2025) | GHS-POP / WorldPop | `data/population/` |
| Relative Wealth Index | [Meta / Chi et al.](https://dataforgood.facebook.com/dfg/tools/relative-wealth-index) | `data/rwi/` |
| Country boundaries | GeoBoundaries | `data/boundaries/` |

Country boundaries can be downloaded from [geoBoundaries](https://www.geoboundaries.org/).

## Workflow

| Notebook | Description |
|---|---|
| `01_data_exploration` | Sanity-check and visualise all input data |
| `02_wbgt_processing` | Daily WBGT → mean annual productivity loss raster (configurable epoch) |
| `03_inequality_analysis` | Per-country CI, quantile ratio, and concentration curve data |
| `04_figures` | Publication-quality maps and plots |

## Project Structure

```
howden_heat/
├── config/config.yaml     # 92 ISO codes, data paths, WBGT URL template, epochs
├── scripts/
│   ├── productivity.py    # WBGT → productivity loss ERF
│   ├── inequality.py      # CI, concentration curve, quantile ratio
│   └── raster_utils.py    # Raster alignment and clipping
├── notebooks/             # Analysis notebooks (run in order)
├── data/                  # Input data (not tracked)
└── results/               # Output CSVs and figures (not tracked)
```
