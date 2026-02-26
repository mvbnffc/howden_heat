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

Climate risk research project analysing heatwave risk using NASA NEX-GDDP-CMIP6 climate model outputs combined with population and wealth data. The goal is to quantify heatwave exposure and vulnerability at a global scale.

## Data

All data lives in `data/` (not tracked in git due to size):

| File | Description |
|------|-------------|
| `nex-gddp-cmip6.return_levels.zarr` | CMIP6 return levels (~87GB Zarr store) |
| `global_pop_2025_CN_1km_R2025A_UA_v1.tif` | Global population raster (1km, 2025) |
| `rwi.tif` | Relative Wealth Index raster |

### Zarr dataset structure

```
Dimensions: model(35) × scenario(5) × return_period(6) × epoch(4) × lat(600) × lon(1440)
Variables:  tasmax_return_level, spei3_return_level, spei12_return_level
Scenarios:  historical, ssp126, ssp245, ssp370, ssp585
Epochs:     1984-2014, (future periods)
Return periods: 20, 50, 100, 200, 500, 1500 years
```

Because the full dataset is ~87GB, always slice spatially or by model/scenario before loading into memory. Use `.sel()` and `.isel()` with lazy loading; avoid `.load()` or `.compute()` on full arrays.

## Code patterns

- Notebooks use `Path.cwd().parent` to find the project root (notebooks live one level below root).
- Data paths are constructed with `os.path.join(root, 'data', filename)`.
- Raster data (population, RWI) is handled with `rasterio`; gridded climate data with `xarray`/`zarr`.
