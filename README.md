# Howden Heatwaves

Research project analysing heatwave risk, using climate model outputs (NEX-GDDP CMIP6) and population/vulnerability data.

## Data

Data files are **not tracked in git** due to size. The `data/` directory contains:

- `nex-gddp-cmip6.return_levels.zarr` — CMIP6 return levels (tasmax, SPEI3, SPEI12)
- `global_pop_2025_CN_1km_R2025A_UA_v1.tif` — Global population raster (1km, 2025)
- `rwi.tif` — Relative Wealth Index raster

## Setup

```bash
conda env create -f environment.yml
conda activate howden_heatwaves
jupyter lab
```

## Structure

```
├── data/           # Local data files (not tracked in git)
├── notebooks/      # Jupyter notebooks
└── environment.yml # Conda environment
```
