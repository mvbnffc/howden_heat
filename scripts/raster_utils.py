"""
Raster utilities: clipping, resampling, and alignment.

All rasters are aligned to a reference grid (typically the population raster)
before inequality analysis.
"""

import numpy as np
import rasterio
import rasterio.mask
from rasterio.warp import reproject, Resampling
from rasterio.transform import from_bounds
import geopandas as gpd
from shapely.geometry import mapping
import tempfile
import os


def get_country_geometry(boundaries_path: str, iso3: str):
    """
    Return the geometry of a country from a boundaries shapefile.

    Parameters
    ----------
    boundaries_path : str
        Path to Natural Earth or similar country boundaries shapefile.
    iso3 : str
        ISO 3166-1 alpha-3 country code.

    Returns
    -------
    shapely geometry or None
    """
    gdf = gpd.read_file(boundaries_path)

    # Try common ISO column names
    for col in ["ADM0_A3", "ISO_A3", "iso_a3", "GID_0", "ISO3", "shapeGroup"]:
        if col in gdf.columns:
            match = gdf[gdf[col] == iso3]
            if not match.empty:
                return match.geometry.unary_union

    raise ValueError(f"Could not find ISO3 '{iso3}' in {boundaries_path}. "
                     f"Available columns: {list(gdf.columns)}")


def clip_raster_to_geometry(raster_path: str, geometry) -> tuple[np.ndarray, dict]:
    """
    Clip a raster to a geometry and return the array and metadata.

    Parameters
    ----------
    raster_path : str
        Path to input GeoTIFF.
    geometry : shapely geometry
        Clipping geometry.

    Returns
    -------
    array : np.ndarray, shape (height, width)
    meta : dict  rasterio metadata for the clipped raster
    """
    geom = [mapping(geometry)]
    with rasterio.open(raster_path) as src:
        out_image, out_transform = rasterio.mask.mask(src, geom, crop=True, nodata=np.nan)
        out_meta = src.meta.copy()
        out_meta.update({
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform,
            "nodata": np.nan,
        })
        array = out_image[0].astype(np.float32)
        nodata = src.nodata
        if nodata is not None:
            array[array == nodata] = np.nan

    return array, out_meta


def resample_to_reference(
    source_array: np.ndarray,
    source_meta: dict,
    ref_meta: dict,
    resampling: Resampling = Resampling.bilinear,
) -> np.ndarray:
    """
    Resample source_array onto the grid defined by ref_meta.

    Parameters
    ----------
    source_array : np.ndarray, shape (H, W)
    source_meta : dict  rasterio metadata of the source
    ref_meta : dict     rasterio metadata of the reference grid
    resampling : Resampling  rasterio resampling method

    Returns
    -------
    np.ndarray, shape (ref_meta['height'], ref_meta['width'])
    """
    dest = np.full(
        (ref_meta["height"], ref_meta["width"]),
        fill_value=np.nan,
        dtype=np.float32,
    )

    reproject(
        source=source_array,
        destination=dest,
        src_transform=source_meta["transform"],
        src_crs=source_meta["crs"],
        dst_transform=ref_meta["transform"],
        dst_crs=ref_meta["crs"],
        resampling=resampling,
        src_nodata=np.nan,
        dst_nodata=np.nan,
    )

    return dest


def align_rasters(
    pop_path: str,
    rwi_path: str,
    risk_path: str,
    geometry,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Clip and align population, RWI, and risk rasters to a common grid.

    The population raster defines the reference grid. RWI and risk are
    resampled to match it using nearest-neighbour (wealth) and bilinear
    (risk) interpolation respectively.

    Parameters
    ----------
    pop_path : str   Path to population GeoTIFF.
    rwi_path : str   Path to RWI GeoTIFF.
    risk_path : str  Path to productivity loss GeoTIFF.
    geometry :       Shapely geometry for the country.

    Returns
    -------
    pop_array, rwi_array, risk_array : np.ndarray  (all same shape)
    """
    pop_array, pop_meta = clip_raster_to_geometry(pop_path, geometry)
    rwi_raw, rwi_meta = clip_raster_to_geometry(rwi_path, geometry)
    risk_raw, risk_meta = clip_raster_to_geometry(risk_path, geometry)

    # RWI: nearest neighbour (categorical wealth scores)
    rwi_array = resample_to_reference(rwi_raw, rwi_meta, pop_meta,
                                      resampling=Resampling.nearest)
    # Risk: bilinear (continuous values)
    risk_array = resample_to_reference(risk_raw, risk_meta, pop_meta,
                                       resampling=Resampling.bilinear)

    # Mask RWI nodata (-999 in Meta RWI dataset)
    rwi_array[rwi_array == -999] = np.nan

    return pop_array, rwi_array, risk_array
