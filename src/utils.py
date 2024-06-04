"""General utility functions."""

import getpass
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

import ee
import folium
import geopandas as gpd
import rasterio

from config import DYNAMIC_WORLD_DIR, TIFS_DIR


def enforce_epsg(gdf: gpd.GeoDataFrame, EPSG: int) -> gpd.GeoDataFrame:
    """Takes a gdf and enforces the EPSG of the class instance on it."""
    if gdf.crs != f"epsg:{EPSG}":
        gdf = gdf.to_crs(epsg=EPSG)
    return gdf


def tif_share_bounds(tif_path_1: Path, tif_path_2: Path) -> bool:
    """Check if two tif files share the same bounds.

    Returns:
    - bool: True if the tif files share the same bounds, False otherwise.
    """

    def compare_bounds(bounds1, bounds2, tolerance=1e-6):
        return all(abs(b1 - b2) < tolerance for b1, b2 in zip(bounds1, bounds2))

    with rasterio.open(tif_path_1) as dataset:
        bounds1 = dataset.bounds

    with rasterio.open(tif_path_2) as dataset:
        bounds2 = dataset.bounds

    return compare_bounds(bounds1, bounds2)


def authenticate_Google_Earth_Engine():
    """Authenticate the Earth Engine API"""
    try:
        ee.Authenticate()
        if getpass.getuser() == "viktorduepedersen":
            print("Initializing Earth Engine with Viktor's project")
            ee.Initialize(project="master-thesis-413813")
        else:
            print("Initializing Earth Engine with Aske's project")
            ee.Initialize(project="masterthesis-aske")
    except Exception as e:
        print(e)


def save_json(data, path, verbose=False) -> None:
    """Save data to a json file."""
    if verbose:
        print(f"Saving data to {path}")
    with open(path, "w") as outfile:
        json.dump(obj=data, fp=outfile)


def group_tifs() -> Dict[str, List[Path]]:
    chip_id_to_tifs: Dict[str, List[Path]] = defaultdict(list)

    for tif in TIFS_DIR.iterdir():
        chip_id = tif.name.split("_")[0]
        chip_id_to_tifs[chip_id].append(tif)

    # validate all tifs under each chip_id share the same bounds
    for chip_id, tifs in chip_id_to_tifs.items():
        for i, tif in enumerate(tifs[:-1]):
            assert tif_share_bounds(
                tif, tifs[i + 1]
            ), f"{chip_id}: {tif} and {tifs[i + 1]} do not share the same bounds"

    # sort tifs by date
    for chip_id, tifs in chip_id_to_tifs.items():
        chip_id_to_tifs[chip_id] = sorted(tifs, key=lambda tif: tif.name.split("_")[1])

    return chip_id_to_tifs


def save_folium_map(map: folium.Map, outname: str) -> None:
    """Save a Folium map as an .html file"""
    assert outname.endswith(".html"), "Output file must be an .html file"
    outpath = DYNAMIC_WORLD_DIR / outname
    print("Saving map to ", outpath)
    map.save(outpath)
