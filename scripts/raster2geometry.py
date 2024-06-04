"""
This file converts the raster files to geopandas dataframes.

Simply pass a path to the directory containing the raster files and the function will write the geopandas dataframe to the out_dir.

"""

from pathlib import Path
from typing import List

import geopandas as gpd
import rasterio
from rasterio.features import shapes
from shapely.geometry import shape
from tqdm import tqdm


def raster2geo(raster_dir: Path, out_dir: Path):
    assert raster_dir.exists(), f"{raster_dir} does not exist"
    out_dir.mkdir(exist_ok=True, parents=True)

    raster_files: List[Path] = list(raster_dir.glob("*.tif"))

    for raster_path in tqdm(raster_files, desc="Converting rasters to geodataframes"):
        with rasterio.open(raster_path) as src:
            image = src.read(1)  # Read the first band
            transform = src.transform

        # Step 3: Convert raster to vector
        mask = image != 0  # Assuming 0 is the value for no data
        vector_shapes = shapes(image, mask=mask, transform=transform)

        # Step 4: Create GeoDataFrame
        polygons = []
        values = []  # Store landcover values here
        for geom, value in vector_shapes:
            polygons.append(shape(geom))
            values.append(value)

        gdf = gpd.GeoDataFrame({"landcover": values, "geometry": polygons})
        gdf["landcover"] = gdf["landcover"].astype(int)
        # Optional: Set the CRS (Coordinate Reference System) to match the input raster
        gdf.crs = src.crs
        gdf = gdf.to_crs(epsg=25832)

        gdf.to_file(out_dir / f"{raster_path.stem}.shp")
    return


if __name__ == "__main__":
    from config import DENMARK_DW_DIR

    raster2geo(
        raster_dir=DENMARK_DW_DIR / "Raster", out_dir=DENMARK_DW_DIR / "Geometry"
    )
