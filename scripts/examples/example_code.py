import io
import zipfile
from pathlib import Path
from typing import List

import ee
import geopandas as gpd
import pandas as pd
import requests

from config import ENERGI_STYRELSEN_DIR, EPSG_MAPPING
from src.data.utils import get_cordinates_from_address
from src.dynamic_world import DynamicWorldBasemap


def load_SAtlas_Solar_Classification_Data():
    solar_path = Path("assets/satlas-solar-data/2024-01_solar.shp")

    if not solar_path.exists():
        print("Solar data not found, downloading it now...")
        # download the solar data
        solar_download_ref = "https://pub-956f3eb0f5974f37b9228e0a62f449bf.r2.dev/outputs/renewable/latest_solar.shp.zip"

        response = requests.get(solar_download_ref)
        response.raise_for_status()

        # unzip the response
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall("assets/satlas-solar-data")

    solar_gdf = gpd.read_file(solar_path)

    DWB = DynamicWorldBasemap(
        area_name="Denmark", date_ranges=[("2024-01-01", "2024-12-31")]
    )

    DK_shape: List[ee.geometry.Geometry] = DWB.create_polygon(
        DWB.get_country_LSIB_coordinates("Denmark")[0], flip=False
    )

    def ee_geometry_to_geojson(ee_geometry_list):
        geojsons = []
        for geom in ee_geometry_list:
            geojson = ee.Geometry(geom).getInfo()
            geojsons.append(geojson)
        return geojsons

    def create_geodataframe(geojson_list):
        feature_collection = {"type": "FeatureCollection", "features": []}

        for geom in geojson_list:
            feature = {"type": "Feature", "properties": {}, "geometry": geom}
            feature_collection["features"].append(feature)

        # Convert the GeoJSON FeatureCollection to a GeoDataFrame
        gdf = gpd.GeoDataFrame.from_features(feature_collection["features"])

        return gdf

    # Example usage
    geojson_list = ee_geometry_to_geojson(DK_shape)  # Convert ee.Geometry to GeoJSON
    DK_shape_gdf = create_geodataframe(geojson_list)  # Create GeoDataFrame

    # Only keep the solar panels that are within the Denmark shape
    solar_gdf_joined = gpd.sjoin(solar_gdf, DK_shape_gdf, how="inner", op="intersects")

    return DK_shape_gdf, solar_gdf_joined


def load_Energistyrelsen_data():
    outpath = ENERGI_STYRELSEN_DIR / "Energistyrelsen-jord.xlsx"

    if not outpath.exists():
        print("Energistyrelsen data not found, creating it now...")
        energistyrelsen = pd.read_excel(
            ENERGI_STYRELSEN_DIR / "Energistyrelsen-solceller.xlsx"
        )

        # filter
        energistyrelsen = energistyrelsen[
            energistyrelsen["Etableringstype (kun solceller)"] == "Jord"
        ]

        def create_full_address(row):
            try:
                return f"{row['StreetName']} {row['HouseNumber']} {str(int(row['PostalCode']))} {row['City']}"
            except ValueError:
                return None

        energistyrelsen["full_address"] = energistyrelsen.apply(
            create_full_address, axis=1
        )

        for address in energistyrelsen[
            "full_address"
        ].unique():  # Optimize by iterating over unique addresses
            try:
                latt, long = get_cordinates_from_address(address)
            except:
                latt, long = None, None

            energistyrelsen.loc[
                energistyrelsen["full_address"] == address, ["latitude", "longitude"]
            ] = latt, long

        # Convert to GeoDataFrame before saving to ensure latitude and longitude are correctly set
        energistyrelsen_gdf = gpd.GeoDataFrame(
            energistyrelsen,
            geometry=gpd.points_from_xy(
                energistyrelsen.latitude, energistyrelsen.longitude
            ),
            crs=EPSG_MAPPING["Denmark"],
        )

        energistyrelsen_gdf.to_excel(outpath, index=False)
    else:
        energistyrelsen = pd.read_excel(outpath)
        # Convert to GeoDataFrame when loading
        energistyrelsen_gdf = gpd.GeoDataFrame(
            energistyrelsen,
            geometry=gpd.points_from_xy(
                energistyrelsen.longitude, energistyrelsen.latitude
            ),
            crs=EPSG_MAPPING["Denmark"],
        )

    return energistyrelsen_gdf
