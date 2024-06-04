from sys import path

path.append("..")
from src.data_handlers import (
    read_wind_turbines,
    radial_polygon_from_point,
    process_wind_turbines,
)
from src.DataBaseManager import DBMS
from sqlalchemy import create_engine
import geopandas as gpd
import pandas as pd
from tqdm import tqdm
from geopy.distance import geodesic
from config import EPSG_MAPPING
from src.utils import enforce_epsg
import folium
import seaborn as sns
import matplotlib.pyplot as plt


def load_ENS_wind():
    wind_turbines_raw = read_wind_turbines()

    wind_turbines = process_wind_turbines(wind_turbines_raw)

    wind_turbines.set_geometry("geometry", inplace=True)

    wind_turbines["centroid_ENS"] = wind_turbines.geometry.centroid

    wind_turbines.drop_duplicates(subset=["Møllenummer (GSRN)"], inplace=True)

    return wind_turbines


def load_SATLAS_wind():
    DB = DBMS()

    Q = """
        SELECT  object_id, geometries
        FROM lulc
        WHERE area = 'Denmark' 
        AND data_origins = 'SATLAS' 
        AND name = 'Wind Turbine'
        """

    DB.server.start()
    local_port = str(DB.server.local_bind_port)

    engine = create_engine(
        "postgresql://{}:{}@{}:{}/{}".format(
            DB.username, DB.password, "127.0.0.1", local_port, DB.db_name
        )
    )

    SATLAS_turbines = gpd.GeoDataFrame.from_postgis(
        Q, engine, geom_col="geometries"
    ).drop_duplicates(subset=["object_id"])

    DB.server.stop()

    SATLAS_turbines = SATLAS_turbines[["object_id", "geometries"]]
    SATLAS_turbines.set_geometry("geometries", inplace=True)
    SATLAS_turbines["centroid_SATLAS"] = SATLAS_turbines.geometries.centroid
    SATLAS_turbines.drop_duplicates(subset=["object_id"], inplace=True)

    return SATLAS_turbines


def dist_join(gdf1, gdf2):
    # gdf1 and gdf2 are your GeoDataFrames with a 'centroid' column
    # Ensure that 'centroid' is set as the active geometry
    # set crs tp EPSG:{EPSG_MAPPING["Denmark"]
    # first set CRS to EPSG:4326

    gdf1.set_geometry("centroid_ENS", inplace=True)
    gdf2.set_geometry("centroid_SATLAS", inplace=True)

    gdf1 = gdf1.set_crs(epsg=4326)
    gdf2 = gdf2.set_crs(epsg=4326)

    gdf1 = enforce_epsg(gdf1, EPSG_MAPPING["Denmark"])
    gdf2 = enforce_epsg(gdf2, EPSG_MAPPING["Denmark"])

    # Perform the nearest neighbor join
    result = gpd.sjoin_nearest(gdf1, gdf2, how="left", distance_col="distance")

    return result.to_crs(epsg=4326)


def main():
    wind_turbines = load_ENS_wind()
    SATLAS_turbines = load_SATLAS_wind()

    merged = dist_join(wind_turbines, SATLAS_turbines)

    return merged, wind_turbines


def plot_row(merged, threshold=0.5, IX=0):
    close_turbines = merged.loc[merged["distance"] < threshold]

    merged["centroid_SATLAS"] = merged.geometries.centroid

    merged["distance"] = merged.apply(
        lambda row: geodesic(
            (row["centroid_ENS"].y, row["centroid_ENS"].x),
            (row["centroid_SATLAS"].y, row["centroid_SATLAS"].x),
        ).kilometers,
        axis=1,
    )

    close_turbines.sort_values("distance", inplace=True)
    close_turbines.reset_index(drop=True, inplace=True)

    close_turbines = close_turbines.set_geometry("centroid_ENS")
    m = folium.Map(
        location=[
            merged.centroid_ENS.y.mean(),
            merged.centroid_ENS.x.mean(),
        ],
        zoom_start=12,
    )

    row = close_turbines.iloc[IX]
    folium.Marker(
        location=[row["centroid_ENS"].y, row["centroid_ENS"].x],
        popup=f"ENSKILDE: {row['Møllenummer (GSRN)']}",
    ).add_to(m)

    close_turbines.set_geometry("centroid_SATLAS", inplace=True)
    row = close_turbines.iloc[IX]

    folium.Marker(
        location=[row["centroid_SATLAS"].y, row["centroid_SATLAS"].x],
        popup=f"SATLAS: {row['object_id']}",
    ).add_to(m)

    return m


# make a function that caluclates the area of a circle given the diameter
from math import pi


def circle_area(diameter):
    radius = diameter / 2
    return pi * radius**2


def plot_threshold(merged, wind_turbines, threshold=0.5):
    plt.figure(figsize=(16, 8))  # Adjusted for side-by-side plots
    plt.style.use("fivethirtyeight")

    merged["calc_area"] = merged["Rotor-diameter (m)"].apply(
        lambda x: circle_area(x) / 10**6
    )
    wind_turbines["calc_area"] = wind_turbines["Rotor-diameter (m)"].apply(
        lambda x: circle_area(x) / 10**6
    )

    merged["distance"] = merged["distance"] / 10**3

    # Plot 1
    plt.subplot(1, 2, 1)  # 1 row, 2 columns, 1st subplot
    sns.histplot(
        merged["calc_area"] / (wind_turbines["calc_area"].sum()),
        bins=100,
        cumulative=True,
        stat="density",
    )
    plt.xlabel("Area")
    plt.ylabel("Percent of Area km^2")
    threshold_area_percentage = merged.loc[merged["distance"] < threshold][
        "calc_area"
    ].sum() / (wind_turbines["calc_area"].sum())
    plt.axhline(
        y=threshold_area_percentage,
        color="red",
        linestyle="--",
        label=f"Threshold: {threshold}",
        linewidth=2,
    )
    plt.title("Cumulative Distribution of Calculated Areas")
    plt.legend()

    # Plot 2
    plt.subplot(1, 2, 2)  # 1 row, 2 columns, 2nd subplot
    sns.histplot(merged["distance"], bins=100, cumulative=True, stat="density")
    plt.xlabel("Distance (km)")
    plt.ylabel("Percentage of Rows")
    threshold_percentage = (
        merged.loc[merged["distance"] < threshold].shape[0] / merged.shape[0]
    )
    plt.axhline(
        y=threshold_percentage,
        color="red",
        linestyle="--",
        label=f"Threshold: {threshold} km",
        linewidth=2,
    )
    plt.title("Cumulative Distribution of Distances")
    plt.legend()

    plt.tight_layout()  # Adjust layout to not overlap
    plt.show()
