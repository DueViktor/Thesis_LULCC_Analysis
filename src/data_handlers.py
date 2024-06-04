import math
import os
import warnings
from collections import defaultdict
from typing import Tuple

import geopandas as gpd
import pandas as pd
import rasterio
from pyproj import Proj, transform
from rasterio.features import shapes
from rasterio.io import MemoryFile
from shapely.geometry import Point, Polygon, shape
from tqdm import tqdm

from config import DATA_DIR

warnings.filterwarnings("ignore", category=FutureWarning)


def utm_to_latlon(easting, northing) -> Tuple[int, int]:
    proj_utm = Proj(proj="utm", zone=32, ellps="WGS84", south=False)
    proj_latlon = Proj(proj="latlong", datum="WGS84")
    lon, lat = transform(proj_utm, proj_latlon, easting, northing)
    return (lat, lon)


def read_wind_turbines(filename="anlaeg.xlsx", subset=None):
    # Read raw data
    moeller = pd.read_excel(DATA_DIR / filename, header=13, usecols="A:O")

    # Convert date values to datetime
    # moeller["Dato for oprindelig nettilslutning"] = pd.to_datetime(
    #    moeller["Dato for oprindelig nettilslutning"]
    # )
    # moeller["Dato for afmeldning"] = pd.to_datetime(moeller["Dato for afmeldning"])
    # moeller["Dato for afmeldning"] = pd.to_datetime(moeller["Dato for afmeldning"])

    # Caluclate Acitve Field
    moeller["ACTIVE"] = False

    moeller.loc[moeller["Dato for afmeldning"].isna()]["ACTIVE"] = True

    # Fix Data Error
    moeller.loc[moeller["Type af placering"] == "Land"]["Type af placering"] = "LAND"
    # Calculate latitude and longitude values from coordinates
    lats, lons = [], []
    if subset:
        moeller = moeller.sort_values(
            by="Dato for oprindelig nettilslutning", ascending=False
        ).head(subset)

    for _, row in tqdm(moeller.iterrows()):
        easting, northing = (
            row["X (øst) koordinat \nUTM 32 Euref89"],
            row["Y (nord) koordinat \nUTM 32 Euref89"],
        )

        if easting == None or northing == None:
            lat = None
            lon = None
        elif isinstance(northing, pd._libs.tslibs.nattype.NaTType) or isinstance(
            easting, pd._libs.tslibs.nattype.NaTType
        ):
            lat = None
            lon = None
        elif easting == "LAND" or northing == "LAND":
            lat = None
            lon = None
        else:
            try:
                lat, lon = utm_to_latlon(easting, northing)
            except:
                print(easting, northing)
                break

        if str(lat) == "nan" or str(lon) == "nan":
            lat = None
            lon = None

        lats.append(lat)
        lons.append(lon)

    moeller["lat"] = lats
    moeller["lon"] = lons

    # return moeller

    # Create geopandas geometry
    moeller.dropna(subset=["lon", "lat"], inplace=True)
    moeller["geometry"] = moeller.apply(
        lambda row: Point(row["lon"], row["lat"]), axis=1
    )
    # set crs of gdf moeller to 4326
    moeller = gpd.GeoDataFrame(moeller, geometry="geometry")
    moeller.crs = "EPSG:4326"
    return moeller


def radial_polygon_from_point(lat, lon, radius):
    # Define constants
    num_points = 64  # Number of points to define the polygon
    earth_radius = 6371000  # Earth's radius in meters

    # Calculate the offsets in radians
    d = radius / earth_radius  # Angular distance in radians
    lat_rad = math.radians(lon)  # Convert latitude to radians
    lon_rad = math.radians(lat)  # Convert longitude to radians

    # Create points
    polygon_points = []
    for i in range(num_points):
        angle = math.radians(float(i) / num_points * 360)
        lat_point = math.asin(
            math.sin(lat_rad) * math.cos(d)
            + math.cos(lat_rad) * math.sin(d) * math.cos(angle)
        )
        lon_point = lon_rad + math.atan2(
            math.sin(angle) * math.sin(d) * math.cos(lat_rad),
            math.cos(d) - math.sin(lat_rad) * math.sin(lat_point),
        )
        lat_point_deg = math.degrees(lat_point)
        lon_point_deg = math.degrees(lon_point)
        polygon_points.append((lon_point_deg, lat_point_deg))

    # Create and return a Shapely Polygon
    return Polygon(polygon_points)


def format_windturbines(wind_turbines):
    # Convert to GeoDataFrame
    wind_turbines = gpd.GeoDataFrame(
        wind_turbines, geometry=gpd.points_from_xy(wind_turbines.lon, wind_turbines.lat)
    )
    wind_turbines.crs = "EPSG:4326"

    return wind_turbines


def add_wind_turbines():
    # read wind turbines
    wind_turbines = read_wind_turbines()

    wind_turbines["Dato for oprindelig nettilslutning"] = wind_turbines[
        "Dato for oprindelig nettilslutning"
    ].dt.strftime("%Y-%m-%d")
    wind_turbines["Dato for afmeldning"] = wind_turbines[
        "Dato for afmeldning"
    ].dt.strftime("%Y-%m-%d")

    wind_turbines["geometry"] = wind_turbines.apply(
        lambda x: radial_polygon_from_point(
            x["lon"], x["lat"], x["Rotor-diameter (m)"] / 2
        ),
        axis=1,
    )
    wind_turbines["name"] = "Wind Turbine"
    wind_turbines["data_origins"] = "Energistyrelsen"
    wind_turbines["area"] = "Denmark"

    all_rows = []

    wind_turbines["Dato for afmeldning"].fillna("NaN", inplace=True)
    for _, row in tqdm(wind_turbines.iterrows()):
        start_year = row["Dato for oprindelig nettilslutning"][:4]
        if int(start_year) < 2016:
            start_year = "2016"
        if row["Dato for afmeldning"] != "NaN":
            end_year = row["Dato for afmeldning"][:4]
        else:
            end_year = "2024"
        years_active = [str(i) for i in range(int(start_year), int(end_year) + 1)]

        for year in years_active:
            row["year"] = year
            cols = [
                "Møllenummer (GSRN)",
                "Kapacitet (kW)",
                "Rotor-diameter (m)",
                "geometry",
                "name",
                "year",
                "area",
                "data_origins",
            ]
            all_rows.append(row[cols])

    wind_turbines = gpd.GeoDataFrame(all_rows, geometry="geometry")

    gdf_dissolved = wind_turbines.dissolve()
    geometry_wkt = gdf_dissolved.geometry.geometry.iloc[
        0
    ].wkt  # Example for the first geometry

    return wind_turbines, geometry_wkt


def prepare_polygons_for_DB(wind_turbines, intersecting_chips,object_id_col="Møllenummer (GSRN)"):
    windmill_chips = gpd.overlay(wind_turbines, intersecting_chips, how="intersection")

    #print("Inner Inner 1:", windmill_chips.columns)
    turbine_ids = windmill_chips[object_id_col].unique()

    turbine_chips = []

    for id in turbine_ids:
        chipids = windmill_chips[windmill_chips[object_id_col] == id][
            "chipid"
        ].unique()
        for chipid in chipids:
            dissolved_windmill_chip = windmill_chips[
                (windmill_chips["chipid"] == chipid)
                & (windmill_chips[object_id_col] == id)
            ].dissolve()
            turbine_chips.append(dissolved_windmill_chip)

    try:
        wind_turbine_chips = gpd.GeoDataFrame(pd.concat(turbine_chips), geometry="geometry")
    except:
        # Create empty GeoDataFrame with columns "data_origins","name_1","year","chipid","object_id_col","area","geometry"
        wind_turbine_chips = gpd.GeoDataFrame(
            columns=[
                "data_origins",
                "name",
                "year",
                "chipid",
                "object_id",
                "area",
                "geometry",
            ]
        )
        return wind_turbine_chips
    

    joined = wind_turbines[
        ["data_origins", object_id_col, "year", "area"]
    ].merge(wind_turbine_chips, on=object_id_col, how="inner")

    db_ready_frame = joined[
        [
            "data_origins",
            "name_1",
            "year",
            "chipid",
            object_id_col,
            "area",
            "geometry",
        ]
    ]

    db_ready_frame.rename(
        columns={"name_1": "name", object_id_col: "object_id"}, inplace=True
    )
    print(f"ADDING {db_ready_frame.shape[0]} ROWS")
    return db_ready_frame


def get_area_of_raster_tif(src, image):
    # Determine the no-data value from the metadata (if it exists)
    no_data_value = src.nodata

    # Count the number of data pixels
    # If there's no no-data value specified, count all non-zero pixels
    if no_data_value is not None:
        data_pixels = (image != no_data_value).sum()
    else:
        data_pixels = (image != 0).sum()

    # Calculate the total area (10m x 10m per pixel)
    total_area = data_pixels * 10 * 10  # in square meters

    return total_area


def binary2tif(tif_bytes):
    # Use MemoryFile to open the file from bytes
    with MemoryFile(tif_bytes) as memfile:
        with memfile.open() as src:  # `dataset` here is equivalent to `src` when using rasterio.open()
            # Accessing the transform directly
            transform = src.transform

            # For example, to read the first band
            band1 = src.read(1)

    return band1, transform, src


def rasterfile2geo(filecontents):
    image, transform, src = binary2tif(filecontents)

    raster_area = get_area_of_raster_tif(src, image)

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
    gdf = gdf.to_crs(epsg=4326)

    return gdf


def raster_dict2geo(file_contents, area="Denmark", test=False):
    frames = defaultdict(dict)

    for filename, file_contents in tqdm(
        file_contents.items(), desc="Converting rasterfiles to polygons..."
    ):
        chip_id = "_".join(filename.split("_")[0:3])
        year = filename.split("_")[3]
        frames[chip_id][year] = dict()
        frames[chip_id][year]["GeoFrame"] = rasterfile2geo(file_contents)

        frames[chip_id][year]["GeoFrame"]["year"] = year
        frames[chip_id][year]["GeoFrame"]["area"] = area
        frames[chip_id][year]["GeoFrame"]["chipid"] = chip_id
        frames[chip_id][year]["GeoFrame"]["data_origins"] = "DynamicWorld"

    concatted_frames = pd.concat(
        [
            value_inner["GeoFrame"]
            for value_outer in frames.values()
            for value_inner in value_outer.values()
        ]
    )

    return concatted_frames


def read_all_tifs2geo(path, year="", area="Denmark", test=False):
    frames = defaultdict(dict)
    files = os.listdir(path)

    if test:
        limit = 50
    else:
        limit = len(files)

    for filename in tqdm(files[:limit]):
        if year not in filename:
            continue

        chip_id = int(filename.split("_")[0])
        year = filename.split("_")[1]
        frames[chip_id][year] = dict()
        frames[chip_id][year]["GeoFrame"] = raster2geo(path, filename)

        frames[chip_id][year]["GeoFrame"]["year"] = year
        frames[chip_id][year]["GeoFrame"]["area"] = area
        frames[chip_id][year]["GeoFrame"]["chipid"] = chip_id
        frames[chip_id][year]["GeoFrame"]["data_origins"] = "DynamicWorld"

    return frames


def process_wind_turbines(wind_turbines):
    wind_turbines['Dato for oprindelig nettilslutning'] = wind_turbines['Dato for oprindelig nettilslutning'].dt.strftime('%Y-%m-%d')
    wind_turbines['Dato for afmeldning'] = wind_turbines['Dato for afmeldning'].dt.strftime('%Y-%m-%d')

    wind_turbines['geometry'] = wind_turbines.apply(lambda x: radial_polygon_from_point(x['lon'], x['lat'], x['Rotor-diameter (m)']/2), axis=1)
    wind_turbines['name'] = 'Wind Turbine'
    wind_turbines['data_origins'] = 'Energistyrelsen'
    wind_turbines['area'] = 'Denmark'
    
    all_rows = []
    

    wind_turbines['Dato for afmeldning'].fillna('NaN', inplace=True)
    for _,row in tqdm(wind_turbines.iterrows()):
        start_year = row['Dato for oprindelig nettilslutning'][:4]
        if int(start_year) < 2016:
            start_year = '2016'
        if row['Dato for afmeldning'] != 'NaN':
            end_year = row['Dato for afmeldning'][:4]
        else:
            end_year = '2024'
        years_active = [str(i) for i in range(int(start_year), int(end_year)+1)]

        for year in years_active:
            row['year'] = year
            cols = ['Møllenummer (GSRN)','Kapacitet (kW)','Rotor-diameter (m)','geometry', 'name', 'year','area','data_origins']
            all_rows.append(row[cols])
            
    wind_turbines = gpd.GeoDataFrame(all_rows, geometry='geometry')

    return wind_turbines


def raster2geo(data_dir: str, file: str):
    raster_path = data_dir + file
    with rasterio.open(raster_path) as src:
        image = src.read(1)  # Read the first band
        transform = src.transform

    raster_area = get_area_of_raster_tif(src, image)

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
    gdf = gdf.to_crs(epsg=4326)

    return gdf


def geo_overlay(base_layer, overlay):
    pass


def get_yearly_intersection(gdf_before, gdf_after):
    # Ensure both GeoDataFrames are in the same CRS
    if gdf_before.crs != tif_after.crs:
        tif_after = gdf_after.to_crs(gdf_before.crs)

    # Perform spatial join to find intersecting geometries
    intersections = gpd.sjoin(gdf_before, gdf_after, how="inner", op="intersects")

    # Calculate intersection geometries and capture landcover_left and landcover_right
    intersection_data = [
        {
            "geometry": gdf_before.geometry.loc[idx].intersection(
                gdf_after.geometry.loc[row["index_right"]]
            ),
            "landcover_left": gdf_before.loc[idx, "landcover"],
            "landcover_right": gdf_after.loc[row["index_right"], "landcover"],
        }
        for idx, row in intersections.iterrows()
    ]

    # Create a new GeoDataFrame from the intersection data
    intersection_gdf = gpd.GeoDataFrame(intersection_data, crs=gdf_before.crs)

    # Filter out empty geometries
    intersection_gdf = intersection_gdf[~intersection_gdf.geometry.is_empty]

    intersection_gdf["area"] = round(intersection_gdf.area / 10000, 2)

    intersection_gdf = intersection_gdf.loc[
        intersection_gdf["landcover_left"] != intersection_gdf["landcover_right"]
    ]

    summed_gdf = (
        intersection_gdf[["landcover_left", "landcover_right", "area"]]
        .groupby(["landcover_left", "landcover_right"])
        .sum()
        .reset_index()
        .sort_values(["landcover_left", "area"], ascending=False)
    )

    return intersection_gdf, summed_gdf


def get_turbine_photos(wind_turbine_df):
    pass

