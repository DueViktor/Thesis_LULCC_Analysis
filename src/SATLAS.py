##########################################

## SATLAS

##########################################


import requests
import zipfile
import shutil
import geopandas as gpd
from src.dynamic_world import DynamicWorldBasemap
from src.utils import authenticate_Google_Earth_Engine as authenticate
import ee
import os
from tqdm import tqdm
import pandas as pd 
from src.data_handlers import radial_polygon_from_point, prepare_polygons_for_DB
from src.DataBaseManager import DBMS

from config import SATLAS_SOLAR_URL,SATLAS_WIND_URL,DEFAULT_WIND_TURBINE_RADIUS


def load_shp_from_zip_url(url):

    try:
        # Step 1 & 2: Download the ZIP file
        local_zip_path = "temp_download.zip"
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_zip_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        # Step 3: Unzip the file
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            zip_ref.extractall("temp_extracted")
        
        # Step 4: Load the .shp file into a GeoDataFrame
        shp_path = None
        for root, dirs, files in os.walk("temp_extracted"):
            for file in files:
                if file.endswith(".shp"):
                    shp_path = os.path.join(root, file)
                    break
            if shp_path:
                break
        
        if not shp_path:
            raise FileNotFoundError("No .shp file found in the ZIP archive.")
        
        
        gdf = gpd.read_file(shp_path)

        os.remove(local_zip_path)
        shutil.rmtree("temp_extracted")



    except Exception as e:
        print(f"Error reading file: {e}")
        print('SETTING SHX TO RESTORE')
        os.environ['SHAPE_RESTORE_SHX'] = 'YES'

        stripped_url = url.replace('.zip','')

        r = requests.get(stripped_url)

        shp_path = "temp_shape_file"
        with open(shp_path+'.shp', 'wb') as f:
            f.write(r.content)
        


        extensions = ['.shp', '.shx', '.dbf', '.prj']

        gdf = gpd.read_file(shp_path + '.shp')
        # if the url contains wind create a column 'category' with value 'Wind Turbine' else 'Solar Panel'
        if 'wind' in url:
            gdf['category'] = 'Wind Turbine'
        else:
            gdf['category'] = 'Solar Panel'
        
        for ext in extensions:
            try:
                os.remove(shp_path + ext)
            except FileNotFoundError:
                print(f"File {shp_path + ext} not found for deletion.")
        
        os.environ['SHAPE_RESTORE_SHX'] = 'NO'

    
    return gdf


def get_SATLATS_data(year=None, month=None,solar=True,wind=True):

    if not year:
        time_filter = "latest"

    else:
        time_filter = f"{year}-{month}"


    WIND =  f"https://pub-956f3eb0f5974f37b9228e0a62f449bf.r2.dev/outputs/renewable/{time_filter}_wind.shp.zip"
    SOLAR = f"https://pub-956f3eb0f5974f37b9228e0a62f449bf.r2.dev/outputs/renewable/{time_filter}_solar.shp.zip"
    # Load the data
    if solar:
        gdf_solar = load_shp_from_zip_url(SOLAR)
    else:
        gdf_solar = None

    if wind:
        gdf_wind = load_shp_from_zip_url(WIND)
        if not solar:
            return gdf_wind
    else:
        gdf_wind = None
        return gdf_solar
    



    return gdf_wind, gdf_solar









def ee_geometry_to_geojson(ee_geometry_list):
    geojsons = []
    for geom_list in ee_geometry_list:
        for geom in geom_list:
            geojson = ee.Geometry(geom).getInfo()
            geojsons.append(geojson)
    return geojsons

def create_geodataframe(geojson_list):
    feature_collection = {'type': 'FeatureCollection', 'features': []}

    for geom in geojson_list:
        feature = {'type': 'Feature', 'properties': {}, 'geometry': geom}
        feature_collection['features'].append(feature)

    # Convert the GeoJSON FeatureCollection to a GeoDataFrame
    gdf = gpd.GeoDataFrame.from_features(feature_collection['features'])

    return gdf

# Get LSIB boundaries using google earth engine for each country and dissolve in gdf

# Load the LSIB data



def get_LSIB_as_gdf(country):
    
    DWB = DynamicWorldBasemap(area_name=country,
            date_ranges=[
                ("2016-01-01", "2016-12-31")])
    
    polys = DWB.create_polygon(DWB.get_country_LSIB_coordinates(country), flip=False)

    # Example usage
    geojson_list = ee_geometry_to_geojson([polys])  # Convert ee.Geometry to GeoJSON
    gdf = create_geodataframe(geojson_list)  # Create GeoDataFrame

    return gdf.dissolve().set_crs("EPSG:4326")


def format_date(date):

    year = date.split("-")[0]
    return year


def to_DB_format(gdf,year,country):
    
    gdf['year'] = year
    gdf['area'] = country
    gdf['data_origins'] = "SATLAS"
    to_DB_format
    if gdf.shape[0] == 0:
        gdf["name"]='empty'
        return gdf
    
    gdf['name'] = "Wind Turbine" if "wind" in gdf['category'].values.tolist()[0].lower() else "Solar Panel"
    return gdf

def get_polygon_chips(gdf,year,country):
    geometry_wkt = gdf.dissolve().geometry.geometry.iloc[0].wkt
    DB = DBMS()
    intersecting_chips = DB.read(
        "GET_INTERSECTING_CHIPS",
        {"_AREA_": country, "_GEOMETRY_": geometry_wkt,'_YEAR_':year},
        geom_query=True,
    )

    #print("Inner 1:",gdf.columns)
    gdf = to_DB_format(gdf,year,country)

    #print("Inner 2:",gdf.columns)

    return prepare_polygons_for_DB(gdf, intersecting_chips,object_id_col="object_id_col")



def get_SATLAS_data_within_LSIB(country_gdfs,gdf_solar,gdf_wind,year):
    solar_gdfs, wind_gdfs = {}, {}

    for country,gdf in tqdm(country_gdfs.items(),desc="Finding SATLAS data within each LSIB boundary"):
        print(country)
        gdf_wind_filtered_full = gdf_wind[gdf_wind.within(gdf.unary_union)] 
        wind_gdfs[country] = gdf_wind_filtered_full

        gdf_solar_filtered_full = gdf_solar[gdf_solar.within(gdf.unary_union)]

        solar_gdfs[country] = gdf_solar_filtered_full
    
    
    # Create HASH ID for each object WT_ID and PV_ID in column object_id
    for country in tqdm(country_gdfs.keys(),desc="Creating HASH ID for each object"):
        solar_gdfs[country]["object_id_col"] = solar_gdfs[country].apply(lambda x: hash(tuple(x)),axis=1)
        wind_gdfs[country]["object_id_col"] = wind_gdfs[country].apply(lambda x: hash(tuple(x)),axis=1)
    
    #print("1:",wind_gdfs[country].columns)
    # Create polygons for wind turbines
    for country,wind_gdf in wind_gdfs.items():
        wind_gdf['geometry'] = wind_gdf.apply(lambda row: radial_polygon_from_point(row['geometry'].x,row['geometry'].y,DEFAULT_WIND_TURBINE_RADIUS[year]),axis=1)
        wind_gdfs[country] = wind_gdf
   
    #print("2:",wind_gdfs[country].columns)
    all_DB_ready_data = []
    # Get chipid for each polygon
    for country,wind_gdf in tqdm(wind_gdfs.items(),desc="Getting polygon chips for each wind turbine"): 
        #print("3:",wind_gdfs[country].columns)
        DB_ready_data = get_polygon_chips(wind_gdf,year,country)
        all_DB_ready_data.append(DB_ready_data)

    for country,solar_gdf in tqdm(solar_gdfs.items(),desc="Getting polygon chips for each solar panel"):
        #print("4:",wind_gdfs[country].columns)¡£@$£½$¥{[¥½$£$$$]}
        DB_ready_data = get_polygon_chips(solar_gdf,year,country)
        all_DB_ready_data.append(DB_ready_data)
    
    DB_ready_data = pd.concat(all_DB_ready_data)

    return DB_ready_data,(solar_gdfs,wind_gdfs)



## MAIN FUNCTION
def upload_SATLAS_data(countries,years):
    


    SATLAS = {}

    for year in tqdm(years,desc="Getting SATLAS solar and wind data for each year"):
        print(year)
        year_ = format_date(year[1])
        print(year_)

        gdf_wind, gdf_solar = get_SATLATS_data(year=str(int(year_)+1),month="01")

        try:
            gdf_wind, gdf_solar = get_SATLATS_data(year=str(int(year_)+1),month="01")
        except:
            print(f"Could not get SATLAS data for year {year_}")
            continue

        SATLAS[year_] = {"wind":gdf_wind,"solar":gdf_solar}


    # Get LSIBs
    country_gdfs = {}
    authenticate()
    for country in tqdm(countries,desc="Getting LSIB data for each country"):
        country_gdfs[country] =  get_LSIB_as_gdf(country)

    # Run through all years
    DB_upload_list = []
    for year, data in SATLAS.items():
        DB_ready_data,wind_solar_list = get_SATLAS_data_within_LSIB(country_gdfs,data["solar"],data["wind"],year)
        DB_upload_list.append(DB_ready_data)


    DB_ready_data = pd.concat(DB_upload_list)


    DB = DBMS()

    DB.add_land_cover_type(DB_ready_data)
    
    return DB_upload_list,SATLAS,DB_ready_data,wind_solar_list




if __name__ == "__main__":

    """
    DB_ready_data = upload_SATLAS_data(['Denmark','Israel','Netherlands','Estonia'],[["2016-01-01", "2016-12-31"],
                                                                                     ["2017-01-01", "2017-12-31"],
                                                                                        ["2018-01-01", "2018-12-31"],
                                                                                        ["2019-01-01", "2019-12-31"],
                                                                                        ["2020-01-01", "2020-12-31"]
                                                                                     
                                                                                     ])


    
    """
    """
    DB_ready_data = upload_SATLAS_data(['Denmark','Israel','Netherlands','Estonia'],[["2016-01-01", "2016-12-31"],
                                                                                     ["2017-01-01", "2017-12-31"],
                                                                                        ["2018-01-01", "2018-12-31"],
                                                                                        ["2019-01-01", "2019-12-31"],
                                                                                        ["2020-01-01", "2020-12-31"],
                                                                                        ["2021-01-01", "2021-12-31"],
                                                                                     ["2022-01-01", "2022-12-31"],
                                                                                        ["2023-01-01", "2023-12-31"]
                                                                                     
                                                                                     ])
    
    """
    DB_ready_data = upload_SATLAS_data(['Denmark','Israel','Netherlands','Estonia'],[
                                                                                     ["2017-01-01", "2017-12-31"],
                                                                                        ["2018-01-01", "2018-12-31"],
                                                                                        ["2019-01-01", "2019-12-31"],
                                                                                        ["2020-01-01", "2020-12-31"],
                                                                                        ["2021-01-01", "2021-12-31"],
                                                                                     ["2022-01-01", "2022-12-31"]
                                                                                     
                                                                                     ])
                                                                                     