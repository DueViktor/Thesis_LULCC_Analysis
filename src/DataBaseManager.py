import getpass
import os
import sys
from io import BytesIO

import geopandas as gpd
import pandas as pd
from geoalchemy2 import Geometry, WKTElement
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from sqlalchemy import create_engine, text, types
from sshtunnel import SSHTunnelForwarder
from tqdm import tqdm

from config import LAND_COVER_LEGEND

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_handlers import raster_dict2geo

QUERY_CATALOG = {
    "read": {
        "SUMMED_AREA_PER_YEAR_PER_AREA" : """
            SELECT 
                area,
                name,
                year,
                ST_Area(ST_Transform(ST_Union(geometries), __EPSG__)) / 1000000.0 AS result_geom_area_km2 
            FROM 
                lulc 
            WHERE 
                area = '__AREA__' AND 
                (data_origins = 'SATLAS' OR data_origins = 'DynamicWorld')
            GROUP BY 
                area, name, year;

            """,
        "WHAT_TURNED_INTO_RENEWABLES": """
            SELECT 
                area AS country,
                lulc_category_from,
                lulc_category_to,
                SUM(area_km2) AS area_converted_to_renewables
            FROM 
                land_use_change
            WHERE 
                lulc_category_to IN ('Wind Turbine', 'Solar Panel')
                AND lulc_category_from != lulc_category_to
                AND year_from = 2016
                AND year_to = 2023
            GROUP BY 
                area, 
                lulc_category_from, 
                lulc_category_to
            ORDER BY 
                area, 
                lulc_category_to, 
                SUM(area_km2) DESC;""",
        "chip_greater_than" : """
            SELECT 
                chipid, 
                ST_Area(ST_Transform(ST_Union(geometries), __EPSG__)) / 1000000.0 AS area_sq_km
            FROM 
                lulc
                WHERE year = '2016-01-01' AND data_origins = 'DynamicWorld' AND area = '__COUNTRY__'
            GROUP BY 
                chipid
            HAVING 
                ST_Area(ST_Transform(ST_Union(geometries), __EPSG__)) / 1000000.0 > __KILOMETER_THRESHOLD__
                """,
        "GET_DW_LANDCOVER": """SELECT * FROM lulc 
    WHERE 
    Area = '_AREA_'
    AND chipid in (_CHIPIDS_)
    AND (year <= '_END_YEAR_-01-01' AND year >= '_START_YEAR_-01-01')
    AND (data_origins = 'DynamicWorld' OR data_origins = 'SATLAS')
    """,  # AND data_origins = 'DynamicWorld',
    "GET_ONLY_DW_LANDCOVER": """SELECT * FROM lulc 
    WHERE 
    Area = '_AREA_'
    AND chipid in (_CHIPIDS_)
    AND (year <= '_END_YEAR_-01-01' AND year >= '_START_YEAR_-01-01')
    AND (data_origins = 'DynamicWorld')
    """,
    "GET_AGGREGATED_LANDCOVER": """SELECT name, ST_Union(geometries) as geometries 
                                   FROM lulc WHERE 
                                   and name  = ''
                                   data_origins = 'DynamicWorld' 
                                   AND year = '_YEAR_-01-01' 
                                   AND area = '_AREA_'
                                   GROUP BY name
                                   """,
    "GET_ONLY_CHIPIDS_FROM_AREA":""" SELECT chipid,count(distinct year)  as num_years FROM lulc
                                        WHERE area='_AREA_' AND data_origins = 'DynamicWorld'
                                        group by chipid
                                        HAVING count(distinct year) = 8
                                        ORDER BY CAST(split_part(split_part(chipid, '_', 1), '-', 1) AS INTEGER)""",
    "GET_CHIPIDS_FROM_AREA":""" SELECT DISTINCT chipid FROM lulc WHERE area = '_AREA_' 
                                AND chipid not in (SELECT DISTINCT chipid from land_use_change
                                WHERE area = '_AREA_' and year_from = _YEAR_FROM_ 
                                AND year_to =  _YEAR_TO_)""",

    "GET_LANDCOVER": """SELECT * FROM lulc
                        WHERE area='_AREA_' AND year = '_YEAR_-01-01'
                        AND data_origins = 'DynamicWorld'""",
    "GET_SATLAS_AND_DW_LANDCOVER": """SELECT * FROM lulc
                        WHERE area='_AREA_' AND year = '_YEAR_-01-01'
                        AND data_origins = 'DynamicWorld' OR data_origins = 'SATLAS'""",

    "GET_CHIP_LANDCOVER": """SELECT chipid,
                                    name,
                                    ST_Union(geometries) AS geometries FROM lulc
                        WHERE area='_AREA_' AND year = '_YEAR_-01-01'
                        AND data_origins = 'DynamicWorld' AND chipid = '_CHIPID_'
                        GROUp BY chipid, name""",
    

        "GET_DRIVE_FOLDERS": "SELECT foldername,area FROM drive_folders",
        "GET_EXISTING_CHIPS": """SELECT chipid,num_dates  FROM (
                            SELECT chipid,COUNT(distinct year) as num_dates 
                            FROM lulc
                            WHERE area = '_AREA_' 
                            GROUP BY chipid ) iq
                            WHERE num_dates = _NUM_DATES_
                             """,
        "GET_CHIPIDS": """
                        SELECT * FROM (
                        SELECT DISTINCT chipid,
                        CAST(split_part(split_part(chipid, '_', 1), '-', 1) AS INTEGER) as poly_int,
                        CAST(split_part(split_part(chipid, '_', 2), '-', 1) AS INTEGER) as lat_int, 
                        CAST(split_part(split_part(chipid, '_', 3), '-', 1) AS INTEGER) as lon_int 
                            
                            FROM lulc WHERE area = '_AREA_'
                        AND split_part(split_part(chipid, '_', 1), '-', 1) = '_SUB_AREAID_' 
                        ) iq
                        ORDER BY 
                        poly_int,lon_int,lat_int
                        """,
        "GET_FINISHED_SUBPOLY": """SELECT polygon_index FROM sub_polygons WHERE area = '_AREA_'""",
        "GET_INTERSECTING_CHIPS": """
                                SELECT * FROM lulc
                                WHERE area = '_AREA_' 
                                AND year = '_YEAR_-01-01' 
                                AND ST_Intersects(geometries, ST_GeomFromText('_GEOMETRY_', 4326))
                                """,


        "CALCULATE_LULC_INTERSECTION": 
        """ 
        SELECT *,
       intersection_area_sq_km / preceding_area_sq_km * 100 AS percent_change
FROM (
    SELECT preceding_year.chipid,
           preceding_year.name AS preceding_year_name,
           current_year.name AS current_year_name,
           ST_Intersection(preceding_year.result_geom_area, current_year.result_geom_area) AS land_use_change,
           ST_Area(ST_Transform(preceding_year.result_geom_area, 25832)) / 1000000.0 AS preceding_area_sq_km,
           ST_Area(ST_Transform(ST_Intersection(preceding_year.result_geom_area, current_year.result_geom_area), 25832)) / 1000000.0 AS intersection_area_sq_km
    FROM (
        SELECT chipid,
               name,
               COALESCE(ST_Difference(DynamicWorld.lulc_polygon, SATLAS.lulc_polygon), DynamicWorld.lulc_polygon) AS result_geom_area
        FROM (
            SELECT chipid,
                   name,
                   ST_Union(geometries) AS lulc_polygon
            FROM lulc
            WHERE area = '_AREA_'
              AND year = '_FROM_YEAR_-01-01'
              AND data_origins = 'DynamicWorld'
              AND chipid in _CHIPID_LIST_
            GROUP BY chipid,name
        ) AS DynamicWorld
        CROSS JOIN (
            SELECT ST_Union(geometries) AS lulc_polygon
            FROM lulc
            WHERE area = '_AREA_'
              AND year = '_FROM_YEAR_-01-01'
              AND data_origins = 'SATLAS'
              AND chipid in _CHIPID_LIST_
        ) AS SATLAS
        UNION ALL
        SELECT chipid,
               name,
               COALESCE(ST_Difference(Solar.lulc_polygon, Wind.lulc_polygon), Solar.lulc_polygon) AS result_geom_area
        FROM (
            SELECT chipid,
                   name,
                   ST_Union(geometries) AS lulc_polygon
            FROM lulc
            WHERE area = '_AREA_'
              AND year = '_FROM_YEAR_-01-01'
              AND data_origins = 'SATLAS'
              AND chipid in _CHIPID_LIST_
              AND name = 'Solar Panel'
            GROUP BY chipid,name
        ) AS Solar
        CROSS JOIN (
            SELECT ST_Union(geometries) AS lulc_polygon
            FROM lulc
            WHERE area = '_AREA_'
              AND year = '_FROM_YEAR_-01-01'
              AND data_origins = 'SATLAS'
              AND chipid in _CHIPID_LIST_
              AND name = 'Wind Turbine'
        ) AS Wind
        UNION ALL
        SELECT chipid,
               name,
               Wind.lulc_polygon AS result_geom_area
        FROM (
            SELECT chipid,
                   name,
                   ST_Union(geometries) AS lulc_polygon
            FROM lulc
            WHERE area = '_AREA_'
              AND year = '_FROM_YEAR_-01-01'
              AND data_origins = 'SATLAS'
              AND chipid in _CHIPID_LIST_
              AND name = 'Wind Turbine'
            GROUP BY chipid,name
        ) AS Wind
    ) AS preceding_year
    INNER JOIN (
        SELECT chipid,
              name,
               COALESCE(ST_Difference(DynamicWorld.lulc_polygon, SATLAS.lulc_polygon), DynamicWorld.lulc_polygon) AS result_geom_area
        FROM (
            SELECT 
                   chipid,
                   name,
                   ST_Union(geometries) AS lulc_polygon
            FROM lulc
            WHERE area = '_AREA_'
              AND year = '_TO_YEAR_-01-01'
              AND data_origins = 'DynamicWorld'
              AND chipid in _CHIPID_LIST_
            GROUP BY chipid,name
        ) AS DynamicWorld
        CROSS JOIN (
            SELECT ST_Union(geometries) AS lulc_polygon
            FROM lulc
            WHERE area = '_AREA_'
              AND year = '_TO_YEAR_-01-01'
              AND data_origins = 'SATLAS'
              AND chipid in _CHIPID_LIST_
        ) AS SATLAS
        UNION ALL
        SELECT chipid,
               name,
               COALESCE(ST_Difference(Solar.lulc_polygon, Wind.lulc_polygon), Solar.lulc_polygon) AS result_geom_area
        FROM (
            SELECT chipid,
                   name,
                   ST_Union(geometries) AS lulc_polygon
            FROM lulc
            WHERE area = '_AREA_'
              AND year = '_TO_YEAR_-01-01'
              AND data_origins = 'SATLAS'
              AND chipid in _CHIPID_LIST_
              AND name = 'Solar Panel'
            GROUP BY chipid,name
        ) AS Solar
        CROSS JOIN (
            SELECT ST_Union(geometries) AS lulc_polygon
            FROM lulc
            WHERE area = '_AREA_'
              AND year = '_TO_YEAR_-01-01'
              AND data_origins = 'SATLAS'
              AND chipid in _CHIPID_LIST_
              AND name = 'Wind Turbine'
        ) AS Wind
        UNION ALL
        SELECT chipid,
               name,
               Wind.lulc_polygon AS result_geom_area
        FROM (
            SELECT chipid,
                   name,
                   ST_Union(geometries) AS lulc_polygon
            FROM lulc
            WHERE area = '_AREA_'
              AND year = '_TO_YEAR_-01-01'
              AND data_origins = 'SATLAS'
              AND chipid in _CHIPID_LIST_
              AND name = 'Wind Turbine'
            GROUP BY chipid,name
        ) AS Wind
    ) AS current_year ON 
                        preceding_year.chipid = current_year.chipid AND 
                        ST_Intersects(preceding_year.result_geom_area, current_year.result_geom_area)
) ooq
ORDER BY intersection_area_sq_km DESC;


        """, # --WHERE preceding_year_name != current_year_name HAS BEEN REMOVED
"GET_CHIP_GRAPH": """
                  SELECT area,chipid,lulc_category_from,lulc_category_to,sum(area_km2) as changed_area
                    FROM land_use_change luc
                    WHERE area_km2 > 0.001 AND 
                        luc.year_to = _YEAR_TO_ AND luc.year_from = _YEAR_FROM_
                        AND lulc_category_from != lulc_category_to and area = '_AREA_'
                    GROUP BY area,chipid,lulc_category_from,lulc_category_to
                    ORDER BY area,chipid,sum(area_km2) desc
                  """,
"GET_SINGLE_CHIP_GRAPH": """
                  SELECT area,chipid,lulc_category_from,lulc_category_to,sum(area_km2) as changed_area
                    FROM land_use_change luc
                    WHERE area_km2 > 0.001 AND 
                        luc.year_to = _YEAR_TO_ AND luc.year_from = _YEAR_FROM_
                        AND lulc_category_from != lulc_category_to and area = '_AREA_'
                        AND chipid = '_CHIPID_'
                    GROUP BY area,chipid,lulc_category_from,lulc_category_to
                    ORDER BY area,chipid,sum(area_km2) desc
                  """,
    "GET_LANDCOVER_CHANGE": """
                            SELECT lulc_category_from,lulc_category_to,area_km2
                            FROM land_use_change

                            WHERE area='Denmark' AND year_from = 2016 AND year_to = 2017
                            AND lulc_category_from != lulc_category_to

                            """,
    "GET_LANDCOVER_CHANGE_WITH_PARAMS": """
                            SELECT lulc_category_from,lulc_category_to,area_km2
                            FROM land_use_change

                            WHERE area='_AREA_' AND year_from = _YEAR_FROM_ AND year_to = _YEAR_TO_
                            """,

    "GET_SOLAR_WIND_OVERLAP": """
                               

                                SELECT main.*,wind.wind_turbine_km2,solar.solar_panel_km2,
                                100*yearly_overlap_km2/wind.wind_turbine_km2 as percentage_of_wind_turbine_area,
                                100*yearly_overlap_km2/solar.solar_panel_km2 as percentage_of_solar_panel_area, 
                                100*yearly_overlap_km2/(solar.solar_panel_km2+wind.wind_turbine_km2)
                                as percentage_of_renewable_area 
                                    
                                FROM 

                                    --MAIN
                                (
                                SELECT area,year,SUM(intersection_area)/1000000 as yearly_overlap_km2
                                    FROM(
                                SELECT
                                a.area,
                                a.year,
                                a.chipid,
                                ST_Area(ST_Transform(ST_Intersection(a.geometries, b.geometries),25832)) AS intersection_area
                                    --,
                                --a.geometries AS a_geometry,
                                --b.geometries AS b_geometry
                                FROM
                                (SELECT * FROM lulc where name = 'Solar Panel' and data_origins = 'SATLAS') a
                                INNER JOIN
                                (SELECT * FROM lulc where name = 'Wind Turbine' and data_origins = 'SATLAS') b
                                ON
                                a.year = b.year
                                AND a.chipid = b.chipid
                                AND a.area = b.area
                                AND a.geometries && b.geometries -- Ensures there's a bounding box intersection before attempting the more costly ST_Intersection
                                AND ST_Intersects(a.geometries, b.geometries)
                                --AND a.ctid != b.ctid -- Prevents a row from joining with itself


                                    ) iq


                                    GROUP BY area,year

                                    ) as main




                                    INNER JOIN 

                                    -- WIND
                                    (SELECT 
                                    area,
                                    year,
                                    SUM(ST_Area(ST_Transform(geometries,25832)))/1000000 as wind_turbine_km2
                                    FROM lulc

                                    WHERE name = 'Wind Turbine' AND data_origins = 'SATLAS'
                                    GROUP BY area,year
                                    ) as wind

                                    on main.area = wind.area AND main.year = wind.year

                                    INNER JOIN 


                                    -- SOLAR 
                                    (SELECT 
                                    area,
                                    year,
                                    SUM(ST_Area(ST_Transform(geometries,25832)))/1000000 as solar_panel_km2
                                    FROM lulc

                                    WHERE name = 'Solar Panel' AND data_origins = 'SATLAS'
                                    GROUP BY area,year) as solar




                                    on main.area = solar.area AND main.year = solar.year
                                    """,

        "GET_LUC_VERIFICATION": """
                                                            
                                SELECT year_from,year_to,lulc_category_from,lulc_category_to,SUM(area_km2) as area_km2, ST_Union(geom) as geometries from land_use_change 
                                WHERE ((lulc_category_from ='Crops' AND lulc_category_to ='Grass') OR 
                                    (lulc_category_from ='Grass' AND lulc_category_to ='Crops') OR 
                                    (lulc_category_from ='Trees' AND lulc_category_to ='Crops') OR 
                                    (lulc_category_from ='Crops' AND lulc_category_to ='Trees') OR
                                    (lulc_category_from ='Trees' AND lulc_category_to ='Grass') OR 
                                    (lulc_category_from ='Grass' AND lulc_category_to ='Trees'))	

                                AND ((year_from = 2016 AND year_to = 2017) OR 
                                    (year_from = 2016 AND year_to = 2023))

                                AND area = 'Denmark'
                                AND chipid in (_CHIPIDS_)
                                    
                                GROUP BY year_from,year_to,lulc_category_from,lulc_category_to

                                """
        },
        "write": {
            "INSERT_LANDCOVER": """INSERT INTO landcover (Area, Name, Year, data_origins, ChipID, Geometry) VALUES ('_AREA_', '_NAME_', '_YEAR_', '_DATA_ORIGIN_', '_CHIPID_',')""",
            "INSERT_DRIVE_FOLDER": """
                    INSERT INTO drive_folders (area, foldername) 
                    VALUES ('_AREA_', '_FOLDERNAME_') 
                    """,
            "INSERT_FINISHED_SUBPOLY": """
                    INSERT INTO sub_polygons (area, polygon_index) 
                    VALUES ('_AREA_', '_POLYGON_INDEX_') 
                    """,
        },
    }


class DriveManager:
    def __init__(self):
        # Specify the scopes and service account file
        self.SCOPES = ["https://www.googleapis.com/auth/drive"]
        self.SERVICE_ACCOUNT_FILE = "askemeineche_google_drive_secret.json"

        self.flow = InstalledAppFlow.from_client_secrets_file(
            self.SERVICE_ACCOUNT_FILE, self.SCOPES
        )
        self.creds = self.flow.run_local_server(port=0)

        self.drive = build("drive", "v3", credentials=self.creds)

        self.DBMS = DBMS()

    def update_database(self):
        folders = self.DBMS.read("GET_DRIVE_FOLDERS", {})

        for _, row in tqdm(
            folders.iterrows(), desc="Checking Each Folder for New Data"
        ):
            self.search_folder_and_download_files(
                folder_name=row["foldername"], area=row["area"]
            )

    def search_folder_and_download_files(
        self, folder_name, area="Denmark", test=False, pageSize=100
    ):
        # Step 1: Search for the folder by name to get its ID.
        folder_query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and trashed=false"
        folder_result = (
            self.drive.files().list(q=folder_query, fields="files(id, name)").execute()
        )
        folders = folder_result.get("files", [])

        if not folders:
            print(f"No folder found with the name: {folder_name}")
            return
        folder_id = folders[0][
            "id"
        ]  # Assuming the first result is the folder you're looking for
        print(f"Folder ID for '{folder_name}': {folder_id}")

        page_token = None
        pageNum = 1
        cc = 0
        while True:
            file_contents = {}
            file_ids = []
            print(f"Downloading Files From Page {pageNum}")
            # Step 2: Search for the files inside the folder with pagination.
            files_query = f"'{folder_id}' in parents and trashed=false"
            files_result = (
                self.drive.files()
                .list(
                    q=files_query,
                    pageSize=pageSize,
                    fields="nextPageToken, files(id, name)",
                    pageToken=page_token,
                )
                .execute()
            )
            files = files_result.get("files", [])
            page_token = files_result.get("nextPageToken", None)

            if not files:
                print(f"No more files found in the folder: {folder_name}")
                break
            else:
                print(f"Files in folder '{folder_name}':")
                for file in tqdm(files, desc="Downloading files from current page..."):
                    content = self.download_file(file["id"])
                    file_contents[file["name"]] = content
                    file_ids.append(file["id"])

                geoframe = raster_dict2geo(file_contents, area=area)
                try:
                    self.DBMS.add_land_cover_type(geoframe)

                    self.delete_files_by_ids(file_ids)
                except:
                    print("Could not upload to DB")
                    geoframe.to_file(f'/output/errors/{file["id"]}.shp')

                pageNum += 1
            if page_token is None:
                break
            if test:
                break

        return file_contents

    def delete_files_by_ids(self, file_ids):
        """
        Delete files from Google Drive given a dictionary of file IDs.

        Args:
            file_ids_dict (dict): A dictionary with file IDs as keys.
        """
        delete_counter = 0
        for ix, file_id in tqdm(
            enumerate(file_ids), desc="Deleting uploaded files from Drive"
        ):
            try:
                # Attempt to delete each file by ID
                self.drive.files().delete(fileId=file_id).execute()
                delete_counter += 1
            except Exception as e:
                # Handle potential errors, such as the file not existing or lacking permission
                print(f"Could not delete file with ID {file_id}. Error: {e}")

        print(f"{delete_counter} OUT OF {ix+1} WHERE SUCCESSFULLY DELETED")

    def download_file(self, file_id):
        """
        Downloads Content from Google Drive Folders as Raster Tifs
        """

        # Request to get the file's metadata and content
        request = self.drive.files().get_media(fileId=file_id)

        # Use BytesIO object as a buffer
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        # Download the file's content into memory
        done = False
        while not done:
            status, done = downloader.next_chunk()
            # print(f"Download {int(status.progress() * 100)}%.")

        # The file's content is now in fh, which you can access like this:
        fh.seek(0)  # Move to the start of the BytesIO buffer
        file_content = fh.read()  # Read the content of the file into memory

        return file_content


class DBMS:
    """Database Manager System.
    Håndterer Lasse (Marius <3)
    """

    def __init__(self):
        if getpass.getuser() == "viktorduepedersen":
            self.username = "viktor"
            self.password = "ye6X8ja(JaF<4>Uv"
        else:
            self.username = "aske"
            self.password = "4sciiAgain$tHuman1ty"

        self.host_address = "192.168.1.150"
        self.port = 5432
        self.db_name = "GIS"
        self.server = SSHTunnelForwarder(
            ("89.150.135.220", 11234),
            ssh_username="viktor"
            if getpass.getuser() == "viktorduepedersen"
            else "aske",
            ssh_pkey="~/.ssh/id_rsa_viktor"
            if getpass.getuser() == "viktorduepedersen"
            else "~/.ssh/id_rsa_aske",
            remote_bind_address=("192.168.1.150", 5432),
        )

    def handle_queries(self, query_name, params, func="read", geom_query=False,geom_col="geometries"):
        query = QUERY_CATALOG[func][query_name]
        for query_element, element_value in params.items():
            query = query.replace(query_element, element_value)
        #print(query)
        if func == "write":
            return query

        if geom_query:
            return gpd.GeoDataFrame.from_postgis(
                query, self.engine, geom_col=geom_col
            )
        else:
            return pd.read_sql(query, self.engine)

    def read(self, query_name, params, geom_query=False,geom_col="geometries"):
        self.server.start()
        local_port = str(self.server.local_bind_port)

        self.engine = create_engine(
            "postgresql://{}:{}@{}:{}/{}".format(
                self.username, self.password, "127.0.0.1", local_port, self.db_name
            )
        )

        query_results = self.handle_queries(query_name, params, geom_query=geom_query,geom_col=geom_col)

        self.server.stop()

        return query_results

    def write(self, query_name, values):
        self.server.start()
        local_port = str(self.server.local_bind_port)

        self.engine = create_engine(
            "postgresql://{}:{}@{}:{}/{}".format(
                self.username, self.password, "127.0.0.1", local_port, self.db_name
            )
        )

        query = text(self.handle_queries(query_name, values, func="write"))

        print(query)
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()

        self.server.stop()

    def format_DW_geodf_for_DBMS(self, gdf):
        gdf["name"] = [LAND_COVER_LEGEND[LULC_id] for LULC_id in gdf.landcover.values]
        gdf.drop(columns=["landcover"], inplace=True)

        return gdf

    def add_land_use_change(self,gdf):

        self.server.start()
        local_port = str(self.server.local_bind_port)

        self.engine = create_engine(
            "postgresql://{}:{}@{}:{}/{}".format(
                self.username, self.password, "127.0.0.1", local_port, self.db_name
            )
        )


        dtypes = {
            "area": types.VARCHAR(255),
            "chipid": types.VARCHAR(255),
            "year_from": types.INTEGER,
            "year_to": types.INTEGER,
            'lulc_category_from': types.VARCHAR(255),
            'lulc_category_to': types.VARCHAR(255),
            'percent_change': types.FLOAT,
            'from_category_area_sq_km': types.FLOAT,
            'area_km2': types.FLOAT,
            "geom": Geometry("GEOMETRY", srid=4326)
        }

        if "object_id" in gdf.columns:
            dtypes["object_id"] = types.VARCHAR(100)

        # Use 'dtype' parameter to specify SQL types for the GeoDataFrame columns
        gdf.to_sql(
            'land_use_change',
            self.engine,
            if_exists="append",
            index=False,
            dtype=dtypes,
        )

        self.server.stop()


    def add_add_change(self,gdf):

        self.server.start()
        local_port = str(self.server.local_bind_port)

        self.engine = create_engine(
            "postgresql://{}:{}@{}:{}/{}".format(
                self.username, self.password, "127.0.0.1", local_port, self.db_name
            )
        )


        dtypes = {
            "area": types.VARCHAR(255),
            "chipid": types.VARCHAR(255),
            "year_from": types.INTEGER,
            "year_to": types.INTEGER,
            'lulc_category_from': types.VARCHAR(255),
            'lulc_category_to': types.VARCHAR(255),
            'percent_change': types.FLOAT,
            'from_category_area_sq_km': types.FLOAT,
            'area_km2': types.FLOAT,
            "geom": Geometry("GEOMETRY", srid=4326)
        }

        if "object_id" in gdf.columns:
            dtypes["object_id"] = types.VARCHAR(100)

        # Use 'dtype' parameter to specify SQL types for the GeoDataFrame columns
        gdf.to_sql(
            'land_use_change',
            self.engine,
            if_exists="append",
            index=False,
            dtype=dtypes,
        )

        self.server.stop()

    def add_land_cover_type(self, gdf, table_name="lulc"):
        print("Uploading to DB....")

        self.server.start()
        local_port = str(self.server.local_bind_port)

        self.engine = create_engine(
            "postgresql://{}:{}@{}:{}/{}".format(
                self.username, self.password, "127.0.0.1", local_port, self.db_name
            )
        )

        if gdf.shape[0] == 0:
            return 1

        # Assuming 'gdf' is your GeoDataFrame and 'engine' is your SQLAlchemy engine
        if gdf["data_origins"].values[0] == "DynamicWorld":
            gdf = self.format_DW_geodf_for_DBMS(gdf)

        gdf["geometries"] = gdf["geometry"].apply(
            lambda x: WKTElement(x.wkt, srid=4326)
        )

        gdf.drop(columns=["geometry"], inplace=True)

        # Convert 'Year' column to date if it's not already in datetime format
        gdf["year"] = pd.to_datetime(gdf["year"])

        dtypes = {
            "data_origins": types.VARCHAR(255),
            "name": types.VARCHAR(255),
            "year": types.DATE,
            "chipid": types.INTEGER,
            "area": types.VARCHAR(100),
            "geometries": Geometry("GEOMETRY", srid=4326),
        }

        if "object_id" in gdf.columns:
            dtypes["object_id"] = types.VARCHAR(100)

        # Use 'dtype' parameter to specify SQL types for the GeoDataFrame columns
        gdf.to_sql(
            table_name,
            self.engine,
            if_exists="append",
            index=False,
            dtype=dtypes,
        )

        self.server.stop()

        return 0


if __name__ == "__main__":
    db = DBMS()
    print(
        db.read(
            "GET_DW_LANDCOVER",
            {"_AREA_": "DK", "_START_YEAR_": "2010", "_END_YEAR_": "2024"},
        )
    )
