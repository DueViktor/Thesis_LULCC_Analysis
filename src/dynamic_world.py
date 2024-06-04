import math
import time
import warnings
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import ee
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Polygon
from tqdm import tqdm

from src.DataBaseManager import DBMS
from src.utils import authenticate_Google_Earth_Engine as authenticate

warnings.filterwarnings("ignore", category=FutureWarning)

def default_global_gdf():
    return gpd.GeoDataFrame(pd.DataFrame(columns=["chipid", "geometry"]), geometry="geometry")


@dataclass
class DynamicWorldBasemap:
    area_name: str
    date_ranges: List[Tuple[str, str]]
    area_polygons: Optional[List[ee.Geometry.Polygon]] = None
    test_IDs: Optional[List[str]] = False
    testing: bool = False
    grid_size_meters: int = 10000

    global_gdf: gpd.GeoDataFrame = field(default_factory=default_global_gdf)

    def __post_init__(self):
        authenticate()
        
        self.DBMS = DBMS()

        params = {"_AREA_": self.area_name, "_NUM_DATES_": str(len(self.date_ranges))}
        self.existing_chips = self.DBMS.read(
            "GET_EXISTING_CHIPS", params=params
        ).chipid.values

        self.existing_chips = sorted(
            list(set([i.split("-")[0] for i in self.existing_chips]))
        )

        self.finished_subpolys = self.DBMS.read(
            "GET_FINISHED_SUBPOLY", {"_AREA_": self.area_name}
        ).polygon_index.values.tolist()

        print(self.existing_chips)

    def get_coordinates(self, geometry):
        # This function is a placeholder. In practice, you would use getInfo()
        # or other methods depending on your needs, which may require client-server communication.
        coords = geometry.coordinates().getInfo()
        return coords

    def create(self) -> (List[Dict], List[List], List[Dict]):
        """
        This is the main function to create the Dynamic World classification map for a given area and date range.
        """

        # Authenticate Google Earth Engine Project
        authenticate()

        # Call the classifcation function
        return self.get_DW_classification(
            date_ranges=self.date_ranges,
            area_name=self.area_name,
            area_polygons=self.area_polygons,
        )

    def get_country_LSIB_coordinates(self, country_name: str = "Denmark") -> Tuple:
        # Load the LSIB dataset.
        lsib = ee.FeatureCollection("USDOS/LSIB_SIMPLE/2017")

        # Filter the dataset for Denmark.
        denmark_boundary = lsib.filter(ee.Filter.eq("country_na", country_name))

        # Get the geometry of Denmark.
        denmark_geometry = denmark_boundary.geometry()

        # Function to extract coordinates (this part is conceptual; see explanation below).

        # Example usage (be cautious with getInfo() due to potential for large data transfers).
        denmark_coords = self.get_coordinates(denmark_geometry)

        # Since using getInfo() on large geometries can be inefficient or exceed memory limits,
        # consider alternative approaches for handling or visualizing large datasets.

        return denmark_coords, denmark_geometry

    def flip_coords(self, coords):
        return [[coord[1], coord[0]] for coord in coords]

    def flip_polygon(self, geometry):
        """
        Flip longitude and latitude coordinates in an ee.Geometry object.

        Parameters:
        - geometry: An ee.Geometry object

        Returns:
        - A new ee.Geometry object with flipped coordinates
        """
        # Extract coordinates from the original geometry

        coords = geometry.coordinates().getInfo()

        if geometry.getInfo()["type"] == "Polygon":
            # Flip the coordinates
            flipped_coords = [
                [[coord[1], coord[0]] for coord in part] for part in coords
            ]
            flipped_geometry = ee.Geometry.Polygon(flipped_coords)

        elif geometry.getInfo()["type"] == "MultiPolygon":
            flipped_coords = [
                [[[coord[1], coord[0]] for coord in subpoly] for subpoly in part]
                for part in coords
            ]

            flipped_geometry = ee.Geometry.MultiPolygon(flipped_coords)

        # Create a new geometry with the flipped coordinates

        return flipped_geometry

    def create_polygon(self, coords: List, flip: bool = True):
        if flip:
            return [ee.Geometry.Polygon(self.flip_coords(coord[0])) for coord in coords]
        else:
            return [ee.Geometry.Polygon(coord[0]) for coord in coords]

    def get_polygon_boundaries(self, poly_list):
        max_lon = 0
        min_lon = 180
        max_lat = 0
        min_lat = 180

        for poly in poly_list:
            for sub_poly in poly:
                for coord_set in sub_poly:
                    if coord_set[0] > max_lat:
                        max_lat = coord_set[0]

                    if coord_set[0] < min_lat:
                        min_lat = coord_set[0]

                    if coord_set[1] > max_lon:
                        max_lon = coord_set[1]

                    if coord_set[1] < min_lon:
                        min_lon = coord_set[1]

        return max_lon, min_lon, max_lat, min_lat

    def get_cell_size(self, country_lat: float):
        """
        This function calculates the cell size for the grid based on the grid_size_meters parameter.
        Basically, it needs to convert the desired grid size in meters to degrees for latitude and longitude.
        """

        # Convert meters to degrees for latitude
        meters_latitude = self.grid_size_meters
        meters_longitude = self.grid_size_meters
        degrees_latitude = meters_latitude / 111000

        # Convert meters to degrees for longitude at Copenhagen's latitude
        degrees_longitude = meters_longitude / (
            111000 * math.cos(country_lat * (math.pi / 180))
        )

        return {"lat": degrees_latitude, "lon": degrees_longitude}

    def create_chip_boundary(self, lon, lat, cell_size):
        return [
            [lat, lon],
            [lat, lon + cell_size["lon"]],
            [lat + cell_size["lat"], lon + cell_size["lon"]],
            [lat + cell_size["lat"], lon],
            [lat, lon],
        ]

    def create_country_grid(
        self, boundaries, cell_size, area_polygon, area_polygon_index
    ):
        """
        This function should probably be broken up, but here goes.

        """
        # The max and min lat and lon are used to calculate the number of grid cells in each direction.
        max_lat = boundaries[0]
        min_lat = boundaries[1]

        max_lon = boundaries[2]
        min_lon = boundaries[3]

        # The number of grid cells in each direction is used to create the grid.
        latitudes = math.ceil((max_lat - min_lat) / cell_size["lat"])
        longitudes = math.ceil((max_lon - min_lon) / cell_size["lon"])

        intersecting_chips = []
        intersecting_chip_ids = []

        cur_intersecting_chips = []
        cur_intersecting_chips_ids = []

        raw_chipids = {}
        raw_chips = {}

        # Break all is essentially a test parameter, which allows you to break the loop early.
        break_all = False

        # Looping over each latitude index
        for lat_idx in tqdm(range(latitudes), desc="Getting Each new Latitude Chip"):
            # This is most definitely a test parameter, which allows you to break the loop early.
            if self.testing and area_polygon_index > 5:
                break_all = True
                continue

            # Test parameter
            if break_all:
                break

            # Looping over each longitude index
            for lon_idx in tqdm(
                range(longitudes), desc="Getting Each new Longitude Chip"
            ):
                # Creating the area unique ID, which will be used in the Database
                cur_idx = f"{area_polygon_index}_{lon_idx}_{lat_idx}"

                # Test parameter
                if self.test_IDs and cur_idx not in self.test_IDs:
                    continue

                # This is a delta update mechanism, that skips chips that are already correctly saved in the database.
                if self.testing == False and cur_idx in self.existing_chips:
                    print(f"SKIPPING CHIP {cur_idx}")
                    continue

                # For each n = 20 intersecting chips, we get the DynamicWorld classifications and exports them to the Google Drive
                if len(cur_intersecting_chips) >= 10:
                    # Add the current chips to the list of all intersecting chips
                    intersecting_chips += cur_intersecting_chips
                    intersecting_chip_ids += cur_intersecting_chips_ids

                    # Here we get the DW classifications for the current intersecting chips and export them to the Google Drive
                    print("EXPORTING")
                    if not self.testing:
                        self.get_DW_for_polygons(
                            cur_intersecting_chips,
                            self.date_ranges,
                            cur_intersecting_chips_ids,
                        )

                    # Reset the current intersecting chips
                    cur_intersecting_chips = []
                    cur_intersecting_chips_ids = []

                # Saving the raw chips and chipids which can be useful for debugging and visualization
                lon = min_lon + lon_idx * cell_size["lon"]
                lat = min_lat + lat_idx * cell_size["lat"]

                chip_boundary = self.create_chip_boundary(lon, lat, cell_size)

                chip_polygon = ee.Geometry.Polygon([chip_boundary])

                raw_chips[cur_idx] = chip_polygon
                raw_chipids[cur_idx] = [lat_idx, lon_idx]

                # This is where the intersection between the grid cell and the area polygon is found.
                inter = chip_polygon.intersection(
                    **{"right": area_polygon, "maxError": 1}
                )

                # If the intersection is not empty, save the chip.
                # The intersection can be empty, if e.g. the grid cell is above open water
                if len(inter.getInfo()["coordinates"]) > 0:
                    cur_intersecting_chips.append(inter)
                    cur_intersecting_chips_ids.append(cur_idx)
                    if self.testing:
                        intersecting_chips.append(inter)
                        cur_intersecting_chips_ids.append(cur_idx)

                # Scaffolding for validation
                if len(intersecting_chips) != 0 and len(intersecting_chips) % 5 == 0:
                    print(len(intersecting_chips))

        # If there are any remaining intersecting chips, get them and export them to the Google Drive
        if len(cur_intersecting_chips) > 0:
            print("getting the last bunch")
            intersecting_chips += cur_intersecting_chips
            print("REMAINING CHIPS :", cur_intersecting_chips_ids)
            if not self.testing:
                self.get_DW_for_polygons(
                    cur_intersecting_chips, self.date_ranges, cur_intersecting_chips_ids
                )

        self.DBMS.write(
            "INSERT_FINISHED_SUBPOLY",
            {"_AREA_": self.area_name, "_POLYGON_INDEX_": str(area_polygon_index)},
        )

        return intersecting_chips, raw_chipids, raw_chips

    def export_single_DW_chip(self, dw_image, roi, start_date, ix):
        # Define export parameters.
        export_params = {
            "image": dw_image.clip(roi),
            "description": "land_cover_mode",
            "scale": 10,
            "region": roi,
            "fileFormat": "GeoTIFF",
            "fileNamePrefix": f'{ix}_{start_date.replace("-","_")}',
            "folder": f"{self.area_name}DynamicWorld",
        }

        # Start the export task.
        export_task = ee.batch.Export.image.toDrive(**export_params)
        export_task.start()

    def get_single_DW_chip(self, ix, start_date, end_date, roi):
        coordinates = self.flip_coords(roi["coordinates"][0])
        roi_polygon = Polygon(coordinates)

        new_row = pd.DataFrame([{"chipid": ix, "geometry": roi_polygon}])

        self.global_gdf = pd.concat([self.global_gdf, new_row], ignore_index=True)

    def get_single_DW_chip(self, ix, start_date, end_date, roi):
        try:
            tasks = ee.batch.Task.list()

            # Filter for tasks that are in the QUEUED state
            queued_tasks = [task for task in tasks if task.state in ["QUEUED", "READY"]]

            # If There are more than 3000 queued export tasks, take a breather
            while len(queued_tasks) > 3000:
                print("Queue Exceeding 3000 tasks.")
                print("Chilling for a bit...\n")
                time.sleep(10)

                queued_tasks = [
                    task for task in tasks if task.state in ["QUEUED", "READY"]
                ]

                continue

            # Filter the Dynamic World dataset.
            dw_image = (
                ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
                .filterDate(start_date, end_date)
                .filterBounds(roi)
                .select("label")
                .mode()
            )

            # Export the Dynamic World classification map for the given region and date range.
            self.export_single_DW_chip(dw_image, roi, start_date, ix)

            return 1

        except Exception as E:
            print(E)
            return 0

        return 1

    def get_DW_for_polygons(
        self, polygon_list, date_ranges, cur_intersecting_chips_ids
    ):
        failed = []
        succesful_exports = 0

        print(f"EXPORTING {len(polygon_list)} CHIPS FOR {len(date_ranges)} DATE RANGES")
        print(f"TOTAL EXPORTS: {len(polygon_list) * len(date_ranges)}")
        # Here we loop over each intersection between the chip grid and the area polygon
        for ix, polygon in tqdm(
            enumerate(polygon_list), desc="Getting and Exporting Each DW Chip from GEE"
        ):
            chip_id = cur_intersecting_chips_ids[ix]
            # Scaffold for validation
            has_printed = False

            # Semantic change
            roi = polygon

            try:
                # Error I "fixed" very early - would like to omit, but afraid of the consequences :(
                roi = self.flip_polygon(polygon)
            except:
                failed.append([ix] * len(date_ranges))
                continue

            # Here we loop over each date range
            # For each intersecting polygon we get the Dynamic World classifications for each date range
            for range_index in range(len(date_ranges)):
                start_date, end_date = (
                    date_ranges[range_index][0],
                    date_ranges[range_index][1],
                )

                # This mighttttt not be necessary now, however, not sure if it's worth the risk to remove it.
                if roi.getInfo()["type"] == "MultiPolygon":
                    for number, sub_poly in enumerate(roi.getInfo()["coordinates"]):
                        sub_roi = ee.Geometry.Polygon(sub_poly)
                        area_ = sub_roi.area().divide(10**6).getInfo()
                        if area_ > 100 and not has_printed:
                            has_printed = True
                            print(ix, "SUBPOLYGON AREA:", area_)

                        # Here we get the DW classifications for each sub-polygon
                        # Inside this function the classifications are exported to the Google Drive
                        succesful_exports += self.get_single_DW_chip(
                            str(chip_id)
                            + "-"
                            + chr(
                                97 + number
                            ),  # This ID addition is in case of multiple sub-polygons. This simply adds a letter to the ID (1-a, 1-b, 1-c, etc.)
                            start_date,
                            end_date,
                            sub_roi,
                        )

                else:
                    area_ = roi.area().divide(10**6).getInfo()
                    if area_ > 100 and not has_printed:
                        # This is a validation to see if there are any bugs in the design of the grid.
                        # They should.... not be able to exceed 100 km^2, but may very well be smaller.
                        has_printed = True
                        print(ix, "POLYGON AREA:", area_)

                    # Here we get the DW classifications for each sub-polygon
                    # Inside this function the classifications are exported to the Google Drive
                    succesful_exports += self.get_single_DW_chip(
                        chip_id, start_date, end_date, roi
                    )

                # Scaffold for validation
                if succesful_exports % 50 == 0:
                    print(f"{succesful_exports} SUCCESSFUL EXPORTS")

    def get_sub_area_grid_params(self):
        cell_sizes = []
        all_boundaries = []

        # Looping over each sub-polygon (if present)
        # and getting the boundaries and cell sizes for each
        for poly in self.area_polygons:
            # This simply returns the coordinates of the polygon
            poly_coords = self.get_coordinates(poly)

            # Getting the max and min lat and lon for the polygon - essentially the bounding box for the maximums size of the grid
            boundaries = self.get_polygon_boundaries([poly_coords])
            all_boundaries.append(boundaries)

            # Getting the cell size for the grid
            # While this is done for each sub-polygon, the cell size should, hopefully, be the same for all sub-polygons
            cell_size = self.get_cell_size(country_lat=np.mean(boundaries[:2]))
            cell_sizes.append(cell_size)

        return cell_sizes, all_boundaries

    def get_sub_area_DW_classification_map(self, cell_sizes, all_boundaries):
        """
        This essentially takes all the computed polygons so far, and creates the actual grid of 10km x 10km chips over the area.
        It loops over each sub-polygon, and creates the grid for each, and then finds the intersections between the grid and the sub-polygon.
        """

        all_raw_chips = []
        all_intersecting_chips = []
        all_raw_chipids = []

        # Looping over each sub-polygon-Index and creating the grid for each
        for sub_area_index in range(len(cell_sizes)):
            if sub_area_index in self.finished_subpolys:
                print(f"Skipping entire subregion with index {sub_area_index}")
                continue

            boundaries = all_boundaries[sub_area_index]
            cell_size = cell_sizes[sub_area_index]

            # Inside this function, the grid is created and the intersections are found
            # Additionally, the intersection-polygons are used as the ROI for the Dynamic World dataset
            # The Dynamic World classifications are accessed through the Google Earth Engine, and exported to the uses Google Drive
            # The Name of the folder is saved in the Database, such that the listen.py script can access the files, export them
            # transform them, and then upload them to the database.
            intersecting_chips, raw_chipids, raw_chips = self.create_country_grid(
                boundaries,
                cell_size,
                self.flip_polygon(self.area_polygons[sub_area_index]),
                sub_area_index,
            )
            all_raw_chips.append(raw_chips)
            all_intersecting_chips.append(intersecting_chips)
            all_raw_chipids.append(raw_chipids)

        self.intersecting_chips = all_intersecting_chips
        return all_raw_chips, all_intersecting_chips, all_raw_chipids

    def get_DW_classification(
        self,
        date_ranges: List[Tuple[str, str]] = [("2023-01-01", "2023-12-31")],
        area_name: str = "Denmark",
        area_polygons: Optional[List[ee.Geometry.Polygon]] = None,
    ) -> (List[Dict], List[List], List[Dict]):
        """
        This Function takes the stated area as well as the assigned date_ranges and performs a series of steps in order to create the LULC classification maps.
        First it'll get the geographical shape of the country, using the LSIB system. If the country shape consists of multiple geographies, it'll create a polygon for each of them.
        Then each polygon will be used in order to create the smallest possible 10km x 10km grid over the geography, and then find the intersections between the grid and the geography.
        These intersections are used as the REGION OF INTEREST (ROI) for the Dynamic World dataset, and then the classification maps are created and exported using the Google Earth Engine.
        """

        self.date_ranges = date_ranges

        # Super random case - if you witsh to run the code on a non-country area.
        if isinstance(area_polygons, type(None)):
            # Get the coordinates of the country shape
            coords = self.get_country_LSIB_coordinates(area_name)

            print("Got Coordinates")

            # Create the list of polygons for the country
            self.area_polygons = self.create_polygon(coords, flip=False)

            print("Created Polygons")

            # Using the coordinates from the country shape, get the boundaries of the country
            # Minimum latitude, maximum latitude, minimum longitude, maximum longitude
            # This allows us to calcualte the cell size for the grid
            cell_sizes, all_boundaries = self.get_sub_area_grid_params()

            print("Create Grid")

        else:
            # No real other scenario is planned for, but if you have a list of polygons, you can use them here.
            do_something_else = False

        # A lot happens in here
        print("Creating Intersection Chip Grid")
        return self.get_sub_area_DW_classification_map(cell_sizes, all_boundaries)


if __name__ == "__main__":
    pass
