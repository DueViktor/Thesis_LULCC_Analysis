from sys import path
#path.append('../')
from src.DataBaseManager import DBMS
import numpy as np
from src.verify_DW_crops import create_chip_chunks,sql_list_strings
from tqdm import tqdm
import time
import os
import sys


import geopandas as gpd
import rasterio
from rasterio.windows import Window
from rasterio.features import rasterize
from rasterio.transform import from_origin,from_bounds
from rasterio.io import MemoryFile

from typing import List, Dict

import pandas as pd

from collections import defaultdict
import json

from config import LAND_COVER_LEGEND
numeric_legend = {v:k for k,v in LAND_COVER_LEGEND.items()}

def replace_tif(path):
    # Read the first band back from the saved file to return
    with rasterio.open(path) as dataset:
        band1 = dataset.read(1)
    
    return band1

def merge_df(df1,df2):
    # Merging DataFrames on multiple columns with an outer join
    merged_df = pd.merge(df1, df2, on=['2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023'], how='outer', suffixes=('_df1', '_df2'))

    # Fill NaN with 0s for numerical operation
    merged_df['num_tiles_df1'].fillna(0, inplace=True)
    merged_df['num_tiles_df2'].fillna(0, inplace=True)

    # Summing up the 'num_tiles' columns
    merged_df['num_tiles'] = merged_df['num_tiles_df1'].astype(int) + merged_df['num_tiles_df2'].astype(int)

    # Select the relevant columns (if desired) or drop the intermediary columns
    final_df = merged_df.drop(columns=['num_tiles_df1', 'num_tiles_df2'])

    return final_df


def calculate_combined_bounds(datasets):
    """Calculate the combined bounds of all datasets."""
    bounds = [ds.bounds for ds in datasets]
    min_left = min(b.left for b in bounds)
    min_bottom = min(b.bottom for b in bounds)
    max_right = max(b.right for b in bounds)
    max_top = max(b.top for b in bounds)
    return (min_left, min_bottom, max_right, max_top)

def read_and_pad_geotiffs(paths):
    datasets = [rasterio.open(path) for path in paths]
    combined_bounds = calculate_combined_bounds(datasets)
    min_left, min_bottom, max_right, max_top = combined_bounds

    # Determine the output resolution as the finest (smallest) of the input resolutions
    pixel_size_x = min(ds.res[0] for ds in datasets)
    pixel_size_y = min(ds.res[1] for ds in datasets)
    
    # Calculate output dimensions more carefully to avoid off-by-one errors
    out_width = int(round((max_right - min_left) / pixel_size_x))
    out_height = int(round((max_top - min_bottom) / pixel_size_y))
    
    # Prepare metadata for output
    out_meta = datasets[0].meta.copy()
    out_meta.update({
        "driver": "GTiff",
        "height": out_height,
        "width": out_width,
        "transform": rasterio.transform.from_bounds(min_left, min_bottom, max_right, max_top, out_width, out_height)
    })
    
    # Process each dataset individually
    for dataset, path in zip(datasets, paths):
        data = dataset.read(1)
        # Calculate offsets more accurately
        off_x = int(round((dataset.bounds.left - min_left) / pixel_size_x))
        off_y = int(round((max_top - dataset.bounds.top) / pixel_size_y))
        # Prepare output filename
        dirname, basename = os.path.split(path)
        filename, ext = os.path.splitext(basename)
        output_filename = os.path.join(dirname, f"{filename}{ext}")
        # Create output dataset
        with rasterio.open(output_filename, 'w', **out_meta) as out_ds:
            # Initialize output with zeros
            out_ds.write(np.zeros((out_height, out_width), dtype=out_meta['dtype']), 1)
            # Write data to the new file, adjusting for the calculated window
            window = Window(off_x, off_y, min(data.shape[1], out_width - off_x), min(data.shape[0], out_height - off_y))
            out_ds.write(data, 1, window=window)


    # Close all datasets
    for ds in datasets:
        ds.close()


def replace_raster_band1(original_path, new_path, new_band1_data):
    """
    Create a new raster file with the first band replaced by new data, preserving the other bands and original geographic information.

    Parameters:
    - original_path: str, path to the original raster file.
    - new_path: str, path where the new raster file will be saved.
    - new_band1_data: np.array, new data for the first band, must match the dimensions of the original first band.

    Returns:
    - None
    """
    # Open the original raster file to read metadata and data
    with rasterio.open(original_path, 'r') as src:
        # Check if the new data dimensions match the first band's dimensions
        if new_band1_data.shape != (src.height, src.width):
            raise ValueError("New data dimensions do not match the original first band's dimensions")

        # Copy metadata from the original file
        meta = src.meta.copy()

        # Create a new raster file using the same metadata
        with rasterio.open(new_path, 'w', **meta) as dst:
            # Write new data to the first band
            dst.write(new_band1_data, 1)

            # Write original data to all other bands
            for i in range(2, src.count + 1):
                band_data = src.read(i)
                dst.write(band_data, i)


def polygon_to_raster(gdf, output_path, pixel_size=10):
    """
    Convert a GeoDataFrame of polygons and landcover IDs to a raster file on disk.

    Args:
        gdf (geopandas.GeoDataFrame): GeoDataFrame containing polygon geometries and a 'category_id' column.
        output_path (str): Path where the raster file will be saved.
        pixel_size (float): Desired pixel size in meters.

    Returns:
        np.ndarray: An array representing the first band of the created raster dataset.
    """
    # Ensure the GeoDataFrame has a projected CRS suitable for meters
    if gdf.crs.is_geographic:
        # Assuming the data is in a CRS that can be projected to a UTM automatically
        gdf = gdf.to_crs(gdf.estimate_utm_crs())

    # Calculate bounds and dimensions
    bounds = gdf.total_bounds
    width = int((bounds[2] - bounds[0]) / pixel_size)
    height = int((bounds[3] - bounds[1]) / pixel_size)

    # Define the transformation
    transform = from_bounds(*bounds, width=width, height=height)

    # Rasterize the data
    out_array = rasterize(
        [(shape, val) for shape, val in zip(gdf.geometry, gdf['category_id'])],
        out_shape=(height, width),
        transform=transform,
        fill=0,  # Background value
        all_touched=True,
        dtype=rasterio.uint8
    )

    # Save raster data to file
    with rasterio.open(
        output_path, 'w',
        driver='GTiff',
        height=height,
        width=width,
        count=1,
        dtype='uint8',
        crs=gdf.crs,
        transform=transform
    ) as dataset:
        dataset.write(out_array, 1)

    # Read the first band back from the saved file to return
    with rasterio.open(output_path) as dataset:
        band1 = dataset.read(1)
    
    return band1





def save_raster(path, array, transform=None, crs='EPSG:4326'):
    """
    Save a numpy array as a GeoTIFF file.

    Args:
    path (str): Path including filename where the raster will be saved.
    array (numpy.ndarray): 2D numpy array containing the raster data.
    transform (rasterio.Affine, optional): Affine transform for the raster. If None, a default will be created.
    crs (str, optional): Coordinate reference system for the raster. Defaults to 'EPSG:4326'.
    """
    # Default transform (assuming each pixel is 1x1 degree, change as needed)
    if transform is None:
        transform = from_origin(-180, 90, 1, 1)

    # Define metadata for the raster file
    metadata = {
        'driver': 'GTiff',
        'height': array.shape[0],
        'width': array.shape[1],
        'count': 1,
        'dtype': array.dtype,
        'crs': crs,
        'transform': transform
    }

    # Write the array data to a GeoTIFF file
    with rasterio.open(path, 'w', **metadata) as dst:
        dst.write(array, 1)



def make_sequence_df(changes,change_dpath="data/dynamicity/change_sequences.csv"):
    years = [i for i in range(2016,2024)]

    

    sequence_dict = defaultdict(int)
    for chipid,dd in changes.items():

        for pix, change_sequence in dd.items():
            
            change_sequence_str = '|'.join([str(i) for i in change_sequence])

            sequence_dict[change_sequence_str]+=1

                
        

    sequence_frame_dict = {str(y):[] for y in years}
    sequence_frame_dict['num_tiles'] = []
            
    lens = 0
    x = 0 
    for change_sequence_str,counts in sequence_dict.items():

            
        for year_ix,cat_id in enumerate(change_sequence_str.split('|')):
            
            sequence_frame_dict[str(years[year_ix])].append(int(cat_id))

        if len(change_sequence_str.split('|')) != 8:
            for j in range(8-len(change_sequence_str.split('|'))):
                sequence_frame_dict[str(years[-1-j])].append(0)

        
        sequence_frame_dict["num_tiles"].append(int(counts))


        
        x+=1

    

    sequence_frame = pd.DataFrame(sequence_frame_dict)
    
    #sequence_frame.to_csv("deubug_dis.csv")

    # if the change path exists then do this
    if os.path.exists(change_dpath):
        change_df = pd.read_csv(change_dpath)[list(sequence_frame.columns)]
        if 1==0:
            print(sequence_frame.columns)
            print(change_df.columns)

            for ix in range(len(change_df.columns)):
                print(change_df.columns[ix],type(change_df.columns[ix]),change_df[change_df.columns[ix]][0],type(change_df[change_df.columns[ix]][0]))
                print(sequence_frame.columns[ix],type(sequence_frame.columns[ix]),sequence_frame[sequence_frame.columns[ix]][0],type(sequence_frame[sequence_frame.columns[ix]][0]))
                print('\n\n')
            

        sequence_frame = merge_df(change_df,sequence_frame)
        print("SUCCESSFUL MERGE")

    


    return sequence_frame


def delete_file(file_path):
    """
    Deletes a file at the specified path.

    Parameters:
    - file_path: str, the path to the file to be deleted.

    Returns:
    - None, but prints a message confirming deletion or an error.
    """
    try:
        os.remove(file_path)
    except:
        pass


# Example usage:
# delete_file('path_to_your_file.txt')


# Example usage:
# import numpy as np
# array = np.random.randint(0, 255, (100, 100), dtype=np.uint8)  # Example data
# save_raster('path_to_output.tif', array)



def get_all_chipids(DB):

    AREA = "Denmark"

    Q1 = "GET_ONLY_CHIPIDS_FROM_AREA"
    Q1_params = {"_AREA_":AREA}


    chips = DB.read(Q1,params=Q1_params)
    og_size = chips.chipid.nunique()

    # if "data/dynamicity/dynamicity.csv" exists
    if os.path.exists("data/dynamicity/dynamicity.csv"):
        chipids = pd.read_csv("data/dynamicity/dynamicity.csv")['chip'].unique().tolist()

        chips = chips[~chips["chipid"].isin(chipids)]

    new_size = chips.chipid.nunique()

    print(f"OG: {og_size} \nNew Size: {new_size}")

    return chips



def process_gdf(gdf,numeric_legend=numeric_legend):
    gdf["geometry"] = gdf["geometries"]
        
    #creat ecolumn category_id using the numeric_legend and column name
    gdf["category_id"] = gdf["name"].map(numeric_legend)

    return gdf




def main(chips: pd.DataFrame,DB,AREA = 'Denmark',verbose=True):

    # Query name
    Q2 = "GET_CHIP_LANDCOVER"


    # Duration list : This list is for calculating remaining time
    durations = []

    # the years to process
    years = [i for i in range(2016,2024)]

    # Get chipids from dataframe 
    chips_to_handle = chips.chipid.values.tolist()

    # Counter to keep track of while loop
    cnum = 1

    # Scaffolding
    num_chips = len(chips_to_handle)
    
    global_start_time = time.time()

    # loop over all current chips
    while len(chips_to_handle)>0:
        
        # Initialized essential datastructures
        dynamicity = defaultdict(list)
        changes = defaultdict(dict)
        

        # Item runtime is calculated from here
        start_time = time.time()
        
        # Get the first chip from the list
        chip = chips_to_handle[0]
        changes[chip] = defaultdict(list)
        print(f"BEGINNING ON CHIP {chip} nr {cnum}")
        

        # Localt chip data structure
        chip_data = {}


        # This list simply keeps track of whether the chips have multiple shapes
        shapes = []


        if verbose:
            print("GETTING CHIP FOR EACH YEAR")
        # Loop over each year
        redo = False
        for year in tqdm(years,desc="Getting each yearly chip"):

            # Query paramters 
            Q2_params = {"_CHIPID_":chip,"_YEAR_":str(year),"_AREA_":AREA}

            # This try except is just in case we are clogging the DB. Happens occationally
            try:
                # Get the current chip as a GDF
                landcover = DB.read(Q2,params=Q2_params,geom_col="geometries",geom_query=True)
            except Exception as e:
                # Else just take a nap and try again
                time.sleep(30)
                # write exception to std err
                print(e,file=sys.stderr)
                redo = True
                break
                
            # make geometry column from geometries to geometry and rename columns accoding to our legend
            landcover = process_gdf(landcover)

   
            # Convert the GDF to a raster file
            chip_data[year] = polygon_to_raster(landcover,output_path = f"data/dynamicity/rastertifs/{chip}_{year}.tif")

            # append the shaoe of each year of the chip to the shapes list
            shapes.append(chip_data[year].shape)

        # If the DB was clogged just rerun the current chip
        if redo:
            continue

        # If the chip from all years have different shapes then resize the smaller tifs
        if len(set(shapes))>1:
            print ("reshaping")
            print("PRESENT SHAPES",set(shapes))
            read_and_pad_geotiffs([f"data/dynamicity/rastertifs/{chip}_{year}.tif" for year in years])
            for year in years:
                chip_data[year] = replace_tif(f"data/dynamicity/rastertifs/{chip}_{year}.tif")


        else:

            if verbose:
                print("DIMENSIONS MATCH")

    


        
        maxlen = max([v.flatten().shape[0] for v in chip_data.values()])

        raster_array = [[] for i in range(maxlen)]

        print("Array Length:",list(chip_data.values())[0].flatten().shape[0])

        
        for k,rgdf in tqdm(chip_data.items(),desc="creating the change sequences"):
            # save the value of the pixel under its flattened index in the changes dict list
            for ix,i in enumerate(rgdf.flatten()):
                # the value of the chip in that given pixel is stored in changes as well as the raster array
                changes[chip][ix].append(i)
                raster_array[ix].append(i)
        if verbose:
            print("SEQUENCE CREATED")
        # This simply converts the number of different values present in the tile over time to a number
        # s.t a change sequence [1,1,1,1,1,1,1,1] will have num changes = 0
        raster_array_ = []
        raster_array_stats = []
        for sublist in raster_array:
            if 0 in sublist:
                raster_array_.append(-99)
            else:
                raster_array_.append(len(set(sublist))-1)
                raster_array_stats.append(len(set(sublist))-1)

        raster_array = raster_array_

        # Append the dynamic statistics to the dynamicity frame 
        dynamicity["chip"].append(chip)
        dynamicity["num_changed_tiles"].append(sum(raster_array_stats))
        dynamicity["median"] = np.median(raster_array_stats)
        dynamicity["max"] = np.max(raster_array_stats)

        if verbose:
            print("ANALYSIS CONCLUDED")

        # reshape the raster to a matrix
        raster_array = np.array(raster_array).reshape(rgdf.shape)


        if verbose:
            print("SAVING DYNAMIC COUNTS AS GEOTIFF")
        # this function effectively saves the dynamic array on a copy of the current chip to preserve the geographic information in the tiff
        replace_raster_band1(original_path=f"data/dynamicity/rastertifs/{chip}_{year}.tif", 
                            new_path = f"data/dynamicity/rastertifs/{chip}_dynamics.tif", 
                            new_band1_data=raster_array)
        
        if verbose:
            print("DELTA SAVING SEQUENCE DF")
        # this performs a delta save of the current sequence DF - can be a bit resource consuming
        sequence_df = make_sequence_df(changes)
        sequence_df.to_csv("data/dynamicity/change_sequences.csv")

        if verbose:
            print("DELTA SAVING DYNAMICITY DF")

        # Appends the current chip to previous chips 
        # if the dynamicity.csv file does not exist then create it
        if os.path.exists("data/dynamicity/dynamicity.csv"):
            pd.DataFrame(dynamicity).to_csv("data/dynamicity/dynamicity.csv",mode='a',header=False)
        else:
            pd.DataFrame(dynamicity).to_csv("data/dynamicity/dynamicity.csv")


        if verbose:
            print("DELETING SUPPORT FILES")
        # deletes the support files
        for year in years:
            delete_file(f"data/dynamicity/rastertifs/{chip}_{year}.tif")

        # removes the chip if it has successfully run
        print("FINISHED", chips_to_handle.pop(0))

        # increment the count
        
        
        # Calculate the ets 
        end_time = time.time()
        duration = (end_time-start_time)/60
        durations.append(duration)
        print("DONE")
        ETA = np.mean(durations)*(num_chips-cnum)

        print(f"CURRENT CHIP TOOK {str(duration)} MINUTES")
        print(f"{num_chips-(cnum)} CHIPS LEFT. ESTIMATED COMPLETION TIME IN {str(ETA)} MINUTES WITH AVERAGE CHIP RUNTIME OF {str(np.mean(durations))}\n\n")

        cnum+=1
        

        
        
    





if __name__ == "__main__":
    DB = DBMS()

    

    print("Getting New ")
    chips = get_all_chipids(DB)

    main(chips,DB)

