# from sys import path
# path.append('..')
from src.DataBaseManager import DBMS
import geopandas as gpd
import pandas as pd
import folium
from config import (
    LAND_COVER_PALETTE,
    LAND_COVER_LEGEND,
    EPSG_MAPPING,
    LEGEND_TO_PALETTE,
)
import matplotlib.pyplot as plt
from IPython.display import IFrame

from tqdm import tqdm

from src.measure_LULC import create_chip_chunks


def get_all_chips_from_area(area_name):
    """
    Get all chips from an area
    :param area_name: Name of the area
    :return: List of chips
    """
    dbms = DBMS()

    chips = dbms.read("GET_ONLY_CHIPIDS_FROM_AREA", {"_AREA_": area_name})[
        "chipid"
    ].tolist()

    return chips


def sql_list_strings(list_):
    return ",".join(["'" + str(i) + "'" for i in list_])


DB = DBMS()


def get_DW_chips(chip_ids, year, DB=DB):
    results = DB.read(
        "GET_ONLY_DW_LANDCOVER",
        {
            "_CHIPIDS_": sql_list_strings(chip_ids),
            "_AREA_": "Denmark",
            "_START_YEAR_": year,
            "_END_YEAR_": year,
        },
        geom_query=True,
    )

    return results


def verify_DW_crops(year=2016):
    print("Getting chips from", year)

    gdfs = []

    # get all chips
    chips = get_all_chips_from_area(area_name="Denmark")
    chip_chunks = create_chip_chunks(chips, 20)

    print("Reading LandbrugsGIS data")
    dpath = f"data/LandbrugsGIS/Markblokke{year}.shp"
    df = gpd.read_file(dpath)
    df = df.to_crs(epsg=4326)

    df.sindex

    for chip_chunk in tqdm(chip_chunks, desc="Looping through chunks..."):
        print("Getting DW data")
        DW_data = get_DW_chips(chip_chunk, str(year))
        # dissolve the data by name
        print("Dissolving...")
        DW_data_dissolved = DW_data.dissolve(by="name").reset_index()

        DW_data_dissolved.sindex

        print("Finding intersecting areas")
        intersecting_areas = gpd.overlay(DW_data_dissolved, df, how="intersection")

        intersecting_areas_DK = intersecting_areas.to_crs(
            f'EPSG:{EPSG_MAPPING["Denmark"]}'
        )
        # caluclate the area of the intersecting areas grouped by year and landcover type
        print("Calculating areas...")
        intersecting_areas_DK["area"] = intersecting_areas_DK["geometry"].area / 10**6
        intersecting_areas_DK["year"] = year

        gdfs.append(intersecting_areas_DK)

    # save counts

    aggregated = pd.concat(gdfs)

    output = (
        aggregated[["name", "area"]]
        .groupby(["name"])
        .sum()
        .sort_values(by="area", ascending=False)
    )
    output["percent"] = output["area"].apply(
        lambda x: round(x / output["area"].sum() * 100, 2)
    )
    output.to_csv(f"output/verification/intersecting_areas_{year}.csv")

    # save aggregated as .shp
    aggregated.to_file(f"output/verification/intersecting_areas_{year}.shp")


def plots(aggregated, year=2016):
    # save map

    m = folium.Map(location=[56, 10], zoom_start=6)

    intersecting_areas = aggregated  # .reset_index()

    # add the dissolved intersecting multipolygon to the map
    intersecting_areas_dissolved = intersecting_areas[
        intersecting_areas["name"] != "Crops"
    ].dissolve()
    folium.GeoJson(
        intersecting_areas_dissolved["geometry"],
        style_function=lambda x: {
            "fillColor": f'{"red"}',
            "color": f'{"red"}',
            "weight": 2,
        },
    ).add_to(m)

    # intersecting_areas_dissolved = intersecting_areas[intersecting_areas['name']=='Crops'].dissolve()
    # folium.GeoJson(intersecting_areas_dissolved['geometry'],style_function=lambda x: {'fillColor': f'{"green"}', 'color': f'{"green"}', 'weight': 2}).add_to(m)

    # save the map as html
    # save the map as html
    m.save(f"output/verification/intersecting_areas_{year}.html")


def read_verification_results(year=2016):
    table = pd.read_csv(f"../output/verification/intersecting_areas_{year}.csv")
    print("loaded table")
    # gdf = gpd.read_file(f"../output/verification/intersecting_areas_{year}.shp")
    # print('loaded gdf')
    # gdf = gdf.to_crs(epsg=EPSG_MAPPING["Plotting"])[['name','geometry']].dissolve(by='name')
    # print('Dissolved')
    return table, None  # ,gdf


def plot_tabular_data(table, year, save=False, metric="percent"):
    table["plot_name"] = table["name"]
    table.loc[table["percent"] < 1, "plot_name"] = "Other"
    plt.style.use("fivethirtyeight")

    # set colors using LEGEND_TO_PALETTE dictionary except for other (grey)'
    plot_frame = table[["plot_name", metric]].groupby("plot_name").sum()
    colors = [LEGEND_TO_PALETTE.get(name, "grey") for name in plot_frame.index]

    plot_frame.plot(
        kind="pie",
        y=metric,
        figsize=[16, 9],
        fontsize=12,
        autopct="%1.1f%%",
        colors=colors,
        legend=False,
        title=f"Overlapping Dynamic World Categories for official cropfields in Denmark {year}",
    )

    if save:
        plt.savefig(f"output/verification/intersecting_areas_PIE_{year}.png")
    else:
        plt.show()


def plot_misclassified_croplands_map(gdf, year=2016, save=False):
    gdf.crs = "EPSG:4326"

    m = folium.Map(location=[56, 10], zoom_start=6)

    for idx, row in gdf.iterrows():
        if idx != "Crops":
            edge_color = "red"
        else:
            edge_color = "blue"
            continue

        folium.GeoJson(
            row["geometry"],
            style_function=lambda x: {
                "fillColor": f"{edge_color}",
                "color": f"{edge_color}",
                "weight": 2,
            },
        ).add_to(m)

    if save:
        m.save(f"output/verification/intersecting_areas_MAP_{year}.html")

    return m


def print_verification_results(year=2016):
    data = pd.read_csv(f"../output/verification/intersecting_areas_{year}.csv")

    # plot pie_chart from data using percent column
    data = data.set_index("name")
    data = data.drop("area", axis=1)
    data.plot.pie(y="percent", autopct="%1.1f%%")
    # remove legend
    plt.legend().remove()
    plt.tight_layout()
    plt.show()

    # m = IFrame(src=f, width=800, height=350)

    return f"../output/verification/intersecting_areas_{year}.html", data


if __name__ == "__main__":
    for year in [2016, 2017, 2023]:
        verify_DW_crops(year)
