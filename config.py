from pathlib import Path

ROOT = Path(__file__).parent

DATA_DIR = ROOT / "data"

DENMARK_DW_DIR = DATA_DIR / "DenmarkDynamicWorld"

# Data directories
DYNAMIC_WORLD_DIR = DATA_DIR / "Dynamic_World"
TIFS_DIR = DATA_DIR / "TIFs"
TIFS_ANALYSIS_DIR = DATA_DIR / "TIFs_Analysis"
ENERGI_STYRELSEN_DIR = DATA_DIR / "EnergiStyrelsen"

# Plotting directories
PLOTS_DIR = ROOT / "plots"
RASTER_TIF_PLOTS_DIR = PLOTS_DIR / "Raster_TIF"
GEOMETRY_TIF_PLOTS_DIR = PLOTS_DIR / "Geometry_TIF"
EXAMPLE_ASSETS = ROOT / "examples" / "assets"
EXAMPLE_VERIFICATION_FILES = EXAMPLE_ASSETS / "verification-files"

# CONSTANTS
LAND_COVER_PALETTE = {
    0: "#419BDF",
    1: "#397D49",
    2: "#88B053",
    3: "#7A87C6",
    4: "#E49635",
    5: "#DFC35A",
    6: "#C4281B",
    7: "#A59B8F",
    8: "#B39FE1",
    9: "#0984E3",
    10: "#2d3436",
}

LAND_COVER_LEGEND = {
    0: "Water",
    1: "Trees",
    2: "Grass",
    3: "Flooded vegetation",
    4: "Crops",
    5: "Shrub & Scrub",
    6: "Built Area",
    7: "Bare ground",
    8: "Snow & Ice",
    9: "Wind Turbine",
    10: "Solar Panel",
}

LAND_COVER_DESC = {
    0: "Permanent and seasonal water bodies",
    1: "Includes primary and secondary forests, as well as large-scale plantations",
    2: "Natural grasslands, livestock pastures, and parks",
    3: "Mangroves and other inundated ecosystems",
    4: "Include row crops and paddy crops",
    5: "Sparse to dense open vegetation consisting of shrubs",
    6: "Low- and high-density buildings, roads, and urban open space",
    7: "Deserts and exposed rock",
    8: "Permanent and seasonal snow cover",
    9: "Wind turbine coordinates with their rotor-blade radius",
    10: "Solar panel shapes",
}

LEGEND_TO_PALETTE = {v: LAND_COVER_PALETTE[k] for k, v in LAND_COVER_LEGEND.items()}

LEGEND_TO_PALETTE['Artificial Land Use'] = "#e17055"
LEGEND_TO_PALETTE['Natural Areas'] = "#00b894"
LEGEND_TO_PALETTE['Renewable Energy'] = "#00cec9"



SATLAS_SOLAR_URL = "https://pub-956f3eb0f5974f37b9228e0a62f449bf.r2.dev/outputs/renewable/_PLACEHOLDER__solar.shp.zip"
SATLAS_WIND_URL = "https://pub-956f3eb0f5974f37b9228e0a62f449bf.r2.dev/outputs/renewable/_PLACEHOLDER__wind.shp.zip"
DEFAULT_WIND_TURBINE_RADIUS = {
    "2016": 52.60484442693974,
    "2017": 109.23853211009174,
    "2018": 129.1451612903226,
    "2019": 86.35833333333333,
    "2020": 127.29411764705883,
    "2021": 152.47413793103448,
    "2022": 140.96969696969697,
    "2023": 155.25925925925927,
}

# EPSG Mapping - generated by copilot
EPSG_MAPPING = {
    "Plotting": 4326,
    "World": 6933,  # validated
    "Denmark": 25832,  # validated
    "Estonia": 3301, # måske
    "Netherlands": 28992, # måske
    "Israel": 2039,  # måske
}

DK_AREA_MAPPING = {
    "0": "Fanø",
    "1": "Rømø",
    "2": "Læsø",
    "3": "Ærø",
    "4": "Samsø",
    "5": "Bornholm",
    "6": "Møn",
    "7": "Mors",
    "8": "Langeland",
    "9": "Als",
    "10": "Lolland",  # HERFRA ER DE RET STORE, SÅ OVERVEJ AT BRUG MAX_CHIP_IDS OG CHUNK_IDX TIL AT PLOTTE LIDT AF GANGEN
    "11": "Fyn",
    "12": "Nørrejyske Ø",
    "13": "Sjælland + Falster",
    "14": "Midt-og-Sønderjylland",
}

# sizes in km2
DK_ISLAND_SIZES = {
    "Fanø": 56,
    "Rømø": 129,
    "Læsø": 118,
    "Ærø": 88,
    "Samsø": 112,
    "Bornholm": 588.3,
    "Møn": 218,
    "Mors": 363.3,
    "Langeland": 284,
    "Als": 321,
    "Lolland": 1243,
    "Fyn": 3100,
    "Nørrejyske Ø": 4685,
    "Sjælland + Falster": 7031+514,
}

COUNTRY_SIZES = {
    "Denmark": 42952,
    "Estonia": 45339,
    "Netherlands": 41850,
    "Israel": 22145,
}

# https://data.worldbank.org/indicator/AG.LND.TOTL.K2?locations=DK-NL-IL-EE
TERRESTIAL_COUNTRY_SIZES = {
    "Israel": 21640,
    "Netherlands": 33670,
    "Denmark":40000,
    "Estonia":42750
}


# based on https://colorhunt.co/palette/32012f524c42e2dfd0f97300
# https://matplotlib.org/stable/users/explain/colors/colors.html
COUNTRY_COLORS = {
        "Denmark": "tab:orange",
        "Estonia": "tab:green",
        "Netherlands": "tab:blue",
        "Israel": "tab:olive",
    }

# https://flatuicolors.com/palette/au
CLUSTER_COLORS = {
    "0": "#f9ca24",
    "1": "#e056fd",
    "2": "#686de0",
    "3": "#ff7979",
    "4": "#6ab04c",
    "unassigned": "#535c68",
}