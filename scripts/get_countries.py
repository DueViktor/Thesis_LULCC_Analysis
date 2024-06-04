"""
Iterate over a list of countries and contacts Google Earth Engine and gets the data for each country.
"""

from src.dynamic_world import DynamicWorldBasemap

for country in [
    "Denmark",
    # "Oman",
    "Estonia",
    "Latvia",
    "Israel",
    "Netherlands",
    "Ireland",
]:
    print("=" * 20, country.upper(), "=" * 20)

    DWB = DynamicWorldBasemap(
        area_name=country,
        date_ranges=[
            ("2016-01-01", "2016-12-31"),
            ("2017-01-01", "2017-12-31"),
            ("2018-01-01", "2018-12-31"),
            ("2019-01-01", "2019-12-31"),
            ("2020-01-01", "2020-12-31"),
            ("2021-01-01", "2021-12-31"),
            ("2022-01-01", "2022-12-31"),
            ("2023-01-01", "2023-12-31"),
        ],
    )
    DWB.create()
