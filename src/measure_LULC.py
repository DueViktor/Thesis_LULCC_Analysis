import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import time
import traceback

from tqdm import tqdm

from src.DataBaseManager import DBMS


def get_all_chips_from_area(area_name, year_from, year_to):
    """
    Get all chips from an area
    :param area_name: Name of the area
    :return: List of chips
    """
    dbms = DBMS()

    chips = dbms.read(
        "GET_CHIPIDS_FROM_AREA",
        {"_AREA_": area_name, "_YEAR_FROM_": str(year_from), "_YEAR_TO_": str(year_to)},
    )["chipid"].tolist()

    return chips


def upload_results(gdf):
    """
    Upload results
    :param gdf: GeoDataFrame
    :return: None
    """
    dbms = DBMS()

    dbms.add_land_use_change(gdf)


def sql_list_from_list(list):
    """
    Convert a list to a SQL list
    :param list: List
    :return: SQL list
    """
    sql_list = "("
    for i in range(len(list)):
        sql_list += f"'{list[i]}'"
        if i != len(list) - 1:
            sql_list += ","
    sql_list += ")"

    return sql_list


def calculate_lulc_polygon_intersection(area_name, from_year, to_year, chipids):
    """
    Calculate the intersection of LULC polygons with a polygon
    :param area_name: Name of the area
    :param from_year: Year to start from
    :param to_year: Year to end at
    :param chip_id: ID of the chip
    :return: Intersection of LULC polygons with a polygon
    """
    dbms = DBMS()

    chipid_sql_list = sql_list_from_list(chipids)

    land_use_change_gdf = dbms.read(
        "CALCULATE_LULC_INTERSECTION",
        {
            "_AREA_": area_name,
            "_FROM_YEAR_": str(from_year),
            "_TO_YEAR_": str(to_year),
            "_CHIPID_LIST_": chipid_sql_list,
        },
    )

    return land_use_change_gdf


def format_for_db(gdf, area, from_year, to_year):
    """
    Format the GeoDataFrame for the database
    :param gdf: GeoDataFrame
    :return: Formatted GeoDataFrame
    """
    DB_cols = [
        "area",
        "chipid",
        "year_from",
        "year_to",
        "lulc_category_from",
        "lulc_category_to",
        "area_km2",
        "from_category_area_sq_km",
        "percent_change",
        "geom",
    ]

    gdf["area"] = area
    gdf["year_from"] = from_year
    gdf["year_to"] = to_year
    gdf = gdf.rename(
        columns={
            "land_use_change": "geom",
            "preceding_year_name": "lulc_category_from",
            "current_year_name": "lulc_category_to",
            "intersection_area_sq_km": "area_km2",
            "preceding_area_sq_km": "from_category_area_sq_km",
        }
    )

    return gdf[DB_cols]


def create_chip_chunks(chips, chunk_size):
    """
    Create chunks of chips
    :param chips: List of chips
    :param chunk_size: Size of the chunk
    :return: Chunks of chips
    """
    chunks = []
    for i in range(0, len(chips), chunk_size):
        chunks.append(chips[i : i + chunk_size])

    return chunks


def calculate_lulc_for_country(country_name, years, chunk_size=8):
    """
    Calculate the LULC for a country
    :param country_name: Name of the country
    :param from_year: Year to start from
    :param to_year: Year to end at
    :return: LULC for a country
    """

    for year_from in tqdm(years[:-1], desc="from year"):
        year_to = years[years.index(year_from) + 1]
        print(f"getting all chips for {country_name}....")
        chips = get_all_chips_from_area(country_name, year_from, year_to)
        print(f"got {len(chips)} chips for {country_name}....")

        chip_chunks = create_chip_chunks(chips, chunk_size)

        for chip_chunk in tqdm(
            chip_chunks,
            desc=f"Looping through each chunk consisting of {chunk_size} chips",
        ):
            gdf = calculate_lulc_polygon_intersection(
                country_name, year_from, year_to, chip_chunk
            )

            gdf = format_for_db(gdf, country_name, year_from, year_to)

            upload_results(gdf)


# Create a function that sends an email with the exception from my try except statement


def send_email(password):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    # Email addresses
    sender_email = "aske.osv@gmail.com"
    receiver_email = "askm@itu.dk"

    # Create the email head
    email = MIMEMultipart()
    email["From"] = sender_email
    email["To"] = receiver_email
    email["Subject"] = "Error in LULC calculation"

    # Add the body to the email

    with open("error_log.txt", "r") as file:
        body = file.read()
        email.attach(MIMEText(body, "plain"))

    # Create SMTP session for sending the mail
    session = smtplib.SMTP("smtp.gmail.com", 587)
    session.starttls()  # enable security
    session.login(sender_email, password)  # login with mail_id and password
    text = email.as_string()
    session.sendmail(sender_email, receiver_email, text)

    # Terminate the session
    session.quit()


def main():
    """
    Main function
    """

    years = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
    # years = [2016,2023]

    countries = ["Denmark", "Estonia"]

    # make try except, but if it fails, write the exception to the error_file
    c = 1

    while True:
        try:
            for country in tqdm(countries, desc="Looping through each country"):
                print("Calculating LULC for:".upper(), country.upper())
                calculate_lulc_for_country(country, years)

        except Exception as e:
            # Open the file in append mode to add to the file
            with open("error_log.txt", "a") as file:
                # Writing the exception as a string
                file.write(f"An exception occurred at iteration {c}: {str(e)}\n")

                # Optionally, write the full traceback
                file.write("Detailed traceback:\n")
                traceback.print_exc(file=file)

                # sleep 60 seconds
            time.sleep(60)
            print(f"RESUMING {c}")
            c += 1

            # send_email()


if __name__ == "__main__":
    main()
