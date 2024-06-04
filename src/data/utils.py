import json
from pathlib import Path

from geopy.geocoders import Nominatim


def save_json(data, output_path: Path):
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f)


def load_json(input_path: Path):
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data

def get_cordinates_from_address(address: str) -> tuple:
    geolocator = Nominatim(user_agent="geoapiExercises")
    location = geolocator.geocode(address)

    if location:
        return (location.latitude, location.longitude)
    else:
        return None, None

   