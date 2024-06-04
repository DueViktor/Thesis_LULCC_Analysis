import os
import requests
import zipfile

def download_and_extract_zip(url, extract_to):
    """Download a zip file from a URL and extract it to a specified directory."""
    # Make sure the target directory exists
    os.makedirs(extract_to, exist_ok=True)
    
    # Download the file
    response = requests.get(url)
    if response.status_code == 200:
        zip_path = os.path.join(extract_to, 'temp.zip')
        with open(zip_path, 'wb') as file:
            file.write(response.content)
        
        # Extract the zip file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        
        # Clean up the downloaded zip file
        os.remove(zip_path)
    else:
        print(f"Failed to download the file: Status code {response.status_code}")

def remove_underscores(directory):
    """Remove underscores from filenames in the specified directory."""
    for filename in os.listdir(directory):
        new_filename = filename.replace('_', '')
        os.rename(
            os.path.join(directory, filename),
            os.path.join(directory, new_filename)
        )

# URL of the zip file
url = "https://landbrugsgeodata.fvm.dk/Download/Markblokke/Markblokke_YEAR.zip"

# Path where the zip file will be extracted
extract_to = 'data/LandbrugsGIS/'

# Download and extract the zip file

for year in ("2016","2017","2023"):
    url_YEAR = url.replace('YEAR',year)
    download_and_extract_zip(url_YEAR, extract_to)

# Remove underscores from the extracted files' names
remove_underscores(extract_to)

print("Files have been processed and underscores have been removed.")
