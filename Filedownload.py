import requests
import zipfile
import os

def sourcefiledownload():
    # URL of the file to download
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip"
    # File name to save the downloaded file
    output_file = "source.zip"
    # Download the file
    response = requests.get(url)
    # Save the content to a file
    if response.status_code == 200:
        with open(output_file, "wb") as file:
            file.write(response.content)
        print(f"File downloaded successfully as '{output_file}'")
    else:
        print(f"Failed to download file. Status code: {response.status_code}")
    # Path to the ZIP file
    zip_path = "source.zip"
    # Destination folder to extract files
    destination_path = "./unzipped_folder"
    # Create the destination folder if it doesn't exist
    os.makedirs(destination_path, exist_ok=True)
    # Extract the ZIP file
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(destination_path)
        print(f"Files extracted to '{destination_path}'")

