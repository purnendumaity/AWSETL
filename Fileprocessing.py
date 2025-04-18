import pandas as pd
import json
import xml.etree.ElementTree as ET
import os
import glob
import time
import datetime
import Filedownload
import AWSs3operation as awss3
import AWSdboperation as awsdb

#Set all the required path of log and output filenames
input_folder = "./unzipped_folder"
log_file = "./log/etl_log.txt"
output_file = "./output/transformed_data.csv"
unique_record_output_file = "./output/unique_transformed_data.csv"

# Record program start time
start_time = time.time()

# Overwrite the log file by opening it in "w" mode once at the beginning
with open(log_file, "w") as log:
    log.write("")  # Clears file content

# Logging function
def log_message(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as log:
        log.write(f"{timestamp} - {message}\n")
    print(f"{timestamp} - {message}")

#sourcefiledownload operation
Filedownload.sourcefiledownload()

#Delay of 1 seconds for safety
time.sleep(1)

# Upload all files from local input folder to 'input/' directory in S3
awss3.upload_multiplefilefromfolder_to_s3(input_folder, 'input')
log_message("All input files loading to AWS S3 bucket input directory completed")

# Get full file paths in a list
file_paths = glob.glob(os.path.join(input_folder, "*"))
#print(file_paths)

# Function to extract data from CSV
def extract_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        #adding source detail in data to track record source
        df["source"] = file_path
        log_message(f"Extracted record_count from CSV: {file_path} is: {len(df)}")
        return df
    except Exception as e:
        log_message(f"Error extracting CSV {file_path}: {e}")
        return pd.DataFrame()

# Function to extract data from JSON
def extract_json(file_path):
    try:
        data = []
        with open(file_path, "r") as file:
            for line in file:
                data.append(json.loads(line))
        df = pd.DataFrame(data)
        # adding source detail in data to track record source
        df["source"] = file_path
        log_message(f"Extracted record_count from JSON: {file_path} is: {len(df)}")
        return df
    except Exception as e:
        log_message(f"Error extracting JSON {file_path}: {e}")
        return pd.DataFrame()

# Function to extract data from XML
def extract_xml(file_path):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        data = []
        for person in root.findall("person"):
            name = person.find("name").text
            height = float(person.find("height").text)
            weight = float(person.find("weight").text)
            data.append({"name": name, "height": height, "weight": weight})
        df = pd.DataFrame(data)
        # adding source detail in data to track record source
        df["source"] = file_path
        log_message(f"Extracted record_count from XML: {file_path} is: {len(df)}")
        return df
    except Exception as e:
        log_message(f"Error extracting XML {file_path}: {e}")
        return pd.DataFrame()

# Master extraction function
def extract_data(file_paths):
    all_data = []
    for file in file_paths:
        if file.endswith(".csv"):
            all_data.append(extract_csv(file))
        elif file.endswith(".json"):
            all_data.append(extract_json(file))
        elif file.endswith(".xml"):
            all_data.append(extract_xml(file))
    combined_df = pd.concat(all_data, ignore_index=True)
    log_message(f"Total all records after extraction: {len(combined_df)}")
    return combined_df

# Transformation function
def transform_data(df):
    df["height"] = df["height"] * 0.0254  # Convert inches to meters
    df["weight"] = df["weight"] * 0.453592  # Convert pounds to kg
    log_message("Transformed data: Converted height to meters and weight to kg")
    return df

# Load function to save CSV
def load_data(df, output_file,dataflag):
    try:
        df.to_csv(output_file, index=False)
        if dataflag == "allrecords":
            log_message(f"Loaded all transformed data into CSV: {output_file}")
            log_message(f"Total number of all records from all source files: {len(df)}")
        elif dataflag == "uniquerecords":
            log_message(f"Total number of unique records from all source files: {len(df)}")
            log_message(f"Loaded unique transformed data into CSV: {output_file}")
    except Exception as e:
        log_message(f"Error saving transformed data: {e}")

# Running the Overall ETL Process
log_message("ETL process started")
# Extract
log_message(f"Total Number of Input File:  {len(file_paths)}")
df_extracted = extract_data(file_paths)
log_message("Extraction section completed")
# Transform
df_transformed = transform_data(df_extracted)
log_message("Transformation section completed")
# Load
log_message("Loading section started")
load_data(df_transformed, output_file, "allrecords")
# data pattern check
log_message("Data Pattern Check started")
# Create a new DataFrame without the "source" column and extract unique records
newdf = df_transformed.drop(columns=["source"]).drop_duplicates()
if len(newdf) < len(df_transformed):
    log_message("Data Not Unique so loading unique records")
    load_data(newdf, unique_record_output_file, "uniquerecords")
log_message("Data Pattern Check completed")
log_message("Loading section completed")
log_message("ETL process completed")

#Delay of 1 seconds for safety
time.sleep(1)

#AWS S3 storage operation processing
#local_input_folder = input_folder
#output_file_path = unique_record_output_file
output_finalfile_name = os.path.basename(unique_record_output_file)
# Upload output file to 'output/' directory in S3
awss3.upload_singlefile_to_s3(unique_record_output_file, f'output/{output_finalfile_name}')
log_message("Output file loading to AWS S3 bucket output directory completed")

#AWS mysql processing
awsdb.filetoawsmysqltable(unique_record_output_file,"finaldata")
log_message("AWS mysql db operation completed")

# Record Program end time
end_time = time.time()
# Calculate Program total runtime
total_seconds = end_time - start_time
# Convert to hh:mm:ss format with milliseconds
formatted_time = time.strftime("%H:%M:%S", time.gmtime(total_seconds))
log_message(f"Total program runtime (hh:mm:ss format) is: {formatted_time}")

