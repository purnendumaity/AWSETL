=============This repository has below folder structure===========
after unzipping all raw/actual files get saved in subfolder /unzipped_folder
logfile gets save inside subfolder log
output file gets saved inside subfolder output
AWSETL
--subdirectory--/log
--subdirectory--/output
--subdirectory--/unzipped_folder
Filedownload.py
Fileprocessing.py
AWSs3operation.py
AWSdboperation.py
.env file
AWScloudshell_commands.txt ( sample command to use mysql db from AWScloudshell )
Readme.txt
source.zip

=========This repository has 4 Python program file amd 1 env file =============
First pre-requisites are an AWS free tier account, creation of S3 bucket- input,output folder, 
creation of AWS RDS mysql instance, creating a database, creating appropriate inbound/outbound 
network rule so that from local machine remote S3 bucket and mysql DB connection is allowed.
env file is used for various AWS and mysql credential related, for safety here it is blank
we use python dotenv, os package to load this credentials during runtime.

-----Filedownload.py----
It utilizes Python requests package to download the source.zip file from internet URL Location
and after unzipping action saves all unzipped files in subfolder /unzipped_folder
source.zip file is being saved directly in project folder ./

-----AWSs3operation.py----
It utilizes Python boto3 package to connect to AWS S3 bucket and upload local files to respective 
remote directory

-----AWSdboperation.py----
It utilizes pandas, sqlalchemy, pymysql package to connect to AWS RDS Mysql instance and 
load the local final csv file output to the remote table.

---2nd run the Fileprocessing.py----
This is the overall ETL Processing program, it calls the Filedownload.py, AWSs3operation.py,AWSdboperation.py
After all import section we need to set the required path of log and output filenames as below
I have noticed that combine data file record is not unique;
So I have created an additional outputfile with unique records only.
example path of log and output filenames:
input_folder = "./unzipped_folder"
log_file = "./log/etl_log.txt"
output_file = "./output/transformed_data.csv"
unique_record_output_file = "./output/unique_transformed_data.csv"
Program has various functions defined as below:
log_message function manages the log file creation
extract_csv, extract_json,extract_xml these functions extracts data from csv,json,xml file respectively
extract_data function combines all extracted data
transform_data function modified the height and weight
load_data function loads the final data into respective output csv file


=========== Post run ETL log file sample =====================
2025-04-18 22:28:31 - All input files loading to AWS S3 bucket input directory completed
2025-04-18 22:28:31 - ETL process started
2025-04-18 22:28:31 - Total Number of Input File:  9
2025-04-18 22:28:31 - Extracted record_count from CSV: ./unzipped_folder\source1.csv is: 5
2025-04-18 22:28:31 - Extracted record_count from JSON: ./unzipped_folder\source1.json is: 4
2025-04-18 22:28:31 - Extracted record_count from XML: ./unzipped_folder\source1.xml is: 4
2025-04-18 22:28:31 - Extracted record_count from CSV: ./unzipped_folder\source2.csv is: 5
2025-04-18 22:28:31 - Extracted record_count from JSON: ./unzipped_folder\source2.json is: 4
2025-04-18 22:28:31 - Extracted record_count from XML: ./unzipped_folder\source2.xml is: 4
2025-04-18 22:28:31 - Extracted record_count from CSV: ./unzipped_folder\source3.csv is: 5
2025-04-18 22:28:31 - Extracted record_count from JSON: ./unzipped_folder\source3.json is: 4
2025-04-18 22:28:31 - Extracted record_count from XML: ./unzipped_folder\source3.xml is: 4
2025-04-18 22:28:31 - Total all records after extraction: 39
2025-04-18 22:28:31 - Extraction section completed
2025-04-18 22:28:31 - Transformed data: Converted height to meters and weight to kg
2025-04-18 22:28:31 - Transformation section completed
2025-04-18 22:28:31 - Loading section started
2025-04-18 22:28:31 - Loaded all transformed data into CSV: ./output/transformed_data.csv
2025-04-18 22:28:31 - Total number of all records from all source files: 39
2025-04-18 22:28:31 - Data Pattern Check started
2025-04-18 22:28:31 - Data Not Unique so loading unique records
2025-04-18 22:28:31 - Total number of unique records from all source files: 13
2025-04-18 22:28:31 - Loaded unique transformed data into CSV: ./output/unique_transformed_data.csv
2025-04-18 22:28:31 - Data Pattern Check completed
2025-04-18 22:28:31 - Loading section completed
2025-04-18 22:28:31 - ETL process completed
2025-04-18 22:28:33 - Output file loading to AWS S3 bucket output directory completed
2025-04-18 22:28:35 - AWS mysql db operation completed
2025-04-18 22:28:35 - Total program runtime (hh:mm:ss format) is: 00:00:07


============ Additional Notes=======================

 
