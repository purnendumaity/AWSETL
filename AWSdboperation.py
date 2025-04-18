from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

def filetoawsmysqltable(filepath,mytablename):
    # Load environment variables from .env file
    load_dotenv()
    # Access credentials securely
    aws_dbusername = os.getenv('mysqlusername')
    aws_dbpassword = os.getenv('mysqlpassword')

    # AWS DB configuration
    rds_host = "awsmysqldb.cvuueekqwmty.ap-south-1.rds.amazonaws.com"
    db_name = "awsmydb"
    username = aws_dbusername
    password = aws_dbpassword
    port = 3306  # Default MySQL port
    csv_file_path = filepath
    table_name = mytablename

    # Create SQLAlchemy engine
    engine = create_engine(f"mysql+pymysql://{username}:{password}@{rds_host}:{port}/{db_name}")
    # Load CSV file into pandas DataFrame
    df = pd.read_csv(csv_file_path)
    # Upload DataFrame to RDS (replace if exists)
    df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
    print(f"Successfully uploaded {len(df)} rows to table `{table_name}` in RDS MySQL.")
    # Count rows in the uploaded table
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
        row_count = result.scalar()
        print(f"Table `{table_name}` now contains {row_count} rows.")