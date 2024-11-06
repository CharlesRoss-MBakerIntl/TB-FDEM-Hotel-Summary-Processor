#Import Packages
import requests
import traceback
import os
import json
import boto3
import pandas as pd
from dotenv import load_dotenv
from github_python_fetch import fetch_function
from query_package import get_query_package


#Load Environment Variables
load_dotenv()  # Load environment
token = os.getenv('GITHUBTOKEN')    # GitHub toke
access = os.getenv('ACCESS')    # AWS access token
secret = os.getenv('SECRET')    # AWS secret token
username = os.getenv('USER')    # AWS RDS username
password = os.getenv('PASSWORD')    #AWS RDS password
server = os.getenv('SERVER')    # AWS RDS server
db = os.getenv('DB')    # AWS RDS database
bucket = os.getenv('BUCKET')    # AWS RDS bucket name
project_folder = os.getenv('PROJECTFOLDER')     # AWS S3 Project Folder Path
active_folder = os.getenv('ACTIVEFOLDER')    # AWS S3 Project Active Folder Path
archive_folder = os.getenv('ARCHIVEFOLDER')     # AWS S3 Project Archive Folder Path



def process_table(event, context):

    #----------------------------------------------------------------

    # Access RDS Functions via GitHub
    try:
        rds_connector_url = 'https://raw.githubusercontent.com/CharlesRoss-MBakerIntl/Tidal-Basin-Functions/main/rds_connector.py' # Set url to python file of github
        rds_connector = fetch_function(rds_connector_url, token) # Pull function from github using requests
        exec(rds_connector) # Execute the file

    except Exception as e:
        print(traceback.print_exc())
        raise Exception(f"Failed to Pull RDS Functions from GitHub") from e
    


    #----------------------------------------------------------------

    # Access RDS Functions via GitHub
    try:
        s3_manager_url = 'https://raw.githubusercontent.com/CharlesRoss-MBakerIntl/Tidal-Basin-Functions/main/rds_connector.py' # Set url to python file of github
        s3_manager = fetch_function(s3_manager_url, token) # Pull function from github using requests
        exec(s3_manager) # Execute the file

    except Exception as e:
        print(traceback.print_exc())
        raise Exception(f"Failed to Pull RDS Functions from GitHub") from e
    


    #----------------------------------------------------------------

    # Pull Query Package from File
    try:
        query_package = get_query_package()

    except Exception as e:
        print(traceback.print_exc())
        raise Exception(f"Failed to Pull Query Package from query_package.py") from e


    #----------------------------------------------------------------

    # Connect to AWS RDS Database
    try:
        conn, cursor = rds_connection(username, password, db, server) # Connect to RDS Database
        pass

    except Exception as e:
        print(traceback.print_exc())
        raise Exception(f"Failed to Connect to AWS RDS Database") from e   


    #----------------------------------------------------------------

    # Create Instance of RDS Table
    try:
        rds = RDS(conn, cursor, query_package) # Create Instance of RDS Table

    except Exception as e:
        print(traceback.print_exc())
        raise Exception(f"Failed to Produce RDS Table from RDSTablePull in rds_connector.py") from e



    #----------------------------------------------------------------

    # Create a Boto3 client for S3
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=access,
            aws_secret_access_key=secret
        )

    except Exception as e:
        print(traceback.print_exc())
        raise Exception(f"Failed to Connect to S3 with Secret and Access Key") from e
        


    #----------------------------------------------------------------

    # Add Active File to Project S3
    try:
        update_active_data(s3 = s3, 
                           bucket = bucket, 
                           project_folder = project_folder, 
                           active_folder = active_folder, 
                           file_name = "Active-FDEM-Hotel-Summary.csv", 
                           data = rds.df)

    except Exception as e:
        print(traceback.print_exc())
        raise Exception(f"Failed to Update Active Data for {project_folder}") from e 
    



     #----------------------------------------------------------------

    # Add Archive Versions to Project S3
    try:
        add_archive_folder(s3 = s3, 
                           bucket = bucket, 
                           project_folder = project_folder, 
                           archive_folder = archive_folder, 
                           limit = 50,
                           versions = rds.cleaning_versions)

    except Exception as e:
        print(traceback.print_exc())
        raise Exception(f"Failed to Add Archive Folder for {project_folder}") from e 
    
    
    

    # TODO implement
    return {
        
        'statusCode': 200,
        'body': json.dumps('Hotel Summaries Table Processed')
    }




