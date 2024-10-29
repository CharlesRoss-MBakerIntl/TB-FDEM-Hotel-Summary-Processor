#Import Packages
import requests
import traceback
import os
import json
import pandas as pd
from dotenv import load_dotenv
from github_python_fetch import fetch_function
from query_package import get_query_package


#Load Environment Variables
load_dotenv()  # Load environment
token = os.getenv('GITHUBTOKEN')  # GitHub toke
access = os.getenv('ACCESS') # AWS access token
secret = os.getenv('SECRET') # AWS secret token
username = os.getenv('USER') # AWS RDS username
password = os.getenv('PASSWORD') # AWS RDS password
server = os.getenv('SERVER') # AWS RDS server
db = os.getenv('DB') # AWS RDS database



import json

def process_table(event, context):

    #----------------------------------------------------------------

    # Access RDS Functions via GitHub
    try:
        rds_functions_url = 'https://raw.githubusercontent.com/CharlesRoss-MBakerIntl/Tidal-Basin-Functions/main/rds_connector.py' # Set url to python file of github
        rds_functions = fetch_function(rds_functions_url, token) # Pull function from github using requests
        exec(rds_functions) # Execute the file

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
        #conn, cursor = rds_connection(username, password, db, server) # Connect to RDS Database
        pass

    except Exception as e:
        print(traceback.print_exc())
        raise Exception(f"Failed to Connect to AWS RDS Database") from e   


    #----------------------------------------------------------------

    # Create Instance of RDS Table
    try:
        rds = RDSTablePull(conn, cursor, query_package) # Create Instance of RDS Table

    except Exception as e:
        print(traceback.print_exc())
        raise Exception(f"Failed to Produce RDS Table from RDSTablePull in rds_connector.py") from e 
    
    
    
    # TODO implement
    return {
        
        'statusCode': 200,
        'body': json.dumps('Hotel Summaries Table Processed')
    }
