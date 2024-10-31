import pandas as pd
import traceback
from datetime import datetime


################    ADD ACTIVE POWER BI REPORT DATA TO PROJECT FOLDER    ######################

#----------------------------------------------------------------

# Add Active Data to Project Folder
def update_active_data(s3, bucket, project_folder, active_folder, file_name, data):

    #Set Folder Prefix to Active Data Folder
    folder_prefix = project_folder + active_folder + file_name

    # Check if the file exists in the specified folder
    try:
        #Pull Results for File in S3 Bucket
        result = s3.list_objects_v2(Bucket=bucket, Prefix=folder_prefix)

        #Check if File Exists in Folder
        file_exists = 'Contents' in result and any(item['Key'] == folder_prefix for item in result['Contents'])

        #If the file exists
        if file_exists:
            try:
                
                # Update the file with a new version
                s3.put_object(Bucket = bucket, Key = folder_prefix, Body = data.to_csv())
            
            except Exception as e:
                raise Exception(f"Error: Failed to Overwrite Active csv file {file_name} in {project_folder}")
        else:
            raise Exception(f"Error: Could not update active csv, '{file_name}' does not exist within {project_folder}")
        
    except Exception as e:
        raise Exception(f"Error: Could not update active csv file in {project_folder}: {e}")
    





################    ADD CLEANING VERSIONS TO ARCHIVED S3    ######################

#----------------------------------------------------------------

def add_archived_versions(s3, bucket, project_folder, archive_folder, date_folder, versions):
    
    #Set Step Count for File Nameing
    step_count = 1

    try:
        for item in versions:

            #Create Number Count
            if step_count < 10:
                count = f"0{step_count}"
            else:
                count = f"{step_count}"

            # Create File Name
            if item['Field'] == None:
                file_name = f"{count} - {item['Step']}.csv"
            else:
                file_name = f"{count} - {item['Field']} - {item['Step']}.csv"

            #Create Folder Path to Today's Folder
            folder_prefix = project_folder + archive_folder + date_folder + file_name

            #Upload to Folder
            try:
                s3.put_object(Bucket = bucket, Key = folder_prefix, Body = item['Result'].to_csv())
            except Exception as e:
                raise Exception(f"Error: Failed to Upload {file_name} to S3 {project_folder + archive_folder + date_folder}")

            #Add to File Count After Complete
            step_count += 1

    except Exception as e:
        raise Exception(f"Error: Failed to Cycle Through RDS Cleaning Versions While Updating to S3.  {e}")
    





################    MANAGE FOLDER LIMIT FOR ARCHIVE FOLDER IN PROJECT    ######################

#----------------------------------------------------------------

def monitor_and_maintain_archive_limit(s3, bucket, project_folder, archive_folder, limit = 30):

    #Set Folder Prefix
    folder_prefix = project_folder + archive_folder

    # List objects within the specified folder
    result = s3.list_objects_v2(Bucket=bucket, Prefix=folder_prefix, Delimiter='/')

    #Check if Folders Exist in Result
    if 'CommonPrefixes' in result:
        subfolders = result['CommonPrefixes']   #Find All Sub-Folders within Passed Folder
        folder_count = len(subfolders)   #Store Folder Count

        # Initialize iteration counter
        iteration_counter = 0
        max_iterations = 150  # Set a threshold for maximum iterations

        # Continue deleting folders until the count is within limit
        while folder_count > limit:
            
            #Monitor Iterations of While Loop
            iteration_counter += 1
            if iteration_counter > max_iterations:
                    raise Exception("Error: Script stuck in while loop while cleaning folders to set limit, update process broken until resolved.")
            
            #Set Empty Folder Dates
            folder_dates = []

            #Iterate Through Sub Folders
            for folder in subfolders:
                folder_key = folder['Prefix']
                folder_objects = s3.list_objects_v2(Bucket=bucket, Prefix=folder_key)

                #Find Oldest Object in Folder
                if 'Contents' in folder_objects:
                    oldest_object = min(folder_objects['Contents'], key=lambda x: x['LastModified'])
                    folder_dates.append((folder_key, oldest_object['LastModified']))
                else:
                    raise Exception(f"Error: Contents not found in Folder List in {folder_key}")

            # Sort folders by date and delete the oldest one
            folder_dates.sort(key=lambda x: x[1])
            oldest_folder_key = folder_dates[0][0]
            
            #Delete Excess Folders by Oldest First
            try:
                s3.delete_object(Bucket=bucket, Key=oldest_folder_key)
            
            except Exception as e:
                raise Exception (f"Error: Failed to delete excess folders when cleaning achrive folder for {project_folder}")

            # Update the folder count and subfolders list
            result = s3.list_objects_v2(Bucket=bucket, Prefix=folder_prefix, Delimiter='/')
            subfolders = result['CommonPrefixes']
            folder_count = len(subfolders)





################    ADD NEW ARCHIVE FOLDER FOR DATE IN ARCHIVE    ######################

#----------------------------------------------------------------

def add_archive_folder(s3, bucket, project_folder, archive_folder, limit, versions):
    
    #Set Folder Prefix
    folder_prefix = project_folder + archive_folder

    
    #Monitor and Clean Archive Folder
    try:
        monitor_and_maintain_archive_limit(s3 = s3,
                                        bucket = bucket, 
                                        project_folder = project_folder,
                                        archive_folder = archive_folder,
                                        limit = limit)
    except Exception as e:
        print(traceback.print_exc())
        raise Exception(f"Error: Failed to Clean Archive Folder before Upload") from e


    #Set Folder Name
    date_folder = datetime.now().strftime('%Y-%m-%d') + '/'

    #Add Date Folder to Archive Folder
    try:
        s3.put_object(Bucket=bucket, Key=folder_prefix + date_folder)
    except Exception as e:
        print(traceback.print_exc())
        raise Exception(f"Error: Failed to Upload {date_folder} Archive Folder") from e

    #Add Archiveds CSV Files to Folder
    try:
        add_archived_versions(s3 = s3,
                            bucket = bucket,
                            project_folder = project_folder,
                            archive_folder = archive_folder, 
                            date_folder = date_folder,
                            versions = versions)
    except Exception as e:
        print(traceback.print_exc())
        raise Exception(f"Error: Failed to Upload Archive Cleaning Versions CSVs to {date_folder} Archive Folder") from e