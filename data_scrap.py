import requests
import pandas as pd
from tqdm import tqdm
import threading
import io

import boto3
from botocore.exceptions import ClientError



# stID = {
#         155 : ['COMOX A', 1944, 2024],
#         6781: ['HOPEDALE (AUT)', 1942, 2024],
#         6354: ['GREENWOOD A', 1942, 2024],
#         3987: ['ARMSTRONG (AUT)', 1953, 2024],
#         5126: ['TRENTON A', 1953, 2024],
#         2832: ['COLD LAKE A', 1952, 2024],
#         1633: ['CAPE PARRY A', 1957, 2024],
#         3649: ['PILOT MOUND (AUT)', 1957, 2024],
#         1556: ['HAINES JUNCTION', 1944, 2024],
#         5642: ['WRIGHT',1967,2024]
#         }



bucket_name="bucket_name"
region = 'us-east-1'
s3_client = boto3.client('s3', region_name=region)
def create_S3(bucket_name=bucket_name):

    """
    This function is used to create S3 bucket with desired name.
    INPUT:
    1. bucket_name-> s3::/bucket
    """

    try:
        s3_client.create_bucket(
            Bucket=bucket_name
            )
        print(f"Bucket '{bucket_name}' created successfully in region '{region}'")

    except ClientError as e:
        print(f"Error creating bucket: {e}")


def _upload_csv_to_s3(id, csv_buffer, file_name,bucket_name=bucket_name):

    """
    This function is takes particular csv file & feeds that into s3 bucket with name ID
    INPUT:
    1. id->int it must match name of station id
    2. file_name->int ,it must match year names of recording in station id
    3. bucket_name-> s3::/bucket

    """

    folder_path = f"{bucket_name}/{id}/{file_name}"
    
    try:
        s3_client.put_object(Bucket=bucket_name, Key=folder_path, Body=csv_buffer.getvalue())
        print(f"File '{file_name}' uploaded to '{bucket_name}/{id}' in bucket '{bucket_name}'")

    except ClientError as e:
        print(f"Error uploading file: {e}")


def _scarp(stationID,start_year,end_year):
    """
    This function takes station ID & years and start and end years as input to grep csv from http link below.

    INPUT: 
    1. stationID->int, it must be among 8000 stations presnet.
    2. start_year->int, it must be refernced according to reading dates
    3. end_year->int, it must be refernced according to reading dates


    """
    for year in tqdm(range(start_year,end_year+1)):
        base_url = "http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID={}&Year={}&timeframe=2".format(stationID, year)
        response = requests.get(base_url)
        csv_buffer = io.BytesIO(response.content)
        _upload_csv_to_s3(id=stationID,csv_buffer=csv_buffer,file_name=str(year)+".csv")
        print(f"File downloaded and saved as {year}.csv into s3")



def helper_thread_function(start,end,df):
    """
    This function takes start & end of dataframe.

    INPUT:
    1. start-> int, start id of dataframe
    2. end-> ing, end if of dataframe
    3. df-> dataframe

    """
    for i in tqdm(range(start, end)):
        frame = df.iloc[i]
        _scarp(frame['STN_ID'], start_year=frame['FIRST_DATE'], end_year=frame['LAST_DATE'])

def _count_objects_in_bucket(id,start_year,end_year,bucket_name=bucket_name):

    """
    This is helper function while doing consistency_check of data files test
    INPUT:
    1. id-> int, it is station id
    2. start_year-> int
    3. end_year-> int 
    4. bucket_name-> s3:://bucket

    """

    total_objects = 0
    paginator = s3_client.get_paginator('list_objects_v2')
    
    # Construct the prefix path within the bucket
    prefix = f"{bucket_name}/{id}/"

    try:
        # Paginate through the objects within the specified prefix
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                total_objects += len(page['Contents'])
    
    except ClientError as e:
        print(f"Error occurred: {e}")
        return None  # Return None in case of an error

    # Return the id, total number of objects, and the expected object count
    return id, total_objects, end_year - start_year + 1, total_objects==(end_year-start_year+1)



def consistency_check(df):
    for i in range(df.shape[0]):
        lox=df.loc[i]
        print(_count_objects_in_bucket(id=lox['STN_ID'], start_year=lox['FIRST_DATE'],end_year=lox['LAST_DATE']))




if __name__ == "__main__":
    #creating s3 bucket
    create_S3()

    #reference dataframe
    #only nearabout 80/8090 station data is taken into account.
    df=pd.read_csv("data/station_table/station_table.csv")

    #columns to keep.
    df=df[["STN_ID","FIRST_DATE","LAST_DATE"]]
    
    #defining no of threads and logic
    num_threads = 8
    total_rows = df.shape[0]
    rows_per_thread = total_rows // num_threads
    threads = []

    for i in range(num_threads):
        start_index = i * rows_per_thread
        end_index = total_rows if i == num_threads - 1 else (i + 1) * rows_per_thread
        thread = threading.Thread(target=helper_thread_function, args=(start_index, end_index,df))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print('All threads have completed.')

    #performing files consistency test.
    consistency_check(df)




     





    

    
