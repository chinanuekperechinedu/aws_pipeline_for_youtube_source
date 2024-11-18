import botocore.exceptions
import kagglehub

import boto3
import botocore

import glob

from dotenv import load_dotenv
import os

import logging

load_dotenv()


# Download latest version
path = kagglehub.dataset_download("datasnaek/youtube-new")

print("Path to dataset files:", path)

BUCKET_NAME = os.getenv('BUCKET_NAME')
FOLDER_NAME_JSON = os.getenv('FOLDER_NAME_JSON')
FOLDER_NAME_CSV = os.getenv('FOLDER_NAME_CSV')

path = os.getenv('FILE_PATH')

logging.basicConfig(level=logging.INFO,
    filename='app.log',
    filemode='a',
    format='{asctime} - {levelname} - {message}',
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

logger = logging.getLogger()

def main():

    try:

        json_files = glob.glob(f'{path}/*.json')
        csv_files = glob.glob(f'{path}/*.csv')

        s3 = boto3.client('s3')
    
        logger.info('================================== starting json file upload ==================================')
        
        for filename in json_files:
            key = "%s/%s" % (FOLDER_NAME_JSON, os.path.basename(filename))
            logger.info(f"Putting {filename} as {key}")
            print(f'uploading {os.path.basename(filename)}')
            s3.upload_file(filename, BUCKET_NAME, key)

        logger.info('================================== json file upload completed ==================================')

        logger.info('================================== starting csv file upload ==================================')

        for filename in csv_files:
            filepath = os.path.basename(filename)
            key = f"{FOLDER_NAME_CSV}{filepath[:2].lower()}/{filepath}" 
            logger.info(f"Putting {filename} as {key}")
            print(f'uploading {os.path.basename(filename)}')
            s3.upload_file(filename, BUCKET_NAME, key)

        logger.info('================================== csv file upload completed ==================================')

    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'LimitExceededException':
            logger.warning('API call limit exceeded; backing off and retrying...')
        else:
            raise error

   
if __name__ == "__main__":
    main()