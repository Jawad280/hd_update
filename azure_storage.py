from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import logging
import io
import time

load_dotenv()

CONNECTION_STRING = os.getenv('CONNECTION_STRING')
CONTAINER_NAME = os.getenv('CONTAINER_NAME')
blob_service_client = BlobServiceClient.from_connection_string(conn_str=CONNECTION_STRING)

logging.basicConfig(
    level=logging.INFO, 
    handlers=[
        logging.StreamHandler()  # Ensures logs are sent to stdout/stderr
    ]
)
logger = logging.getLogger('azure_storage.py')


def upload_file_to_azure(f, filename:str):
    if not CONNECTION_STRING or not CONTAINER_NAME:
        raise ValueError("CONNECTION_STRING or CONTAINER_NAME is not set in the environment variables.")

    try:
        start_time = time.time()
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        blob_client = container_client.get_blob_client(filename)

        blob_client.upload_blob(f, overwrite=True)

        end_time = time.time()

        time_taken = end_time - start_time
        logger.info(f"File {filename} uploaded successfully in {time_taken:.2f} seconds !")
    except Exception as e:
        logger.error(f"Error has occured : {e}")
        raise e
    
def get_file_from_azure(filename: str):
    if not CONNECTION_STRING or not CONTAINER_NAME:
        raise ValueError("CONNECTION_STRING or CONTAINER_NAME is not set in the environment variables.")
    
    try:
        start_time = time.time()
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        blob_client = container_client.get_blob_client(filename)

        blob_data = blob_client.download_blob()
        byte_steam = io.BytesIO(blob_data.readall())

        end_time = time.time()

        time_taken = end_time - start_time
        logger.info(f"File {filename} retrieved successfully in {time_taken:.2f} seconds")
        return byte_steam
    except Exception as e:
        logger.error(f"Error has occured : {e}")
        raise e

def delete_file_from_azure(filename: str):
    if not CONNECTION_STRING or not CONTAINER_NAME:
        raise ValueError("CONNECTION_STRING or CONTAINER_NAME is not set in the environment variables.")
    
    try:
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        blob_client = container_client.get_blob_client(filename)
        blob_client.delete_blob()
        logger.info(f"File {filename} deleted successfully")
    except Exception as e:
        logger.error(f"Error has occured : {e}")
        raise e