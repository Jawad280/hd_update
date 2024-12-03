from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from azure_storage import upload_file_to_azure, get_file_from_azure
from dotenv import load_dotenv
from io import BytesIO
import os
import csv
import logging

logging.basicConfig(
    level=logging.INFO, 
    handlers=[
        logging.StreamHandler()  # Ensures logs are sent to stdout/stderr
    ]
)
logger = logging.getLogger('google_sheets_utils.py')

load_dotenv()
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

def stream_csv_to_azure(sheet_name, filename):
    SERVICE_ACCOUNT_FILE = get_file_from_azure('sheets_service_account.json')
    SCOPES = ['https://googleapis.com/auth/spreadsheets.readonly']
    credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=credentials)

    try:
        # Fetch data from specific sheet
        logger.info(f"Begin fetching sheet : {sheet_name}....")
        response = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=sheet_name
        ).execute()

        rows = response.get('values', [])
        if not rows:
            raise Exception(f"No data found in sheet: {sheet_name}")
        
        # Convert the data to CSV
        buffer = BytesIO()
        writer = csv.writer(buffer)
        writer.writerows(rows)
        buffer.seek(0)

        # Upload to Azure Storage
        upload_file_to_azure(buffer, filename)
        logger.info(f"Sheet {sheet_name} has been successfully uploaded as {filename}")
    except Exception as e:
        logger.info(f"An error occured : {e}")
        raise e
    



