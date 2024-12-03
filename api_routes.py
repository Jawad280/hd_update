from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from tasks import process_embeddings
from celery.result import AsyncResult
from celery_app import celery_app
from clean_data import create_packages
from datetime import datetime
from google_sheets_utils import stream_csv_to_azure
import logging

load_dotenv()

router = APIRouter()

logging.basicConfig(
    level=logging.INFO, 
    handlers=[
        logging.StreamHandler()  # Ensures logs are sent to stdout/stderr
    ]
)
logger = logging.getLogger('main.py')

@router.get("/")
async def hello():
    return "Hey Jawad"

@router.get("/status/{task_id}")
async def get_status(task_id: str):
    task_result = AsyncResult(task_id, app=celery_app)
    return {
        "task_id": task_id,
        "status": task_result.status
    }

@router.post("/upload")
async def upload_xlsx(files: list[UploadFile] = File(...)):
    try:
        # Step 1 : Fetch all google sheets, convert to CSV & upload to Azure
        logger.info("================================")
        logger.info("1. Fetching Google Sheets -> Converting to CSV -> Uploading to Azure")
        logger.info("================================")
        stream_csv_to_azure(sheet_name="highlight_pages", filename="highlights.csv")
        stream_csv_to_azure(sheet_name="cash_discount", filename="cash_discounts.csv")
        # stream_csv_to_azure(sheet_name="", filename="product_feed.csv")
        stream_csv_to_azure(sheet_name="product_payment_methods", filename="payment_methods.csv")

        # Step 2 : Process xlsx & upload to Azure
        logger.info("================================")
        logger.info("2. Cleaning up xlsx -> Creating packages.csv")
        logger.info("================================")
        packages = await create_packages(files=files)

        # Step 3 : Start the embedding in background
        logger.info("================================")
        logger.info("3. Starting embedding in background -> Embedding, Tokens")
        logger.info("================================")
        task = process_embeddings.apply_async()
        
        return JSONResponse(
            status_code=200,
            content={
                "message": f"{packages} & Embedding is running in the background for task_id : {task.id}",
                "startTime": datetime.now().isoformat()
            }
        )
        
    except Exception as e:
        logger.error(f"Error has occured : {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

