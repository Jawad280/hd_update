from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from tasks import process_embeddings
from celery.result import AsyncResult
from celery_app import celery_app
from clean_data import create_packages
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
    return {"task_id": task_id, "status": task_result.status, "result": task_result.result}

@router.post("/upload")
async def upload_xlsx(files: list[UploadFile] = File(...)):
    try:
        # Step 1 : Process xlsx & upload to Azure
        logger.info("Cleaning up xlsx")
        packages = await create_packages(files=files)

        # Step 2 : Start the embedding in background
        task = process_embeddings.apply_async()
        
        return JSONResponse(
            status_code=200,
            content={"message": f"{packages} & Embedding is running in the background for task_id : {task.id}"}
        )
        
    except Exception as e:
        logger.error(f"Error has occured : {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

