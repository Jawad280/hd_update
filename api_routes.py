from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from clean_data import create_packages
from embedding_data import create_package_embeddings
import logging

load_dotenv()

router = APIRouter()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('main.py')

@router.get("/")
async def hello():
    return "Hey Jawad"

@router.post("/upload")
async def upload_xlsx(files: list[UploadFile] = File(...)):
    try:
        # First merge xlsx, clean data & upload to blob
        packages = await create_packages(files=files)
        embedding_res = await create_package_embeddings()
        
        return JSONResponse(
            status_code=200,
            content={"message": embedding_res}
        )
        
    except Exception as e:
        logger.error(f"Error has occured : {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

