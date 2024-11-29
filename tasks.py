from celery_app import celery_app
from embedding_data import create_package_embeddings
import logging

logging.basicConfig(
    level=logging.INFO, 
    handlers=[
        logging.StreamHandler()  # Ensures logs are sent to stdout/stderr
    ]
)
logger = logging.getLogger('tasks.py')

@celery_app.task
def process_embeddings():
    try:
        # Create embeddings
        embeddings_response = create_package_embeddings()
        print(embeddings_response)
    except Exception as e:
        logger.error(f"Error occurred during the embedding process: {e}")
        return f"Error: {str(e)}"