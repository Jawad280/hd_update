from celery import Celery
import os
from dotenv import load_dotenv
import ssl

os.environ.clear()

load_dotenv()

celery_app = Celery(
    "tasks",
    broker=os.getenv("CELERY_BROKER_URL"),
    backend=os.getenv("CELERY_BROKER_URL"),
    include=['tasks']
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)