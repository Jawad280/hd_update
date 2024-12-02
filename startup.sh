#!/bin/bash

# Start FastAPI
uvicorn __init__:create_app --host 0.0.0.0 --port 8000 &

# Start celery
celery -A celery_app.celery_app worker -l info --logfile=- &

wait