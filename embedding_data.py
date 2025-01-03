from openai import AsyncAzureOpenAI
from dotenv import load_dotenv
import os
import numpy as np
import pandas as pd
import logging
import io
import asyncio
from datetime import datetime
from azure_storage import upload_file_to_azure, get_file_from_azure
from tokenizer import batch_tokenize_docs
from knowledge_base_generator import concat_sheets

logging.basicConfig(
    level=logging.INFO, 
    handlers=[
        logging.StreamHandler()  # Ensures logs are sent to stdout/stderr
    ]
)
logger = logging.getLogger('embedding_data.py')

load_dotenv()

AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_VERSION = os.getenv("AZURE_OPENAI_VERSION")
TEXT_EMBEDDING_MODEL = os.getenv("TEXT_EMBEDDING_MODEL")

client = AsyncAzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    api_version=AZURE_OPENAI_VERSION,
    azure_endpoint=AZURE_OPENAI_ENDPOINT
)

def get_embedding_sync(text, model=TEXT_EMBEDDING_MODEL):
    text = text.replace("\n", " ")
    resp = asyncio.run(client.embeddings.create(input=[text], model=model)) 
    embedding_vector = resp.data[0].embedding
    return embedding_vector


def create_package_embeddings():
    # Step 1 : Retrieve file from csv
    packages = get_file_from_azure(filename="packages.csv")

    df_main = pd.read_csv(packages)
    df_cash = pd.read_csv(get_file_from_azure('cash_discount.csv'))
    df_feed = pd.read_csv(get_file_from_azure('product_feed.csv'))
    df_payment = pd.read_csv(get_file_from_azure('product_payment_methods.csv'))

    logger.info("Beginning embedding process")
    knowledge_base = concat_sheets(df_main=df_main, df_cash=df_cash, df_feed=df_feed, df_payment=df_payment)
    # For testing just take 2 rows 
    # knowledge_base = knowledge_base[:2]

    logger.info("Generating json docs")
    batch_tokenize_docs(df=knowledge_base)

    emb_matrix_name = []

    # Step 2 : Process the knowledge base to generate embeddings in batch processing 
    for i, item in enumerate(knowledge_base['package_name']):
        emb_matrix_name.append(get_embedding_sync(item))
        if i%1000==0: logger.info(f"idx : {i}")
    
    emb_matrix_name = np.array(emb_matrix_name)
    logger.info(f"Embed matrix shape: {emb_matrix_name.shape}")

    # Step 3 : Save the embedding matrix to Azure
    byte_io = io.BytesIO()
    np.save(byte_io, emb_matrix_name)
    byte_io.seek(0)

    upload_file_to_azure(f=byte_io, filename='embed_matrix.npy')

    return f"Embeddings created & uploaded to Azure at {datetime.now().isoformat()}"
