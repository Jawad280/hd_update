from openai import AsyncAzureOpenAI
from dotenv import load_dotenv
import os
import numpy as np
import pandas as pd
import logging
import io
from azure_storage import upload_file_to_azure, get_file_from_azure
from tokenizer import batch_tokenize_docs

logging.basicConfig(level=logging.INFO)
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

async def get_embedding(text, model=TEXT_EMBEDDING_MODEL):
    text = text.replace("\n", " ")
    resp = await client.embeddings.create(input=[text], model=model)
    embedding_vector = resp.data[0].embedding

    return embedding_vector


async def create_package_embeddings():

    packages = get_file_from_azure(filename="packages.csv")

    logger.info("Beginning embedding process")
    knowledge_base = pd.read_csv(packages)
    knowledge_base = knowledge_base[:2]

    logger.info("Generating json docs")
    batch_tokenize_docs(df=knowledge_base)

    emb_matrix_name = []

    for i, item in enumerate(knowledge_base['package_name']):
        emb_matrix_name.append(await get_embedding(item))
        if i%1000==0: logger.info(f"idx : {i}")
    
    emb_matrix_name = np.array(emb_matrix_name)
    logger.info(f"Embed matrix shape: {emb_matrix_name.shape}")

    byte_io = io.BytesIO()
    np.save(byte_io, emb_matrix_name)
    byte_io.seek(0)

    upload_file_to_azure(f=byte_io, filename='embed_matrix.npy')

    return "Embeddings created & uploaded to Azure !"
