from typing import List
from pythainlp.tokenize import word_tokenize
from azure_storage import upload_file_to_azure
import pandas as pd
import json
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('tokenizer.py')

load_dotenv()

def batch_tokenize_docs(df:pd.DataFrame):
    texts = []
    for i in range(len(df)):
        text = f"""
        package_name : {df.loc[i, 'package_name']} 
        """
        texts.append(text)


    tokenized_docs = []
    original_indices = []

    logger.info(f"Starting tokenization of {len(texts)} documents...")

    for idx, text in enumerate(texts):
        try:
            if pd.isna(text) or not isinstance(text, str):
                continue

            # Normalize & Tokenize
            text = ' '.join(str(text).split())
            tokens = word_tokenize(text, engine="newmm")
            tokens = [token for token in tokens if token.strip()]

            if tokens:
                tokenized_docs.append(tokens)
                original_indices.append(idx)

        except Exception as e:
            logger.error(f"Error tokenizing document {idx} : {str(e)}")
            continue
    
    data = {
        "tokens": tokenized_docs,
        "indices": original_indices
    }

    data = json.dumps(data, ensure_ascii=False)
    upload_file_to_azure(f=data, filename="tokenized_docs.json")
    logger.info("Successfully uploaded tokenised docs")