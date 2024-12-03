from azure.storage.blob import BlobServiceClient
from azure_storage import upload_file_to_azure, get_file_from_azure
from dotenv import load_dotenv
from io import BytesIO
import pandas as pd
import numpy as np
import re
import os
import logging

load_dotenv()

CONNECTION_STRING = os.getenv('CONNECTION_STRING')
CONTAINER_NAME = os.getenv('CONTAINER_NAME')
blob_service_client = BlobServiceClient.from_connection_string(conn_str=CONNECTION_STRING)

logging.basicConfig(
    level=logging.INFO, 
    handlers=[
        logging.StreamHandler()  # Ensures logs are sent to stdout/stderr
    ]
)
logger = logging.getLogger('clead_data.py')

new_column_names = [
    'package_name',
    'package_picture',
    'url',
    'price',
    'cash_discount',
    'installment_month',
    'price_after_cash_discount',
    'installment_limit',
    'price_to_reserve_for_this_package',
    'shop_name',
    'category',
    'preview',
    'selling_point',
    'brand',
    'min_max_age',
    'locations',
    'price_details',
    'package_details',
    'important_info',
    'payment_info',
    'general_info',
    'early_signs_for_diagnosis',
    'how_to_diagnose',
    'hdcare_summary',
    'common_question',
    'know_this_disease',
    'courses_of_action',
    'signals_to_proceed_surgery',
    'get_to_know_this_surgery',
    'comparisons',
    'getting_ready',
    'recovery',
    'side_effects',
    'review_4_5_stars',
    'brand_promote',
    'faq',
]

def clean_location_info(df, column_name):

  patterns = [
      r"When to open:.+\n",
      r"Parking lot:.+\n",
      r'How to transport:.+\n',
      r'Google Maps link: ',
  ]
  combined_pattern = "|".join(patterns)

  # Handle non-string values before applying regex
  def clean_value(x):
    # Convert non-strings to strings (if possible)
    if not pd.api.types.is_string_dtype(x):
      x = str(x)  # Try converting to string
    # Handle missing values (e.g., NaN)
    if pd.isna(x):
      x = ""  # Replace with an empty string for missing values
    return re.sub(combined_pattern, '', x)

  df[column_name] = df[column_name].apply(clean_value)
  return df

def convert_to_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def convert_to_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0

def convert_to_str(value):
    if value is None:
        return ""
    return str(value)

def clean(df):
    extracted_cols = df['P3, Cash Discount, Installment/month'].str.extract(r'P3: (\d*) THB, Cash: (\.|\d)* THB, Installment/month: (.*)')
    extracted_cols.columns = ['P3', 'Cash Discount', 'Installment/month']

    # Converting the columns to numeric types
    extracted_cols['P3'] = pd.to_numeric(extracted_cols['P3'])
    extracted_cols['Cash Discount'] = pd.to_numeric(extracted_cols['Cash Discount'])

    # Adding the new column "price after cash discount"
    extracted_cols['Price After Cash Discount'] = extracted_cols['P3'] - extracted_cols['Cash Discount']

    df.drop(columns=['Category Tags', 'Meta Keywords', 'Meta Description', 'Brand Ranking (Position)'], inplace=True)

    for i, col in enumerate(extracted_cols.columns):
        df.insert(df.columns.get_loc('P3, Cash Discount, Installment/month') + 1 + i, col, extracted_cols[col])

    df.drop(columns=['P3, Cash Discount, Installment/month'], inplace=True)
    df['Installment/month'] = np.where(df['Installment/month'] == 'N/A', '', df['Installment/month'])

    df = clean_location_info(df.copy(), "Locations, Time Open/Close, How to Transport, Parking, Google Maps")

    current_columns = df.columns.tolist()
    if len(current_columns) != len(new_column_names):
        raise ValueError("The number of columns in the CSV does not match the number of new column names")

    column_mapping = {current: new for current, new in zip(current_columns, new_column_names)}

    # Rename columns
    df.rename(columns=column_mapping, inplace=True)

    numerical_cols = df.select_dtypes(include=['number']).columns
    df[numerical_cols] = df[numerical_cols].fillna(np.nan)

    # For categorical columns, fill missing values with an empty string
    categorical_cols = df.select_dtypes(include=['object']).columns
    df[categorical_cols] = df[categorical_cols].fillna("-")

    # Apply the conversion functions before exporting to CSV
    df['url'] = df['url'].apply(convert_to_str)
    df['price'] = df['price'].apply(convert_to_float)
    df['cash_discount'] = df['cash_discount'].apply(convert_to_float)
    df['price_after_cash_discount'] = df['price_after_cash_discount'].apply(convert_to_float)
    df['price_to_reserve_for_this_package'] = df['price_to_reserve_for_this_package'].apply(convert_to_float).replace(np.nan, 0.0)
    df['installment_month'] = df['installment_month'].apply(convert_to_str).replace("", "0")
    df['installment_limit'] = df['installment_limit'].apply(convert_to_str).replace("", "-")

    return df

async def create_packages(files):
    if not files or len(files) == 0:
        raise ValueError("No files provided.")
    
    f1, f2 = None, None
    df = None

    if len(files) > 1:
        f1 = await files[0].read()
        f2 = await files[1].read()
    else:
        f1 = await files[0].read()
    
    if f2:
        df_part_1 = pd.read_excel(f1)
        df_part_2 = pd.read_excel(f2)
        df = pd.concat([df_part_1, df_part_2])
        logging.info("Merged xlsx files")
    else:
        df = pd.read_excel(f1)
        logging.info("Read single file successfully")
    
    df = clean(df)

    csv_content = df.to_csv(index=False)
    logger.info("Converted df to CSV format")

    upload_file_to_azure(f=csv_content, filename="packages.csv")
    return "Uploaded packages.csv"
    


