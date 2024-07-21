from pyspark.sql import SparkSession, Row
import requests
import json
import logging
from io import StringIO
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create a Spark session with S3 configuration
spark = SparkSession.builder \
    .appName("APIDataExtraction") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIAWBCPXRNOSEAX746W") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.cores", "2") \
    .getOrCreate()

def fetch_data_from_api(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Check for HTTP errors
        if isinstance(response.json(), list):
            df = pd.DataFrame(response.json())
        else:
            df = pd.json_normalize(response.json())  # Normalize if it's a nested JSON object
        
        logging.info(f"Successfully fetched data from {api_url}")
        return(df)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API {api_url}: {e}")
        return ""

api_urls = {
    "orders": "http://127.0.0.1:5000/orders",
    "reviews": "http://127.0.0.1:5000/reviews",
    "payments": "http://127.0.0.1:5000/payments"
}

s3_base_path = "s3a://paralleldots"

for table, url in api_urls.items():
    data = fetch_data_from_api(url)
    # if data:
    try:
        # Use StringIO to simulate file-like object
        # json_data = StringIO(data)
        # df = spark.read.json(data)
        # df.show()
        # Save DataFrame to S3 as Parquet
        df = spark.createDataFrame(data)
        print(df.show())
        # Convert RDD to DataFrame
        # df = spark.read.json(rdd)

        # df.write.format("parquet").mode("overwrite").save(f"{s3_base_path}/{table}")
        logging.info(f"Successfully saved data for {table} to S3 bucket")
    except Exception as e:
        logging.error(f"Error processing data for {table}: {e}")
    # else:
    #     logging.warning(f"No data received for {table}")

# Stop the Spark session
spark.stop()
