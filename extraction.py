from pyspark.sql import SparkSession
import requests
import pandas as pd
import psycopg2
from pyspark.sql import Row
import sys

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Extract Data from Multiple Sources") \
    .getOrCreate()

# Function to extract data from API
def extract_from_api(api_url):
    response = requests.get(api_url)
    data = response.json()
    return spark.createDataFrame(pd.json_normalize(data))

# Function to extract data from PostgreSQL
def extract_from_pgdb(db_url, query):
    conn = psycopg2.connect(db_url)
    df = pd.read_sql(query, conn)
    conn.close()
    return spark.createDataFrame(df)

# Function to extract data from CSV
def extract_from_csv(csv_path):
    return spark.read.csv(csv_path, header=True, inferSchema=True)

# Example usage
if __name__ == "__main__":
    api_url = "http://127.0.0.1:5000/payments"
    db_url = "postgresql://user:password@host:port/dbname"
    query = "SELECT * FROM tablename"
    csv_path = "path/to/your/file.csv"

    # Extract data from each source
    api_df = extract_from_api(api_url)
    pg_df = extract_from_pgdb(db_url, query)
    csv_df = extract_from_csv(csv_path)

    # Show the extracted data
    api_df.show()
    pg_df.show()
    csv_df.show()

    # Stop the SparkSession
    spark.stop()
# ---------------------------
from pyspark.sql import SparkSession
import requests
import pandas as pd
import psycopg2
from pyspark.sql import Row
import logging

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Extract Data from Multiple Sources") \
    .getOrCreate()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_from_api(api_url):
    """Extract data from an API."""

    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Raise HTTPError for bad responses
        data = response.json()
        
        if not data:
            logger.info("No data available from API endpoint.")
            sys.exit(1)  # Exit the program with an error status

    except requests.RequestException as e:
        logger.error(f"Error fetching data from API: {e}")

    if data:
        return spark.createDataFrame(pd.json_normalize(data))
    else:
        logger.warning("No data fetched from API.")
        sys.exit(1)  # Exit the program with an error status

def extract_from_pgdb(db_url, queries):
    """Extract data from multiple PostgreSQL tables."""
    dataframes = []
    
    for query in queries:
        try:
            conn = psycopg2.connect(db_url)
            df = pd.read_sql(query, conn)
            conn.close()
            dataframes.append(spark.createDataFrame(df))
        
        except psycopg2.Error as e:
            logger.error(f"Error fetching data from PostgreSQL: {e}")
    
    return dataframes

def extract_from_csv(csv_paths):
    """Extract data from multiple CSV files."""
    dataframes = []
    
    for csv_path in csv_paths:
        try:
            df = spark.read.csv(csv_path, header=True, inferSchema=True)
            dataframes.append(df)
        
        except Exception as e:
            logger.error(f"Error reading CSV file {csv_path}: {e}")
    
    return dataframes

# Example usage
if __name__ == "__main__":
    api_urls = [
        {"url": "http://127.0.0.1:5000/payments"},
        {"url": "http://127.0.0.1:5000/orders"},
        {"url": "http://127.0.0.1:5000/reviews"}
    ]
    
    db_url = "postgresql://user:password@host:port/dbname"
    queries = [
        "SELECT * FROM ecom.customers",
        "SELECT * FROM table2",
        # Add more queries as needed
    ]
    
    csv_paths = [
        "path/to/your/file1.csv",
        "path/to/your/file2.csv",
        # Add more CSV file paths as needed
    ]

    # Extract data from each source
    api_dfs = [extract_from_api(api["url"], api.get("params")) for api in api_urls]
    pg_dfs = extract_from_pgdb(db_url, queries)
    csv_dfs = extract_from_csv(csv_paths)

    # Show the extracted data
    for df in api_dfs:
        df.show()
    
    for df in pg_dfs:
        df.show()
    
    for df in csv_dfs:
        df.show()

    # Stop the SparkSession
    spark.stop()

