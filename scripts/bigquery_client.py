from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

bigquery_client = bigquery.Client()
