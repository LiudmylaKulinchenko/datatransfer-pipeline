import configparser
import dask.dataframe as dd
from dask import delayed
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from google.cloud import bigquery

from bigquery_client import bigquery_client

config = configparser.ConfigParser()
config.read("config.ini")

DATASET_ID = config["GoogleBigQueryConfig"]["dataset_id"]
PROJECT_ID = config["GoogleBigQueryConfig"]["project_id"]
TABLE_NAME = config["GoogleBigQueryConfig"]["table_name"]


def dates_list_from_bigquery(client: bigquery.Client) -> list[str]:
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    tables = list(client.list_tables(dataset_ref))

    # Assumes table names are in the format "ga_sessions_YYYYMMDD"
    dates = [
        table.table_id.split("_")[2]
        for table in tables
        if len(table.table_id.split("_")) > 2 and table.table_id.split("_")[2].isdigit()
    ]
    print("Successfully generated list of BigQuery dates. Dates length: ", len(dates))
    return dates


def get_dataframe_from_bigquery(sql_query: str, client: bigquery.Client) -> pd.DataFrame:
    """Function to fetch data from BigQuery and return a DataFrame."""
    try:
        df = client.query(sql_query).result().to_dataframe()
    except Exception as e:
        print(f"Error fetching data: {e}")
        df = pd.DataFrame()  # Return empty DataFrame on error
    return df



@delayed
def fetch_bigquery_data_for_date(date: str, client: bigquery.Client) -> pd.DataFrame:
    """Delayed function to fetch data."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}{date}`"
    df = get_dataframe_from_bigquery(query, client)
    return df


def fetch_concurrent_data(dates: list[str], client: bigquery.Client) -> dd.DataFrame:
    # Wrap fetching function with Dask's delayed
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(fetch_bigquery_data_for_date, date, client) for date in dates]
        delayed_dfs = [future.result() for future in futures]  # Results will be a list of Pandas DataFrames
    # Use Dask to compute all delayed operations into a Dask DataFrame
    dask_df = dd.from_delayed(delayed_dfs)
    return dask_df


if __name__ == "__main__":
    start = datetime.now()

    available_dates = dates_list_from_bigquery(bigquery_client)
    # Compute the Dask DataFrame using threads
    all_data_ddf = fetch_concurrent_data(available_dates, bigquery_client)

    print(all_data_ddf, type(all_data_ddf))

    finish = datetime.now()
    print(
        f"Start time: {start.strftime('%H:%M:%S')}\nFinish time: {finish.strftime('%H:%M:%S')}\nExecution: {(finish - start).total_seconds()} s")
