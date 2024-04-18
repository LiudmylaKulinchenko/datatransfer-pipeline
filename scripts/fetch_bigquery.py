import asyncio
import configparser
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import dask.dataframe as dd
import pandas as pd
from dask import delayed
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

    dates = []
    for table in tables:
        # Assumes table names are in the format "ga_sessions_YYYYMMDD"
        parts = table.table_id.split("_")
        if len(parts) > 2 and parts[2].isdigit():
            dates.append(parts[2])

    print("Successfully generated list of BigQuery dates. Dates length: ", len(dates))

    return dates


def get_dataframe_from_bigquery(
    sql_query: str, client: bigquery.Client
) -> pd.DataFrame:
    """Function to fetch data from BigQuery and return a DataFrame."""
    try:
        df = client.query(sql_query).result().to_dataframe()
    except Exception as e:
        print(f"Error fetching data: {e}")
        df = pd.DataFrame()  # Return empty DataFrame on error
    return df


@delayed
def fetch_data_delayed(date: str, client: bigquery.Client) -> pd.DataFrame:
    """Delayed function to fetch data."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}{date}`"
    df = get_dataframe_from_bigquery(query, client)
    return df


async def fetch_bigquery_data_for_date(
    date: str, client: bigquery.Client
) -> dd.DataFrame:
    """Asynchronous wrapper to use Dask's delayed function."""
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        dask_df = await loop.run_in_executor(pool, fetch_data_delayed, date, client)
    return dask_df


async def fetch_concurrent_data(
    dates: list[str], client: bigquery.Client
) -> dd.DataFrame:
    """Fetch data concurrently using Dask and asyncio."""
    tasks = [fetch_bigquery_data_for_date(date, client) for date in dates]
    delayed_frames = await asyncio.gather(*tasks)
    # Compute all delayed operations into a Dask DataFrame
    dask_df = dd.from_delayed(delayed_frames)
    return dask_df


# Use this in your main block
if __name__ == "__main__":
    start = datetime.now()

    # available_dates = ["20170312", "20170313", "20170314"]  # Example dates
    available_dates = dates_list_from_bigquery(bigquery_client)
    all_data_ddf = asyncio.run(fetch_concurrent_data(available_dates, bigquery_client))
    print(all_data_ddf, type(all_data_ddf))
    all_data_ddf.to_parquet()

    finish = datetime.now()

    print(
        f"Start time: {start.strftime('%H:%M:%S')}\n"
        f"Finish time: {finish.strftime('%H:%M:%S')}\n"
        f"Execution: {(finish - start).total_seconds()} s"
    )
