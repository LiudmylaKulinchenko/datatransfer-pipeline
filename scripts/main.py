import asyncio
from datetime import datetime

from scripts.bigquery_client import bigquery_client
from scripts.fetch_bigquery import dates_list_from_bigquery, fetch_concurrent_data


def main() -> None:
    available_dates = dates_list_from_bigquery(bigquery_client)
    all_data_ddf = asyncio.run(fetch_concurrent_data(available_dates, bigquery_client))
    print(all_data_ddf, type(all_data_ddf))
    try:
        print("Saving data to parquet...")
        all_data_ddf.to_parquet("..data/full_bigquery_data.parquet", engine="pyarrow")
        print("Bigquery data saved to parquet")
    except Exception as e:
        print("During saving Bigquery data to parquet exception occured: ", e)


if __name__ == "__main__":
    start = datetime.now()

    main()

    finish = datetime.now()
    print(
        f"Start time: {start.strftime('%H:%M:%S')}\n"
        f"Finish time: {finish.strftime('%H:%M:%S')}\n"
        f"Execution: {(finish - start).total_seconds()} s"
    )
