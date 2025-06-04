import logging
import datetime as dt

from prefect import flow, task

from dotenv import load_dotenv
from qsconnect import Client


load_dotenv()

logger = logging.getLogger(__name__)


@task(
    name="download-prices-fmp",
    description="Download prices from FMP",
    tags=["data", "fmp", "prices"],
)
def download_prices_fmp(start_date: dt.date, run_date: dt.date) -> None:

    logger.info(f"Downloading prices from FMP for {start_date} to {run_date}")
    client = Client()
    stock_list = client.stock_list("stock")

    # Filter stocks
    stock_list_filtered = stock_list[stock_list["type"] == "stock"]
    stock_list_filtered = stock_list_filtered[
        stock_list_filtered["exchangeShortName"].isin(["NASDAQ", "NYSE"])
    ]
    stock_list_filtered = stock_list_filtered[stock_list_filtered["price"] > 5]

    # Get benchmark ETFs
    benchmarks = ["SPY", "QQQ", "IWM"]

    # Download historical prices
    client.historical_prices(
        stock_list_filtered["symbol"].tolist() + benchmarks,
        start_date=start_date,
        end_date=run_date,
        cache=True,
        api_calls_per_minute=2900,
    )

    # Store data in database
    all_files = client.detect_cached_files()
    client.load_cached_files_to_database(
        all_files,
        fresh=False,
    )


@task(
    name="download-fundamentals-fmp",
    description="Download fundamentals from FMP",
    tags=["data", "fmp", "fundamentals"],
)
def download_fundamentals_fmp(
    start_date: dt.date, run_date: dt.date, api_buffer_seconds: int = 10
) -> None:
    client = Client()

    statement_types = [
        "income-statement",
        "balance-sheet-statement",
        "cash-flow-statement",
        "ratios",
    ]

    # Download financial statements
    client.fetch_bulk_financial_statements(
        statement_type=statement_types,
        periods="all",
        start_year=start_date.year,
        end_year=run_date.year,
        api_buffer_seconds=api_buffer_seconds,
    )

    # Validate cached files
    cached_files_df = client.detect_cached_files()
    client.detect_missing_cached_files(
        statement_type=statement_types,
        periods="all",
        start_year=start_date.year,
        end_year=run_date.year,
    )
    client.detect_duplicate_cached_files(return_duplicates_only=True)

    # Load data into database
    client.load_cached_files_to_database(
        cached_files_df=cached_files_df,
        fresh=False,
    )


@flow(name="download-data", description="Download data from FMP")
def main(start_date: dt.date, run_date: dt.date, api_buffer_seconds: int = 10) -> None:

    download_prices_fmp(start_date, run_date),
    download_fundamentals_fmp(start_date, run_date, api_buffer_seconds),


if __name__ == "__main__":

    main(
        start_date=dt.date(2010, 1, 1),
        run_date=dt.date(2025, 5, 13),
        api_buffer_seconds=10,
    )
