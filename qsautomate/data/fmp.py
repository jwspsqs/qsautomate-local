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
    """
    Download historical stock prices from FMP and store them in the database.

    This function downloads historical price data for all US stocks (NASDAQ, NYSE) with a price above $5,
    as well as selected benchmark ETFs (SPY, QQQ, IWM), for the specified date range. The data is cached
    and then loaded into the database.

    Parameters
    ----------
    start_date : datetime.date
        The start date for the price data download (inclusive).
    run_date : datetime.date
        The end date for the price data download (inclusive).

    Returns
    -------
    None
        This function does not return a value. Data is stored in the database as a side effect.

    Notes
    -----
    - Only stocks listed on NASDAQ and NYSE with a price greater than $5 are included.
    - Benchmark ETFs (SPY, QQQ, IWM) are always included.
    - Data is cached and then loaded into the database.
    """
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
        fresh=True,
    )


@task(
    name="download-fundamentals-fmp",
    description="Download fundamentals from FMP",
    tags=["data", "fmp", "fundamentals"],
)
def download_fundamentals_fmp(
    start_date: dt.date, run_date: dt.date, api_buffer_seconds: int = 10
) -> None:
    """
    Download and store fundamental financial statement data from FMP.

    This function downloads bulk financial statements (income statement, balance sheet, cash flow statement, ratios)
    for all available stocks for all periods between the specified years. It validates and deduplicates cached files,
    and loads the data into the database.

    Parameters
    ----------
    start_date : datetime.date
        The start date for the data download. Only the year is used.
    run_date : datetime.date
        The end date for the data download. Only the year is used.
    api_buffer_seconds : int, optional
        Number of seconds to wait between API calls to avoid rate limits (default is 10).

    Returns
    -------
    None
        This function does not return a value. Data is stored in the database as a side effect.

    Notes
    -----
    - Downloads all available periods for each statement type.
    - Validates and deduplicates cached files before loading.
    - Performs a full rebuild of the database with the new data.
    """
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
    client.delete_duplicate_cached_files(dry_run=False)

    # Load data into database
    client.load_cached_files_to_database(
        cached_files_df=cached_files_df,
        fresh=True,  # Full rebuild
    )
