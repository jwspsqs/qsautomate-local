import datetime as dt

from prefect import task
from prefect import get_run_logger

from qsconnect import Client

from qsautomate.config.prefect import DATA_TASK_CONFIG


@task(
    name="download-prices-fmp",
    description="Download prices from FMP",
    tags=["data", "fmp", "prices"],
    **DATA_TASK_CONFIG,
)
def download_prices_fmp(start_date: dt.date, run_date: dt.date) -> None:
    logger = get_run_logger()
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


@task(
    name="download-fundamentals-fmp",
    description="Download fundamentals from FMP",
    tags=["data", "fmp", "fundamentals"],
    **DATA_TASK_CONFIG,
)
def download_fundamentals_fmp(
    start_date: dt.date, run_date: dt.date, api_buffer_seconds: int = 10
) -> None:
    logger = get_run_logger()
    logger.info(f"Downloading fundamentals from FMP for {start_date} to {run_date}")
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


@task(
    name="build-datalake-fmp",
    description="Cache downloaded files from FMP into DuckDB",
    tags=["data", "fmp", "datalake"],
    **DATA_TASK_CONFIG,
)
def build_datalake_fmp() -> None:
    logger = get_run_logger()
    logger.info("Building datalake from cached FMP files")
    client = Client()

    # Store in a new database instance (destroys existing database)
    cached_files = client.detect_cached_files()
    client.load_cached_files_to_database(
        cached_files,
        fresh=True,
    )

    client.delete_cached_files(cached_files_df=cached_files)
