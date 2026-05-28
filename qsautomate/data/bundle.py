from zipline.data import bundles
from zipline.data.bundles import register

from prefect import task
from prefect import get_run_logger

from qsconnect import Client

from qsautomate.config.prefect import BACKTEST_TASK_CONFIG


@task(
    name="build-zipline-bundle",
    description="Build Zipline bundle",
    tags=["data", "bundle"],
    **BACKTEST_TASK_CONFIG,
)
def build_zipline_bundle(bundle_name: str) -> None:
    logger = get_run_logger()
    logger.info(f"Building Zipline bundle: {bundle_name}")

    # Connect to DB and stage data
    client = Client()
    client.connect_to_database(read_only=True)
    client.ingest_zipline_bundle_from_fmp_tables(bundle_name=bundle_name)

    # Avoid duplicate registration errors
    try:
        register(bundle_name)
    except KeyError:
        pass

    bundles.ingest(bundle_name)
    logger.info(f"Bundle {bundle_name} built successfully")
