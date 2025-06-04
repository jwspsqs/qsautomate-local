import logging

from zipline.data import bundles
from zipline.data.bundles import register

from prefect import task

from dotenv import load_dotenv
from qsconnect import Client


load_dotenv()

logger = logging.getLogger(__name__)


@task(
    name="build-zipline-bundle",
    description="Build Zipline bundle",
    tags=["data", "bundle"],
)
def build_zipline_bundle(bundle_name: str) -> None:

    # Connect to DB and stage data
    client = Client()
    client.connect_to_database()
    client.ingest_zipline_bundle_from_fmp_tables()

    # Avoid duplicate registration errors
    try:
        register(bundle_name)
    except KeyError:
        pass

    bundles.ingest(bundle_name)


@task(name="build-zipline-bundle", description="Build Zipline bundle")
def main(bundle_name: str) -> None:
    build_zipline_bundle(bundle_name)


if __name__ == "__main__":

    main(
        bundle_name="historical_prices_fmp",
    )
