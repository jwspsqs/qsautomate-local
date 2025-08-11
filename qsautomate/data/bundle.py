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
    """
    Build and ingest a Zipline data bundle from FMP database tables.

    This function connects to the database, stages the necessary data for Zipline,
    registers the bundle (if not already registered), and ingests the bundle for use
    in Zipline backtests.

    Parameters
    ----------
    bundle_name : str
        The name of the Zipline bundle to register and ingest.

    Returns
    -------
    None
        This function does not return a value. The bundle is ingested as a side effect.

    Notes
    -----
    - The function connects to the database and stages data using the `qsconnect.Client`.
    - Duplicate bundle registration errors are avoided by catching `KeyError`.
    - The ingested bundle will be available for Zipline backtests under the specified name.
    """
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
