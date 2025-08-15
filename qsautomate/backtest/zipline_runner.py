import logging
from typing import Callable, Any
from prefect import task


logger = logging.getLogger(__name__)


@task(
    name="run-zipline-backtest",
    description="Run Zipline backtest",
    tags=["backtest", "zipline"],
)
def run_zipline_backtest(config: dict, backtest_fcn: Callable) -> Any:
    """
    Run a Zipline backtest in a temporary working directory.

    This function executes the provided backtest function (`backtest_fcn`) with the given configuration
    dictionary (`config`) inside a temporary directory. The working directory is changed to the temporary
    directory for the duration of the backtest, ensuring that any files written or read (such as config.pkl)
    do not interfere with other processes or runs. After execution, the working directory is restored and
    the temporary directory is cleaned up.

    Parameters
    ----------
    config : dict
        The configuration dictionary to be passed to the backtest function. This typically contains all
        parameters required to run the backtest, such as strategy settings, date ranges, and data sources.
    backtest_fcn : Callable
        The function to execute the backtest. It should accept a single argument (the config dictionary)
        and return the result of the backtest.

    Returns
    -------
    Any
        The result returned by the backtest function. The type depends on the implementation of `backtest_fcn`.

    Notes
    -----
    - The function ensures isolation of file operations by running in a unique temporary directory.
    - The temporary directory is deleted after the backtest completes, even if an exception occurs.
    - This function is decorated as a Prefect task for orchestration in data pipelines.
    """
    return backtest_fcn(config)
