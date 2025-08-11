from omega.utils.ibcontroller import IBC, Watchdog, connect
from omega import Omega


def connect_to_ibkr(client_id: int = 1, **kwargs) -> Omega:
    """
    Connect to Interactive Brokers (IBKR) using Omega and IBController.

    This function initializes an IBController (IBC) instance for the IB Gateway in paper trading mode,
    sets up an Omega application, and starts a Watchdog to manage the connection. The Omega app is then
    started and returned.

    Parameters
    ----------
    client_id : int, optional
        The client ID to use for the IBKR connection (default is 1).
    **kwargs
        Additional keyword arguments passed to the Omega constructor.

    Returns
    -------
    Omega
        An instance of the Omega application, connected to IBKR.

    Notes
    -----
    - Uses IBController (IBC) version 10.30 in gateway mode with paper trading.
    - The Watchdog is started on port 4002 (default for IB Gateway paper trading).
    - The function blocks until the Omega app is stopped.
    """
    ibc = IBC(
        "10.30",
        gateway=True,
        trading_mode="paper",
    )
    app = Omega(connect_on_startup=False, **kwargs)
    wd = Watchdog(
        ibc,
        app,
        port=4002,
    )
    wd.start()
    app.run()
    return app
