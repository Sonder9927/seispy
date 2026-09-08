from pathlib import Path
from typing import Any

from obspy.clients.fdsn import Client
from obspy.core.inventory import Inventory

EARTHSCOPE_URL = "https://service.earthscope.org"


def download_inventory(
    output_file: str | Path | None = None,
    *,
    client: str = EARTHSCOPE_URL,
    username: str | None = None,
    password: str | None = None,
    level: str = "response",
    **query: Any,
) -> Inventory:
    """Download station metadata from an FDSN service.

    Args:
        output_file: Optional destination for a StationXML copy.
        client: ObsPy FDSN client name or service URL.
        username: Username for restricted services.
        password: Password for restricted services.
        level: FDSN metadata detail level.
        **query: Additional filters accepted by ``Client.get_stations``.

    Returns:
        The downloaded ObsPy inventory.

    Raises:
        ValueError: If only one credential is supplied.

    Examples:
        >>> inventory = download_inventory(
        ...     "stations.xml", network="NZ", station="WEL", channel="BH?"
        ... )
        >>> len(inventory.networks) >= 0
        True
    """
    fdsn = _client(client, username, password)
    inventory = fdsn.get_stations(level=level, **query)
    if output_file is not None:
        path = Path(output_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        inventory.write(str(path), format="STATIONXML")
    return inventory


def _client(base_url, username=None, password=None):
    if (username is None) != (password is None):
        raise ValueError("username and password must be provided together")
    kwargs = {"user": username, "password": password} if username else {}
    return Client(base_url, **kwargs)
