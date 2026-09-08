from pathlib import Path
from typing import Any

import pandas as pd
from obspy import UTCDateTime
from obspy.clients.fdsn import Client


def download_earthquake_events(
    starttime: Any,
    endtime: Any,
    output_csv: str | Path | None = None,
    *,
    client: str = "USGS",
    **query: Any,
) -> pd.DataFrame:
    """Download and normalize an earthquake catalog from an FDSN service.

    Args:
        starttime: Start of the query as an ObsPy-compatible time value.
        endtime: End of the query as an ObsPy-compatible time value.
        output_csv: Optional destination for the normalized catalog.
        client: ObsPy FDSN client name or service URL.
        **query: Additional filters accepted by ``Client.get_events``.

    Returns:
        A time-sorted data frame with origin, depth in kilometers, magnitude,
        and magnitude type.

    Examples:
        >>> events = download_earthquake_events(
        ...     "2025-01-01", "2025-02-01", minmagnitude=5.5
        ... )
        >>> "time" in events.columns
        True
    """
    catalog = Client(client).get_events(
        starttime=UTCDateTime(starttime), endtime=UTCDateTime(endtime), **query
    )
    rows = []
    for event in catalog:
        origin = event.preferred_origin() or (event.origins[0] if event.origins else None)
        magnitude = event.preferred_magnitude() or (
            event.magnitudes[0] if event.magnitudes else None
        )
        if origin is None or magnitude is None:
            continue
        rows.append(
            {
                "time": origin.time.datetime,
                "longitude": origin.longitude,
                "latitude": origin.latitude,
                "depth": origin.depth / 1000 if origin.depth is not None else None,
                "mag": magnitude.mag,
                "magnitude_type": magnitude.magnitude_type,
            }
        )
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame["time"] = pd.to_datetime(frame["time"], utc=True)
        frame = frame.sort_values("time").reset_index(drop=True)
        frame["time"] = frame["time"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f").str[:23] + "Z"
    if output_csv is not None:
        path = Path(output_csv)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(path, index=False, encoding="utf-8")
    return frame
