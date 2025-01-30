import re

from typing import Dict, Optional

def interval_to_milliseconds(interval: str) -> Optional[int]:
    """Convert a interval string to milliseconds

    Args:
        interval: interval string, e.g.: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w

    Returns:
         int value of interval in milliseconds
         None if interval prefix is not a decimal integer
         None if interval suffix is not one of m, h, d, w

    """
    seconds_per_unit: Dict[str, int] = {
        "s": 1,
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60,
    }
    try:
        return int(interval[:-1]) * seconds_per_unit[interval[-1]] * 1000
    except (ValueError, KeyError):
        return None


def interval_to_minutes(interval: str) -> int:
    """
    Converts a string like '1m', '5m', '1h', '1d', '1w'
    into the corresponding number of minutes.
    Raises ValueError for invalid formats.
    """
    # Match patterns like '5m', '1h', '2d', '1w'
    match = re.match(r"(\d+)([mhdw])", interval.lower())
    if not match:
        raise ValueError(f"Invalid interval format: '{interval}'")

    value_str, unit = match.groups()
    value = int(value_str)

    if unit == 'm':
        return value
    elif unit == 'h':
        return value * 60
    elif unit == 'd':
        return value * 60 * 24
    elif unit == 'w':
        return value * 60 * 24 * 7
    else:
        raise ValueError(f"Unknown time unit: '{unit}'")


def search_suitable_interval(available_intervals, target_interval):
    """
    Searchs for the next smallest available interval.
    If no suitable interval is available, it raises an error.

    Args:
        available_intervals (list): List of available intervals (e.g., ['1m', '5m', '1h']).
        target_interval (str): Desired aggregation interval (e.g., '5m', '1h').

    Returns:
        str: smallest available interval
    """
    # Sort available intervals by their numeric minute value
    available_intervals_sorted = sorted(
        available_intervals,
        key=interval_to_minutes
    )

    target_minutes = interval_to_minutes(target_interval)
    chosen = None

    # Traverse sorted intervals (smallest to largest),
    # updating 'chosen' whenever we find one <= target
    for iv in available_intervals_sorted:
        if interval_to_minutes(iv) <= target_minutes:
            chosen = iv

    if chosen is None:
        # Means none of the intervals are <= the target
        raise ValueError(f"No suitable interval <= {target_interval} found!")

    return chosen