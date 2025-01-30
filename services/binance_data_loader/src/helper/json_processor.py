"""
Module for loading and saving historical OHLCV data in JSON format.

This module provides two functions: `load_last_close_time_from_json` to load the last close time
from a JSON file, and `save_to_json` to save OHLCV (Open, High, Low, Close, Volume) data to a JSON file
for a specified trading symbol and interval.

Dependencies:
    - json
    - pandas
    - pathlib

Example:
    To load the last close time and save new data:

    ```python
    symbol = "BNBBTC"
    interval = "1h"
    last_close_time = load_last_close_time_from_json(symbol, interval)
    new_data = [...]  # List of OHLCV data
    save_to_json(new_data, symbol, interval)
    ```
"""

import json
import pandas as pd
from pathlib import Path
from typing import List, Optional


def load_last_close_time_from_json(symbol: str, interval: str) -> Optional[int]:
    """
    Loads the last close time in the JSON file for the given symbol and interval.

    This function reads a JSON file containing OHLCV data for the specified trading symbol
    and interval and retrieves the close time of the last candlestick in the data. If the
    file or data does not exist, it returns `None`.

    Parameters
    ----------
    symbol : str
        Name of the trading pair, e.g., "BNBBTC".
    interval : str
        Binance Kline interval, e.g., "1m" for 1 minute.

    Returns
    -------
    Optional[int]
        Last close time in milliseconds as an integer, or `None` if the file or data is not found.

    Notes
    -----
    - The JSON file should be named in the format `<symbol>_<interval>.json`.
    - This function expects each entry in the JSON file to contain a 'close time' field.

    Examples
    --------
    Load the last close time for the "BNBBTC" trading pair on a 1-hour interval:

    ```python
    last_close_time = load_last_close_time_from_json("BNBBTC", "1h")
    if last_close_time:
        print(f"Last close time: {last_close_time}")
    else:
        print("No data available.")
    ```
    """
    file_path = Path(f"{symbol}_{interval}.json")
    if file_path.exists():
        with open(file_path, "r") as file:
            data = json.load(file)
            if data:
                last_close_time = data[-1]['close time']
                return last_close_time
    return None


def save_to_json(data: List[List], symbol: str, interval: str) -> None:
    """
    Saves the given OHLCV data to a JSON file for the specified trading symbol and interval.

    This function takes a list of OHLCV data and saves it as a JSON file, named in the format
    `<symbol>_<interval>.json`, in the `sample_data` directory. If the directory does not exist,
    it will be created.

    Parameters
    ----------
    data : List[List]
        List of OHLCV data, where each inner list represents a candlestick with fields such as
        open time, open price, high, low, close price, volume, etc.
    symbol : str
        Name of the trading pair, e.g., "BNBBTC".
    interval : str
        Candlestick interval, e.g., "1m" for 1 minute, "1h" for 1 hour.

    Returns
    -------
    None

    Notes
    -----
    - The JSON file will be saved in the `sample_data` directory. If the directory does not exist,
      it will be created.
    - The function expects `data` to match the expected column names, as listed in the `columns` variable.

    Examples
    --------
    Save OHLCV data for the "BNBBTC" trading pair on a 1-hour interval:

    ```python
    new_data = [
        [1609459200000, "0.0010", "0.0011", "0.0009", "0.0010", "1000", 1609462800000, "1.0", 10, "500", "0.5", None]
        # More data entries...
    ]
    save_to_json(new_data, "BNBBTC", "1h")
    ```

    """
    # Column names matching the expected format in the OHLCV data
    columns = [
        'open time', 'open', 'high', 'low', 'close', 'volume',
        'close time', 'quote asset volume', 'number of trades',
        'taker buy base asset volume', 'taker buy quote asset volume', "delete"
    ]
    df = pd.DataFrame(data, columns=columns)

    # Define output path and ensure the directory exists
    output_path = Path("sample_data")
    output_path.mkdir(exist_ok=True)
    out_json = output_path / f"{symbol}_{interval}.json"

    # Save the data to JSON format
    df.to_json(out_json, orient="records", date_format="iso")
