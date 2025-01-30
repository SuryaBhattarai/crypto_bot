"""
Script for loading JSON data into a Pandas DataFrame for flexible data manipulation.

This script provides a function, `load_json_to_dataframe`, which loads candlestick (OHLCV) data
from a specified JSON file into a Pandas DataFrame. This function is designed to enable users to
freely manipulate the data after loading, such as creating subsets, aggregating data, or performing
custom analysis.

Dependencies:
    - pandas
    - pathlib

Example:
    Load a specific JSON file into a DataFrame and start manipulating the data:

    ```python
    from pathlib import Path

    # Load the JSON data
    file_path = Path("sample_data/BTCUSDT_1h.json")
    df = load_json_to_dataframe(file_path)

    # Example manipulations
    if df is not None:
        # Display the first few rows
        print(df.head())

        # Filter rows where the closing price is above a threshold
        df_above_threshold = df[df["close"] > 50000]

        # Calculate average volume
        avg_volume = df["volume"].mean()
        print(f"Average Volume: {avg_volume}")

        # Resample data to daily frequency if the interval is 1-hour
        if df["interval"].iloc[0] == "1h":
            df_daily = df.resample("D", on="open time").agg({
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum"
            })
            print(df_daily)
    ```
"""

import pandas as pd
from pathlib import Path
from typing import Optional

def load_json_to_dataframe(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Loads a specific JSON file into a Pandas DataFrame for data manipulation.

    The JSON file should follow the naming convention `<symbol>_<interval>.json`, where `symbol` is
    the trading pair (e.g., "BTCUSDT") and `interval` is the candlestick interval (e.g., "1h").

    Parameters
    ----------
    file_path : pathlib.Path
        Path to the specific JSON file to load.

    Returns
    -------
    pd.DataFrame or None
        A DataFrame containing OHLCV data from the JSON file, or `None` if the file could not be loaded.

    Notes
    -----
    - The function adds a `open date` column to the DataFrame, for better readability.
    - The JSON file should be formatted as a list of OHLCV data records.

    Examples
    --------
    Load a specific JSON file into a DataFrame:

    ```python
    file_path = Path("sample_data/BTCUSDT_1h.json")
    df = load_json_to_dataframe(file_path)
    print(df.head())
    ```
    """
    # Check if the file exists
    if not file_path.exists() or not file_path.is_file():
        print(f"File not found: {file_path}")
        return None

    # Extract symbol and interval from the filename (e.g., BTCUSDT_1h.json)
    try:
        symbol, interval = file_path.stem.split("_")
    except ValueError:
        print(f"File name does not match expected format '<symbol>_<interval>.json': {file_path.name}")
        return None

    # Load JSON data into a DataFrame
    try:
        df = pd.read_json(file_path)

        # Ensure 'open time' is in datetime format for easy resampling and manipulation
        if "open time" in df.columns:
            df["open date"] = pd.to_datetime(df["open time"], unit="ms")
            df.insert(0, 'open date', df.pop('open date'))

        return df
    except ValueError as e:
        print(f"Error reading {file_path.name}: {e}")
        return None
