"""
Module for fetching historical candlestick data (klines) from Binance.

This module contains the `get_klines` function, which retrieves historical candlestick data
from Binance using the Binance API. The function is a modified version of `get_historical_klines`
from the `python-binance` package, adapted to use milliseconds for more precise data retrieval
and to allow appending data incrementally.

Dependencies:
    - python-binance
    - time
    - typing

Example:
    Initialize the Binance client and call `get_klines` with appropriate parameters:

    ```python
    from binance import Client
    from binance.helpers import date_to_milliseconds
    import time

    client = Client(api_key, api_secret)
    symbol = "BNBBTC"
    interval = "1h"
    start_ts = date_to_milliseconds("1 Jan, 2021")

    klines = get_klines(client, symbol, interval, start_ts)
    ```
"""
import time
import pandas as pd

from binance.helpers import interval_to_milliseconds
from binance import Client
from decimal import Decimal
from typing import List


class BinanceDataLoader:
    def __init__(self, logger):
        self.logger = logger

    def get_klines(self, client: Client, symbol: str, interval: str, start_ts: int) -> List[List]:
        """
        Fetches historical candlestick data (klines) from Binance, starting from a specified timestamp.

        This function is a modified version of `get_historical_klines` from the `python-binance` package.
        It is adapted to support fetching data from a specified timestamp (in milliseconds) to allow
        for more precise data retrieval and to enable appending new data to already fetched records.

        Parameters
        ----------
        client : binance.Client
            Binance API Client instance used for making API calls.
        symbol : str
            Trading pair symbol (e.g., "BNBBTC").
        interval : str
            Candlestick interval (e.g., "1m" for 1 minute, "1h" for 1 hour).
        start_ts : int
            Start timestamp in milliseconds from which data should be fetched.

        Returns
        -------
        List[List]
            List of OHLCV (Open, High, Low, Close, Volume) values for each candlestick interval,
            where each item in the list contains a list of values for a single candlestick.

        Notes
        -----
        - The function makes repeated API calls to retrieve up to 500 candlesticks per call.
        - If the symbol was not yet listed on Binance at `start_ts`, the timestamp is automatically
          incremented until valid data is retrieved.
        - The function sleeps for 1 second after every 3 API calls to avoid hitting Binance API limits.

        Raises
        ------
        BinanceAPIException
            If there is an error with the Binance API request.

        Examples
        --------
        Initialize the Binance client and retrieve historical data:

        ```python
        from binance import Client
        client = Client(api_key, api_secret)
        symbol = "BNBBTC"
        interval = "1h"
        start_ts = 1609459200000  # Example timestamp in milliseconds

        klines = BinanceHandler.get_klines(client, symbol, interval, start_ts)
        ```

        """

        # Initialize list to store output data
        output_data = []

        # Set maximum limit per API call
        limit = 500

        # Convert interval to milliseconds
        timeframe = interval_to_milliseconds(interval)

        idx = 0
        # Flag to handle cases where the start date is before the symbol was listed
        symbol_existed = False
        while True:
            # Fetch klines starting from the specified timestamp
            temp_data = client.get_klines(
                symbol=symbol,
                interval=interval,
                limit=limit,
                startTime=start_ts
            )

            # Check if the symbol is available on Binance
            if not symbol_existed and len(temp_data):
                symbol_existed = True

            if symbol_existed:
                # Append data to output list
                output_data += temp_data

                # Update start timestamp to the last entry's timestamp plus the interval
                start_ts = temp_data[-1][0] + timeframe
            else:
                # If symbol is not available yet, increment start timestamp
                start_ts += timeframe

            idx += 1
            # Exit loop if fewer than `limit` entries were retrieved, indicating the end of available data
            if len(temp_data) < limit:
                break

            # Sleep for 1 second after every 3 API calls to respect rate limits
            if idx % 3 == 0:
                time.sleep(1)

        return output_data

    def load_candlestick_data(self, client: Client, symbol: str, interval: str, start_ts: int) -> pd.DataFrame:
        """
        Loads candlestick data from Binance starting at a specific timestamp.

        Parameters
        ----------
        client : binance.Client
            Binance client instance for API interaction.
        symbol : str
            Name of the trading pair, e.g., "BNBBTC".
        interval : str
            Candlestick interval, e.g., "1m" for 1 minute.
        start_ts : int
            Start timestamp in milliseconds for fetching candlestick data.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the candlestick data with the following columns:
            ['timestamp', 'open time', 'open', 'high', 'low', 'close', 'volume', 'close time', 'number of trades'].
        """
        self.logger.info(
            f"Loading candlestick data from BINANCE for '{symbol}' with interval '{interval}' starting from {start_ts}.")

        # Fetch klines from Binance
        klines = self.get_klines(client, symbol, interval, start_ts)
        self.logger.info(f"Successfully loaded {len(klines)} candlestick records for '{symbol}'.")

        # Convert klines to DataFrame
        columns = [
            'open time', 'open', 'high', 'low', 'close', 'volume',
            'close time', 'quote asset volume', 'number of trades',
            'taker buy base asset volume', 'taker buy quote asset volume', "delete"
        ]
        df = pd.DataFrame(klines, columns=columns)

        # Add 'timestamp' in UTC datetime format
        df["timestamp"] = pd.to_datetime(df["open time"], unit="ms").dt.tz_localize("UTC")
        df.insert(0, 'timestamp', df.pop('timestamp'))  # Move 'timestamp' to the first column

        # Remove unnecessary columns
        df.drop(['quote asset volume', 'taker buy base asset volume', 'taker buy quote asset volume', 'delete'],
                axis=1, inplace=True)

        # Ensure correct data types for financial precision
        df["open"] = df["open"].apply(Decimal)
        df["high"] = df["high"].apply(Decimal)
        df["low"] = df["low"].apply(Decimal)
        df["close"] = df["close"].apply(Decimal)
        df["volume"] = df["volume"].apply(Decimal)

        return df
