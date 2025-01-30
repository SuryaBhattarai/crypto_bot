import requests
import pandas as pd
from datetime import datetime

# Define the endpoint and parameters
BASE_URL = "https://api.binance.com"
KLINES_ENDPOINT = "/api/v3/klines"


def get_historical_data(symbol="LINKUSDT", interval="1d", start_str="1 Jan, 2020", limit=1000):
    """
    Fetch historical candlestick data from Binance.

    :param symbol: The trading pair, e.g., LINKUSDT
    :param interval: The interval for candlesticks (1m, 5m, 1h, 1d, etc.)
    :param start_str: The start date as a string, e.g., "1 Jan, 2020"
    :param limit: The maximum number of data points to retrieve per call (default is 1000)
    :return: A DataFrame with date, open, high, low, close, and volume
    """
    # Prepare the request
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": int(datetime.strptime(start_str, "%d %b, %Y").timestamp() * 1000),
        "limit": limit
    }

    # Request data from Binance
    url = BASE_URL + KLINES_ENDPOINT
    response = requests.get(url, params=params)
    response.raise_for_status()

    # Process the data
    data = response.json()
    df = pd.DataFrame(data, columns=[
        "timestamp", "open", "high", "low", "close", "volume", "close_time",
        "quote_asset_volume", "number_of_trades", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume",
        "ignore"
    ])

    # Convert timestamp to datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms')
    df.set_index("timestamp", inplace=True)

    # Keep only relevant columns
    return df[["open", "high", "low", "close", "volume"]].astype(float)


# Fetch data
df = get_historical_data(symbol="LINKUSDT", interval="1d", start_str="1 Jan, 2020")
print(df.head())

# Save data
df.to_json("chainlink_data.json", orient="records", date_format="iso")
