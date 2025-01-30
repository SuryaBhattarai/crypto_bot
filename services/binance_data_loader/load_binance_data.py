"""
Start script for loading and processing historical candlestick data (klines) from Binance.

This script is designed to load candlestick data from Binance based on configurations specified
in two YAML files: `config.yml` and `trading_pairs.yml`. The script fetches data for specified
trading pairs (e.g., BTCUSDT) at specific intervals (e.g., 1 minute, 1 hour) and stores it in
JSON files locally.

The script performs two main operations:
1. **Initial Data Load**: If no previous data exists for a trading pair, it fetches data from
   the specified start date.
2. **Incremental Data Update**: If previous data exists, it continues fetching data from the
   last recorded `close time` to avoid redundant data retrieval.

Configuration Files
-------------------
- **config.yml**: This file contains sensitive information and paths required for the script
  to operate, including:

  - **Binance API credentials** (`api_key` and `api_secret`): These are used to authenticate
    requests to the Binance API. Since these credentials are sensitive, `config.yml` is
    intentionally excluded from version control by adding it to `.gitignore`. You will need
    to create this file manually and populate it with your own Binance API credentials.

  - **Log directory path** (`log/path`): Specifies the local directory where log files will
    be stored. The logger will create this directory (if it doesn't exist) and write logs to
    a rotating log file to track the script's operations.

- **trading_pairs.yml**: This file defines the trading pairs, intervals, and start dates for
  data retrieval. Each entry in this file specifies:

  - **symbol**: The trading pair symbol (e.g., BTCUSDT).
  - **interval**: The time interval for each candlestick (e.g., "1m" for 1 minute).
  - **start_date**: The date from which to start fetching data if no previous data exists.

  The `trading_pairs.yml` file allows you to customize which trading pairs are processed and
  the granularity of the data.

Workflow
--------
1. **Setup**: The script reads `config.yml` to load API credentials and configure the logger.
   It then loads `trading_pairs.yml` to determine the trading pairs and intervals to process.
2. **Data Retrieval**: For each trading pair:
   - If no previous data exists, the script performs an **initial load** from the specified `start_date`.
   - If previous data is found, the script performs an **incremental update** starting from
     the last recorded `close time`. This approach minimizes redundant API requests by only
     fetching new data since the last recorded candlestick.
3. **Logging**: All operations are logged to a file located in the path specified by `log/path`
   in `config.yml`.

Example
-------
To run this script, make sure to create `config.yml` and `trading_pairs.yml` in the same directory.
Then execute:

"""
from binance.client import Client
from binance.helpers import date_to_milliseconds
from pathlib import Path

from src.config.config_loader import load_config
from src.data_download.binance_data_loader import BinanceDataLoader
from src.db.database_handler import connect_to_database
from src.db.postgres_operations import PostgresOperations
from src.config.logger_config import setup_logger


from typing import List, Dict

def process_trading_pairs(pairs: List[Dict], client, connection) -> None:
    """
    Processes each trading pair from the configuration, checks the Binance system status,
    retrieves candlestick data, and saves it to the database.

    Parameters
    ----------
    pairs : List[Dict]
        List of trading pair configurations with each dictionary containing 'symbol',
        'interval', and 'start_date' keys.
    client : binance.Client
        Binance client instance for API interaction.
    connection : psycopg2 connection
        Database connection object for inserting data.

    Returns
    -------
    None

    Notes
    -----
    - The function checks the Binance system status before each data retrieval. If Binance
      is unavailable, it skips the current trading pair.
    - Ensures the trading pair and source exist in the database before data processing.
    - Saves the candlestick data directly to the database.

    Example
    -------
    ```python
    pairs = [{'symbol': 'BNBBTC', 'interval': '1h', 'start_date': '1 Jan, 2021'}]
    process_trading_pairs(pairs, client, connection)
    ```
    """
    bh = BinanceDataLoader(logger)
    pg = PostgresOperations(logger)
    source_name = "Binance"

    for pair in pairs:
        symbol = pair['symbol']
        interval = pair['interval']
        start_date = pair['start_date']

        try:
            # Check Binance system status
            status = client.get_system_status()
            if status.get('status') != 0:
                logger.error(f"Binance is not available. Skipping data load for '{symbol}' with interval '{interval}'.")
                continue

            # Ensure trading pair exists in the database
            trading_pair_id = pg.get_or_create_trading_pair(connection, symbol, interval, source_name)

            # Get the last close time from the database or use the start_date
            last_close_time = pg.get_last_close_time(connection, trading_pair_id)
            if last_close_time:
                start_ts = last_close_time + 1
            else:
                start_ts = date_to_milliseconds(start_date)  # Convert start_date to milliseconds

            # Load candlestick data from Binance
            df_klines = bh.load_candlestick_data(client, symbol, interval, start_ts)

            df_klines["trading_pair_id"] = trading_pair_id
            df_klines.insert(0, 'trading_pair_id', df_klines.pop('trading_pair_id'))  # Move 'timestamp' to the first column

            if not df_klines.empty:
                # Save data to the database using PostgreSQL COPY
                pg.copy_import_candlestick_data(connection, df_klines)

                logger.info(f"Data for '{symbol}' with interval '{interval}' has been successfully processed.")
            else:
                logger.warning(f"No new data available for '{symbol}' with interval '{interval}'. Skipping.")

        except Exception as e:
            logger.error(f"Error occurred while processing '{symbol}' with interval '{interval}'. ERROR: {e}")


if __name__ == "__main__":
    conf_path = Path(__file__).resolve().parent / "/app/config.yml"
    pairs_conf_path = Path(__file__).resolve().parent / "/app/trading_pairs.yml"

    config = load_config(conf_path)

    log_path = Path(config['log']['path'])
    logger = setup_logger(log_path)

    api_key = config['binance']['api_key']
    api_secret = config['binance']['api_secret']
    client = Client(api_key, api_secret)

    pairs = load_config(pairs_conf_path)

    conn = connect_to_database(config['postgres'])
    process_trading_pairs(pairs['trading_pairs'], client=client, connection=conn)
    conn.close()
