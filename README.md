# CryptoBot: Binance Historical Candlestick Data Loader

This project, **CryptoBot**, is designed to load and process historical candlestick (OHLCV) data from Binance. The application retrieves data for specified trading pairs and time intervals, storing it locally in JSON files. It supports both initial data loading and incremental data updates to minimize redundant API requests. The project is structured with modular scripts, making it easy to configure, extend, and maintain.

## Table of Contents
- [Features](#features)
- [Setup](#setup)
- [Configuration](#configuration)
  - [config.yml](#configyml)
  - [trading_pairs.yml](#trading_pairsyml)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Modules Overview](#modules-overview)
  - [load_binance_data.py](#load_binance_datapy)
  - [data_processor.py](#data_processorpy)
  - [logger_config.py](#logger_configpy)
  - [config_loader.py](#config_loaderpy)
  - [api_handler.py](#api_handlerpy)
- [Logging](#logging)

## Features
- **Fetch Historical Data**: Load historical OHLCV (Open, High, Low, Close, Volume) data for specified trading pairs and intervals.
- **Incremental Updates**: Avoid redundant API requests by resuming from the last recorded `close time`.
- **JSON Output**: Data is saved in JSON format, organized by trading pair and interval.
- **Logging**: All operations are logged, including data retrieval, file operations, and error handling.

## Setup
### Prerequisites
- Python 3.7 or higher
- `pip` for package installation

### Installation
1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/cryptobot.git
   cd cryptobot
   ```
2. **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```
Ensure python-binance, pandas, PyYAML, and python-json-logger are installed, as they are required by the project.

API Key
To use the Binance API, you need to set up an account and obtain your API credentials from Binance API Management.

## Configuration
### config.yml
The config.yml file contains sensitive configuration data, such as your Binance API credentials and the directory path for log files. This file is excluded from version control for security.

**Note: Make sure your config.yml is securely stored and excluded from version control to protect your API credentials.**

Example config.yml

```yaml
binance:
  api_key: "your_api_key"
  api_secret: "your_api_secret"

log:
  path: "logs/"
```
### trading_pairs.yml
This file defines the trading pairs, intervals, and start dates for data retrieval. Each entry specifies:

- symbol: The trading pair symbol (e.g., "BTCUSDT").
- interval: The time interval for each candlestick (e.g., "1m" for 1 minute).
- start_date: The start date for initial data load if no previous data exists.

Example trading_pairs.yml
```yaml
trading_pairs:
  - symbol: "BTCUSDT"
    interval: "1h"
    start_date: "1 Jan, 2020"
  - symbol: "ETHUSDT"
    interval: "1d"
    start_date: "1 Jan, 2021"
```

## Usage
Ensure that config.yml and trading_pairs.yml are properly configured as described above.

Run the script:

```bash
python load_binance_data.py
```
The script will initialize the logger, load configurations, and start fetching data based on the trading pairs defined in trading_pairs.yml. Data will be saved in JSON format under the sample_data/ directory.

## Project Structure
```graphql
crypto_bot/
├── logs/                     # Directory for log files
├── sample_data/              # Directory for JSON data files
├── src/
│   ├── config/
│   ├── data_download/
│   ├── helper/
├── scripts/
│   ├── load_json_to_dataframe.py  # Script for loading JSON data into a DataFrame for testing
├── config.yml                # Main configuration file (user-defined)
├── trading_pairs.yml         # Defines trading pairs and intervals (user-defined)
├── requirements.txt          # Project dependencies
└── README.md                 # Project documentation
```


## Modules Overview
### **load_binance_data.py**
This is the main script for loading and processing historical candlestick data. It:

- Loads configurations from `config.yml` and `trading_pairs.yml`.
- Initializes the logger and Binance API client.
- Fetches and saves data for each trading pair, resuming from the last close time if data already exists.

### **data_processor.py**
Contains helper functions for handling JSON data files:

- `load_last_close_time_from_json(symbol, interval)`: Loads the last close time for a trading pair.
- `save_to_json(data, symbol, interval)`: Saves OHLCV data to a JSON file with the naming convention `<symbol>_<interval>.json`.

### **logger_config.py**
Sets up the logger for the project:

- Uses a rotating file handler with JSON formatting to store logs in the directory specified by `log/path` in `config.yml`.

### **config_loader.py**
Loads configuration settings from config.yml:

- `load_config(file_path)`: Reads and returns the YAML configuration file as a dictionary.

### **api_handler.py**
Handles interaction with the Binance API:

- `get_klines(client, symbol, interval, start_ts)`: Fetches historical candlestick data from Binance starting from a specified timestamp.

## Scripts
`load_json_to_dataframe.py`
This script, located in the scripts folder, is designed for loading and manipulating JSON data stored in sample_data. It’s primarily used for data testing, allowing for flexible manipulation, filtering, and aggregation of OHLCV data.

Usage:
To load and interact with the data in the JSON file:

```python
from pathlib import Path
from services.binance_data_loader.src.scripts.load_json_to_dataframe import load_json_to_dataframe

# Specify the path to the JSON file
file_path = Path("sample_data/LINKUSDT_1h.json")

# Load the data into a DataFrame
df = load_json_to_dataframe(file_path)

# Example manipulation after loading
if df is not None:
    print(df.head())  # Display first rows
    df_filtered = df[df["close"] > 50000]  # Filter rows
    avg_volume = df["volume"].mean()  # Calculate average volume
    print(f"Average Volume: {avg_volume}")
```

The script adds metadata columns for symbol and interval based on the JSON filename, allowing users to easily distinguish between different trading pairs and intervals in their data.

## Logging
Logging is handled by `logger_config.py`:

- Log files are stored in JSON format in the directory specified in `config.yml`.
- The logger uses a rotating file handler, which rotates log files when they reach 1 MB, retaining up to 3 backups.
- Logs include timestamps, log levels, and messages, which are useful for tracking script execution and debugging.

All logs, including data load start and end times, API interaction status, and errors, are recorded for better traceability and troubleshooting.
