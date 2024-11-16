
from binance.client import Client
import pandas as pd
import psycopg2
from datetime import datetime

API_KEY = 'e3mAo1MdRxhM8XGY3q4ARM1D1VZI7oW5Lj199bEYKsLuEnS2dkKU4Rkr0q9AdPIV'
API_SECRET = 'cCrg8eyoBG2V85HX1dd7MQ5Xb3ZocbGiwbTHPl3E9ipq2Bz3ihA7YCmpXLBS3aCR'

client = Client(API_KEY, API_SECRET)

start_date = "2024-11-01"
end_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Database connection parameters
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="postgres",
    user="postgres",
    password="password"
)

# Create a cursor to interact with the database
cursor = conn.cursor()

# Create table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS linkusdt_klines_hist_new (
    open_time TIMESTAMP PRIMARY KEY,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    close_time TIMESTAMP, 
    quote_asset_volume NUMERIC,
    number_of_trades NUMERIC,
    taker_buy_base_asset_volume NUMERIC, 
    taker_buy_quote_asset_volume NUMERIC,
    ignore NUMERIC
);
"""
cursor.execute(create_table_query)
conn.commit()

# Fetch the latest open_time from the database to determine the most recent record
cursor.execute("SELECT MAX(open_time) FROM linkusdt_klines_hist_new")
latest_open_time = cursor.fetchone()[0]

# Use the latest open_time as the new start date if data is already in the database
if latest_open_time:
    start_date = latest_open_time.strftime("%Y-%m-%d %H:%M:%S")

# Retrieve klines data from Binance
klines_data = []
for kline in client.get_historical_klines("LINKUSDT", Client.KLINE_INTERVAL_1MINUTE, start_date, end_date):
    klines_data.append(kline)

# Define columns based on Binance API Kline format
columns = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time", 
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume", 
    "taker_buy_quote_asset_volume",
    "ignore"
]

# Create a DataFrame from collected klines data
df = pd.DataFrame(klines_data, columns=columns)

# Convert open_time and close_time to readable datetime format
df["open_time"] = pd.to_datetime(df["open_time"], unit='ms')
df["close_time"] = pd.to_datetime(df["close_time"], unit='ms')

# Filter out rows that are already in the database
if latest_open_time:
    df = df[df["open_time"] > latest_open_time]

# Insert new data into the database
for _, row in df.iterrows():
    cursor.execute("""
    INSERT INTO linkusdt_klines_hist_new (
        open_time, open, high, low, close, volume, close_time, quote_asset_volume,
        number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (open_time) DO NOTHING
    """, (
        row["open_time"], row["open"], row["high"], row["low"], row["close"],
        row["volume"], row["close_time"], row["quote_asset_volume"],
        row["number_of_trades"], row["taker_buy_base_asset_volume"],
        row["taker_buy_quote_asset_volume"], row["ignore"]
    ))

# Commit transaction and close the cursor and connection
conn.commit()
cursor.close()
conn.close()

print("New data loaded into PostgreSQL successfully!")
