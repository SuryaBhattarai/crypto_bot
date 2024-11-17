-- Enable the TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create the "sources" table
CREATE TABLE IF NOT EXISTS sources (
    id SERIAL PRIMARY KEY,                 -- Unique ID for the data source
    name VARCHAR(50) UNIQUE NOT NULL,      -- Name of the source, e.g., "Binance", "Coinbase", "Federal Reserve"
    type VARCHAR(20) NOT NULL,             -- Type of the source, e.g., "exchange", "macro", etc.
    description TEXT                       -- Additional information about the source
);

-- Create the "trading_pairs" table
CREATE TABLE IF NOT EXISTS trading_pairs (
    id SERIAL PRIMARY KEY,                                      -- Unique ID for the trading pair
    source_id INT REFERENCES sources(id) ON DELETE CASCADE,     -- Reference to the source table
    symbol VARCHAR(20) NOT NULL,                                -- Trading pair symbol, e.g., "BTC/USD", "ETH/USD"
    type VARCHAR(20) NOT NULL,                                  -- Type of data, e.g., "crypto", "fiat", "macro"
    UNIQUE (source_id, symbol)                                  -- Ensure uniqueness per source and trading pair
);

-- Create the "candlesticks" table (fact table)
CREATE TABLE IF NOT EXISTS candlesticks (
    trading_pair_id INT REFERENCES trading_pairs(id) ON DELETE CASCADE,     -- Reference to the trading pair
    timestamp TIMESTAMPTZ NOT NULL,                                         -- Timestamp for the candlestick
    open_time BIGINT,                                                       -- Opening time in milliseconds
    open NUMERIC(18, 8),                                                    -- Open price
    high NUMERIC(18, 8),                                                    -- Highest price
    low NUMERIC(18, 8),                                                     -- Lowest price
    close NUMERIC(18, 8),                                                   -- Close price
    volume NUMERIC(18, 8),                                                  -- Trading volume
    close_time BIGINT,                                                      -- Closing time in milliseconds
    number_of_trades INTEGER,                                               -- Number of trades provided by the source
    UNIQUE (trading_pair_id, timestamp)                                     -- Ensure uniqueness per trading pair and timestamp
);

-- Convert the "candlesticks" table into a TimescaleDB Hypertable
SELECT * FROM create_hypertable('candlesticks', by_range('timestamp'));
SELECT * FROM add_dimension('candlesticks', by_hash('trading_pair_id', 8));

-- Create indexes to improve query performance
CREATE INDEX IF NOT EXISTS idx_candlesticks_pair_time ON candlesticks (trading_pair_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_candlesticks_open_time ON candlesticks (trading_pair_id, open_time);
CREATE INDEX IF NOT EXISTS idx_candlesticks_close_time ON candlesticks (trading_pair_id, close_time);

-- Insert example data into the "sources" table (optional)
INSERT INTO sources (name, type, description)
VALUES
    ('Binance', 'exchange', 'Binance cryptocurrency exchange'),
    ('Coinbase', 'exchange', 'Coinbase cryptocurrency exchange'),
    ('Federal Reserve', 'macro', 'M2 Money Supply from Federal Reserve')
ON CONFLICT DO NOTHING;

-- Insert example data into the "trading_pairs" table (optional)
INSERT INTO trading_pairs (source_id, symbol, type)
VALUES
    ((SELECT id FROM sources WHERE name = 'Binance'), 'LINK/USDT', 'crypto'),
    ((SELECT id FROM sources WHERE name = 'Binance'), 'LINK/BTC', 'crypto'),
    ((SELECT id FROM sources WHERE name = 'Federal Reserve'), 'M2', 'macro')
ON CONFLICT DO NOTHING;

