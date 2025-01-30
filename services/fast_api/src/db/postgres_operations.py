import re

from typing import Optional
from io import StringIO


class PostgresOperations:
    def build_time_bucket_part(self, interval_str: str) -> str:
        """
        Builds the time bucket interval string for use with
        time_bucket(interval, timestamp), for example:
        '5m' -> '5 minutes', '1h' -> '1 hour', etc.
        """
        match = re.match(r"(\d+)([mhdw])", interval_str.lower())
        if not match:
            raise ValueError(f"Invalid interval format: '{interval_str}'")

        value_str, unit = match.groups()
        value = int(value_str)

        # Mapping to "minutes", "hours", "days", "weeks"
        unit_map = {
            'm': 'minute',
            'h': 'hour',
            'd': 'day',
            'w': 'week'
        }

        singular = unit_map[unit]
        # For values > 1, append "s" -> "5 minutes", "2 days", etc.
        if value == 1:
            return f"{value} {singular}"
        else:
            return f"{value} {singular}s"

    def get_available_intervals_for_trading_pair(self, conn, symbol: str) -> list[str]:
        """
        Returns all available intervals for a given trading pair symbol
        from the 'trading_pairs' table.

        Args:
            conn: An open psycopg2 connection to the database.
            symbol: The trading pair symbol (e.g. 'BTC/USD').

        Returns:
            A list of intervals (e.g. ['1m', '5m', '1h']).
        """
        with conn.cursor() as cur:
            query = """
                SELECT DISTINCT interval
                FROM trading_pairs
                WHERE symbol = %s
                ORDER BY interval;
            """
            cur.execute(query, (symbol,))
            rows = cur.fetchall()
            intervals = [row[0] for row in rows]

        return intervals

    def get_candlestick_data(self, conn, symbol: str, target_interval: str, interval: str, start_date: Optional[str]):
        """
        Creates a SQL statement for TimescaleDB that performs:
          - Aggregation on a chosen time bucket (e.g., '5 minutes')
          - Joins the 'candlesticks' table to 'trading_pairs'
          - Filters by the given symbol and an available interval
          - Optional start_date to query data starting at giving date
        """
        target_interval = self.build_time_bucket_part(target_interval)

        timestamp_condition = ""
        if start_date:
            timestamp_condition = f"AND timestamp > '{start_date}'"

        query = f"""
        SELECT json_agg(
            json_build_object(
                'open_time', bucket_time,
                'low', low,
                'high', high,
                'open', open,
                'close', close,
                'volume', volume,
                'number_of_trades', number_of_trades
            )
        ) AS data
        FROM (
            SELECT
                time_bucket('{target_interval}', t1.timestamp) AS bucket_time,
                MIN(t1.low) AS low,
                MAX(t1.high) AS high,
                FIRST(t1.open, t1.timestamp) AS open,
                LAST(t1.close, t1.timestamp) AS close,
                SUM(t1.volume) AS volume,
                SUM(t1.number_of_trades) AS number_of_trades
            FROM candlesticks t1
            JOIN trading_pairs t2
                ON t1.trading_pair_id = t2.id
            WHERE
                t2.symbol = '{symbol}'
                AND t2.interval = '{interval}'
                {timestamp_condition}
            GROUP BY bucket_time
            ORDER BY bucket_time
        ) AS sub;
        """.strip()

        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
            conn.close()

        data = row[0] if row and row[0] else []
        return data
