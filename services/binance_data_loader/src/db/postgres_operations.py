from io import StringIO


class PostgresOperations:
    def __init__(self, logger):
        """
        Initialize the PostgresOperations class.

        Args:
            logger: Logger instance for logging.
        """
        self.logger = logger

    def copy_import_candlestick_data(self, connection, df_klines, table_name="candlesticks"):
        """
        Uses PostgreSQL COPY to efficiently load candlestick data from a DataFrame into the database.

        Args:
            connection: psycopg2 database connection object.
            df_klines: Pandas DataFrame containing the candlestick data.
            table_name: Target table name in the database.
        """
        try:
            self.logger.info(f"Start import of {len(df_klines)} rows into {table_name}.")

            # Prepare the DataFrame for COPY by converting it to CSV in memory
            output = StringIO()
            df_klines.to_csv(output, index=False, header=False)  # Exclude index and headers
            output.seek(0)  # Move cursor to the beginning of the StringIO object

            # Execute the COPY command
            with connection.cursor() as cursor:
                cursor.copy_expert(
                    f"""
                    COPY {table_name} (
                        trading_pair_id, timestamp, open_time, open, high, low, close, volume, close_time, number_of_trades
                    ) FROM STDIN WITH CSV;
                    """,
                    output,
                )
            connection.commit()
            self.logger.info(f"Successfully copied {len(df_klines)} rows into {table_name}.")
        except Exception as e:
            self.logger.error(f"Error copying data to PostgreSQL: {e}")
            connection.rollback()

    def get_or_create_source(self, connection, source_name: str, source_type: str = "exchange",
                             description: str = None) -> int:
        """
        Fetches the source_id for a given source name.
        If the source does not exist, it will be created.

        Parameters
        ----------
        connection : psycopg2 connection
            Database connection object.
        source_name : str
            Name of the source (e.g., 'Binance').
        source_type : str, optional
            Type of the source (default is 'exchange').
        description : str, optional
            Additional information about the source.

        Returns
        -------
        int
            The source ID of the fetched or newly created source.
        """
        try:
            # Check if the source exists
            query = "SELECT id FROM sources WHERE name = %s"
            with connection.cursor() as cursor:
                cursor.execute(query, (source_name,))
                result = cursor.fetchone()

                if result:
                    source_id = result[0]
                    self.logger.info(f"Source '{source_name}' already exists with ID {source_id}.")
                    return source_id

                # Create the source if it does not exist
                cursor.execute(
                    """
                    INSERT INTO sources (name, type, description)
                    VALUES (%s, %s, %s)
                    RETURNING id
                    """,
                    (source_name, source_type, description),
                )
                source_id = cursor.fetchone()[0]
                connection.commit()
                self.logger.info(f"Created new source '{source_name}' with ID {source_id}.")
                return source_id
        except Exception as e:
            self.logger.error(f"Error getting or creating source '{source_name}': {e}")
            connection.rollback()
            raise

    def get_or_create_trading_pair(self, connection, symbol: str, interval: str, source_name: str) -> int:
        """
        Fetches the trading_pair_id for a given trading pair symbol, source, and interval.
        If the trading pair does not exist, it will be created.

        Parameters
        ----------
        connection : psycopg2 connection
            Database connection object.
        symbol : str
            Trading pair symbol (e.g., 'BTCUSDT').
        interval : str
            Candlestick interval (e.g., '1m', '1h').
        source_name : str
            Name of the source (e.g., 'Binance').
        source_type : str, optional
            Type of the source (default is 'exchange').

        Returns
        -------
        int
            The trading_pair_id for the given symbol, source, and interval.
        """
        try:
            with connection.cursor() as cursor:
                source_id = self.get_or_create_source(connection, source_name)

                # Check if the trading pair with the interval exists for the source
                cursor.execute(
                    """
                    SELECT id 
                    FROM trading_pairs 
                    WHERE symbol = %s AND interval = %s AND source_id = %s
                    """,
                    (symbol, interval, source_id),
                )
                trading_pair_result = cursor.fetchone()
                if trading_pair_result:
                    trading_pair_id = trading_pair_result[0]
                else:
                    # Insert the trading pair if it doesn't exist
                    cursor.execute(
                        """
                        INSERT INTO trading_pairs (symbol, interval, source_id, type)
                        VALUES (%s, %s, %s, %s) RETURNING id
                        """,
                        (symbol, interval, source_id, "crypto"),
                    )
                    trading_pair_id = cursor.fetchone()[0]
                    self.logger.info(
                        f"Created new trading pair '{symbol}' with interval '{interval}' and ID {trading_pair_id}.")

                connection.commit()
                return trading_pair_id
        except Exception as e:
            self.logger.error(f"Error fetching or creating trading pair '{symbol}' with interval '{interval}': {e}")
            connection.rollback()
            raise

    def get_last_close_time(self, connection, trading_pair_id: int) -> int:
        """
        Fetches the last recorded close time for a trading pair from the database.

        Parameters
        ----------
        connection : psycopg2 connection
            Database connection object.
        trading_pair_id : int
            ID of the trading pair.

        Returns
        -------
        int
            The last recorded close time in milliseconds, or None if no records exist.
        """
        query = """
            SELECT MAX(close_time) AS last_close_time
            FROM candlesticks
            WHERE trading_pair_id = %s;
        """
        try:
            with connection.cursor() as cursor:
                cursor.execute(query, (trading_pair_id,))
                result = cursor.fetchone()
                if result and result[0]:
                    self.logger.info(f"Last close time for trading pair {trading_pair_id}: {result[0]}")
                    return result[0]
                else:
                    self.logger.info(f"No candlestick data found for trading pair {trading_pair_id}.")
                    return None
        except Exception as e:
            self.logger.error(f"Error fetching last close time for trading pair {trading_pair_id}: {e}")
            raise

