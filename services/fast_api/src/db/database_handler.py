import psycopg2

def connect_to_database(db_params):
    """
    Establishes a connection to the database using the provided parameters.

    Args:
        db_params (dict): Dictionary containing database connection parameters.

    Returns:
        connection: A psycopg2 connection object if successful, None otherwise.
    """
    connection = psycopg2.connect(**db_params)
    return connection

