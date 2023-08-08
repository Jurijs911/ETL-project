from get_secret import get_secret
import pg8000.native
from get_last_time import get_last_time
from dotenv import load_dotenv

load_dotenv()

secret = get_secret()


class MissingRequiredEnvironmentVariables (Exception):
    """
    Raised when attempts to connect to the database with missing or invalid
    variables.
    """

    def __init__(self, db_user, db_database, db_host, db_port, db_password):
        """
        Initialise the MissingRequiredEnvironmentVariables exception.
        """
        self.user = db_user
        self.database = db_database
        self.host = db_host
        self.port = db_port
        self.password = db_password


def get_design_add(
    db_user=secret["username"],
    db_database=secret["dbname"],
    db_host=secret["host"],
    db_port=secret["port"],
    db_password=secret["password"],
):
    """
    It retrieves the necessary database credentials from the secret and
    establishes a connection to the database. It then determines the search
    interval using the 'get_last_time' function for the 'design' table,
    queries the table for data created or updated in that interval, and
    returns the result as a list of dictionaries.

    Args:
    db_user:
    The username for the database connection.
    db_database:
    The name of the database to connect to.
    db_host:
    The host address for the database connection.
    db_port:
    The port number for the database connection.
    db_password:
    The password for the database connection.

    Raises:
    MissingRequiredEnvironmentVariables:
    If any of the required database connection variables is missing or invalid.

    Exception:
    If there's a DatabaseError during the connection.

    Returns:
    list:
    A list of dictionaries containing data for the 'design' table.
    """

    """
    CONNECTION
    """
    if not all([db_user, db_database, db_host, db_port, db_password]):
        raise MissingRequiredEnvironmentVariables(
            db_user, db_database, db_host, db_port, db_password)

    try:
        conn = pg8000.native.Connection(
            user=db_user,
            database=db_database,
            host=db_host,
            port=db_port,
            password=db_password,
        )
    except pg8000.exceptions.DatabaseError:
        raise Exception("Database error")

    conn = pg8000.native.Connection(
        user=db_user,
        database=db_database,
        host=db_host,
        port=db_port,
        password=db_password,
    )
    """
    DETERMINE SEARCH INTERVAL
    """
    search_interval = get_last_time("design")

    """
    QUERY DATA CREATED IN LAST SEARCH INTERVAL
    """

    conn.run('SET search_path TO "kp-test-source", public;')

    query = "SELECT * FROM design WHERE \
        created_at > :search_interval OR last_updated > :search_interval;"
    params = {"search_interval": search_interval}
    rows = conn.run(query, **params)

    created_data = []

    for row in rows:
        item = {
            "design_id": row[0],
            "created_at": row[1],
            "last_updated": row[2],
            "design_name": row[3],
            "file_location": row[4],
            "file_name": row[5],
        }
        created_data.append(item)
    return created_data
