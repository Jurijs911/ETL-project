from get_secret import get_secret
import pg8000.native
from get_last_time import get_last_time
from dotenv import load_dotenv

load_dotenv()

secret = get_secret()


class MissingRequiredEnvironmentVariables (Exception):
    """
    Exception raised when attempts to connect to the database with missing
    environment variables.
    """

    def __init__(self, db_user, db_database, db_host, db_port, db_password):
        """
        Initializes the MissingRequiredEnvironmentVariables exception.
        """
        self.user = db_user
        self.database = db_database
        self.host = db_host
        self.port = db_port
        self.password = db_password


def get_purchase_order_add(
    db_user=secret["username"],
    db_database=secret["dbname"],
    db_host=secret["host"],
    db_port=secret["port"],
    db_password=secret["password"],
):
    """
    Establish a connection to the database and retrieve the data for the
    'purchase_order' table.

    This function connects to the specified database using the secret
    environment variables. It then determines the search interval using the
    'get_last_time' function for the 'purchase_order' table. It queries the
    'purchase_order' table to retrieve data created or last updated within the
    determined search interval. The retrieved data is returned as a list of
    dictionaries.

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
    If any required database environment variable is missing.

    Exception:
    If a database error occurs while connecting.

    Returns:
    list[dict]:
    A list of dictionaries containing the data for the 'purchase_order' table.
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
    search_interval = get_last_time("purchase_order")

    """
    QUERY DATA CREATED IN LAST SEARCH INTERVAL
    """

    conn.run('SET search_path TO "kp-test-source", public;')

    query = "SELECT * FROM purchase_order WHERE \
        created_at > :search_interval OR last_updated > :search_interval;"
    params = {"search_interval": search_interval}
    rows = conn.run(query, **params)

    created_data = []
    for row in rows:
        item = {
            "purchase_order_id": row[0],
            "created_at": row[1],
            "last_updated": row[2],
            "staff_id": row[3],
            "counterparty_id": row[4],
            "item_code": row[5],
            "item_quantity": row[6],
            "item_unit_price": row[7],
            "currency_id": row[8],
            "agreed_delivery_date": row[9],
            "agreed_payment_date": row[10],
            "agreed_delivery_location_id": row[11],
        }
        created_data.append(item)
    return created_data
