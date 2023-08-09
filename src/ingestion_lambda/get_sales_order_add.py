from get_secret import get_secret
import pg8000.native
from get_last_time import get_last_time
from dotenv import load_dotenv

load_dotenv()

secret = get_secret()


class MissingRequiredEnvironmentVariables (Exception):
    """
    Exception raised when attempting to connect to the database
    with missing or invalid variables.
    """

    def __init__(self, db_user, db_database, db_host, db_port, db_password):
        """
        Initialises the MissingRequiredEnvironmentVariables exception.
        """
        self.user = db_user
        self.database = db_database
        self.host = db_host
        self.port = db_port
        self.password = db_password


def get_sales_order_add(
    db_user=secret["username"],
    db_database=secret["dbname"],
    db_host=secret["host"],
    db_port=secret["port"],
    db_password=secret["password"],
):
    """
    Retrieve new sales order data from the source database.

    Connects to the database using the provided or default credentials.
    Determines the search interval using the last retrieved timestamp.
    Queries the sales_order table for data created or updated within the
    search interval.

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
    A list of dictionaries containing sales_order data.
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
    search_interval = get_last_time("sales_order")

    """
    QUERY DATA CREATED IN LAST SEARCH INTERVAL
    """

    conn.run('SET search_path TO "kp-test-source", public;')

    query = "SELECT * FROM sales_order WHERE \
        created_at > :search_interval OR last_updated > :search_interval;"
    params = {"search_interval": search_interval}
    rows = conn.run(query, **params)

    created_data = []
    for row in rows:
        item = {
            "sales_order_id": row[0],
            "created_at": row[1],
            "last_updated": row[2],
            "design_id": row[3],
            "staff_id": row[4],
            "counter_party_id": row[5],
            "units_sold": row[6],
            "unit_price": row[7],
            "currency_id": row[8],
            "agreed_delivery_date": row[9],
            "agreed_payment_date": row[10],
            "agreed_delivery_location_id": row[11],
        }
        created_data.append(item)

    return created_data
