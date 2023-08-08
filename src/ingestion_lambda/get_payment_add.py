from get_secret import get_secret
import pg8000.native
from get_last_time import get_last_time
from dotenv import load_dotenv

load_dotenv()

secret = get_secret()


class MissingRequiredEnvironmentVariables (Exception):
    """
    Exception raised when attempts to connect to the database with missing
    required variables.
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


def get_payment_add(
    db_user=secret["username"],
    db_database=secret["dbname"],
    db_host=secret["host"],
    db_port=secret["port"],
    db_password=secret["password"],
):
    """
    Connects to the database and retrieves payment data created or updated
    since the last search interval.

    This function establishes a connection to the PostgreSQL database using
    the provided credentials. It determines the search interval by calling the
    get_last_time function with the table name 'payment'. Then, it queries the
    'payment' table to fetch all rows that have 'created_at' or 'last_updated'
    timestamps greater than the search interval.

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
    If any of the required environment variables are missing.

    Returns:
    list:
    A list of dictionaries, each representing a payment record.
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
    search_interval = get_last_time("payment")

    """
    QUERY DATA CREATED IN LAST SEARCH INTERVAL
    """

    conn.run('SET search_path TO "kp-test-source", public;')

    query = "SELECT * FROM payment WHERE \
        created_at > :search_interval OR last_updated > :search_interval;"
    params = {"search_interval": search_interval}
    rows = conn.run(query, **params)

    created_data = []
    for row in rows:
        item = {
            "payment_id": row[0],
            "created_at": row[1],
            "last_updated": row[2],
            "transaction_id": row[3],
            "counterparty_id": row[4],
            "payment_amount": row[5],
            "currency_id": row[6],
            "payment_type_id": row[7],
            "paid": row[8],
            "payment_date": row[9],
            "company_ac_number": row[10],
            "counterparty_ac_number": row[11],
        }
        created_data.append(item)
    return created_data
