from get_secret import get_secret
import pg8000.native
from get_last_time import get_last_time
from dotenv import load_dotenv

load_dotenv()

secret = get_secret()


class MissingRequiredEnvironmentVariables(Exception):
    """
    Raised when attempts to connect to the database
    with variables that do not exist.
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


def get_counterparty_add(
    db_user=secret["username"],
    db_database=secret["dbname"],
    db_host=secret["host"],
    db_port=secret["port"],
    db_password=secret["password"],
):
    """
    Connects to the database and retrieves counterparty data created or updated
    within the last search interval.

    Args:
        db_user:
        Database user name. Defaults to the value obtained from secret.
        db_database:
        Database name. Defaults to the value obtained from secret.
        db_host:
        Database host. Defaults to the value obtained from secret.
        db_port:
        Database port. Defaults to the value obtained from secret.
        db_password:
        Database password. Defaults to the value obtained from secret.

    Raises:
        MissingRequiredEnvironmentVariables:
        If any required database connection parameters are missing.

    Returns:
        list:
        A list of dictionaries, representing counterparty data:
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
    search_interval = get_last_time("counterparty")

    """
    QUERY DATA CREATED IN LAST SEARCH INTERVAL
    """

    conn.run('SET search_path TO "kp-test-source", public;')

    query = "SELECT * FROM counterparty WHERE \
        created_at > :search_interval OR last_updated > :search_interval;"
    params = {"search_interval": search_interval}
    rows = conn.run(query, **params)

    created_data = []
    for row in rows:
        item = {
            "counterparty_id": row[0],
            "counterparty_legal_name": row[1],
            "legal_address_id": row[2],
            "commercial_contact": row[3],
            "delivery_contact": row[4],
            "created_at": row[5],
            "last_updated": row[6],
        }
        created_data.append(item)

    return created_data
