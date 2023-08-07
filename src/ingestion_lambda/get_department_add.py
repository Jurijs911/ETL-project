from get_secret import get_secret
import pg8000.native
from get_last_time import get_last_time
from dotenv import load_dotenv

load_dotenv()

secret = get_secret()


class MissingRequiredEnvironmentVariables (Exception):
    """
    Raised when attempts to connect to the database with variables which do
    not exist.
    """

    def __init__(self, db_user, db_database, db_host, db_port, db_password):
        """
        Initialize the MissingRequiredEnvironmentVariables exception.
        """
        self.user = db_user
        self.database = db_database
        self.host = db_host
        self.port = db_port
        self.password = db_password


def get_department_add(
    db_user=secret["username"],
    db_database=secret["dbname"],
    db_host=secret["host"],
    db_port=secret["port"],
    db_password=secret["password"],
):
    """
    Connects to the database and retrieves the data for the 'department' table
    created in the last search interval.

    Args:
    db_user:
    Database username. Defaults to the value from secret["username"].
    db_database:
    Database name. Defaults to the value from secret["dbname"].
    db_host:
    Database host. Defaults to the value from secret["host"].
    db_port:
    Database port. Defaults to the value from secret["port"].
    db_password:
    Database password. Defaults to the value from secret["password"].

    Raises:
    MissingRequiredEnvironmentVariables:
    If any of the required environment variables is missing or empty.

    Returns:
    list:
    A list of dictionaries containing department data.
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
    search_interval = get_last_time("department")

    """
    QUERY DATA CREATED IN LAST SEARCH INTERVAL
    """
    conn.run('SET search_path TO "kp-test-source", public;')

    query = "SELECT * FROM department WHERE \
        created_at > :search_interval OR last_updated > :search_interval;"
    params = {"search_interval": search_interval}
    rows = conn.run(query, **params)

    created_data = []
    for row in rows:
        item = {
            "department_id": row[0],
            "department_name": row[1],
            "location": row[2],
            "manager": row[3],
            "created_at": row[4],
            "last_updated": row[5],
        }
        created_data.append(item)
    return created_data
