from get_last_time import get_last_time
from get_secret import get_secret
import pg8000.native
from dotenv import load_dotenv

load_dotenv()

secret = get_secret()
# class MissingRequiredEnvironmentVariables (Exception):
#     """
#         Is produced when attempts to connect to DB
#         with variables which do not exist
#     """

#     def __init__(self, db_user, db_database, db_host, db_port, db_password):
#         self.user = db_user,
#         self.database = db_database,
#         self.host = db_host,
#         self.port = db_port,
#         self.password = db_password


def get_address_add(
    db_user=secret["username"],
    db_database=secret["dbname"],
    db_host=secret["host"],
    db_port=secret["port"],
    db_password=secret["password"],
):
    """
    CONNECTION
    """
    #
    # ADD BACK IN AFTER GITHUB ISSUE RESOLVED
    #  if not all([db_user, db_database, db_host, db_port, db_password]):
    #     raise MissingRequiredEnvironmentVariables(
    #         db_user, db_database, db_host, db_port, db_password)

    # try:
    #     conn = pg8000.native.Connection(
    #         user=db_user,
    #         database=db_database,
    #         host=db_host,
    #         port=db_port,
    #         password=db_password,
    #     )
    # except pg8000.exceptions.DatabaseError:
    #     raise Exception("Database error")

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
    search_interval = get_last_time("address")

    """
    QUERY DATA CREATED IN LAST SEARCH INTERVAL
    """
    #
    # Set schema search order
    conn.run('SET search_path TO "kp-test-source", public;')

    #
    # Query table
    query = "SELECT * FROM address WHERE created_at > :search_interval;"
    params = {"search_interval": search_interval}
    rows = conn.run(query, **params)

    created_data = []
    for row in rows:
        item = {
            "location_id": row[0],
            "address_line_1": row[1],
            "address_line_2": row[2],
            "district": row[3],
            "city": row[4],
            "postal_code": row[5],
            "country": row[6],
            "phone": row[7],
            "created_at": row[8],
            "last_updated": row[9],
        }
        created_data.append(item)
    return created_data
