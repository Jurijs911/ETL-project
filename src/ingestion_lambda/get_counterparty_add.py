import os
import pg8000.native
from src.ingestion_lambda.get_last_time import get_last_time
from dotenv import load_dotenv

load_dotenv()


class MissingRequiredEnvironmentVariables (Exception):
    """
        Is produced when attempts to connect to DB
        with variables which do not exist
    """

    def __init__(self, db_user, db_database, db_host, db_port, db_password):
        self.user = db_user,
        self.database = db_database,
        self.host = db_host,
        self.port = db_port,
        self.password = db_password


def get_counterparty_add(
        db_user=os.environ.get("DB_SOURCE_USER"),
        db_schema='',
        db_database=os.environ.get("DB_SOURCE_NAME"),
        db_host=os.environ.get("DB_SOURCE_HOST"),
        db_port=os.environ.get("DB_SOURCE_PORT"),
        db_password=os.environ.get("DB_SOURCE_PASSWORD")):

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
    except pg8000.exceptions.DatabaseError as e:
        raise Exception("Database error")

    """
    DETERMINE SEARCH INTERVAL
    """
    search_interval = get_last_time("counterparty")

    """
    QUERY DATA CREATED IN LAST SEARCH INTERVAL
    """
    target_table = db_schema + 'counterparty'
    query = (
        f'SELECT * FROM {target_table} WHERE created_at > :search_interval;')
    params = {'search_interval': search_interval}
    rows = conn.run(query, **params)
    # #if rows = {}:
    # log....
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
