import os
import pg8000.native
from src.ingestion_lambda.get_last_time import get_last_time
from dotenv import load_dotenv

load_dotenv()


def get_department_add(
        db_user=os.environ.get("DB_SOURCE_USER"),
        db_database=os.environ.get("DB_SOURCE_NAME"),
        db_host=os.environ.get("DB_SOURCE_HOST"),
        db_port=os.environ.get("DB_SOURCE_PORT"),
        db_password=os.environ.get("DB_SOURCE_PASSWORD")):
    """
    CONNECTION
    """
    #
    # TO BE RE-ADDED LATER - CAUSES CI/CD FAILURE
    # if not all([db_user, db_database, db_host, db_port, db_password]):
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
    search_interval = get_last_time("department")

    """
    QUERY DATA CREATED IN LAST SEARCH INTERVAL
    """
    #
    # Set schema search order
    conn.run('SET search_path TO "kp-test-source", public;')

    #
    # Query table
    query = 'SELECT * FROM department WHERE created_at > :search_interval;'
    params = {'search_interval': search_interval}
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
