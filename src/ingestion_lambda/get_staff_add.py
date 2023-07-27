import os
import pg8000.native
import datetime 
from src.ingestion_lambda.get_last_time import get_last_time
from dotenv import load_dotenv
load_dotenv()

def get_staff_add():
    """
    CONNECTION
    """
    db_user = os.environ.get("DB_SOURCE_USER")
    db_database = os.environ.get("DB_SOURCE_NAME")
    db_host = os.environ.get("DB_SOURCE_HOST")
    db_port = os.environ.get("DB_SOURCE_PORT")
    db_password = os.environ.get("DB_SOURCE_PASSWORD")
    conn = pg8000.native.Connection(
        user=db_user, database=db_database, host=db_host, port=db_port, password=db_password
    )

    """
    DETERMINE SEARCH INTERVAL
    """
    search_interval = get_last_time('staff')

    """
    QUERY DATA CREATED IN LAST SEARCH INTERVAL
    """
    query = f"SELECT * FROM staff WHERE created_at > '{search_interval}';"
    rows = conn.run(query)
    created_data = []
    for row in rows:
        item = {
            "staff_id": row[0],
            "first_name": row[1],
            "last_name": row[2],
            "department_id": row[3],
            "email_address": row[4],
            "created_at": row[5],
            "last_updated": row[6],
        }
        created_data.append(item)

    return created_data