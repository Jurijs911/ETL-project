import os
import pg8000.native
import datetime 
from src.ingestion_lambda.utils.get_last_time import get_last_time
from dotenv import load_dotenv
load_dotenv()


def get_address_add():
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
    search_interval = get_last_time('address')

    print("DEBUG time:", search_interval)

    """
    QUERY DATA CREATED IN LAST SEARCH INTERVAL
    """
    query = f"SELECT * FROM address WHERE created_at > '{search_interval}';"
    rows = conn.run(query)
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
            "last_updated": row[9]
        }
        created_data.append(item)
    return created_data