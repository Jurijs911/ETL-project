import os
import pg8000.native
import datetime 
from src.ingestion_lambda.get_last_time import get_last_time
from dotenv import load_dotenv
load_dotenv()

def get_counterparty_add():
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
    search_interval = get_last_time('counterparty')

    print("DEBUG time:", search_interval)

    """
    QUERY DATA CREATED IN LAST SEARCH INTERVAL
    """
    query = f"SELECT * FROM counterparty WHERE created_at > '{search_interval}';"
    rows = conn.run(query)
    created_data = []
    for row in rows:
        item = {
            "counterparty_id": row[0],
            "counterparty_legal_name": row[1],
            "legal_address_id": row[2],
            "commercial_contact": row[3],
            "delivery_contact": row[4],
            "created_at": row[5],
            "last_updated": row[6]
        }
        created_data.append(item)
    # print(created_data)
    return created_data