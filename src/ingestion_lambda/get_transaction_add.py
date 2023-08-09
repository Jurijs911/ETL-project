import os
import pg8000.native
from src.ingestion_lambda.get_last_time import get_last_time
from dotenv import load_dotenv

load_dotenv()


def get_transaction_add():
    """
    CONNECTION
    """
    db_user = os.environ.get("DB_SOURCE_USER")
    db_database = os.environ.get("DB_SOURCE_NAME")
    db_host = os.environ.get("DB_SOURCE_HOST")
    db_port = os.environ.get("DB_SOURCE_PORT")
    db_password = os.environ.get("DB_SOURCE_PASSWORD")
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
    search_interval = get_last_time("transaction")

    print("DEBUG time:", search_interval)

    """
    QUERY DATA CREATED IN LAST SEARCH INTERVAL
    """
    query = f"SELECT * FROM transaction WHERE created_at > '{search_interval}';"
    rows = conn.run(query)
    created_data = []
    for row in rows:
        item = {
            "transaction_id": row[0],
            "transaction_type": row[1],
            "sales_order_id": row[2],
            "purchase_order_id": row[3],
            "created_at": row[4],
            "last_updated": row[5],
        }
        created_data.append(item)
    return created_data
