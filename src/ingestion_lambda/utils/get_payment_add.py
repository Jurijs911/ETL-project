import os
import pg8000.native
import datetime
from src.ingestion_lambda.utils.get_last_time import get_last_time
from dotenv import load_dotenv
load_dotenv()


def get_payment_add():
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
    search_interval = get_last_time('payment')

    """
    QUERY DATA CREATED IN LAST SEARCH INTERVAL
    """
    query = f"SELECT * FROM payment WHERE created_at > '{search_interval}';"
    rows = conn.run(query)
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
            "counterparty_ac_number": row[11]
        }
        created_data.append(item)
    return created_data
