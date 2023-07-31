import os
import pg8000.native
from src.ingestion_lambda.get_last_time import get_last_time
from dotenv import load_dotenv

load_dotenv()


def get_sales_order_add():
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
    search_interval = get_last_time("sales_order")

    """
    QUERY DATA CREATED IN LAST SEARCH INTERVAL
    """
    query = (
        f"SELECT * FROM sales_order WHERE created_at > :search_interval;"
    )
    params = {'search_interval': search_interval}
    rows = conn.run(query, **params)
    created_data = []
    for row in rows:
        item = {
            "sales_order_id": row[0],
            "created_at": row[1],
            "last_updated": row[2],
            "design_id": row[3],
            "staff_id": row[4],
            "counter_party_id": row[5],
            "units_sold": row[6],
            "unit_price": row[7],
            "currency_id": row[8],
            "agreed_delivery_date": row[9],
            "agreed_payment_date": row[10],
            "agreed_delivery_location_id": row[11],
        }
        created_data.append(item)

    return created_data
