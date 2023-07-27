import os
import pg8000.native
import datetime
from src.ingestion_lambda.utils.get_last_time import get_last_time
from dotenv import load_dotenv
load_dotenv()


def get_design_add():
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
    search_interval = get_last_time('design')

    print("DEBUG time:", search_interval)

    """
    QUERY DATA CREATED IN LAST SEARCH INTERVAL
    """
    query = f"SELECT * FROM design WHERE created_at > '{search_interval}';"
    rows = conn.run(query)
    created_data = []
    print(rows)
    for row in rows:
        item = {
            "design_id": row[0],
            "created_at": row[1],
            "design_name": row[2],
            "file_location": row[3],
            "file_name": row[4],
            "last_updated": row[5]
        }
        created_data.append(item)
    return created_data


get_design_add()

# Table design as D {
#   design_id int [pk, increment, not null]
#   created_at timestamp [not null, default: `current_timestamp`]
#   last_updated timestamp [not null, default: `current_timestamp`]
#   design_name varchar [not null]
#   file_location varchar [not null, note: 'directory location']
#   file_name varchar [not null, note: 'file name']
# }
