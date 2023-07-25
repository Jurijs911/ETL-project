import os
import pg8000.native
from datetime import datetime
import datetime
from dotenv import load_dotenv
load_dotenv()


def get_address_add():
    db_user = os.environ.get("DB_SOURCE_USER")
    db_database = os.environ.get("DB_SOURCE_NAME")
    db_host = os.environ.get("DB_SOURCE_HOST")
    db_port = os.environ.get("DB_SOURCE_PORT")
    db_password = os.environ.get("DB_SOURCE_PASSWORD")
    conn = pg8000.native.Connection(
        user=db_user, database=db_database, host=db_host, port=db_port, password=db_password
    )

    """
    this determines the search interval (5 mins)
    """

    five_mins_ago = datetime.datetime.now() - datetime.timedelta(minutes=5)
    # dt_formatted = five_mins_ago.strftime('%Y, %m, %d, %H, %M, %S, %f')

    # query_str=f"datetime.datetime({dt_formatted})"
    print("time 5 mins ago:", five_mins_ago)

    """
    this section queries the database to return only rows created in the last 5 minutes
    """
    query = f"SELECT * FROM address WHERE created_at < '{five_mins_ago}';"
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
            "phone": row[7]
        }
        created_data.append(item)
    print(rows)
    return created_data
