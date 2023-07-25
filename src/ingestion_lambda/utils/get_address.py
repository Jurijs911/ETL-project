import os
import pg8000
import pg8000.native
from dotenv import load_dotenv
load_dotenv()


def get_address():
    try:
        db_user = os.environ.get("DB_SOURCE_USER")
        db_database = os.environ.get("DB_SOURCE_NAME")
        db_host = os.environ.get("DB_SOURCE_HOST")
        db_port = os.environ.get("DB_SOURCE_PORT")
        db_password = os.environ.get("DB_SOURCE_PASSWORD")
        conn = pg8000.native.Connection(user=db_user, database=db_database,
                                        host=db_host, port=db_port, password=db_password)
        query = "SELECT * FROM address"
        rows = conn.run(query)
        data = []
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
            data.append(item)
        return data
    except pg8000.DatabaseError as db_error:
        print("Something went wrong:", db_error)
    finally:
        conn.close()


print(get_address())
