import pg8000.native 
import os
from dotenv import load_dotenv

load_dotenv()

def create_connection():
    DB_SOURCE_USER = os.getenv('DB_SOURCE_USER')
    DB_SOURCE_HOST = os.getenv('DB_SOURCE_HOST')
    DB_SOURCE_NAME = os.getenv('DB_SOURCE_NAME')
    DB_SOURCE_PORT = os.getenv('DB_SOURCE_PORT')
    DB_SOURCE_PASSWORD = os.getenv('DB_SOURCE_PASSWORD')

    conn = pg8000.native.Connection(
        user=DB_SOURCE_USER, host=DB_SOURCE_HOST, database=DB_SOURCE_NAME, port=DB_SOURCE_PORT, password=DB_SOURCE_PASSWORD
    )
    return conn


def insert_into_dim_design(conn, design_data):
    """
    Insert data into the dim_design table

    :param conn: pg8000 connection 
    :param design_data: list of lists containing data to be inserted
    """

    for design in design_data:
        conn.run("INSERT INTO dim_design (design_id, design_name, file_location, file_name) VALUES (:(design_id), :(design_name), :(file_location), :(file_name))",
                 design_id=design[0], design_name=design[1], file_location=design[2], file_name=design[3])

    loaded_data = []
    for row in conn.run("SELECT * FROM dim_design"):
        loaded_data.append(row)

    conn.close()

    return loaded_data


def insert_into_dim_currency(conn, currency_data):
    """
    Insert data into the dim_currency table

    :param conn: pg8000 connection 
    :param currency_data: list of lists containing data to be inserted
    """

    for currency in currency_data:
        conn.run("INSERT INTO dim_currency (currency_id, currency_code, currency_name) VALUES (:(currency_id), :(currency_code), :(currency_name))",
                 currency_id=currency[0], currency_code=currency[1], currency_name=currency[2])

    loaded_data = []
    for row in conn.run("SELECT * FROM currency_data"):
        loaded_data.append(row)

    conn.close()

    return loaded_data


def insert_into_dim_staff(conn, staff_data):
    """
    Insert data into the dim_staff table

    :param conn: pg8000 connection 
    :param staff_data: list of lists containing data to be inserted
    """

    for staff in staff_data:
        conn.run("INSERT INTO dim_staff (staff_id, first_name, last_name, department_name, location, email_address) VALUES (:(staff_id), :(first_name), :(last_name), :(department_name), :(location), :(email_address))",
                 staff_id=staff[0], first_name=staff[1], last_name=staff[2], department_name=staff[3], location=staff[4], email_address=staff[5])

    loaded_data = []
    for row in conn.run("SELECT * FROM staff_data"):
        loaded_data.append(row)

    conn.close()

    return loaded_data