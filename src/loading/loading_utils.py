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


def insert_into_dim_location(conn, location_data):
    """
    Insert data into the dim_location table

    :param conn: pg8000 connection 
    :param location_data: list of lists containing data to be inserted
    """

    for location in location_data:
        conn.run("INSERT INTO dim_location (location_id, address_line_1, address_line_2, district, city, postal_code, country, phone) VALUES (:(location_id), :(address_line_1, :(address_line_2), :(district), :(city), :(postal_code), :(country), :(phone))",
                 location_id=location[0], address_line_1=location[1], address_line_2=location[2], district=location[3], city=location[4], postal_code=location[5], country=location[6], phone=location[7])

    loaded_data = []
    for row in conn.run("SELECT * FROM location_data"):
        loaded_data.append(row)

    conn.close()

    return loaded_data


def insert_into_dim_date(conn, date_data):
    """
    Insert data into the dim_date table

    :param conn: pg8000 connection 
    :param date_data: list of lists containing data to be inserted
    """

    for date in date_data:
        conn.run("INSERT INTO dim_date (date_id, year, month, day, day_of_week, day_name, month_name, quarter) VALUES (:(date_id), :(year), :(month), :(day), :(day_of_week), :(day_name), :(month_name), :(quarter))",
                 date_id=date[0], year=date[1], month=date[2], day=date[3], day_of_week=date[4], day_name=date[5], month_name=date[6], quarter=date[7])

    loaded_data = []
    for row in conn.run("SELECT * FROM date_data"):
        loaded_data.append(row)

    conn.close()

    return loaded_data


def insert_into_dim_counterparty(conn, counterparty_data):
    """
    Insert data into the dim_counterparty table

    :param conn: pg8000 connection 
    :param counterparty_data: list of lists containing data to be inserted
    """

    for counterparty in counterparty_data:
        conn.run("INSERT INTO dim_counterparty (counterparty_id, counterparty_legal_name, counterparty_legal_address_line_1, counterparty_legal_address_line_2, counterparty_legal_district, counterparty_legal_city, counterparty_legal_postal_code, counterparty_legal_country, counterparty_legal_phone_number) VALUES (:(counterparty_id), :(counterparty_legal_name), :(counterparty_legal_address_line_1), :(counterparty_legal_address_line_2), :(counterparty_legal_district), :(counterparty_legal_city), :(counterparty_legal_postal_code), :(counterparty_legal_country), :(counterparty_legal_phone_number))",
                 counterparty_id=counterparty[0], counterparty_legal_name=counterparty[1], counterparty_legal_address_line_1=counterparty[2], counterparty_legal_address_line_2=counterparty[3], counterparty_legal_district=counterparty[4], counterparty_legal_city=counterparty[5], counterparty_legal_postal_code=counterparty[6], counterparty_legal_country=counterparty[7], counterparty_legal_phone_number=counterparty[8])

    loaded_data = []
    for row in conn.run("SELECT * FROM counterparty_data"):
        loaded_data.append(row)

    conn.close()

    return loaded_data


def insert_into_dim_fact_sales_order(conn, fact_sales_order_data):
    """
    Insert data into the dim_fact_sales_order table

    :param conn: pg8000 connection 
    :param fact_sales_order_data: list of lists containing data to be inserted
    """

    for fact in fact_sales_order_data:
        conn.run("INSERT INTO fact_sales_order (sales_record_id, sales_order_id, created_date, created_time, last_updated_date, last_updated_time, sales_staff_id, counterparty_id, units_sold, unit_price, currency_id, design_id, agreed_payment_date, agreed_delivery_date, agreed_delivery_location_id) VALUES (:(sales_record_id), :(sales_order_id), :(created_date), :(created_time), :(last_updated_date), :(last_updated_time), :(sales_staff_id), :(counterparty_id), :(units_sold), :(unit_price), :(currency_id), :(design_id), :(agreed_payment_date), :(agreed_delivery_date), :(agreed_delivery_location_id))",
                 sales_record_id=fact[0], sales_order_id=fact[1], created_date=fact[2], created_time=fact[3], last_updated_date=fact[4], last_updated_time=fact[5], sales_staff_id=fact[6], counterparty_id=fact[7], units_sold=fact[8], unit_price=fact[9], currency_id=fact[10], design_id=fact[11], agreed_payment_date=fact[12], agreed_delivery_date=fact[13], agreed_delivery_location_id=fact[14])

    loaded_data = []
    for row in conn.run("SELECT * FROM fact_sales_order_data"):
        loaded_data.append(row)

    conn.close()

    return loaded_data





