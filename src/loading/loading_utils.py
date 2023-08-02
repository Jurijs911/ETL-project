import pg8000.native 
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class InputValidationError(Exception):
    pass

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

def get_loaded_data(conn, table_name):
    """
    Retrieve all data from the specified table after insertion.

    :param conn: pg8000 connection
    :param table_name: Name of the table to retrieve data from.
    :return: A list of rows containing the loaded data.
    """
    loaded_data = []
    try:
        for row in conn.run(f"SELECT * FROM {table_name}"):
            loaded_data.append(row)

        conn.close()

    except Exception as e:
        conn.close()
        raise

    return loaded_data




def is_valid_email(email):
    import re
    pattern = r"[^@]+@[^@]+\.[^@]+"
    return bool(re.match(pattern, email))



def insert_into_dim_design(conn, design_data):
    """
    Insert data into the dim_design table

    :param conn: pg8000 connection 
    :param design_data: list of lists containing data to be inserted
    """

    try:
        conn.run("SET search_path TO 'project_team_2';")
        for design in design_data:
            if not isinstance(design[0], int):
                raise InputValidationError
            for index, column in enumerate(design[1:], start=1):
                if not isinstance(column, str):
                    raise InputValidationError
            if len(design) != 4:
                raise InputValidationError
            print("In the try block")
            conn.run("INSERT INTO dim_design(design_id, design_name, file_location, file_name) VALUES (:design_id, :design_name, :file_location, :file_name)",
                     design_id=design[0], design_name=design[1], file_location=design[2], file_name=design[3])

        print("Inserted data")
        #conn.commit()  # Commit the changes to the database
        conn.close()

        return get_loaded_data(conn, "dim_design")

    except InputValidationError as ive:
        conn.close()
        raise

    except Exception as e:
        conn.close()
        raise



def insert_into_dim_currency(conn, currency_data):
    """
    Insert data into the dim_currency table

    :param conn: pg8000 connection 
    :param currency_data: list of lists containing data to be inserted
    """

    try:
        for currency in currency_data:
            if not isinstance(currency[0], int):
                raise InputValidationError
            for index, column in enumerate(currency[1:], start=1):
                if not isinstance(column, str):
                    raise InputValidationError
            if len(currency) != 3:
                raise InputValidationError

            conn.run("INSERT INTO dim_currency (currency_id, currency_code, currency_name) VALUES (:currency_id, :currency_code, :currency_name)",
                     currency_id=currency[0], currency_code=currency[1], currency_name=currency[2])

        conn.close()

        return get_loaded_data(conn, "dim_currency")
    
    except InputValidationError as ive:
        conn.close()
        raise

    except Exception as e:
        conn.close()
        raise


def insert_into_dim_staff(conn, staff_data):
    """
    Insert data into the dim_staff table

    :param conn: pg8000 connection 
    :param staff_data: list of lists containing data to be inserted
    """

    try:
        for staff in staff_data:
            if not isinstance(staff[0], int):
                raise InputValidationError("staff[0] is not an int")
            if not is_valid_email(staff[5]):
                raise InputValidationError("Invalid email address format.")

            for index, column in enumerate(staff[1:-1], start=1):
                if not isinstance(column, str):
                    raise InputValidationError("Should be a string")

            if len(staff) != 6:
                raise InputValidationError

            conn.run("INSERT INTO dim_staff (staff_id, first_name, last_name, department_name, location, email_address) VALUES (:staff_id, :first_name, :last_name, :department_name, :location, :email_address)",
                     staff_id=staff[0], first_name=staff[1], last_name=staff[2], department_name=staff[3], location=staff[4], email_address=staff[5])

        conn.close()

        return get_loaded_data(conn, "dim_staff")
    
    except InputValidationError as ive:
        conn.close()
        raise

    except Exception as e:
        conn.close()
        raise



def insert_into_dim_location(conn, location_data):
    """
    Insert data into the dim_location table

    :param conn: pg8000 connection 
    :param location_data: list of lists containing data to be inserted
    """
    try:
        for location in location_data:
            if not isinstance(location[0], int):
                raise InputValidationError

            for index, column in enumerate(location[1:], start=1):
                if not isinstance(column, str):
                    raise InputValidationError

            if len(location) != 8:
                raise InputValidationError

            conn.run("INSERT INTO dim_location (location_id, address_line_1, address_line_2, district, city, postal_code, country, phone) VALUES (:location_id, :address_line_1, :address_line_2, :district, :city, :postal_code, :country, :phone)",
                    location_id=location[0], address_line_1=location[1], address_line_2=location[2], district=location[3], city=location[4], postal_code=location[5], country=location[6], phone=location[7])

        conn.close()

        return get_loaded_data(conn, "dim_location")

    except InputValidationError as ive:
        conn.close()
        raise

    except Exception as e:
        conn.close()
        raise


def insert_into_dim_date(conn, date_data):
    """
    Insert data into the dim_date table

    :param conn: pg8000 connection 
    :param date_data: list of lists containing data to be inserted
    """            
    try:
        for date in date_data:
            conn.run(
                "INSERT INTO dim_date (date_id, year, month, day, day_of_week, day_name, month_name, quarter) "
                "VALUES (:date_id, :year, :month, :day, :day_of_week, :day_name, :month_name, :quarter)",
                date_id=date[0],
                year=date[1],
                month=date[2],
                day=date[3],
                day_of_week=date[4],
                day_name=date[5],
                month_name=date[6],
                quarter=date[7],
            )

        conn.close()

    except InputValidationError as ive:
        conn.close()
        raise

    except Exception as e:
        conn.close()
        raise

    return date_data



def insert_into_dim_counterparty(conn, counterparty_data):
    """
    Insert data into the dim_counterparty table

    :param conn: pg8000 connection
    :param counterparty_data: list of lists containing data to be inserted
    """

    try:
        for counterparty in counterparty_data:
            if not isinstance(counterparty[0], int):
                raise InputValidationError

            for index, column in enumerate(counterparty[1:]):
                if not isinstance(column, str):
                    raise InputValidationError

            conn.run(
                    "INSERT INTO dim_counterparty (counterparty_id, counterparty_legal_name, counterparty_legal_address_line_1, counterparty_legal_address_line2, counterparty_legal_district, counterparty_legal_city, counterparty_legal_postal_code, counterparty_legal_country, counterparty_legal_phone_number) VALUES (:counterparty_id, :counterparty_legal_name, :counterparty_legal_address_line_1, :counterparty_legal_address_line2, :counterparty_legal_district, :counterparty_legal_city, :counterparty_legal_postal_code, :counterparty_legal_country, :counterparty_legal_phone_number)",
                     counterparty_id=counterparty[0], counterparty_legal_name=counterparty[1], counterparty_legal_address_line_1=counterparty[2], counterparty_legal_address_line2=counterparty[3], counterparty_legal_district=counterparty[4], counterparty_legal_city=counterparty[5], counterparty_legal_postal_code=counterparty[6], counterparty_legal_country=counterparty[7], counterparty_legal_phone_number=counterparty[8])

        conn.close()

    except InputValidationError as ive:
        conn.close()
        raise

    except Exception as e:
        conn.close()
        raise

    return counterparty_data




def insert_into_dim_fact_sales_order(conn, fact_sales_order_data):
    """
    Insert data into the dim_fact_sales_order table

    :param conn: pg8000 connection
    :param fact_sales_order_data: list of lists containing data to be inserted
    """

    try:
        for sale in fact_sales_order_data:
            expected_data_types = [int, str, str, str, str, int, int, int, float, int, int, str, str, int]
            for index, value in enumerate(sale):
                if not isinstance(value, expected_data_types[index]):
                    raise InputValidationError

            try:
                datetime.strptime(sale[1], "%Y-%m-%d")
                
                # Remove the fractional part from the time strings before parsing
                datetime.strptime(sale[2].split(".")[0], "%H:%M:%S")
                
                datetime.strptime(sale[3], "%Y-%m-%d")
                
                # Remove the fractional part from the time strings before parsing
                datetime.strptime(sale[4].split(".")[0], "%H:%M:%S")

                datetime.strptime(sale[11], "%Y-%m-%d")
                datetime.strptime(sale[12], "%Y-%m-%d")

            except ValueError:
                raise InputValidationError

            conn.run("INSERT INTO fact_sales_order(sales_order_id, created_date, created_time, last_updated_date, last_updated_time, sales_staff_id, counterparty_id, units_sold, unit_price, currency_id, design_id, agreed_payment_date, agreed_delivery_date, agreed_delivery_location_id) VALUES (:sales_order_id, :created_date, :created_time, :last_updated_date, :last_updated_time, :sales_staff_id, :counterparty_id, :units_sold, :unit_price, :currency_id, :design_id, :agreed_payment_date, :agreed_delivery_date, :agreed_delivery_location_id)",
                     sales_order_id=sale[0], created_date=sale[1], created_time=sale[2], last_updated_date=sale[3], last_updated_time=sale[4], sales_staff_id=sale[5], counterparty_id=sale[6], units_sold=sale[7], unit_price=sale[8], currency_id=sale[9], design_id=sale[10], agreed_payment_date=sale[11], agreed_delivery_date=sale[12], agreed_delivery_location_id=sale[13])

        conn.close()

    except InputValidationError as ive:
        conn.close()
        raise

    except Exception as e:
        conn.close()
        raise

    return fact_sales_order_data



