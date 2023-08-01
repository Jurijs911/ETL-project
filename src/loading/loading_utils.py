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

            if len(staff) != 7:
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
            if not isinstance(date[0], str):
                raise InputValidationError
            try:
                datetime.strptime(date[0], "%Y-%m-%d")
            except ValueError:
                raise InputValidationError

            if not isinstance(date[1], int):
                raise InputValidationError

            if not isinstance(date[2], int):
                raise InputValidationError

            if not isinstance(date[3], int):
                raise InputValidationError

            if not isinstance(date[4], int):
                raise InputValidationError

            if not isinstance(date[5], str):
                raise InputValidationError

            if not isinstance(date[6], str):
                raise InputValidationError

            if not isinstance(date[7], int):
                raise InputValidationError

            try:
                datetime.strptime(str(date[2]), "%Y-%m-%d")
                datetime.strptime(str(date[3]), "%H:%M:%S")  
                datetime.strptime(str(date[4]), "%Y-%m-%d")
                datetime.strptime(str(date[5]), "%H:%M:%S")  
                datetime.strptime(str(date[12]), "%Y-%m-%d")
                datetime.strptime(str(date[13]), "%Y-%m-%d")
            except ValueError:
                raise InputValidationError

            conn.run("INSERT INTO dim_date (date_id, year, month, day, day_of_week, day_name, month_name, quarter) VALUES (:date_id, :year, :month, :day, :day_of_week, :day_name, :month_name, :quarter)",
                     date_id=date[0], year=date[1], month=date[2], day=date[3], day_of_week=date[4], day_name=date[5], month_name=date[6], quarter=date[7])

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

            conn.run("INSERT INTO dim_counterparty (counterparty_id, name, address_line_1, address_line_2, district, city, postal_code, country, region_id) VALUES (:counterparty_id, :name, :address_line_1, :address_line_2, :district, :city, :postal_code, :country, :region_id)",
                     counterparty_id=counterparty[0], name=counterparty[1], address_line_1=counterparty[2], address_line_2=counterparty[3], district=counterparty[4], city=counterparty[5], postal_code=counterparty[6], country=counterparty[7], region_id=counterparty[8])

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
            expected_data_types = [int, int, str, str, str, str, int, int, int, float, int, int, str, str, int]
            for index, value in enumerate(sale):
                if not isinstance(value, expected_data_types[index]):
                    raise InputValidationError

            try:
                datetime.strptime(sale[2], "%Y-%m-%d")
                datetime.strptime(sale[3], "%H:%M:%S:%f")
                datetime.strptime(sale[4], "%Y-%m-%d")
                datetime.strptime(sale[5], "%H:%M:%S:%f")
                datetime.strptime(sale[12], "%Y-%m-%d")
                datetime.strptime(sale[13], "%Y-%m-%d")
            except ValueError:
                raise InputValidationError

            conn.run("INSERT INTO dim_fact_sales_order (order_id, product_id, order_date, order_time, ship_date, ship_time, quantity, list_price, discount, sales_price, profit, priority, customer_name, region_name, counterparty_id) VALUES (:order_id, :product_id, :order_date, :order_time, :ship_date, :ship_time, :quantity, :list_price, :discount, :sales_price, :profit, :priority, :customer_name, :region_name, :counterparty_id)",
                     order_id=sale[0], product_id=sale[1], order_date=sale[2], order_time=sale[3], ship_date=sale[4], ship_time=sale[5], quantity=sale[6], list_price=sale[7], discount=sale[8], sales_price=sale[9], profit=sale[10], priority=sale[11], customer_name=sale[12], region_name=sale[13], counterparty_id=sale[14])

        conn.close()

    except InputValidationError as ive:
        conn.close()
        raise

    except Exception as e:
        conn.close()
        raise

    return fact_sales_order_data





