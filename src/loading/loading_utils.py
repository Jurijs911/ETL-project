import pg8000.native
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


class InputValidationError(Exception):
    pass


def create_connection(db_user, db_database, db_host, db_port, db_password):
    """Create a connection to the PostgreSQL database
    using the passed connection variables."""

    conn = pg8000.native.Connection(
        user=db_user,
        database=db_database,
        host=db_host,
        port=db_port,
        password=db_password,
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

    except Exception:
        raise

    return loaded_data


def is_valid_email(email):
    """Check if the given email address is in a valid format.

    Returns:
        bool: True if the email address is valid, False otherwise."""

    import re

    pattern = r"[^@]+@[^@]+\.[^@]+"
    return bool(re.match(pattern, email))


def insert_into_dim_design(conn, design_data):
    """
     Insert data into the dim_design table.

    Returns:
        list: A list of rows containing the loaded data after insertion.

    Raises:
        InputValidationError: If the input data does not meet
        the required format for insertion.

    """

    try:
        conn.run("SET search_path TO 'project_team_2';")
        for design in design_data:
            for index, column in enumerate(design):
                if not isinstance(column, str):
                    raise InputValidationError
            if len(design) != 4:
                raise InputValidationError
            print("In the try block")
            conn.run(
                """INSERT INTO dim_design(
                    design_id,
                    design_name,
                    file_location,
                    file_name
                ) VALUES (
                    :design_id,
                    :design_name,
                    :file_location,
                    :file_name
                ) ON CONFLICT (design_id) DO UPDATE
                    SET design_id = :design_id,
                        design_name = :design_name,
                        file_location = :file_location,
                        file_name = :file_name;""",
                design_id=design[0],
                design_name=design[1],
                file_location=design[2],
                file_name=design[3],
            )
        return get_loaded_data(conn, "dim_design")

    except InputValidationError:
        raise

    except Exception:
        raise


def insert_into_dim_currency(conn, currency_data):
    """
     Insert data into the dim_currency table.

    Returns:
        list: A list of rows containing the loaded data after insertion.

    Raises:
        InputValidationError: If the input data does not meet
        the required format for insertion.

    """

    try:
        for currency in currency_data:
            for index, column in enumerate(currency):
                if not isinstance(column, str):
                    raise InputValidationError
            if len(currency) != 3:
                raise InputValidationError

            conn.run(
                """INSERT INTO dim_currency (
                    currency_id,
                    currency_code,
                    currency_name
                ) VALUES (
                    :currency_id,
                    :currency_code,
                    :currency_name
                ) ON CONFLICT (currency_id) DO UPDATE
                    SET currency_id = :currency_id,
                        currency_code = :currency_code,
                        currency_name = :currency_name;""",
                currency_id=currency[0],
                currency_code=currency[1],
                currency_name=currency[2],
            )
        return get_loaded_data(conn, "dim_currency")

    except InputValidationError:
        raise

    except Exception:
        raise


def insert_into_dim_staff(conn, staff_data):
    """
    Insert data into the dim_staff table.

    Returns:
        list: A list of rows containing the loaded data after insertion.

    Raises:
        InputValidationError: If the input data does not meet
        the required format for insertion.
    """

    try:
        for staff in staff_data:
            if not is_valid_email(staff[5]):
                raise InputValidationError("Invalid email address format.")

            for index, column in enumerate(staff):
                if not isinstance(column, str):
                    raise InputValidationError("Should be a string")

            if len(staff) != 6:
                raise InputValidationError

            conn.run(
                """INSERT INTO dim_staff (
                    staff_id,
                    first_name,
                    last_name,
                    department_name,
                    location,
                    email_address
                ) VALUES (
                    :staff_id,
                    :first_name,
                    :last_name,
                    :department_name,
                    :location,
                    :email_address
                ) ON CONFLICT (staff_id) DO UPDATE
                    SET staff_id = :staff_id,
                        first_name = :first_name,
                        last_name = :last_name,
                        department_name = :department_name,
                        location = :location,
                        email_address = :email_address;""",
                staff_id=staff[0],
                first_name=staff[1],
                last_name=staff[2],
                department_name=staff[3],
                location=staff[4],
                email_address=staff[5],
            )

        return get_loaded_data(conn, "dim_staff")

    except InputValidationError:
        raise

    except Exception:
        raise


def insert_into_dim_location(conn, location_data):
    """
    Insert data into the dim_location table.

    Returns:
        list: A list of rows containing the loaded data after insertion.

    Raises:
        InputValidationError: If the input data does not meet
        the required format for insertion.

    """
    try:
        for location in location_data:
            for index, column in enumerate(location):
                if not isinstance(column, str):
                    raise InputValidationError

            if len(location) != 8:
                raise InputValidationError

            conn.run(
                """INSERT INTO dim_location (
                    location_id,
                    address_line_1,
                    address_line_2,
                    district,
                    city,
                    postal_code,
                    country,
                    phone
                ) VALUES (
                    :location_id,
                    :address_line_1,
                    :address_line_2,
                    :district,
                    :city,
                    :postal_code,
                    :country,
                    :phone
                ) ON CONFLICT (location_id) DO UPDATE
                    SET location_id = :location_id,
                        address_line_1 = :address_line_1,
                        address_line_2 = :address_line_2,
                        district = :district,
                        city = :city,
                        postal_code = :postal_code,
                        country = :country,
                        phone = :phone;""",
                location_id=location[0],
                address_line_1=location[1],
                address_line_2=location[2],
                district=location[3],
                city=location[4],
                postal_code=location[5],
                country=location[6],
                phone=location[7],
            )

        return get_loaded_data(conn, "dim_location")

    except InputValidationError:
        raise

    except Exception:
        raise


def insert_into_dim_date(conn, date_data):
    """
    Insert data into the dim_date table.

    Returns:
        list: A list of rows containing the loaded data after insertion.

    Raises:
        InputValidationError: If the input data does not meet
        the required format for insertion.
    """
    try:
        for date in date_data:
            if False in date:  # CHANGE THIS
                raise InputValidationError
            conn.run(
                """INSERT INTO dim_date (
                    date_id,
                    year,
                    month,
                    day,
                    day_of_week,
                    day_name,
                    month_name,
                    quarter
                ) VALUES (
                    :date_id,
                    :year,
                    :month,
                    :day,
                    :day_of_week,
                    :day_name,
                    :month_name,
                    :quarter
                ) ON CONFLICT (date_id) DO UPDATE
                    SET date_id = :date_id,
                        year = :year,
                        month = :month,
                        day = :day,
                        day_of_week = :day_of_week,
                        day_name = :day_name,
                        month_name = :month_name,
                        quarter = :quarter;""",
                date_id=date[0],
                year=date[1],
                month=date[2],
                day=date[3],
                day_of_week=date[4],
                day_name=date[5],
                month_name=date[6],
                quarter=date[7],
            )

    except Exception:
        raise

    return get_loaded_data(conn, "dim_date")


def insert_into_dim_counterparty(conn, counterparty_data):
    """
    Insert data into the dim_counterparty table.

    Returns:
        list: A list of rows containing the loaded data after insertion.

    Raises:
        InputValidationError: If the input data does not meet
        the required format for insertion.
    """

    try:
        for counterparty in counterparty_data:
            for index, column in enumerate(counterparty):
                if not isinstance(column, str):
                    raise InputValidationError

            conn.run(
                """INSERT INTO dim_counterparty (
                    counterparty_id,
                    counterparty_legal_name,
                    counterparty_legal_address_line_1,
                    counterparty_legal_address_line2,
                    counterparty_legal_district,
                    counterparty_legal_city,
                    counterparty_legal_postal_code,
                    counterparty_legal_country,
                    counterparty_legal_phone_number
                ) VALUES (
                    :counterparty_id,
                    :counterparty_legal_name,
                    :counterparty_legal_address_line_1,
                    :counterparty_legal_address_line2,
                    :counterparty_legal_district,
                    :counterparty_legal_city,
                    :counterparty_legal_postal_code,
                    :counterparty_legal_country,
                    :counterparty_legal_phone_number
                ) ON CONFLICT (counterparty_id) DO UPDATE
                    SET counterparty_id = :counterparty_id,
                        counterparty_legal_name = :counterparty_legal_name,
                        counterparty_legal_address_line_1 = \
                            :counterparty_legal_address_line_1,
                        counterparty_legal_address_line2 = \
                            :counterparty_legal_address_line2,
                        counterparty_legal_district = \
                            :counterparty_legal_district,
                        counterparty_legal_city = :counterparty_legal_city,
                        counterparty_legal_postal_code = \
                            :counterparty_legal_postal_code,
                        counterparty_legal_country = \
                            :counterparty_legal_country,
                        counterparty_legal_phone_number = \
                            :counterparty_legal_phone_number;""",
                counterparty_id=counterparty[0],
                counterparty_legal_name=counterparty[1],
                counterparty_legal_address_line_1=counterparty[2],
                counterparty_legal_address_line2=counterparty[3],
                counterparty_legal_district=counterparty[4],
                counterparty_legal_city=counterparty[5],
                counterparty_legal_postal_code=counterparty[6],
                counterparty_legal_country=counterparty[7],
                counterparty_legal_phone_number=counterparty[8],
            )

    except InputValidationError:
        raise

    except Exception:
        raise

    return counterparty_data


def insert_into_dim_fact_sales_order(conn, fact_sales_order_data):
    """
    Insert data into the dim_fact_sales_order table.

    Returns:
        list: A list of rows containing the loaded data after insertion.

    Raises:
        InputValidationError: If the input data does not meet
        the required format for insertion.
    """

    try:
        for sale in fact_sales_order_data:
            for index, value in enumerate(sale):
                if not isinstance(value, str):
                    raise InputValidationError

            try:
                datetime.strptime(sale[1], "%Y-%m-%d")

                # Remove milliseconds from the time strings before parsing
                datetime.strptime(sale[2].split(".")[0], "%H:%M:%S")

                datetime.strptime(sale[3], "%Y-%m-%d")

                # Remove milliseconds from the time strings before parsing
                datetime.strptime(sale[4].split(".")[0], "%H:%M:%S")

            except ValueError:
                raise InputValidationError

            conn.run(
                """INSERT INTO fact_sales_order(
                    sales_order_id, created_date,
                    created_time,
                    last_updated_date,
                    last_updated_time,
                    sales_staff_id,
                    counterparty_id,
                    units_sold,
                    unit_price,
                    currency_id,
                    design_id,
                    agreed_payment_date,
                    agreed_delivery_date,
                    agreed_delivery_location_id
                ) VALUES (
                    :sales_order_id,
                    :created_date,
                    :created_time,
                    :last_updated_date,
                    :last_updated_time,
                    :sales_staff_id,
                    :counterparty_id,
                    :units_sold,
                    :unit_price,
                    :currency_id,
                    :design_id,
                    :agreed_payment_date,
                    :agreed_delivery_date,
                    :agreed_delivery_location_id
                );""",
                sales_order_id=sale[0],
                created_date=sale[1],
                created_time=sale[2],
                last_updated_date=sale[3],
                last_updated_time=sale[4],
                sales_staff_id=sale[5],
                counterparty_id=sale[6],
                units_sold=sale[7],
                unit_price=sale[8],
                currency_id=sale[9],
                design_id=sale[10],
                agreed_payment_date=sale[11],
                agreed_delivery_date=sale[12],
                agreed_delivery_location_id=sale[13],
            )

    except InputValidationError:
        raise

    except Exception:
        raise

    return get_loaded_data(conn, "fact_sales_order")
