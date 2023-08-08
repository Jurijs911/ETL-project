import pg8000.native
import os
import datetime
from src.loading.loading_utils import (
    insert_into_dim_design,
    insert_into_dim_currency,
    insert_into_dim_staff,
    insert_into_dim_location,
    insert_into_dim_date,
    insert_into_dim_counterparty,
    insert_into_dim_fact_sales_order,
)
from dotenv import load_dotenv

load_dotenv()


def create_test_connection():
    test_target_user = os.environ.get("TEST_TARGET_USER")
    test_target_database = os.environ.get("TEST_TARGET_DATABASE")
    test_target_host = os.environ.get("TEST_TARGET_HOST")
    test_target_port = os.environ.get("TEST_TARGET_PORT")
    test_target_password = os.environ.get("TEST_TARGET_PASSWORD")

    conn = pg8000.native.Connection(
        user=test_target_user,
        database=test_target_database,
        host=test_target_host,
        port=test_target_port,
        password=test_target_password,
    )
    return conn


def reset_database(conn):
    # Drop and rebuild the test loading database
    query = """
    DROP SCHEMA project_team_2 CASCADE;

    CREATE SCHEMA project_team_2 AUTHORIZATION project_team_2;

    -- DROP SEQUENCE project_team_2.fact_sales_order_sales_record_id_seq;

    CREATE SEQUENCE project_team_2.fact_sales_order_sales_record_id_seq
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 2147483647
        START 1
        CACHE 1
        NO CYCLE;-- project_team_2.dim_counterparty definition

    -- Drop table

    -- DROP TABLE project_team_2.dim_counterparty;

    CREATE TABLE project_team_2.dim_counterparty (
        counterparty_id int4 NOT NULL,
        counterparty_legal_name varchar NOT NULL,
        counterparty_legal_address_line_1 varchar NOT NULL,
        counterparty_legal_address_line2 varchar NULL,
        counterparty_legal_district varchar NULL,
        counterparty_legal_city varchar NOT NULL,
        counterparty_legal_postal_code varchar NOT NULL,
        counterparty_legal_country varchar NOT NULL,
        counterparty_legal_phone_number varchar NOT NULL,
        CONSTRAINT dim_counterparty_pkey PRIMARY KEY (counterparty_id)
    );


    -- project_team_2.dim_currency definition

    -- Drop table

    -- DROP TABLE project_team_2.dim_currency;

    CREATE TABLE project_team_2.dim_currency (
        currency_id int4 NOT NULL,
        currency_code varchar NOT NULL,
        currency_name varchar NOT NULL,
        CONSTRAINT dim_currency_pkey PRIMARY KEY (currency_id)
    );


    -- project_team_2.dim_date definition

    -- Drop table

    -- DROP TABLE project_team_2.dim_date;

    CREATE TABLE project_team_2.dim_date (
        date_id date NOT NULL,
        "year" int4 NOT NULL,
        "month" int4 NOT NULL,
        "day" int4 NOT NULL,
        day_of_week int4 NOT NULL,
        day_name varchar NOT NULL,
        month_name varchar NOT NULL,
        quarter int4 NOT NULL,
        CONSTRAINT dim_date_pkey PRIMARY KEY (date_id)
    );


    -- project_team_2.dim_design definition

    -- Drop table

    -- DROP TABLE project_team_2.dim_design;

    CREATE TABLE project_team_2.dim_design (
        design_id int4 NOT NULL,
        design_name varchar NOT NULL,
        file_location varchar NOT NULL,
        file_name varchar NOT NULL,
        CONSTRAINT dim_design_pkey PRIMARY KEY (design_id)
    );


    -- project_team_2.dim_location definition

    -- Drop table

    -- DROP TABLE project_team_2.dim_location;

    CREATE TABLE project_team_2.dim_location (
        location_id int4 NOT NULL,
        address_line_1 varchar NOT NULL,
        address_line_2 varchar NULL,
        district varchar NULL,
        city varchar NOT NULL,
        postal_code varchar NOT NULL,
        country varchar NOT NULL,
        phone varchar NOT NULL,
        CONSTRAINT dim_location_pkey PRIMARY KEY (location_id)
    );


    -- project_team_2.dim_staff definition

    -- Drop table

    -- DROP TABLE project_team_2.dim_staff;

    CREATE TABLE project_team_2.dim_staff (
        staff_id int4 NOT NULL,
        first_name varchar NOT NULL,
        last_name varchar NOT NULL,
        department_name varchar NOT NULL,
        "location" varchar NOT NULL,
        email_address varchar NOT NULL,
        CONSTRAINT dim_staff_pkey PRIMARY KEY (staff_id)
    );


    -- project_team_2.fact_sales_order definition

    -- Drop table

    -- DROP TABLE project_team_2.fact_sales_order;

    CREATE TABLE project_team_2.fact_sales_order (
        sales_record_id serial4 NOT NULL,
        sales_order_id int4 NOT NULL,
        created_date date NOT NULL,
        created_time time NOT NULL,
        last_updated_date date NOT NULL,
        last_updated_time time NOT NULL,
        sales_staff_id int4 NOT NULL,
        counterparty_id int4 NOT NULL,
        units_sold int4 NOT NULL,
        unit_price numeric NOT NULL,
        currency_id int4 NOT NULL,
        design_id int4 NOT NULL,
        agreed_payment_date date NOT NULL,
        agreed_delivery_date date NOT NULL,
        agreed_delivery_location_id int4 NOT NULL,
        CONSTRAINT fact_sales_order_pkey PRIMARY KEY (sales_record_id),
        CONSTRAINT fact_sales_order_agreed_delivery_date_fkey
        FOREIGN KEY (agreed_delivery_date)
        REFERENCES project_team_2.dim_date(date_id),
        CONSTRAINT fact_sales_order_agreed_delivery_location_id_fkey
        FOREIGN KEY (agreed_delivery_location_id)
        REFERENCES project_team_2.dim_location(location_id),
        CONSTRAINT fact_sales_order_agreed_payment_date_fkey
        FOREIGN KEY (agreed_payment_date)
        REFERENCES project_team_2.dim_date(date_id),
        CONSTRAINT fact_sales_order_counterparty_id_fkey
        FOREIGN KEY (counterparty_id)
        REFERENCES project_team_2.dim_counterparty(counterparty_id),
        CONSTRAINT fact_sales_order_created_date_fkey
        FOREIGN KEY (created_date)
        REFERENCES project_team_2.dim_date(date_id),
        CONSTRAINT fact_sales_order_currency_id_fkey
        FOREIGN KEY (currency_id)
        REFERENCES project_team_2.dim_currency(currency_id),
        CONSTRAINT fact_sales_order_design_id_fkey
        FOREIGN KEY (design_id)
        REFERENCES project_team_2.dim_design(design_id),
        CONSTRAINT fact_sales_order_last_updated_date_fkey
        FOREIGN KEY (last_updated_date)
        REFERENCES project_team_2.dim_date(date_id),
        CONSTRAINT fact_sales_order_sales_staff_id_fkey
        FOREIGN KEY (sales_staff_id)
        REFERENCES project_team_2.dim_staff(staff_id)
    );
    """
    conn.run(query)


def cleanup_test_data(conn, table_name):
    """
    Cleanup tables after test so it returns back to empty original state to
    verify data has been loaded.
    """
    tables_to_cleanup = [
        "dim_design",
        "dim_currency",
        "dim_staff",
        "dim_date",
        "dim_counterparty",
        "dim_location",
        "fact_sales_order",
    ]

    if table_name in tables_to_cleanup:
        conn.run(f"DELETE FROM {table_name};")
        print(f"Data deleted from {table_name} table.")
    else:
        print(
            f"Table '{table_name}' not found in the "
            "list of tables to cleanup."
        )


def test_insert_into_dim_design():
    """
    Test the insert_into_dim_design function.
    The function should insert data into the dim_design table
    and return the inserted data.
    """

    test_design_data = [
        [1, "Design1", "File1", "File1.jpg"],
        [2, "Design2", "File2", "File2.jpg"],
    ]

    conn = create_test_connection()
    reset_database(conn)
    conn.run('SET search_path TO "project_team_2", public;')
    insert_into_dim_design(conn, test_design_data)

    table_contents = conn.run('SELECT * FROM "dim_design";')

    assert table_contents == test_design_data


def test_insert_into_dim_currency():
    """
    Test the insert_into_dim_currency function.
    The function should insert data into the dim_currency table
    and return the inserted data.
    """
    test_currency_data = [
        [1, "USD", "US Dollar"],
        [2, "GBP", "GB Pound"],
    ]

    conn = create_test_connection()
    conn.run('SET search_path TO "project_team_2", public;')
    insert_into_dim_currency(conn, test_currency_data)

    table_contents = conn.run('SELECT * FROM "dim_currency";')

    assert table_contents == test_currency_data


def test_insert_into_dim_staff():
    """
    Test the insert_into_dim_staff function.
    The function should insert data into the dim_staff table and
    return the inserted data.
    """
    test_staff_data = [
        [101, "Zenab", "Haider", "Sales", "Manchester", "zenab@email.com"],
        [2, "Lisa", "Sco", "Coding", "Birmingham", "lisa.sco@email.com"],
        [102, "Cameron", "P", "Coding", "London", "cameron@example.com"],
    ]

    conn = create_test_connection()
    conn.run('SET search_path TO "project_team_2", public;')
    inserted_data = insert_into_dim_staff(conn, test_staff_data)

    table_contents = conn.run('SELECT * FROM "dim_staff";')

    assert inserted_data == test_staff_data
    assert table_contents == test_staff_data


def test_insert_into_dim_date():
    """
    Test the insert_into_dim_date function.
    The function should insert data into the dim_date table
    and return the inserted data.
    """
    test_date_data = [
        [datetime.date(2023, 7, 1), 2023, 7, 1, 5, "Friday", "July", 3],
        [datetime.date(2023, 8, 5), 2023, 8, 5, 1, "Monday", "August", 3],
        [datetime.date(2023, 7, 24), 2023, 7, 24, 1, "Monday", "July", 3],
        [datetime.date(2023, 7, 30), 2023, 7, 30, 1, "Sunday", "July", 3],
        [datetime.date(2023, 8, 15), 2023, 8, 15, 1, "Monday", "August", 3],
        [datetime.date(2023, 7, 28), 2023, 7, 28, 4, "Monday", "July", 3],
        [datetime.date(2023, 8, 2), 2023, 8, 2, 3, "Tuesday", "August", 3],
    ]

    conn = create_test_connection()
    conn.run('SET search_path TO "project_team_2", public;')
    inserted_data = insert_into_dim_date(conn, test_date_data)

    table_contents = conn.run('SELECT * FROM "dim_date";')

    assert inserted_data == test_date_data
    assert table_contents == test_date_data


def test_insert_into_dim_counterparty():
    """
    Test the insert_into_dim_counterparty function.
    The function should insert data into the dim_counterparty table
    and return the inserted data.
    """

    test_counterparty_data = [
        [
            201,
            "Business Name",
            "123 Apple St",
            "",
            "District 1",
            "Manchester",
            "12345",
            "UK",
            "123-456-7890",
        ],
        [
            202,
            "Business Name",
            "123 Apple St",
            "",
            "District 1",
            "Manchester",
            "12345",
            "UK",
            "123-456-7890",
        ],
    ]

    conn = create_test_connection()
    conn.run('SET search_path TO "project_team_2", public;')
    inserted_data = insert_into_dim_counterparty(conn, test_counterparty_data)

    table_contents = conn.run('SELECT * FROM "dim_counterparty";')

    assert inserted_data == test_counterparty_data
    assert table_contents == test_counterparty_data


def test_insert_into_dim_location():
    """
    Test the insert_into_dim_location function.
    The function should insert data into the dim_location table
    and return the inserted data.
    """
    test_location_data = [
        [
            301,
            "Location Name",
            "Location Address",
            "Location Address 2",
            "City",
            "State",
            "Country",
            "Postal Code",
        ],
        [
            302,
            "Location Name",
            "Location Address",
            "Location Address 2",
            "City",
            "State",
            "Country",
            "Postal Code",
        ],
    ]

    conn = create_test_connection()
    conn.run('SET search_path TO "project_team_2", public;')
    inserted_data = insert_into_dim_location(conn, test_location_data)

    table_contents = conn.run("SELECT * FROM dim_location")

    assert inserted_data == test_location_data
    assert table_contents == test_location_data


def test_insert_into_fact_sales_order():
    """
    Test the insert_into_dim_fact_sales_order function.
    The function should insert data into the fact_sales_order table
    and return the inserted data.
    """
    test_fact_sales_order_data = [
        [
            100,
            "2023-07-01",
            "12:34:56.789000",
            "2023-07-24",
            "15:45:30.123000",
            101,
            201,
            10,
            100.0,
            1,
            1,
            "2023-07-30",
            "2023-08-05",
            301,
        ],
        [
            200,
            "2023-08-15",
            "09:12:45.678000",
            "2023-07-24",
            "09:12:45.678000",
            102,
            202,
            5,
            50.0,
            2,
            2,
            "2023-07-28",
            "2023-08-02",
            302,
        ],
    ]

    expected_fact_sales_order_data = [
        [
            100,
            datetime.date(2023, 7, 1),
            datetime.time(12, 34, 56, 789000),
            datetime.date(2023, 7, 24),
            datetime.time(15, 45, 30, 123000),
            101,
            201,
            10,
            100.0,
            1,
            1,
            datetime.date(2023, 7, 30),
            datetime.date(2023, 8, 5),
            301,
        ],
        [
            200,
            datetime.date(2023, 8, 15),
            datetime.time(9, 12, 45, 678000),
            datetime.date(2023, 7, 24),
            datetime.time(9, 12, 45, 678000),
            102,
            202,
            5,
            50.0,
            2,
            2,
            datetime.date(2023, 7, 28),
            datetime.date(2023, 8, 2),
            302,
        ],
    ]

    conn = create_test_connection()
    conn.run('SET search_path TO "project_team_2", public;')
    insert_into_dim_fact_sales_order(conn, test_fact_sales_order_data)

    table_contents = conn.run('SELECT * FROM "fact_sales_order";')

    assert expected_fact_sales_order_data[0] == table_contents[0][1:]
    assert expected_fact_sales_order_data[1] == table_contents[1][1:]

    reset_database(conn)
