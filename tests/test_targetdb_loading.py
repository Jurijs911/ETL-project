import pg8000.native
import os
from src.loading.loading_utils import ( 
    insert_into_dim_design,
    insert_into_dim_currency,
    insert_into_dim_staff,
    insert_into_dim_location,
    insert_into_dim_date,
    insert_into_dim_counterparty,
    insert_into_dim_fact_sales_order
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


def cleanup_test_data(conn, table_name):
    # Cleanup tables after test so it returns back to empty original state to verify data has been loaded
    tables_to_cleanup = ["dim_design", "dim_currency", "dim_staff", "dim_date", "dim_counterparty", "dim_location", "fact_sales_order"]

    if table_name in tables_to_cleanup:
        conn.run(f"DELETE FROM {table_name};")
        print(f"Data deleted from {table_name} table.")
    else:
        print(f"Table '{table_name}' not found in the list of tables to cleanup.")


def test_insert_into_dim_design():
    """
    Test the insert_into_dim_design function.
    The function should insert data into the dim_design table and return the inserted data.
    """

    test_design_data = [
        [1, "Design1", "File1", "File1.jpg"],
        [2, "Design2", "File2", "File2.jpg"],
    ]

    conn = create_test_connection()
    conn.run('SET search_path TO "project_team_2", public;')
    insert_into_dim_design(conn, test_design_data)

    table_contents = conn.run('SELECT * FROM "dim_design";')
  
    assert table_contents == test_design_data
    cleanup_test_data(conn)

def test_insert_into_dim_currency():
    """
    Test the insert_into_dim_currency function.
    The function should insert data into the dim_currency table and return the inserted data.
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
    The function should insert data into the dim_staff table and return the inserted data.
    """
    test_staff_data = [
        [1, "Zenab", "Haider", "Sales", "Manchester", "zenab@email.com"],
        [2, "Lisa", "Sco", "Coding", "Birmingham", "lisa.sco@email.com"],
    ]

    conn = create_test_connection()
    conn.run('SET search_path TO "project_team_2", public;')
    insert_into_dim_staff(conn, test_staff_data)

    table_contents = conn.run('SELECT * FROM "dim_staff";')

    assert table_contents == test_staff_data

def test_insert_into_dim_date():
    """
    Test the insert_into_dim_date function.
    The function should insert data into the dim_date table and return the inserted data.
    """
    test_date_data = [
        ["2023-07-01", 2023, 7, 1, 5, "Friday", "July", 3],
        ["2023-08-15", 2023, 8, 15, 1, "Monday", "August", 3],
    ]

    conn = create_test_connection()
    conn.run('SET search_path TO "project_team_2", public;')
    insert_into_dim_date(conn, test_date_data)

    table_contents = conn.run('SELECT * FROM "dim_date";')

    assert table_contents == test_date_data

def test_insert_into_dim_counterparty():
    """
    Test the insert_into_dim_counterparty function.
    The function should insert data into the dim_counterparty table and return the inserted data.
    """
    test_counterparty_data = [
        [1, "Business Name", "123 Apple St", None, "District 1", "Manchester", "12345", "UK", "123-456-7890"],
        [2, "ABC Ltd", "456 Balamory St", "Suite 1", "District 2", "London", "56789", "UK", "987-654-3210"],
    ]

    conn = create_test_connection()
    conn.run('SET search_path TO "project_team_2", public;')
    insert_into_dim_counterparty(conn, test_counterparty_data)

    table_contents = conn.run('SELECT * FROM "dim_counterparty";')

    assert table_contents == test_counterparty_data

def test_insert_into_fact_sales_order():
    """
    Test the insert_into_dim_fact_sales_order function.

    The function should insert data into the dim_fact_sales_order table and return the inserted data.
    """
    test_fact_sales_order_data = [
    [1, 1001, "2023-07-24", "12:34:56.789", "2023-07-24", "15:45:30.123", 101, 201, 10, 100.0, 1, 1, "2023-07-30", "2023-08-05", 301],
    [2, 1002, "2023-07-24", "09:12:45.678", "2023-07-24", "09:12:45.678", 102, 202, 5, 50.0, 2, 2, "2023-07-28", "2023-08-02", 302],
    ]

    conn = create_test_connection()
    conn.run('SET search_path TO "project_team_2", public;')
    insert_into_dim_fact_sales_order(conn, test_fact_sales_order_data)

    table_contents = conn.run('SELECT * FROM "fact_sales_order";')

    assert table_contents == test_fact_sales_order_data

