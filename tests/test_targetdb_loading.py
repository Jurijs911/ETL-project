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
    test_target_user=os.environ.get("TEST_TARGET_USER")
    test_target_database=os.environ.get("TEST_TARGET_DATABASE")
    test_target_host=os.environ.get("TEST_TARGET_HOST")
    test_target_port=os.environ.get("TEST_TARGET_PORT")
    test_target_password=os.environ.get("TEST_TARGET_PASSWORD")

    conn = pg8000.native.Connection(
        user=test_target_user,
        database=test_target_database,
        host=test_target_host,
        port=test_target_port,
        password=test_target_password,
    )
    return conn


def cleanup_test_data(conn):
    # Cleanup tables after test so it returns back to empty original state to verify data has been loaded
    tables_to_cleanup = ["dim_design", "dim_currency", "dim_staff", "dim_date", "dim_counterparty", "dim_location", "fact_sales_order"]

    for table in tables_to_cleanup:
        conn.run("DELETE FROM :table_name;", table_name=table)


def test_insert_into_dim_design():
    test_design_data = [
        [1, "Design1", "File1", "File1.jpg"],
        [2, "Design2", "File2", "File2.jpg"],
    ]

    conn = create_test_connection()

    insert_into_dim_design(conn, test_design_data)
    table_contents = conn.run("SELECT * FROM dim_design;")

    assert table_contents == test_design_data

    cleanup_test_data(conn, table_name="test_dim_design")
    conn.close()


