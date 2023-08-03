import pg8000.native
import os
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
    conn.run('SET search_path TO "project_team_2", public;')
    return conn

def test_connection():
    try:
        conn = create_test_connection()
        print("Connection created successfully.")
        conn.close()
    except Exception as e:
        print(f"An error occurred: {str(e)}")

test_connection()
