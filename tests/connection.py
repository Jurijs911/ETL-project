import pg8000.native
import os
from dotenv import load_dotenv

load_dotenv()

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

# conn.run(grant usage on schema project_team_2 to public;)
# conn.run(grant create on schema public to public;)

conn.run('SET search_path TO "kp-test-source", public;')
query = "SELECT * FROM information_schema.tables;"
query2 = "SELECT * FROM address;"
rows = conn.run(query2)
