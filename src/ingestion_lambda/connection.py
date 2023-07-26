import os
import pg8000.native
import boto3
import pg8000
from dotenv import load_dotenv
load_dotenv()

DB_SOURCE_USER = os.getenv('DB_SOURCE_USER')
DB_SOURCE_HOST = os.getenv('DB_SOURCE_HOST')
DB_SOURCE_NAME = os.getenv('DB_SOURCE_NAME')
DB_SOURCE_PORT = os.getenv('DB_SOURCE_PORT')
DB_SOURCE_PASSWORD = os.getenv('DB_SOURCE_PASSWORD')

con = pg8000.native.Connection(user=DB_SOURCE_USER, host=DB_SOURCE_HOST,
                               database=DB_SOURCE_NAME, port=DB_SOURCE_PORT, password=DB_SOURCE_PASSWORD)


print(con.run("SELECT * FROM address;"))


# def db_reader():
#     conn = pg8000.connect()

#     cursor = conn.cursor()
#     cursor.execute("SELECT * FROM address;")
#     rows = cursor.fetchall()
#     print(rows)
#     conn.close()


# db_reader()
