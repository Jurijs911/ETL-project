import os
import pg8000.native

"""
Receives data from csv_reader and manipulates it to match the final database schema

format_fact_sales_order
format_dim_staff
format_dim_location
format_dim_design
format_dim_date
format_dim_currency
format_dim_counterparty
"""

def format_dim_design():
    db_user = os.environ.get("DB_SOURCE_USER")
    db_database = os.environ.get("DB_SOURCE_NAME")
    db_host = os.environ.get("DB_SOURCE_HOST")
    db_port = os.environ.get("DB_SOURCE_PORT")
    db_password = os.environ.get("DB_SOURCE_PASSWORD")
    conn = pg8000.native.Connection(
        user=db_user, database=db_database, host=db_host, port=db_port, password=db_password
    )
  
    query = "SELECT * FROM dim_design"
    rows = conn.run(query)
    formatted_data = []
    for row in rows:
        item = {
            "design_id": row[0],
            "design_name": row[1],
            "file_location": row[2],
            "file_name": row[3]
        }
        formatted_data.append(item)
    
    return formatted_data

print(format_dim_design())

