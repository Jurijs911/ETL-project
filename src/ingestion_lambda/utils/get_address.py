import os
import pg8000
import pg8000.native
import datetime
from dotenv import load_dotenv
load_dotenv()




def get_address():
    """
    This function creates a connection to the database and fetches recently created and updated address records.
    """
    try:
        db_user = os.environ.get("DB_SOURCE_USER")
        db_database = os.environ.get("DB_SOURCE_NAME")
        db_host = os.environ.get("DB_SOURCE_HOST")
        db_port = os.environ.get("DB_SOURCE_PORT")
        db_password = os.environ.get("DB_SOURCE_PASSWORD")
        conn = pg8000.native.Connection(
            user=db_user, database=db_database, host=db_host, port=db_port, password=db_password
        )
        


        """
        this determines the search interval (5 mins)
        """
        query_time = datetime.datetime.now() - datetime.timedelta(minutes = 5)


        """
        this section queries the database to return only rows created in the last 5 minutes
        """
        query_created = "SELECT * FROM address WHERE created_at > query_time"
        rows_created = conn.run(query_created, (query_time,))
        created_data = []
        for row in rows_created:
            item = {
                "location_id": row[0],
                "address_line_1": row[1],
                "address_line_2": row[2],
                "district": row[3],
                "city": row[4],
                "postal_code": row[5],
                "country": row[6],
                "phone": row[7]
            }
            created_data.append(item)

        """
        this section returns rows which were updated in the last 5 minutes
        """

        query_updated = "SELECT * FROM address WHERE last_updated > query_time"
        rows_updated = conn.run(query_updated, (query_time,))
        updated_data = []
        for row in rows_updated:
            item = {
                "location_id": row[0],
                "address_line_1": row[1],
                "address_line_2": row[2],
                "district": row[3],
                "city": row[4],
                "postal_code": row[5],
                "country": row[6],
                "phone": row[7]
            }
            updated_data.append(item)

        return created_data, updated_data
    
    except pg8000.DatabaseError as db_error:
        print("Something went wrong:", db_error)
        raise ConnectionError
    finally:
        conn.close()
    
        """
         remove any duplicate rows from the updated list 
        """

        #iterate over updated list if row in updated list is in created list, delete from from updated list




   
"""
parse created list to add csv function 
"""



"""
parse updated list to csv function
"""

print(get_address())
