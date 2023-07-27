from src.ingestion_lambda.utils.get_address_add import get_address_add
from src.upload_csv import upload_csv
from src.ingestion_lambda.utils.find_most_recent_time import find_most_recent_time
from src.ingestion_lambda.utils.write_updated_time import write_updated_time


address_data = get_address_add()
print(address_data)
if len(address_data) > 0:

    updated_timestamp = find_most_recent_time(address_data)

    upload_csv(address_data, 'address', "kp-northcoder-ingestion-bucket")

    write_updated_time(updated_timestamp, 'address')


# # Set the configuration parameters
# db_config = {}  # Database configuration parameters

# bucket_name = 'my-bucket'  # Name of the S3 bucket
# data_key = 'my-data.csv'  # Key of the CSV file in the S3 bucket
# timestamp_key = 'created_at.txt'  # Key of the timestamp file in the S3 bucket

# # Get the last timestamp from S3
# last_timestamp = get_last_time(bucket_name, timestamp_key)

# # Get new data from the database table
# data = get_data_from_table()

# # Write the new data to the CSV file in S3
# write_data_to_s3(bucket_name, data_key, data)
