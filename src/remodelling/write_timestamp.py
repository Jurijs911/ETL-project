from datetime import datetime
import boto3
import os


def write_timestamp(data, table_name):
    """
    This function takes a list of lists containing timestamp data and a table
    name as input. It identifies the most recent timestamp from the provided
    data and records it in a text file. The text file is then uploaded to the
    S3 bucket, storing the last processed timestamp for that table. The
    timestamps in the 'data' list are compared using the '%Y-%m-%d %H:%M:%S.%f'
    format. Invalid timestamps are ignored.

    Parameters:
        data (list of lists):
        A list containing rows of timestamp data for a specific table.

        table_name (str):
        The name of the table to which the data belongs.
    """

    s3_client = boto3.client("s3")

    last_processed = "1900-1-25 15:20:49.962000"

    for row in data:
        for column in row:
            try:
                if datetime.strptime(
                    column, "%Y-%m-%d %H:%M:%S.%f"
                ) > datetime.strptime(last_processed, "%Y-%m-%d %H:%M:%S.%f"):
                    last_processed = column
            except Exception:
                pass

    with open("last_processed.txt", "w") as f:
        f.write(last_processed)

    s3_client.upload_file(
        "last_processed.txt",
        "kp-northcoders-ingestion-bucket",
        f"{table_name}/last_processed.txt",
    )

    os.remove("last_processed.txt")
