from datetime import datetime
import boto3


def filter_data(data, table_name):
    """
    Filters data based on the last processed timestamp for the given table.

    This function retrieves the last processed timestamp for the specified
    table from an S3 bucket, and then filters the input data based on this
    timestamp. Rows with timestamps earlier than the last processed timestamp
    will be excluded from the filtered data.

    Args:
        data (List[List[str]]):
        The input data as a list of rows, where each row is a list of strings.

        table_name (str):
        The name of the table for which data is being filtered.

    Returns:
        List[List[str]]:
        The filtered data as a list of rows, each row is a list of strings.

    Raises:
        ValueError:
        If the timestamp retrieved from S3 or the timestamps in the input data
        are not in the expected format.
    """

    s3_client = boto3.client("s3")
    timestamp = (
        s3_client.get_object(
            Bucket="kp-northcoders-ingestion-bucket",
            Key=f"{table_name}/last_processed.txt",
        )["Body"]
        .read()
        .decode("utf-8")
    )

    filtered_data = []

    for row in data:
        delete = False
        for field in row:
            try:
                if datetime.strptime(
                    field, "%Y-%m-%d %H:%M:%S.%f"
                ) <= datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f"):
                    delete = True
            except ValueError:
                pass
        if delete is True:
            continue
        else:
            filtered_data.append(row)

    return filtered_data
