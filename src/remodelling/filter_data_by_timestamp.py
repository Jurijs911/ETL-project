from datetime import datetime
import boto3


def filter_data(data, table_name):
    s3_client = boto3.client("s3")

    timestamp = (
        s3_client.get_object(
            Bucket="kp-northcoders-ingestion-bucket",
            Key=f"{table_name}/created_at.txt",
        )["Body"]
        .read()
        .decode("utf-8")
    )

    filtered_data = []

    for row in data:
        delete = False
        for field in row:
            if isinstance(field, datetime) and field < datetime.strptime(
                timestamp, "%Y-%m-%d-%H:%M:%S:%f"
            ):
                delete = True
        if delete is True:
            continue
        else:
            filtered_data.append(row)

    return filtered_data
