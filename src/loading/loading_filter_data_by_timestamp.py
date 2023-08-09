from datetime import datetime
import boto3


def filter_data(data, table_name):
    s3_client = boto3.client("s3")
    timestamp = (
        s3_client.get_object(
            Bucket="kp-northcoders-processed-bucket",
            Key=f"{table_name}/last_loaded.txt",
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
                ) < datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f"):
                    delete = True
            except ValueError:
                raise Exception("ValueError")
        if delete is True:
            continue
        else:
            filtered_data.append(row)

    return filtered_data
