from datetime import datetime
import boto3


def loading_write_timestamp(data, table_name):
    s3_client = boto3.client("s3")

    last_loaded = (
        s3_client.get_object(
            Bucket="kp-northcoders-processed-bucket",
            Key=f"{table_name}/last_loaded.txt",
        )["Body"]
        .read()
        .decode("utf-8")
    )

    for row in data:
        for column in row:
            try:
                if datetime.strptime(
                    column, "%Y-%m-%d %H:%M:%S.%f"
                ) > datetime.strptime(last_loaded, "%Y-%m-%d %H:%M:%S.%f"):
                    last_loaded = column
            except Exception:
                pass

    with open("/tmp//last_loaded.txt", "w") as f:
        f.write(last_loaded)

    s3_client.upload_file(
        "/tmp//last_loaded.txt",
        "kp-northcoders-processed-bucket",
        f"{table_name}/last_loaded.txt",
    )
