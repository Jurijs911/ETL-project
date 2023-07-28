from src.remodelling.copy_timestamps import copy_timestamps
from datetime import datetime
import boto3
from moto import mock_s3


@mock_s3
class Test_Copy_Timestamps:
    def test_copies_timestamp_into_second_bucket(self):
        s3_client = boto3.client("s3", region_name="eu-west-2")

        s3_client.create_bucket(
            Bucket="kp-northcoders-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        s3_client.create_bucket(
            Bucket="kp-northcoders-processed-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        s3_client.put_object(
            Bucket="kp-northcoders-ingestion-bucket",
            Key="sales_order/created_at.txt",
        )

        most_recent = datetime(2020, 7, 25, 15, 20, 49, 962000)

        with open("sales_order.txt", "w") as f:
            f.write(most_recent.strftime("%Y-%m-%d-%H:%M:%S:%f"))

        s3_client.upload_file(
            "sales_order.txt",
            "kp-northcoders-ingestion-bucket",
            "sales_order/created_at.txt",
        )

        copy_timestamps()

        response = (
            s3_client.get_object(
                Bucket="kp-northcoders-processed-bucket",
                Key="sales_order/created_at.txt",
            )["Body"]
            .read()
            .decode("utf-8")
        )

        assert "2020-07-25-15:20:49:962000" in response
