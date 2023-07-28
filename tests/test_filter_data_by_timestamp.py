from src.remodelling.filter_data_by_timestamp import filter_data
from datetime import datetime
import boto3
from moto import mock_s3


@mock_s3
class Test_Remodelling_Filter:
    def test_filters_data(self):
        s3_client = boto3.client("s3", region_name="eu-west-2")

        s3_client.create_bucket(
            Bucket="kp-northcoders-ingestion-bucket",
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

        sample_data = [
            [
                "2",
                datetime(2023, 7, 25, 15, 20, 49, 962000),
                datetime(2023, 7, 25, 15, 20, 49, 962000),
                "100",
                "200",
                "2000",
                "5",
                "20.65",
                "1",
                "2023, 7, 30",
                "2023, 8, 12",
                "2",
            ],
            [
                "2",
                datetime(1999, 7, 25, 15, 20, 49, 962000),
                datetime(1999, 7, 25, 15, 20, 49, 962000),
                "100",
                "200",
                "2000",
                "5",
                "20.65",
                "1",
                "2023, 7, 30",
                "2023, 8, 12",
                "2",
            ],
        ]

        expected = [
            [
                "2",
                datetime(2023, 7, 25, 15, 20, 49, 962000),
                datetime(2023, 7, 25, 15, 20, 49, 962000),
                "100",
                "200",
                "2000",
                "5",
                "20.65",
                "1",
                "2023, 7, 30",
                "2023, 8, 12",
                "2",
            ],
        ]

        result = filter_data(sample_data, "sales_order")

        assert result == expected

    def test_does_not_filter_if_unnecessary(self):
        s3_client = boto3.client("s3", region_name="eu-west-2")

        s3_client.create_bucket(
            Bucket="kp-northcoders-ingestion-bucket",
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

        sample_data = [
            [
                "2",
                datetime(2023, 7, 25, 15, 20, 49, 962000),
                datetime(2023, 7, 25, 15, 20, 49, 962000),
                "100",
                "200",
                "2000",
                "5",
                "20.65",
                "1",
                "2023, 7, 30",
                "2023, 8, 12",
                "2",
            ],
        ]

        expected = [
            [
                "2",
                datetime(2023, 7, 25, 15, 20, 49, 962000),
                datetime(2023, 7, 25, 15, 20, 49, 962000),
                "100",
                "200",
                "2000",
                "5",
                "20.65",
                "1",
                "2023, 7, 30",
                "2023, 8, 12",
                "2",
            ],
        ]

        result = filter_data(sample_data, "sales_order")

        assert result == expected

    def test_filters_all_data_if_no_new_data(self):
        s3_client = boto3.client("s3", region_name="eu-west-2")

        s3_client.create_bucket(
            Bucket="kp-northcoders-ingestion-bucket",
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

        sample_data = [
            [
                "2",
                datetime(1999, 7, 25, 15, 20, 49, 962000),
                datetime(1999, 7, 25, 15, 20, 49, 962000),
                "100",
                "200",
                "2000",
                "5",
                "20.65",
                "1",
                "2023, 7, 30",
                "2023, 8, 12",
                "2",
            ],
        ]

        expected = []

        result = filter_data(sample_data, "sales_order")

        assert result == expected
