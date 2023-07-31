from src.remodelling.read_ingestion_csv import read_ingestion_csv
from src.remodelling.upload_csv import upload_csv
import os
import boto3
from moto import mock_s3


@mock_s3
class Test_read_ingestion_csv:
    def test_reads_correct_data_for_one_table(self):
        s3_client = boto3.client("s3", region_name="eu-west-2")
        s3_client.create_bucket(
            Bucket="ingested-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        currency_data = [
            {
                "currency_id": "1",
                "currency_code": "GBP",
                "created_at": "2023-06-12",
                "last_updated": "2023-06-12",
            },
            {
                "currency_id": "2",
                "currency_code": "USD",
                "created_at": "2022-12-12",
                "last_updated": "2022-12-12",
            },
        ]

        s3_client.put_object(Bucket="ingested-bucket", Key="currency.csv")

        upload_csv(currency_data, "currency", "ingested-bucket")

        expected = {
            "sales_order": [],
            "design": [],
            "currency": [
                ["1", "GBP", "2023-06-12", "2023-06-12"],
                ["2", "USD", "2022-12-12", "2022-12-12"],
            ],
            "staff": [],
            "counterparty": [],
            "address": [],
            "department": [],
        }

        result = read_ingestion_csv("ingested-bucket")

        assert result == expected

        if os.path.exists("currency.csv"):
            os.remove("currency.csv")

    def test_reads_correct_data_for_mutliple_tables(self):
        s3_client = boto3.client("s3", region_name="eu-west-2")
        s3_client.create_bucket(
            Bucket="ingested-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        staff_data = [
            {
                "staff_id": "1",
                "first_name": "Cameron",
                "last_name": "Parsonage",
                "department_id": "3",
                "email_address": "email@address.com",
                "created_at": "2023-06-12",
                "last_updated": "2023-09-12",
            }
        ]
        currency_data = [
            {
                "currency_id": "1",
                "currency_code": "GBP",
                "created_at": "2023-06-12",
                "last_updated": "2023-06-12",
            },
            {
                "currency_id": "2",
                "currency_code": "USD",
                "created_at": "2022-12-12",
                "last_updated": "2022-12-12",
            },
        ]

        s3_client.put_object(Bucket="ingested-bucket", Key="currency.csv")
        s3_client.put_object(Bucket="ingested-bucket", Key="staff.csv")

        upload_csv(currency_data, "currency", "ingested-bucket")
        upload_csv(staff_data, "staff", "ingested-bucket")

        expected = {
            "sales_order": [],
            "design": [],
            "currency": [
                ["1", "GBP", "2023-06-12", "2023-06-12"],
                ["2", "USD", "2022-12-12", "2022-12-12"],
            ],
            "staff": [
                [
                    "1",
                    "Cameron",
                    "Parsonage",
                    "3",
                    "email@address.com",
                    "2023-06-12",
                    "2023-09-12",
                ]
            ],
            "counterparty": [],
            "address": [],
            "department": [],
        }

        result = read_ingestion_csv("ingested-bucket")

        assert result == expected

        if os.path.exists("currency.csv"):
            os.remove("currency.csv")

        if os.path.exists("staff.csv"):
            os.remove("staff.csv")
