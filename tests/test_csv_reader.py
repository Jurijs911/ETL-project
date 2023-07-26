import csv
from src.remodelling.csv_reader import read_ingested_csv
from src.ingestion_csv_utils import add_csv, update_csv
import boto3
from moto import mock_s3


@mock_s3
class Test_read_ingested_csv:
    def test_reads_correct_data(self):
        s3_client = boto3.client("s3", region_name="eu-west-2")
        s3_client.create_bucket(Bucket="ingested-bucket", CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})

        update_data = [
            {"currency_id": "1", "currency_code": "GBP", "created_at": "2023-06-12", "last_updated": "2023-09-12"},
            {"currency_id": "2", "currency_code": "USD", "created_at": "2022-12-12", "last_updated": "2023-01-12"},
        ]
        add_data = [
            {"currency_id": "1", "currency_code": "GBP", "created_at": "2023-06-12", "last_updated": "2023-06-12"},
            {"currency_id": "2", "currency_code": "USD", "created_at": "2022-12-12", "last_updated": "2022-12-12"},
        ]

        s3_client.put_object(Bucket="ingested-bucket", Key="currency/update.csv")
        s3_client.put_object(Bucket="ingested-bucket", Key="currency/add.csv")

        add_csv(add_data, "currency", "ingested-bucket")
        update_csv(update_data, "currency", "ingested-bucket")

        result = read_ingested_csv()

        assert result is False
