import csv
from src.csv_utils import add_csv, update_csv
import boto3
from moto import mock_s3


class Test_add_csv:
    def test_creates_csv_file_with_correct_data(self):
        test_data = [
            {"currency_id": "1", "currency_code": "GBP", "created_at": "2023-06-12", "last_updated": "2023-06-12"},
            {"currency_id": "2", "currency_code": "USD", "created_at": "2022-12-12", "last_updated": "2022-12-12"},
        ]

        add_csv(test_data, "currency")

        with open("add.csv", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                assert ["currency_id", "currency_code", "created_at", "last_updated"] == list(row.keys())

    @mock_s3
    def test_uploads_csv_to_object_in_bucket(self):
        conn = boto3.client("s3", region_name="eu-west-2")

        conn.create_bucket(Bucket="test_bucket", CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})

        test_data = [
            {"currency_id": "1", "currency_code": "GBP", "created_at": "2023-06-12", "last_updated": "2023-06-12"},
            {"currency_id": "2", "currency_code": "USD", "created_at": "2022-12-12", "last_updated": "2022-12-12"},
        ]

        conn.put_object(Bucket="test_bucket", Key="design/add.csv")

        add_csv(test_data, "currency", "test_bucket")

        response = (
            conn.get_object(
                Bucket="test_bucket",
                Key="design/add.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )

        assert "1,GBP,2023-06-12,2023-06-12" in response
        assert "2,USD,2022-12-12,2022-12-12" in response

    @mock_s3
    def test_overwites_previous_csv_file(self):
        conn = boto3.client("s3", region_name="eu-west-2")

        conn.create_bucket(Bucket="test_bucket", CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})

        conn.put_object(Bucket="test_bucket", Key="design/add.csv")

        test_data = [
            {"currency_id": "1", "currency_code": "GBP", "created_at": "2023-06-12", "last_updated": "2023-06-12"},
            {"currency_id": "2", "currency_code": "USD", "created_at": "2022-12-12", "last_updated": "2022-12-12"},
        ]

        add_csv(test_data, "currency", "test_bucket")

        replacement_data = [
            {"currency_id": "3", "currency_code": "EUR", "created_at": "2023-08-03", "last_updated": "2023-08-03"},
            {"currency_id": "4", "currency_code": "CAD", "created_at": "2022-05-10", "last_updated": "2022-05-10"},
        ]

        add_csv(replacement_data, "currency", "test_bucket")

        response = (
            conn.get_object(
                Bucket="test_bucket",
                Key="design/add.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )

        assert "1,GBP,2023-06-12,2023-06-12" not in response
        assert "2,USD,2022-12-12,2022-12-12" not in response
        assert "3,EUR,2023-08-03,2023-08-03" in response
        assert "4,CAD,2022-05-10,2022-05-10" in response


class Test_update_csv:
    def test_creates_csv_file_with_correct_data(self):
        test_data = [
            {"currency_id": "1", "currency_code": "GBP", "created_at": "2023-06-12", "last_updated": "2023-09-12"},
            {"currency_id": "2", "currency_code": "USD", "created_at": "2022-12-12", "last_updated": "2023-01-12"},
        ]

        update_csv(test_data, "currency")

        with open("update.csv", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                assert ["currency_id", "currency_code", "created_at", "last_updated"] == list(row.keys())

    @mock_s3
    def test_uploads_csv_to_object_in_bucket(self):
        conn = boto3.client("s3", region_name="eu-west-2")

        conn.create_bucket(Bucket="test_bucket", CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})

        test_data = [
            {"currency_id": "1", "currency_code": "GBP", "created_at": "2023-06-12", "last_updated": "2023-09-12"},
            {"currency_id": "2", "currency_code": "USD", "created_at": "2022-12-12", "last_updated": "2023-01-12"},
        ]

        conn.put_object(Bucket="test_bucket", Key="design/update.csv")

        update_csv(test_data, "currency", "test_bucket")

        response = (
            conn.get_object(
                Bucket="test_bucket",
                Key="design/update.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )

        assert "1,GBP,2023-06-12,2023-09-12" in response
        assert "2,USD,2022-12-12,2023-01-12" in response

    @mock_s3
    def test_overwites_previous_csv_file(self):
        conn = boto3.client("s3", region_name="eu-west-2")

        conn.create_bucket(Bucket="test_bucket", CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})

        conn.put_object(Bucket="test_bucket", Key="design/update.csv")

        test_data = [
            {"currency_id": "1", "currency_code": "GBP", "created_at": "2023-06-12", "last_updated": "2023-06-12"},
            {"currency_id": "2", "currency_code": "USD", "created_at": "2022-12-12", "last_updated": "2022-12-12"},
        ]

        update_csv(test_data, "currency", "test_bucket")

        replacement_data = [
            {"currency_id": "1", "currency_code": "GBP", "created_at": "2023-06-12", "last_updated": "2023-09-12"},
            {"currency_id": "2", "currency_code": "USD", "created_at": "2022-12-12", "last_updated": "2023-01-12"},
        ]

        update_csv(replacement_data, "currency", "test_bucket")

        response = (
            conn.get_object(
                Bucket="test_bucket",
                Key="design/update.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )

        assert "1,GBP,2023-06-12,2023-06-12" not in response
        assert "2,USD,2022-12-12,2022-12-12" not in response
        assert "1,GBP,2023-06-12,2023-09-12" in response
        assert "2,USD,2022-12-12,2023-01-12" in response
