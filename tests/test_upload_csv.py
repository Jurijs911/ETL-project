from src.remodelling.upload_csv import upload_csv
import boto3
from moto import mock_s3


@mock_s3
class Test_add_csv:
    def test_uploads_csv_to_object_in_bucket(self):
        """
        Test case for the 'upload_csv' function to check that it correctly
        uploads CSV data to a specified object in the S3 bucket.

        It creates an S3 bucket - 'test_bucket', then defines test data
        representing two currency entries and uploads the 'currency.csv' file
        with the initial test data to the 'test_bucket'. The 'upload_csv'
        function uploads additional test data to the 'currency.csv' file in
        the bucket.
        """
        conn = boto3.client("s3", region_name="eu-west-2")

        conn.create_bucket(
            Bucket="test_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        test_data = [
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

        conn.put_object(Bucket="test_bucket", Key="currency.csv")

        upload_csv(test_data, "currency", "test_bucket")

        response = (
            conn.get_object(
                Bucket="test_bucket",
                Key="currency.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )

        assert "1|GBP|2023-06-12|2023-06-12" in response
        assert "2|USD|2022-12-12|2022-12-12" in response

    def test_does_not_overwite_previous_csv_file(self):
        """
        Test case for the 'upload_csv' function to check that it does not
        overwrite the previous CSV file in the S3 bucket and appends new data
        to the existing file.

        It creates an S3 bucket - 'test_bucket', then defines test data
        representing two currency entries and uploads the 'currency.csv' file
        with the new test data to the bucket. Checks if both the initial and
        new data are present in the response.
        """
        conn = boto3.client("s3", region_name="eu-west-2")

        conn.create_bucket(
            Bucket="test_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        conn.put_object(Bucket="test_bucket", Key="currency.csv")

        test_data = [
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

        upload_csv(test_data, "currency", "test_bucket")

        replacement_data = [
            {
                "currency_id": "3",
                "currency_code": "EUR",
                "created_at": "2023-08-03",
                "last_updated": "2023-08-03",
            },
            {
                "currency_id": "4",
                "currency_code": "CAD",
                "created_at": "2022-05-10",
                "last_updated": "2022-05-10",
            },
        ]

        upload_csv(replacement_data, "currency", "test_bucket")

        response = (
            conn.get_object(
                Bucket="test_bucket",
                Key="currency.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )

        assert "1|GBP|2023-06-12|2023-06-12" in response
        assert "2|USD|2022-12-12|2022-12-12" in response
        assert "3|EUR|2023-08-03|2023-08-03" in response
        assert "4|CAD|2022-05-10|2022-05-10" in response
