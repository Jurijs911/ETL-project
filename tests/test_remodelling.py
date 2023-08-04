from src.remodelling.remodelling import lambda_handler
from src.remodelling.upload_csv import upload_csv
from src.remodelling.write_timestamp import write_timestamp
from moto import mock_s3, mock_logs
import boto3


@mock_logs
@mock_s3
class Test_Remodelling:
    def setup_mock(self):
        logs_client = boto3.client("logs", region_name="eu-west-2")
        logs_client.create_log_group(
            logGroupName="/aws/lambda/remodelling-lambda"
        )
        logs_client.create_log_stream(
            logGroupName="/aws/lambda/remodelling-lambda",
            logStreamName="lambda-log-stream",
        )

        s3_client = boto3.client("s3", region_name="eu-west-2")

        s3_client.create_bucket(
            Bucket="kp-northcoders-processed-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        s3_client.create_bucket(
            Bucket="kp-northcoders-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        s3_client.put_object(
            Bucket="kp-northcoders-ingestion-bucket", Key="currency.csv"
        )

        currency_data = [
            {
                "currency_id": "1",
                "currency_code": "GBP",
                "created_at": "2023-06-12 15:20:49.962000",
                "last_updated": "2023-06-12 15:20:49.962000",
            },
            {
                "currency_id": "2",
                "currency_code": "USD",
                "created_at": "2022-12-12 15:20:49.962000",
                "last_updated": "2022-12-12 15:20:49.962000",
            },
        ]

        upload_csv(
            currency_data, "currency", "kp-northcoders-ingestion-bucket"
        )

        s3_client.put_object(
            Bucket="kp-northcoders-ingestion-bucket", Key="design.csv"
        )

        sample_design_data = [
            {
                "design_id": "1",
                "created_at": "2023-7-25 15:20:49.962000",
                "last_updated": "2023-7-25 15:20:49.962000",
                "design_name": "design 1",
                "file_location": "./design.jpg",
                "file_name": "design.jpg",
            },
        ]

        upload_csv(
            sample_design_data, "design", "kp-northcoders-ingestion-bucket"
        )

        s3_client.put_object(
            Bucket="kp-northcoders-ingestion-bucket", Key="sales_order.csv"
        )

        sample_sales_data = [
            {
                "sales_order_id": "2",
                "created_at": "2023-7-25 15:20:49.962000",
                "last_updated": "2023-7-25 15:20:49.962000",
                "design_id": "100",
                "staff_id": "200",
                "counterparty_id": "2000",
                "units_sold": "5",
                "unit_price": "20.65",
                "currency_id": "1",
                "agreed_delivery_date": "2023, 7, 30",
                "agreed_payment_date": "2023, 8, 12",
                "agreed_delivery_location_id": "2",
            },
        ]

        upload_csv(
            sample_sales_data, "sales_order", "kp-northcoders-ingestion-bucket"
        )

        s3_client.put_object(
            Bucket="kp-northcoders-ingestion-bucket", Key="staff.csv"
        )

        sample_staff_data = [
            {
                "staff_id": "1",
                "first_name": "zenab",
                "last_name": "haider",
                "department_id": "1",
                "email_address": "zenab@gmail.com",
                "created_at": "2023-7-25 15:20:49.962000",
                "last_updated": "2023-7-25 15:20:49.962000",
            },
        ]

        upload_csv(
            sample_staff_data, "staff", "kp-northcoders-ingestion-bucket"
        )

        s3_client.put_object(
            Bucket="kp-northcoders-ingestion-bucket", Key="address.csv"
        )

        sample_address = [
            {
                "address_id": "1",
                "address_line_1": "123 apple street",
                "address_line_2": "apple street",
                "district": "bolton",
                "city": "greater manchester",
                "postal_code": "ABC 123",
                "country": "England",
                "phone": "123 456 789",
                "created_at": "2023-7-25 15:20:49.962000",
                "last_updated": "2023-7-25 15:20:49.962000",
            },
        ]

        upload_csv(
            sample_address, "address", "kp-northcoders-ingestion-bucket"
        )

        s3_client.put_object(
            Bucket="kp-northcoders-ingestion-bucket", Key="counterparty.csv"
        )

        sample_counterparty_data = [
            {
                "counterparty_id": "1",
                "counterparty_legal_name": "hello",
                "legal_address_id": "1",
                "commercial_contact": "commercial_contact",
                "delivery_contact": "delivery_contact",
                "created_at": "2023-7-25 15:20:49.962000",
                "last_updated": "2023-7-25 15:20:49.962000",
            },
        ]

        upload_csv(
            sample_counterparty_data,
            "counterparty",
            "kp-northcoders-ingestion-bucket",
        )

        s3_client.put_object(
            Bucket="kp-northcoders-ingestion-bucket",
            Key="currency/last_processed.txt",
        )

        write_timestamp([["2020-7-25 15:20:49.962000"]], "currency")

        s3_client.put_object(
            Bucket="kp-northcoders-ingestion-bucket",
            Key="sales_order/last_processed.txt",
        )

        write_timestamp([["2020-7-25 15:20:49.962000"]], "sales_order")

        s3_client.put_object(
            Bucket="kp-northcoders-ingestion-bucket",
            Key="design/last_processed.txt",
        )

        write_timestamp([["2020-7-25 15:20:49.962000"]], "design")

        s3_client.put_object(
            Bucket="kp-northcoders-ingestion-bucket",
            Key="staff/last_processed.txt",
        )

        write_timestamp([["2020-7-25 15:20:49.962000"]], "staff")

        s3_client.put_object(
            Bucket="kp-northcoders-ingestion-bucket",
            Key="department/last_processed.txt",
        )

        write_timestamp([["2020-7-25 15:20:49.962000"]], "department")

        s3_client.put_object(
            Bucket="kp-northcoders-ingestion-bucket",
            Key="address/last_processed.txt",
        )

        write_timestamp([["2020-7-25 15:20:49.962000"]], "address")

        s3_client.put_object(
            Bucket="kp-northcoders-ingestion-bucket",
            Key="counterparty/last_processed.txt",
        )

        write_timestamp([["2020-7-25 15:20:49.962000"]], "counterparty")

        s3_client.put_object(
            Bucket="kp-northcoders-processed-bucket",
            Key="fact_sales_order.csv",
        )

        s3_client.put_object(
            Bucket="kp-northcoders-processed-bucket",
            Key="dim_design.csv",
        )

        s3_client.put_object(
            Bucket="kp-northcoders-processed-bucket",
            Key="dim_staff.csv",
        )

        s3_client.put_object(
            Bucket="kp-northcoders-processed-bucket",
            Key="dim_location.csv",
        )

        s3_client.put_object(
            Bucket="kp-northcoders-processed-bucket",
            Key="dim_currency.csv",
        )

        s3_client.put_object(
            Bucket="kp-northcoders-processed-bucket",
            Key="dim_date.csv",
        )

        s3_client.put_object(
            Bucket="kp-northcoders-processed-bucket",
            Key="dim_counterparty.csv",
        )

    def iterate_bucket_items(self, bucket):
        """
        Generator that iterates over all objects in a given s3 bucket

        :param bucket: name of s3 bucket
        :return: dict of metadata for an object
        """

        client = boto3.client("s3")
        paginator = client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket)

        for page in page_iterator:
            if page["KeyCount"] > 0:
                for item in page["Contents"]:
                    yield item

    def test_remodelling(self):
        self.setup_mock()

        s3_client = boto3.client("s3", region_name="eu-west-2")

        lambda_handler("event_stub", "context_stub")

        processed_bucket = "kp-northcoders-processed-bucket"

        processed_response = s3_client.list_objects_v2(
            Bucket="kp-northcoders-processed-bucket"
        )

        for item in processed_response["Contents"]:
            assert item["Key"] in (
                "dim_counterparty.csv",
                "dim_currency.csv",
                "dim_date.csv",
                "dim_design.csv",
                "dim_location.csv",
                "dim_staff.csv",
                "fact_sales_order.csv",
            )

        for idx, item in enumerate(
            self.iterate_bucket_items(bucket=processed_bucket)
        ):
            response = s3_client.get_object(
                Bucket=processed_bucket, Key=item["Key"]
            )
            body = response["Body"].read().decode("utf-8").splitlines()

            if idx == 0:
                assert (
                    "1|hello|123 apple street|apple street|bolton|greater "
                    "manchester|ABC 123|England|123 456 789" in body
                )
            if idx == 1:
                assert "1|GBP|British Pound" in body
                assert "2|USD|US Dollar" in body
            if idx == 2:
                assert "2023-7-25,2023|7|25|1|Tuesday|July|3"
            if idx == 3:
                assert "1|design 1|./design.jpg|design.jpg" in body
            if idx == 4:
                assert (
                    "1|123 apple street|apple street|bolton|greater "
                    "manchester|ABC 123|England|123 456 789" in body
                )
            if idx == 5:
                assert len(body) == 0
            if idx == 6:
                assert (
                    "2|2023-7-25|15:20:49.962000|2023-7-25|15:20:49.962000|200"
                    "|2000|5|20.65|1|100|2023, 8, 12|2023, 7, 30|2" in body
                )
