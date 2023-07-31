from src.remodelling.remodelling import lambda_handler
from src.remodelling.upload_csv import upload_csv
from src.remodelling.write_timestamp import write_timestamp
from moto import mock_s3
import boto3


@mock_s3
def test_remodelling():
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

    upload_csv(currency_data, "currency", "kp-northcoders-ingestion-bucket")

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

    upload_csv(sample_design_data, "design", "kp-northcoders-ingestion-bucket")

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

    upload_csv(sample_staff_data, "staff", "kp-northcoders-ingestion-bucket")

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

    upload_csv(sample_address, "address", "kp-northcoders-ingestion-bucket")

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

    lambda_handler(None, None)

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
