from src.remodelling.write_timestamp import write_timestamp
from src.remodelling.filter_data_by_timestamp import filter_data
import boto3
from moto import mock_s3


@mock_s3
def test_writes_timestamp_to_bucket():
    """
    Tests whether the 'write_timestamp' function successfully writes the latest
    processed timestamp to a text file and uploads it to the correct S3 bucket.

    The function creates a mock S3 bucket, then places a sample timestamp file
    ('sales_order/last_processed.txt') in the bucket. It calls the
    'write_timestamp' function with sample timestamp data for the table.
    Then retrieves the timestamp file from the bucket after the write
    operation.
    """

    s3_client = boto3.client("s3", region_name="eu-west-2")

    s3_client.create_bucket(
        Bucket="kp-northcoders-ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    s3_client.put_object(
        Bucket="kp-northcoders-ingestion-bucket",
        Key="sales_order/last_processed.txt",
    )

    write_timestamp([["2020-7-25 15:20:49.962000"]], "sales_order")

    sample_data = [
        [
            "2",
            "2023-7-25 15:20:49.962000",
            "2023-7-25 15:20:49.962000",
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
            "1999-7-25 15:20:49.962000",
            "1999-7-25 15:20:49.962000",
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

    filtered_data = filter_data(sample_data, "sales_order")

    write_timestamp(filtered_data, "sales_order")

    s3_response = (
        s3_client.get_object(
            Bucket="kp-northcoders-ingestion-bucket",
            Key="sales_order/last_processed.txt",
        )["Body"]
        .read()
        .decode("utf-8")
    )

    assert s3_response == "2023-7-25 15:20:49.962000"
