from src.manipulation.read_processed_csv import read_processed_csv
from src.ingestion_csv_utils import upload_csv
import os
import boto3
from moto import mock_s3


@mock_s3
class Test_read_ingested_csv:
    def test_reads_correct_data_for_one_table(self):
        """
        Test the 'read_processed_csv' function when there is data for one table in the processed bucket.

        The function should read the CSV file for the specified table ('fact_sales_order') and return the
        data as a list of lists, where each inner list represents a row in the CSV file. The returned
        dictionary should only contain data for the specified table, and the other tables should have
        empty lists as there is no data for them.
        """

        s3_client = boto3.client("s3", region_name="eu-west-2")
        s3_client.create_bucket(
            Bucket="processed-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        sales_data = [
        {
            "sales_order_id": 2,
            "created_date": "2023-07-25",
            "created_time": "15:20:49:962000",
            "last_updated_date": "2023-07-25",
            "last_updated_time": "15:20:49:962000",
            "sales_staff_id": 100,
            "counterparty_id": 200,
            "units_sold": 2000,
            "unit_price": 20.65,
            "currency_id": 5,
            "design_id": 1,
            "agreed_payment_date": "2023, 7, 30",
            "agreed_delivery_date": "2023, 8, 12",
            "agreed_delivery_location_id": 2,
        },
    ]

        s3_client.put_object(Bucket="processed-bucket", Key="fact_sales_order.csv")

        upload_csv(sales_data, "fact_sales_order", "processed-bucket")

        expected = {
            "fact_sales_order": [
                [
                    "2",
                    "2023-07-25",
                    "15:20:49:962000",
                    "2023-07-25",
                    "15:20:49:962000",
                    "100",
                    "200",
                    "2000",
                    "20.65",
                    "5",
                    "1",
                    "2023, 7, 30",
                    "2023, 8, 12",
                    "2",
                ],
            ],
            "dim_date": [],
            "dim_staff": [],
            "dim_location": [],
            "dim_currency": [],
            "dim_design": [],
            "dim_counterparty": [],
        }

        result = read_processed_csv("processed-bucket")

        print(result)
        assert result == expected

        if os.path.exists("sales.csv"):
            os.remove("sales.csv")



    def test_reads_correct_data_for_mutliple_tables(self):
        """
        Test the 'read_processed_csv' function when there is data for multiple tables in the processed bucket.

        The function should read the CSV files for the specified tables ('fact_sales_order' and 'dim_staff')
        and return the data as a dictionary where each key represents a table name and its value is a list
        of lists containing the data for that table. The other tables should have empty lists as there is no
        data for them.
        """

        s3_client = boto3.client("s3", region_name="eu-west-2")
        s3_client.create_bucket(
            Bucket="processed-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        staff_data = [
            {
                "staff_id": "1",
                "first_name": "Cameron",
                "last_name": "Parsonage",
                "department_name": "Sales",
                "location": "Birmingham",
                "email_address": "email@address.com",
            }
        ]

        sales_data = [
        {
            "sales_order_id": "2",
            "created_date": "2023-07-25",
            "created_time": "15:20:49:962000",
            "last_updated_date": "2023-07-25",
            "last_updated_time": "15:20:49:962000",
            "sales_staff_id": "100",
            "counterparty_id": "200",
            "units_sold": "2000",
            "unit_price": "20.65",
            "currency_id": "5",
            "design_id": "1",
            "agreed_payment_date": "2023, 7, 30",
            "agreed_delivery_date": "2023, 8, 12",
            "agreed_delivery_location_id": "2",
        },

        ]

        s3_client.put_object(Bucket="processed-bucket", Key="fact_sales_order.csv")
        s3_client.put_object(Bucket="processed-bucket", Key="dim_staff.csv")

        upload_csv(sales_data, "fact_sales_order", "processed-bucket")
        upload_csv(staff_data, "dim_staff", "processed-bucket")

        expected = {
            "fact_sales_order": [
                [
                    "2",
                    "2023-07-25",
                    "15:20:49:962000",
                    "2023-07-25",
                    "15:20:49:962000",
                    "100",
                    "200",
                    "2000",
                    "20.65",
                    "5",
                    "1",
                    "2023, 7, 30",
                    "2023, 8, 12",
                    "2",
                ],
            ],
            "dim_date": [],
            "dim_staff": [
                [    
                    "1",
                    "Cameron",
                    "Parsonage",
                    "Sales",
                    "Birmingham",
                    "email@address.com",
                ]
            ],
            "dim_location": [],
            "dim_currency": [],
            "dim_design": [],
            "dim_counterparty": [],
        }


        result = read_processed_csv("processed-bucket")

        assert result == expected

        if os.path.exists("fact_sales_order.csv"):
            os.remove("fact_sales_order.csv")

        if os.path.exists("dim_staff.csv"):
            os.remove("dim_staff.csv")



    