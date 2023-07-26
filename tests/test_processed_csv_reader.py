from src.manipulation.read_processed_csv import read_processed_csv
from src.ingestion_csv_utils import upload_csv
import os
import boto3
from moto import mock_s3


@mock_s3
class Test_read_ingested_csv:
    def test_reads_correct_data_for_one_table(self):
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
                "location": "Birmingham"
                "email_address": "email@address.com",
                
            }
        ]
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
                    "Birmingham"
                    "email@address.com",
                
                ]
            ],
            "dim_location": [],
            "dim_currency": [],
            "dim_design": [],
            "dim_counterparty": [],
        }


        result = read_ingested_csv("ingested-bucket")

        assert result == expected

        if os.path.exists("currency.csv"):
            os.remove("currency.csv")

        if os.path.exists("staff.csv"):
            os.remove("staff.csv")
