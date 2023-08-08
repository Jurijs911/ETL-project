from manipulation_utils import (
    format_dim_counterparty,
    format_dim_currency,
    format_dim_date,
    format_dim_design,
    format_dim_location,
    format_dim_staff,
    format_fact_sales_order,
)
from read_ingestion_csv import read_ingestion_csv
from filter_data_by_timestamp import filter_data
from write_timestamp import write_timestamp
from upload_csv import upload_csv
from datetime import datetime
import logging
import boto3
import time
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

cloudwatch_logs = boto3.client("logs", region_name="eu-west-2")


def log_to_cloudwatch(message, log_group_name, log_stream_name):
    cloudwatch_logs.put_log_events(
        logGroupName=log_group_name,
        logStreamName=log_stream_name,
        logEvents=[
            {"timestamp": int(round(time.time() * 1000)), "message": message},
        ],
    )


def lambda_handler(event, context):
    try:
        ingested_data = read_ingestion_csv()
        for table, data in ingested_data.items():
            filtered_data = filter_data(data, table)
            write_timestamp(filtered_data, table)
            ingested_data[table] = filtered_data
        formatted_sales_orders = format_fact_sales_order(
            ingested_data["sales_order"]
        )
        formatted_designs = format_dim_design(ingested_data["design"])
        formatted_staff = format_dim_staff(
            ingested_data["staff"], ingested_data["department"]
        )
        formatted_locations = format_dim_location(ingested_data["address"])
        formatted_currencies = format_dim_currency(ingested_data["currency"])
        formatted_counterparties = format_dim_counterparty(
            ingested_data["counterparty"], ingested_data["address"]
        )
        formatted_dates = []

        for table in ingested_data:
            for row in ingested_data[table]:
                for item in row:
                    try:
                        if re.search(
                            r"^\d{4}-\d{1,2}-\d{2}",
                            item,
                        ):
                            formatted_date = format_dim_date(item)
                            if formatted_date not in formatted_dates:
                                formatted_dates.append(formatted_date)
                    except TypeError:
                        pass

        bucket_name = "kp-northcoders-processed-bucket"

        if formatted_sales_orders != []:
            upload_csv(
                formatted_sales_orders,
                "fact_sales_order",
                bucket_name,
            )
        if formatted_designs != []:
            upload_csv(formatted_designs, "dim_design", bucket_name)
        if formatted_staff != []:
            upload_csv(formatted_staff, "dim_staff", bucket_name)
        if formatted_locations != []:
            upload_csv(formatted_locations, "dim_location", bucket_name)
        if formatted_currencies != []:
            upload_csv(formatted_currencies, "dim_currency", bucket_name)
        if formatted_counterparties != []:
            upload_csv(
                formatted_counterparties, "dim_counterparty", bucket_name
            )
        if formatted_dates != []:
            upload_csv(formatted_dates, "dim_date", bucket_name)

        with open("/tmp//last_remodel.txt", "w") as f:
            f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

        s3 = boto3.resource("s3")
        s3.Object(
            "kp-northcoders-processed-bucket", "trigger/last_remodel.txt"
        ).put(Body=open("/tmp//last_remodel.txt", "rb"))

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        log_to_cloudwatch(
            str(e), "/aws/lambda/remodelling-lambda", "lambda-log-stream"
        )
        raise  # this triggers the CloudWatch alarm
