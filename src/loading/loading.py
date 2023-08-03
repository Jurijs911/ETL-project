import logging
import boto3
import time
from loading_utils import (
    insert_into_dim_design,
    insert_into_dim_currency,
    insert_into_dim_staff,
    insert_into_dim_location,
    insert_into_dim_date,
    insert_into_dim_counterparty,
    insert_into_dim_fact_sales_order,
    create_connection,
)
from read_processed_csv import read_processed_csv

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

cloudwatch_logs = boto3.client("logs")

def log_to_cloudwatch(message, log_group_name, log_stream_name):
    """Log a message to AWS CloudWatch Logs."""

    cloudwatch_logs.put_log_events(
        logGroupName=log_group_name,
        logStreamName=log_stream_name,
        logEvents=[
            {"timestamp": int(round(time.time() * 1000)), "message": message},
        ],
    )


def lambda_handler(event, context):
    """AWS Lambda function to process data and insert it into
    the respective dimension and fact tables.



    Raises:
        Exception: If an error occurs during data processing or insertion,
        the exception is logged to CloudWatch,
        and a CloudWatch alarm is triggered to alert on the error.
    """

    try:
        bucket_name = "kp-northcoder-data-bucket"

        processed_data = read_processed_csv(bucket_name)

        conn = create_connection()

        inserted_data = {
            "dim_design": insert_into_dim_design(
                conn, processed_data["dim_design"]
            ),
            "dim_currency": insert_into_dim_currency(
                conn, processed_data["dim_currency"]
            ),
            "dim_staff": insert_into_dim_staff(
                conn, processed_data["dim_staff"]
            ),
            "dim_location": insert_into_dim_location(
                conn, processed_data["dim_location"]
            ),
            "dim_date": insert_into_dim_date(conn, processed_data["dim_date"]),
            "dim_counterparty": insert_into_dim_counterparty(
                conn, processed_data["dim_counterparty"]
            ),
            "fact_sales_order": insert_into_dim_fact_sales_order(
                conn, processed_data["fact_sales_order"]
            ),
        }

        conn.close()

        logger.info("Data insertion completed successfully.")

        return inserted_data  # ADDED TO FIX FLAKE ISSUE
        # - decide where inserted_data should be used

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        log_to_cloudwatch(
            str(e), "/aws/lambda/loading-lambda", "lambda-log-stream"
        )
        raise  # this triggers the CloudWatch alarm

    # except Exception as e:
    #     import traceback
    #     traceback.print_exc()
    #     logger.error(f"An error occurred: {str(e)}")
    #     log_to_cloudwatch(str(e), "/aws/lambda/loading-lambda",
    #     "lambda-log-stream")
    #     raise  # this triggers the CloudWatch alarm