import logging
import boto3
import time
from src.ingestion_lambda.get_address_add import get_address_add
from src.remodelling.upload_csv import upload_csv
from src.ingestion_lambda.find_most_recent_time import find_most_recent_time
from src.ingestion_lambda.write_updated_time import write_updated_time
from src.ingestion_lambda.get_counterparty_add import get_counterparty_add
from src.ingestion_lambda.get_currency_add import get_currency_add
from src.ingestion_lambda.get_department_add import get_department_add
from src.ingestion_lambda.get_design_add import get_design_add
from src.ingestion_lambda.get_payment_add import get_payment_add
from src.ingestion_lambda.get_purchase_order_add import get_purchase_order_add
from src.ingestion_lambda.get_sales_order_add import get_sales_order_add
from src.ingestion_lambda.get_staff_add import get_staff_add

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

cloudwatch_logs = boto3.client("logs")


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
        address_data = get_address_add()

        if len(address_data) > 0:
            updated_timestamp = find_most_recent_time(address_data)
            upload_csv(
                address_data, "address", "kp-northcoder-ingestion-bucket"
            )
            write_updated_time(updated_timestamp, "address")

        counterparty_data = get_counterparty_add()

        if len(counterparty_data) > 0:
            updated_timestamp = find_most_recent_time(counterparty_data)
            upload_csv(
                counterparty_data,
                "counterparty",
                "kp-northcoder-ingestion-bucket",
            )
            write_updated_time(updated_timestamp, "counterparty")

        currency_data = get_currency_add()

        if len(currency_data) > 0:
            updated_timestamp = find_most_recent_time(currency_data)
            upload_csv(
                currency_data, "currency", "kp-northcoder-ingestion-bucket"
            )
            write_updated_time(updated_timestamp, "currency")

        department_data = get_department_add()

        if len(department_data) > 0:
            updated_timestamp = find_most_recent_time(department_data)
            upload_csv(
                department_data, "department", "kp-northcoder-ingestion-bucket"
            )
            write_updated_time(updated_timestamp, "department")

        design_data = get_design_add()

        if len(design_data) > 0:
            updated_timestamp = find_most_recent_time(design_data)
            upload_csv(design_data, "design", "kp-northcoder-ingestion-bucket")
            write_updated_time(updated_timestamp, "design")

        payment_data = get_payment_add()

        if len(payment_data) > 0:
            updated_timestamp = find_most_recent_time(payment_data)
            upload_csv(
                payment_data, "payment", "kp-northcoder-ingestion-bucket"
            )
            write_updated_time(updated_timestamp, "payment")

        purchase_order_data = get_purchase_order_add()

        if len(purchase_order_data) > 0:
            updated_timestamp = find_most_recent_time(purchase_order_data)
            upload_csv(
                purchase_order_data,
                "purchase_order",
                "kp-northcoder-ingestion-bucket",
            )
            write_updated_time(updated_timestamp, "purchase_order")

        sales_order_data = get_sales_order_add()

        if len(sales_order_data) > 0:
            updated_timestamp = find_most_recent_time(sales_order_data)
            upload_csv(
                sales_order_data,
                "sales_order",
                "kp-northcoder-ingestion-bucket",
            )
            write_updated_time(updated_timestamp, "sales_order")

        staff_data = get_staff_add()

        if len(staff_data) > 0:
            updated_timestamp = find_most_recent_time(staff_data)
            upload_csv(staff_data, "staff", "kp-northcoder-ingestion-bucket")
            write_updated_time(updated_timestamp, "staff")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        log_to_cloudwatch(
            str(e), "/aws/lambda/remodelling-lambda", "lambda-log-stream"
        )
        raise  # this triggers the CloudWatch alarm
