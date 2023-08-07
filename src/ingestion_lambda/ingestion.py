import logging
import boto3
import time
from datetime import datetime
from get_secret import get_secret
from get_address_add import get_address_add
from upload_csv import upload_csv
from find_most_recent_time import find_most_recent_time
from write_updated_time import write_updated_time
from get_counterparty_add import get_counterparty_add
from get_currency_add import get_currency_add
from get_department_add import get_department_add
from get_design_add import get_design_add
from get_payment_add import get_payment_add
from get_purchase_order_add import get_purchase_order_add
from get_sales_order_add import get_sales_order_add
from get_staff_add import get_staff_add


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

secret = get_secret()

cloudwatch_logs = boto3.client("logs", region_name="eu-west-2")

# Set log group and log stream names
log_group_name = "/aws/lambda/ingestion-lambda"
log_stream_name = "lambda-log-stream"


def log_to_cloudwatch(message, log_group_name, log_stream_name):
    cloudwatch_logs.put_log_events(
        logGroupName="/aws/lambda/ingestion-lambda",
        logStreamName="lambda-log-stream",
        logEvents=[
            {"timestamp": int(round(time.time() * 1000)), "message": message},
        ],
    )


def lambda_handler(
    event,
    context,
    db_user=secret["username"],
    db_database=secret["dbname"],
    db_host=secret["host"],
    db_port=secret["port"],
    db_password=secret["password"],
):
    try:
        address_data = get_address_add(
            db_user, db_database, db_host, db_port, db_password
        )

        if len(address_data) > 0:
            updated_timestamp = find_most_recent_time(address_data)
            upload_csv(
                address_data, "address", "kp-northcoders-ingestion-bucket"
            )
            write_updated_time(updated_timestamp, "address")
            log_to_cloudwatch(
                str("New address data returned"),
                "/aws/lambda/ingestion-lambda",
                "lambda-log-stream",
            )
        else:
            log_to_cloudwatch(
                str("No new address data returned"),
                "/aws/lambda/ingestion-lambda",
                "lambda-log-stream",
            )

        counterparty_data = get_counterparty_add(
            db_user, db_database, db_host, db_port, db_password
        )

        if len(counterparty_data) > 0:
            updated_timestamp = find_most_recent_time(counterparty_data)
            upload_csv(
                counterparty_data,
                "counterparty",
                "kp-northcoders-ingestion-bucket",
            )
            write_updated_time(updated_timestamp, "counterparty")
            log_to_cloudwatch(
                str("New counterparty data returned"),
                "/aws/lambda/ingestion-lambda",
                "lambda-log-stream",
            )
        else:
            log_to_cloudwatch(
                str("No new counterparty data returned"),
                "/aws/lambda/ingestion-lambda",
                "lambda-log-stream",
            )

        currency_data = get_currency_add(
            db_user, db_database, db_host, db_port, db_password
        )

        if len(currency_data) > 0:
            updated_timestamp = find_most_recent_time(currency_data)
            upload_csv(
                currency_data, "currency", "kp-northcoders-ingestion-bucket"
            )
            write_updated_time(updated_timestamp, "currency")
            log_to_cloudwatch(
                str("New currency data returned"),
                "/aws/lambda/ingestion-lambda",
                "lambda-log-stream",
            )
        else:
            log_to_cloudwatch(
                str("No new currency data returned"),
                "/aws/lambda/ingestion-lambda",
                "lambda-log-stream",
            )

        department_data = get_department_add(
            db_user, db_database, db_host, db_port, db_password
        )

        if len(department_data) > 0:
            updated_timestamp = find_most_recent_time(department_data)
            upload_csv(
                department_data,
                "department",
                "kp-northcoders-ingestion-bucket",
            )
            write_updated_time(updated_timestamp, "department")
            log_to_cloudwatch(
                str("New department data returned"),
                "/aws/lambda/ingestion-lambda",
                "lambda-log-stream",
            )
        else:
            log_to_cloudwatch(
                str("No new department data returned"),
                "/aws/lambda/ingestion-lambda",
                "lambda-log-stream",
            )

        design_data = get_design_add(
            db_user, db_database, db_host, db_port, db_password
        )

        if len(design_data) > 0:
            updated_timestamp = find_most_recent_time(design_data)
            upload_csv(
                design_data, "design", "kp-northcoders-ingestion-bucket"
            )
            write_updated_time(updated_timestamp, "design")
            log_to_cloudwatch(
                str("New design data returned"),
                "/aws/lambda/ingestion-lambda",
                "lambda-log-stream",
            )
        else:
            log_to_cloudwatch(
                str("No new design data returned"),
                "/aws/lambda/ingestion-lambda",
                "lambda-log-stream",
            )

        payment_data = get_payment_add(
            db_user, db_database, db_host, db_port, db_password
        )

        if len(payment_data) > 0:
            updated_timestamp = find_most_recent_time(payment_data)
            upload_csv(
                payment_data, "payment", "kp-northcoders-ingestion-bucket"
            )
            write_updated_time(updated_timestamp, "payment")
            log_to_cloudwatch(
                str("New payment data returned"),
                "/aws/lambda/ingestion-lambda",
                "lambda-log-stream",
            )
        else:
            log_to_cloudwatch(
                str("No new payment data returned"),
                "/aws/lambda/ingestion-lambda",
                "lambda-log-stream",
            )

        purchase_order_data = get_purchase_order_add(
            db_user, db_database, db_host, db_port, db_password
        )

        if len(purchase_order_data) > 0:
            updated_timestamp = find_most_recent_time(purchase_order_data)
            upload_csv(
                purchase_order_data,
                "purchase_order",
                "kp-northcoders-ingestion-bucket",
            )
            write_updated_time(updated_timestamp, "purchase_order")
            log_to_cloudwatch(
                str("New purchase_order data returned"),
                "/aws/lambda/ingestion-lambda",
                "lambda-log-stream",
            )
        else:
            log_to_cloudwatch(
                str("No new purchase_order data returned"),
                "/aws/lambda/ingestion-lambda",
                "lambda-log-stream",
            )

        sales_order_data = get_sales_order_add(
            db_user, db_database, db_host, db_port, db_password
        )

        if len(sales_order_data) > 0:
            updated_timestamp = find_most_recent_time(sales_order_data)
            upload_csv(
                sales_order_data,
                "sales_order",
                "kp-northcoders-ingestion-bucket",
            )
            write_updated_time(updated_timestamp, "sales_order")
            log_to_cloudwatch(
                str("New sales_order data returned"),
                "/aws/lambda/ingestion-lambda",
                "lambda-log-stream",
            )
        else:
            log_to_cloudwatch(
                str("No new sales_order data returned"),
                "/aws/lambda/ingestion-lambda",
                "lambda-log-stream",
            )

        staff_data = get_staff_add(
            db_user, db_database, db_host, db_port, db_password
        )

        if len(staff_data) > 0:
            updated_timestamp = find_most_recent_time(staff_data)
            upload_csv(staff_data, "staff", "kp-northcoders-ingestion-bucket")
            write_updated_time(updated_timestamp, "staff")
            log_to_cloudwatch(
                str("New staff data returned"),
                "/aws/lambda/ingestion-lambda",
                "lambda-log-stream",
            )
        else:
            log_to_cloudwatch(
                str("No new staff data returned"),
                "/aws/lambda/ingestion-lambda",
                "lambda-log-stream",
            )

        with open("/tmp//last_ingestion.txt", "r+", newline="") as f:
            f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
            s3_client = boto3.client("s3")
            s3_client.put_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="trigger/last_ingestion.txt",
                Body=f,
            )

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        log_to_cloudwatch(
            str(e), "/aws/lambda/ingestion-lambda", "lambda-log-stream"
        )
        raise  # this triggers the CloudWatch alarm
