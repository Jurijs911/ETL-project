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
    read_processed_csv,
    create_connection
)
from read_processed_csv import read_processed_csv

logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger(__name__)

cloudwatch_logs = boto3.client("logs")

def log_to_cloudwatch(message, log_group_name, log_stream_name):
    """ Log a message to AWS CloudWatch Logs.

    This function sends a log message to AWS CloudWatch Logs with the specified log group and log stream names.
    The message is timestamped with the current time in milliseconds.

    Parameters:
    message (str): The log message to be sent to CloudWatch Logs.
    log_group_name (str): The name of the CloudWatch Logs log group to which the message will be logged.
    log_stream_name (str): The name of the CloudWatch Logs log stream within the log group.

    Note: Before using this function, ensure that the appropriate AWS IAM permissions are set
    to allow the Lambda function or AWS service to write logs to the specified log group.

    Note: The 'cloudwatch_logs' variable should be defined before calling this function,
    representing the Boto3 client for CloudWatch Logs.

    Example usage:
    cloudwatch_logs = boto3.client("logs")
    log_to_cloudwatch("Data insertion completed successfully.", "/aws/lambda/loading-lambda", "lambda-log-stream")
   """
    
    cloudwatch_logs.put_log_events(
        logGroupName=log_group_name,
        logStreamName=log_stream_name,
        logEvents=[
            {
                'timestamp': int(round(time.time() * 1000)),
                'message': message
            },
        ]
    )

def lambda_handler(event, context):
    """AWS Lambda function to handle the data loading process.

    This function is triggered by an event and context provided by AWS Lambda.
    It reads processed data from a specified S3 bucket and inserts it into various dimension and fact tables in the database.
    The inserted data is logged, and any exceptions encountered during the process are caught and logged with error details.
    If an exception occurs, it is re-raised to trigger the CloudWatch alarm.

    Parameters:
    event (dict): The event data passed by AWS Lambda. Not used in this function.
    context (LambdaContext): The context object passed by AWS Lambda. Not used in this function.

    Raises:
    Exception: If any error occurs during the data loading process, it will be caught, logged, and then re-raised.

    Note: Before deploying this Lambda function, ensure that the necessary IAM permissions are set for the
    Lambda function to access the S3 bucket and the database (if applicable).

    Note: The function also uses the 'read_processed_csv' function and the database-related functions
    'insert_into_dim_design', 'insert_into_dim_currency', 'insert_into_dim_staff',
    'insert_into_dim_location', 'insert_into_dim_date', 'insert_into_dim_counterparty',
    and 'insert_into_dim_fact_sales_order', which must be implemented and available in the 'loading_utils' module.
  """
    
    try:
        bucket_name = "kp-northcoder-data-bucket"  
       
        processed_data = read_processed_csv(bucket_name)

        conn = create_connection()

        inserted_data = {
            "dim_design": insert_into_dim_design(conn, processed_data["dim_design"]),
            "dim_currency": insert_into_dim_currency(conn, processed_data["dim_currency"]),
            "dim_staff": insert_into_dim_staff(conn, processed_data["dim_staff"]),
            "dim_location": insert_into_dim_location(conn, processed_data["dim_location"]),
            "dim_date": insert_into_dim_date(conn, processed_data["dim_date"]),
            "dim_counterparty": insert_into_dim_counterparty(conn, processed_data["dim_counterparty"]),
            "fact_sales_order": insert_into_dim_fact_sales_order(conn, processed_data["fact_sales_order"]),
        }

        conn.close()

        logger.info("Data insertion completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        log_to_cloudwatch(str(e), "/aws/lambda/loading-lambda", "lambda-log-stream")
        raise  # this triggers the CloudWatch alarm 

    # except Exception as e:
    #     import traceback
    #     traceback.print_exc()
    #     logger.error(f"An error occurred: {str(e)}")
    #     log_to_cloudwatch(str(e), "/aws/lambda/loading-lambda", "lambda-log-stream")
    #     raise  # this triggers the CloudWatch alarm 
