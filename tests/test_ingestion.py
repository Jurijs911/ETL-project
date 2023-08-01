from src.ingestion_lambda.find_most_recent_time import find_most_recent_time
from src.ingestion_lambda.get_address_add import get_address_add
from src.ingestion_lambda.write_updated_time import write_updated_time
from src.ingestion_lambda.ingestion import lambda_handler, log_to_cloudwatch
import os
from unittest.mock import patch, DEFAULT, ANY, Mock
from moto import mock_s3, mock_logs
from src.upload_csv import upload_csv
import boto3

test_user = os.environ.get("test_user")
test_database = os.environ.get("test_database")
test_host = os.environ.get("test_host")
test_port = os.environ.get("test_port")
test_password = os.environ.get("test_password")


@mock_s3
@mock_logs
def test_lambda_handler_calls_get_address_add():
   # Set up mock S3 service
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket='kp-northcoder-ingestion-bucket')
    conn.Object('kp-northcoder-ingestion-bucket',
                'address/created_at.txt').put(Body='2020-07-25 15:20:49.962000')
    client = boto3.client('logs')
    client.create_log_group(logGroupName='/aws/lambda/remodelling-lambda')
    client.create_log_stream(
        logGroupName='/aws/lambda/remodelling-lambda', logStreamName='lambda-log-stream')

    lambda_handler({}, {}, test_user, test_database,
                   test_host, test_port, test_password)
    get_address_add.assert_called_once()


#     # with patch.multiple('src.ingestion_lambda',
#     #                     get_address_add=DEFAULT,
#     #                     find_most_recent_time=DEFAULT,
#     #                     write_updated_time=DEFAULT,
#     #                     log_to_cloudwatch=DEFAULT
#     #                     ) as mocks:
#     #     mocks['get_address_add'].return_value = ['Test data']
#     #     mocks['find_most_recent_time'].return_value = 'Test timestamp'
#     #     with patch('src.upload_csv.upload_csv') as mock_upload_cvs:

#     #         lambda_handler({}, {})
#     #         mock_upload_cvs.assert_called_once_with(
#     #             ['Test data'], 'address', 'kp-northcoder-ingestion-bucket'
#     #         )

#     with patch('src.ingestion_lambda.get_address_add') as mock_get_address_add, \
#             patch('src.ingestion_lambda.ingestion.log_to_cloudwatch') as mock_log_to_cloudwatch, \
#             patch('src.ingestion_lambda.find_most_recent_time') as mock_find_most_recent_time, \
#             patch('src.upload_csv') as mock_upload_cvs, \
#             patch('src.ingestion_lambda.write_updated_time') as mock_write_updated_time:
#         mock_get_address_add.return_value = ['Test data']

#         mock_find_most_recent_time.return_value = 'Test timestamp'
#         # add 3rd arg with default value as prod db
#         # and change it to test db

#         lambda_handler({}, {})
#         mock_upload_cvs.assert_called_once_with(
#             ['Test data'], 'address', 'kp-northcoder-ingestion-bucket'
#         )
#         mock_write_updated_time.assert_called_once_with(
#             'Test timestamp', 'address'
#         )
#         mock_log_to_cloudwatch.assert_called_once_with(
#             'New data returned', "/aws/lambda/remodelling-lambda", "lambda-log-stream"
#         )
# @mock_s3
# # @patch('pg8000.native.Connection')
# def test_lambda_handler_when_new_data_returned():
#     # mock_db = Mock()
#     # mock_connection.return_value = mock_db
#     # mock_db.run.return_value = ['Test data']
#     # Set up mock S3 service
#     conn = boto3.resource('s3', region_name='us-east-1')
#     conn.create_bucket(Bucket='kp-northcoder-ingestion-bucket')
#     conn.Object('kp-northcoder-ingestion-bucket',
#                 'address/created_at.txt').put(Body='2020-07-25 15:20:49.962000')
#     with patch('src.ingestion_lambda.get_address_add') as mock_get_address_add, \
#         patch('src.ingestion_lambda.find_most_recent_time') as mock_find_most_recent_time, \
#             patch('src.upload_csv.upload_csv') as mock_upload_csv, \
#             patch('src.ingestion_lambda.write_updated_time') as mock_write_updated_time, \
#             patch('src.ingestion_lambda.ingestion.log_to_cloudwatch') as mock_cloudwatch_logs:
#         mock_get_address_add.return_value = ['Test data']
#         mock_find_most_recent_time.return_value = '2020-07-25 15:20:49.962000'
#         lambda_handler({}, {})
#         mock_upload_csv.assert_called_once_with(
#             ['Test data'], 'address', 'kp-northcoder-ingestion-bucket')
#         mock_write_updated_time.assert_called_once_with(
#             '2020-07-25 15:20:49.962000', 'address')
#         mock_cloudwatch_logs.put_log_events.assert_called_once_with(
#             logGroupName='/aws/lambda/remodelling-lambda',
#             logStreamName='lambda-log-stream',
#             logEvents=[{
#                 'timestamp': ANY,
#                 'message': 'New data returned'
#             }]
#         )
