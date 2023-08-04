from src.loading.loading import lambda_handler
from src.loading.loading_write_timestamp \
    import loading_write_timestamp
import os
from unittest.mock import patch
from moto import mock_s3, mock_logs
import boto3
from dotenv import load_dotenv
import pytest


load_dotenv()

test_user = os.environ.get("TEST_SOURCE_USER")
test_database = os.environ.get("TEST_SOURCE_DATABASE")
test_host = os.environ.get("TEST_SOURCE_HOST")
test_port = os.environ.get("TEST_SOURCE_PORT")
test_password = os.environ.get("TEST_SOURCE_PASSWORD")


# Mock all individual loading utils, and assert that they
# are being called once

# Test that error handling is successfully carried out