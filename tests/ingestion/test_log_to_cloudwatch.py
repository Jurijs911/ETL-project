from unittest.mock import patch
from src.ingestion_lambda.ingestion import log_to_cloudwatch
import pytest
import time


def test_log_to_cloudwatch():
    """
    Test the log_to_cloudwatch function.

    This function utilises the unittest.mock library to test the behavior
    of the log_to_cloudwatch function. It patches the cloudwatch_logs object
    and then calls the log_to_cloudwatch function with test data.

    Raises:
        AssertionError:
        If the assertions fail.
    """
    with patch('src.ingestion_lambda.ingestion.cloudwatch_logs')\
         as mock_cloudwatch_logs:
        log_to_cloudwatch('Test message', 'Test group', 'Test stream')
        mock_cloudwatch_logs.put_log_events.assert_called_once_with(
            logGroupName='/aws/lambda/ingestion-lambda',
            logStreamName='lambda-log-stream',
            logEvents=[
                {'timestamp': pytest.approx(
                    int(round(time.time() * 1000)), abs=1000),
                 'message': 'Test message'}]
        )
