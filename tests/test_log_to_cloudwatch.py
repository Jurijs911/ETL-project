from unittest.mock import patch
from src.ingestion_lambda.ingestion import log_to_cloudwatch
import pytest
import time


def test_log_to_cloudwatch():
    with patch('src.ingestion_lambda.ingestion.cloudwatch_logs') as mock_cloudwatch_logs:
        log_to_cloudwatch('Test message', 'Test group', 'Test stream')
        mock_cloudwatch_logs.put_log_events.assert_called_once_with(
            logGroupName='Test group',
            logStreamName='Test stream',
            logEvents=[{'timestamp': pytest.approx(int(round(time.time() * 1000)), abs=1000),
                         'message': 'Test message'}]
        )
