import os
from unittest.mock import MagicMock, patch

# Set dummy environment variables BEFORE lambda_function is imported,
# because module-level constants are computed at import time.
os.environ.setdefault('OCTOPUS_ENERGY_ACCOUNT_NUMBER', 'A-TEST1234')
os.environ.setdefault('OCTOPUS_ENERGY_API_KEY', 'test-octopus-key')
os.environ.setdefault('GIVENERGY_INVERTER_ID', 'CE1234G567')
os.environ.setdefault('GIVENERGY_API_TOKEN', 'test-givenergy-token')
os.environ.setdefault('SOLCAST_PROPERTY_ID', 'test-solcast-id')
os.environ.setdefault('SOLCAST_API_KEY', 'test-solcast-key')
os.environ.setdefault('S3_BUCKET_NAME', 'test-bucket')

# Patch boto3.client before lambda_function imports it at module level
_mock_s3 = MagicMock()
_patcher = patch('boto3.client', return_value=_mock_s3)
_patcher.start()
