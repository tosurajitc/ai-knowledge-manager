# Write the test file
cat > tests/test_data_lake.py << 'EOF'
"""
Unit tests for the S3 Data Lake implementation.

This module contains tests for the S3DataLake class, the DataLakeInterface,
and the S3AccessControl classes.
"""

import os
import unittest
from unittest.mock import patch, MagicMock
import boto3
from botocore.exceptions import ClientError

from src.data_ingestion.data_lake import S3DataLake
from src.data_ingestion.file_utils import S3FileUtils
from src.data_ingestion.data_lake_interface import DataLakeInterface
from src.data_ingestion.s3_access_control import S3AccessControl

class TestS3DataLake(unittest.TestCase):
    """Test cases for the S3DataLake class."""
    
    def setUp(self):
        """Set up test environment."""
        # Mock the boto3 session and clients
        self.mock_session_patcher = patch('boto3.Session')
        self.mock_session = self.mock_session_patcher.start()
        
        # Mock S3 client
        self.mock_s3 = MagicMock()
        self.mock_session.return_value.client.return_value = self.mock_s3
        self.mock_session.return_value.resource.return_value = MagicMock()
        
        # Create a test instance
        self.data_lake = S3DataLake('test-bucket')
    
    def tearDown(self):
        """Clean up after tests."""
        self.mock_session_patcher.stop()
    
    def test_ensure_data_lake_exists(self):
        """Test _ensure_data_lake_exists method."""
        # Mock head_bucket response
        self.mock_s3.head_bucket.return_value = {}
        
        # Call the method
        self.data_lake._ensure_data_lake_exists()
        
        # Assert that put_object was called for each zone
        self.assertEqual(self.mock_s3.put_object.call_count, 4)

    def test_upload_file(self):
        """Test upload_file method."""
        # Call the method with test parameters
        result = self.data_lake.upload_file('test.txt', 'raw')
        
        # Assert that upload_file was called with correct parameters
        self.mock_s3.upload_file.assert_called_once()
        
        # Assert result is True
        self.assertTrue(result)

    def test_list_files(self):
        """Test list_files method."""
        # Mock paginator and response
        mock_paginator = MagicMock()
        self.mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                'Contents': [
                    {'Key': 'raw/file1.txt', 'Size': 100, 'LastModified': '2023-01-01'}
                ]
            }
        ]
        
        # Call the method
        result = self.data_lake.list_files()
        
        # Assert result contains expected files
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['key'], 'raw/file1.txt')

if __name__ == '__main__':
    unittest.main()
EOF
echo "Created test_data_lake.py"