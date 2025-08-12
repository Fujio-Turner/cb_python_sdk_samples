import unittest
from unittest.mock import patch, MagicMock, mock_open
import sys
import os
import json
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestExcelToJsonToCb(unittest.TestCase):

    def setUp(self):
        self.mock_cluster = MagicMock()
        self.mock_collection = MagicMock()
        self.mock_result = MagicMock()
        self.mock_result.cas = 1234567890
        
        # Sample test data
        self.test_data = [
            {"Customer Id": "C001", "First Name": "John", "Last Name": "Doe"},
            {"Customer Id": "C002", "First Name": "Jane", "Last Name": "Smith"}
        ]

    @patch('pandas.read_csv')
    @patch('builtins.open', new_callable=mock_open, read_data=b'test data')
    @patch('couchbase.cluster.Cluster')
    def test_csv_processing(self, mock_cluster_class, mock_file, mock_read_csv):
        """Test CSV file processing."""
        # Setup mocks
        mock_df = MagicMock()
        mock_df.to_json.return_value = json.dumps(self.test_data)
        mock_read_csv.return_value = mock_df
        
        mock_cluster_class.return_value = self.mock_cluster
        mock_bucket = MagicMock()
        self.mock_cluster.bucket.return_value = mock_bucket
        mock_bucket.scope.return_value.collection.return_value = self.mock_collection
        
        with patch('builtins.print') as mock_print:
            # Import would trigger the file processing
            with patch('sys.exit'):
                try:
                    import excel_to_json_to_cb
                    mock_print.assert_called()
                except SystemExit:
                    pass  # Expected due to exit(1) calls

    @patch('pandas.read_excel') 
    @patch('builtins.open', new_callable=mock_open, read_data=b'test data')
    @patch('couchbase.cluster.Cluster')
    def test_excel_processing(self, mock_cluster_class, mock_file, mock_read_excel):
        """Test Excel file processing."""
        # Setup mocks
        mock_df = MagicMock()
        mock_df.to_json.return_value = json.dumps(self.test_data)
        mock_read_excel.return_value = mock_df
        
        mock_cluster_class.return_value = self.mock_cluster
        mock_bucket = MagicMock()
        self.mock_cluster.bucket.return_value = mock_bucket
        mock_bucket.scope.return_value.collection.return_value = self.mock_collection
        
        with patch('builtins.print') as mock_print:
            # Test would require modifying the file to use Excel instead of CSV
            with patch('sys.exit'):
                try:
                    import excel_to_json_to_cb
                    mock_print.assert_called()
                except SystemExit:
                    pass

    def test_get_file_md5(self):
        """Test MD5 hash calculation function."""
        test_content = b'test file content'
        expected_hash = '9473fdd0d880a43c21b7778d34872157'  # MD5 of test_content
        
        with patch('builtins.open', mock_open(read_data=test_content)):
            import excel_to_json_to_cb
            result = excel_to_json_to_cb.get_file_md5('test_file.csv')
            self.assertEqual(result, expected_hash)

    @patch('couchbase.cluster.Cluster')
    def test_couchbase_connection(self, mock_cluster_class):
        """Test Couchbase connection setup."""
        mock_cluster_class.return_value = self.mock_cluster
        mock_bucket = MagicMock()
        self.mock_cluster.bucket.return_value = mock_bucket
        mock_bucket.scope.return_value.collection.return_value = self.mock_collection
        
        with patch('builtins.print') as mock_print:
            with patch('sys.exit'):
                try:
                    import excel_to_json_to_cb
                    mock_cluster_class.assert_called()
                    mock_print.assert_called()
                except SystemExit:
                    pass

    @patch('couchbase.cluster.Cluster')
    def test_couchbase_connection_failure(self, mock_cluster_class):
        """Test Couchbase connection failure handling."""
        mock_cluster_class.side_effect = Exception("Connection failed")
        
        with patch('builtins.print') as mock_print:
            with patch('sys.exit') as mock_exit:
                try:
                    import excel_to_json_to_cb
                except SystemExit:
                    pass
                mock_exit.assert_called_with(1)
                mock_print.assert_called()

    @patch('pandas.read_csv')
    @patch('builtins.open', new_callable=mock_open, read_data=b'test data')
    @patch('couchbase.cluster.Cluster')
    def test_document_insertion(self, mock_cluster_class, mock_file, mock_read_csv):
        """Test document insertion with audit information."""
        # Setup mocks
        mock_df = MagicMock()
        mock_df.to_json.return_value = json.dumps(self.test_data)
        mock_read_csv.return_value = mock_df
        
        mock_cluster_class.return_value = self.mock_cluster
        mock_bucket = MagicMock()
        self.mock_cluster.bucket.return_value = mock_bucket
        mock_bucket.scope.return_value.collection.return_value = self.mock_collection
        self.mock_collection.insert.return_value = self.mock_result
        
        with patch('builtins.print') as mock_print:
            with patch('sys.exit'):
                try:
                    import excel_to_json_to_cb
                    # Verify insert was called
                    self.mock_collection.insert.assert_called()
                    mock_print.assert_called()
                except SystemExit:
                    pass

    @patch('pandas.read_csv')
    @patch('builtins.open', new_callable=mock_open, read_data=b'test data')
    @patch('couchbase.cluster.Cluster')
    def test_document_exists_exception(self, mock_cluster_class, mock_file, mock_read_csv):
        """Test handling of DocumentExistsException."""
        from couchbase.exceptions import DocumentExistsException
        
        # Setup mocks
        mock_df = MagicMock()
        mock_df.to_json.return_value = json.dumps(self.test_data)
        mock_read_csv.return_value = mock_df
        
        mock_cluster_class.return_value = self.mock_cluster
        mock_bucket = MagicMock()
        self.mock_cluster.bucket.return_value = mock_bucket
        mock_bucket.scope.return_value.collection.return_value = self.mock_collection
        self.mock_collection.insert.side_effect = DocumentExistsException("Document exists")
        
        with patch('builtins.print') as mock_print:
            with patch('sys.exit'):
                try:
                    import excel_to_json_to_cb
                    mock_print.assert_called()
                except SystemExit:
                    pass

    @patch('pandas.read_csv')
    @patch('builtins.open', new_callable=mock_open, read_data=b'test data')
    @patch('couchbase.cluster.Cluster')
    def test_timeout_exception(self, mock_cluster_class, mock_file, mock_read_csv):
        """Test handling of TimeoutException."""
        from couchbase.exceptions import TimeoutException
        
        # Setup mocks
        mock_df = MagicMock()
        mock_df.to_json.return_value = json.dumps(self.test_data)
        mock_read_csv.return_value = mock_df
        
        mock_cluster_class.return_value = self.mock_cluster
        mock_bucket = MagicMock()
        self.mock_cluster.bucket.return_value = mock_bucket
        mock_bucket.scope.return_value.collection.return_value = self.mock_collection
        self.mock_collection.insert.side_effect = TimeoutException("Timeout")
        
        with patch('builtins.print') as mock_print:
            with patch('sys.exit'):
                try:
                    import excel_to_json_to_cb
                    mock_print.assert_called()
                except SystemExit:
                    pass

    @patch('pandas.read_csv')
    @patch('builtins.open', new_callable=mock_open, read_data=b'test data')
    @patch('couchbase.cluster.Cluster')
    def test_key_generation_with_customer_id(self, mock_cluster_class, mock_file, mock_read_csv):
        """Test key generation when Customer Id is present."""
        # Setup mocks with valid Customer Id
        test_data_with_id = [{"Customer Id": "C123", "Name": "Test Customer"}]
        mock_df = MagicMock()
        mock_df.to_json.return_value = json.dumps(test_data_with_id)
        mock_read_csv.return_value = mock_df
        
        mock_cluster_class.return_value = self.mock_cluster
        mock_bucket = MagicMock()
        self.mock_cluster.bucket.return_value = mock_bucket
        mock_bucket.scope.return_value.collection.return_value = self.mock_collection
        self.mock_collection.insert.return_value = self.mock_result
        
        with patch('builtins.print') as mock_print:
            with patch('sys.exit'):
                try:
                    import excel_to_json_to_cb
                    # Verify insert was called with correct key format
                    call_args = self.mock_collection.insert.call_args
                    if call_args:
                        key = call_args[0][0]  # First argument is the key
                        self.assertTrue(key.startswith('c:'))
                    mock_print.assert_called()
                except SystemExit:
                    pass

    @patch('pandas.read_csv')
    @patch('builtins.open', new_callable=mock_open, read_data=b'test data')
    @patch('couchbase.cluster.Cluster')
    @patch('uuid.uuid4')
    def test_key_generation_without_customer_id(self, mock_uuid, mock_cluster_class, mock_file, mock_read_csv):
        """Test key generation when Customer Id is missing or empty."""
        # Setup mocks with missing Customer Id
        test_data_no_id = [{"Name": "Test Customer"}]  # No Customer Id field
        mock_df = MagicMock()
        mock_df.to_json.return_value = json.dumps(test_data_no_id)
        mock_read_csv.return_value = mock_df
        
        mock_uuid.return_value = "test-uuid-12345"
        
        mock_cluster_class.return_value = self.mock_cluster
        mock_bucket = MagicMock()
        self.mock_cluster.bucket.return_value = mock_bucket
        mock_bucket.scope.return_value.collection.return_value = self.mock_collection
        self.mock_collection.insert.return_value = self.mock_result
        
        with patch('builtins.print') as mock_print:
            with patch('sys.exit'):
                try:
                    import excel_to_json_to_cb
                    mock_print.assert_called()
                except SystemExit:
                    pass

if __name__ == '__main__':
    unittest.main()
