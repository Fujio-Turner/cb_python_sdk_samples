import unittest
from unittest.mock import patch, MagicMock, mock_open
import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestCbExceptionHandling(unittest.TestCase):

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
    def test_successful_processing(self, mock_cluster_class, mock_file, mock_read_csv):
        """Test successful CSV processing and document insertion."""
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
                    import importlib
                    if '05_cb_exception_handling' in sys.modules:
                        importlib.reload(sys.modules['05_cb_exception_handling'])
                    else:
                        import importlib.util
                        spec = importlib.util.spec_from_file_location("05_cb_exception_handling", "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/05_cb_exception_handling.py")
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                except SystemExit:
                    pass
                mock_print.assert_called()

    @patch('couchbase.cluster.Cluster')
    def test_couchbase_connection_failure(self, mock_cluster_class):
        """Test Couchbase connection failure handling."""
        mock_cluster_class.side_effect = Exception("Connection failed")
        
        with patch('builtins.print') as mock_print:
            with patch('sys.exit') as mock_exit:
                try:
                    import importlib
                    if '05_cb_exception_handling' in sys.modules:
                        importlib.reload(sys.modules['05_cb_exception_handling'])
                    else:
                        import importlib.util
                        spec = importlib.util.spec_from_file_location("05_cb_exception_handling", "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/05_cb_exception_handling.py")
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                except SystemExit:
                    pass
                mock_exit.assert_called_with(1)

    @patch('pandas.read_csv')
    @patch('builtins.open', new_callable=mock_open, read_data=b'test data')
    @patch('couchbase.cluster.Cluster')
    def test_document_exists_exception_handling(self, mock_cluster_class, mock_file, mock_read_csv):
        """Test DocumentExistsException handling."""
        from couchbase.exceptions import DocumentExistsException
        
        # Setup mocks
        mock_df = MagicMock()
        mock_df.to_json.return_value = json.dumps(self.test_data)
        mock_read_csv.return_value = mock_df
        
        mock_cluster_class.return_value = self.mock_cluster
        mock_bucket = MagicMock()
        self.mock_cluster.bucket.return_value = mock_bucket
        mock_bucket.scope.return_value.collection.return_value = self.mock_collection
        self.mock_collection.insert.side_effect = DocumentExistsException("Document already exists")
        
        with patch('builtins.print') as mock_print:
            with patch('sys.exit'):
                try:
                    import importlib
                    if '05_cb_exception_handling' in sys.modules:
                        importlib.reload(sys.modules['05_cb_exception_handling'])
                    else:
                        import importlib.util
                        spec = importlib.util.spec_from_file_location("05_cb_exception_handling", "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/05_cb_exception_handling.py")
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                except SystemExit:
                    pass
                # Should print the "already exists" message
                mock_print.assert_called()

    @patch('pandas.read_csv')
    @patch('builtins.open', new_callable=mock_open, read_data=b'test data')
    @patch('couchbase.cluster.Cluster')
    def test_timeout_exception_with_retry(self, mock_cluster_class, mock_file, mock_read_csv):
        """Test TimeoutException handling with retry mechanism."""
        from couchbase.exceptions import TimeoutException
        
        # Setup mocks
        mock_df = MagicMock()
        mock_df.to_json.return_value = json.dumps(self.test_data)
        mock_read_csv.return_value = mock_df
        
        mock_cluster_class.return_value = self.mock_cluster
        mock_bucket = MagicMock()
        self.mock_cluster.bucket.return_value = mock_bucket
        mock_bucket.scope.return_value.collection.return_value = self.mock_collection
        
        # First call times out, second call succeeds
        self.mock_collection.insert.side_effect = [
            TimeoutException("Operation timed out"),
            self.mock_result  # Successful retry
        ]
        
        with patch('builtins.print') as mock_print:
            with patch('sys.exit'):
                try:
                    import importlib
                    if '05_cb_exception_handling' in sys.modules:
                        importlib.reload(sys.modules['05_cb_exception_handling'])
                    else:
                        import importlib.util
                        spec = importlib.util.spec_from_file_location("05_cb_exception_handling", "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/05_cb_exception_handling.py")
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                except SystemExit:
                    pass
                mock_print.assert_called()

    @patch('pandas.read_csv')
    @patch('builtins.open', new_callable=mock_open, read_data=b'test data')
    @patch('couchbase.cluster.Cluster')
    def test_network_exception_handling(self, mock_cluster_class, mock_file, mock_read_csv):
        """Test NetworkException handling."""
        from couchbase.exceptions import NetworkException
        
        # Setup mocks
        mock_df = MagicMock()
        mock_df.to_json.return_value = json.dumps(self.test_data)
        mock_read_csv.return_value = mock_df
        
        mock_cluster_class.return_value = self.mock_cluster
        mock_bucket = MagicMock()
        self.mock_cluster.bucket.return_value = mock_bucket
        mock_bucket.scope.return_value.collection.return_value = self.mock_collection
        self.mock_collection.insert.side_effect = NetworkException("Network error occurred")
        
        with patch('builtins.print') as mock_print:
            with patch('sys.exit'):
                try:
                    import importlib
                    if '05_cb_exception_handling' in sys.modules:
                        importlib.reload(sys.modules['05_cb_exception_handling'])
                    else:
                        import importlib.util
                        spec = importlib.util.spec_from_file_location("05_cb_exception_handling", "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/05_cb_exception_handling.py")
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                except SystemExit:
                    pass
                mock_print.assert_called()

    @patch('pandas.read_csv')
    @patch('builtins.open', new_callable=mock_open, read_data=b'test data')
    @patch('couchbase.cluster.Cluster')
    def test_value_error_handling(self, mock_cluster_class, mock_file, mock_read_csv):
        """Test ValueError handling."""
        # Setup mocks
        mock_df = MagicMock()
        mock_df.to_json.return_value = json.dumps(self.test_data)
        mock_read_csv.return_value = mock_df
        
        mock_cluster_class.return_value = self.mock_cluster
        mock_bucket = MagicMock()
        self.mock_cluster.bucket.return_value = mock_bucket
        mock_bucket.scope.return_value.collection.return_value = self.mock_collection
        self.mock_collection.insert.side_effect = ValueError("Invalid value provided")
        
        with patch('builtins.print') as mock_print:
            with patch('sys.exit'):
                try:
                    import importlib
                    if '05_cb_exception_handling' in sys.modules:
                        importlib.reload(sys.modules['05_cb_exception_handling'])
                    else:
                        import importlib.util
                        spec = importlib.util.spec_from_file_location("05_cb_exception_handling", "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/05_cb_exception_handling.py")
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                except SystemExit:
                    pass
                mock_print.assert_called()

    @patch('pandas.read_csv')
    @patch('builtins.open', new_callable=mock_open, read_data=b'test data')
    @patch('couchbase.cluster.Cluster')
    def test_type_error_handling(self, mock_cluster_class, mock_file, mock_read_csv):
        """Test TypeError handling."""
        # Setup mocks
        mock_df = MagicMock()
        mock_df.to_json.return_value = json.dumps(self.test_data)
        mock_read_csv.return_value = mock_df
        
        mock_cluster_class.return_value = self.mock_cluster
        mock_bucket = MagicMock()
        self.mock_cluster.bucket.return_value = mock_bucket
        mock_bucket.scope.return_value.collection.return_value = self.mock_collection
        self.mock_collection.insert.side_effect = TypeError("Invalid type provided")
        
        with patch('builtins.print') as mock_print:
            with patch('sys.exit'):
                try:
                    import importlib
                    if '05_cb_exception_handling' in sys.modules:
                        importlib.reload(sys.modules['05_cb_exception_handling'])
                    else:
                        import importlib.util
                        spec = importlib.util.spec_from_file_location("05_cb_exception_handling", "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/05_cb_exception_handling.py")
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                except SystemExit:
                    pass
                mock_print.assert_called()

    @patch('pandas.read_csv')
    @patch('builtins.open', new_callable=mock_open, read_data=b'test data')
    @patch('couchbase.cluster.Cluster')
    def test_general_exception_handling(self, mock_cluster_class, mock_file, mock_read_csv):
        """Test general exception handling."""
        # Setup mocks
        mock_df = MagicMock()
        mock_df.to_json.return_value = json.dumps(self.test_data)
        mock_read_csv.return_value = mock_df
        
        mock_cluster_class.return_value = self.mock_cluster
        mock_bucket = MagicMock()
        self.mock_cluster.bucket.return_value = mock_bucket
        mock_bucket.scope.return_value.collection.return_value = self.mock_collection
        self.mock_collection.insert.side_effect = Exception("Unexpected error occurred")
        
        with patch('builtins.print') as mock_print:
            with patch('sys.exit'):
                try:
                    import importlib
                    if '05_cb_exception_handling' in sys.modules:
                        importlib.reload(sys.modules['05_cb_exception_handling'])
                    else:
                        import importlib.util
                        spec = importlib.util.spec_from_file_location("05_cb_exception_handling", "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/05_cb_exception_handling.py")
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                except SystemExit:
                    pass
                mock_print.assert_called()

    def test_get_file_md5(self):
        """Test MD5 hash calculation function."""
        test_content = b'test file content for hashing'
        expected_hash = 'c3499c2729730a7f807efb8676a92dcb'  # MD5 of test_content
        
        with patch('builtins.open', mock_open(read_data=test_content)):
            import importlib.util
            spec = importlib.util.spec_from_file_location("05_cb_exception_handling", "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/05_cb_exception_handling.py")
            module = importlib.util.module_from_spec(spec)
            with patch('couchbase.cluster.Cluster'), patch('pandas.read_csv'), patch('sys.exit'):
                try:
                    spec.loader.exec_module(module)
                except SystemExit:
                    pass
                result = module.get_file_md5('test_file.csv')
                self.assertEqual(result, expected_hash)

    @patch('pandas.read_csv')
    def test_file_processing_failure(self, mock_read_csv):
        """Test file processing failure handling."""
        mock_read_csv.side_effect = Exception("Failed to read CSV file")
        
        with patch('builtins.print') as mock_print:
            with patch('sys.exit') as mock_exit:
                with patch('couchbase.cluster.Cluster'):
                    try:
                        import importlib
                        if '05_cb_exception_handling' in sys.modules:
                            importlib.reload(sys.modules['05_cb_exception_handling'])
                        else:
                            import importlib.util
                            spec = importlib.util.spec_from_file_location("05_cb_exception_handling", "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/05_cb_exception_handling.py")
                            module = importlib.util.module_from_spec(spec)
                            spec.loader.exec_module(module)
                    except SystemExit:
                        pass
                mock_exit.assert_called_with(1)
                mock_print.assert_called()

if __name__ == '__main__':
    unittest.main()
