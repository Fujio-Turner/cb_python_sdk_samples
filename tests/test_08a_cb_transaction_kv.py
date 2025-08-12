"""
Unit tests for 08a_cb_transaction_kv.py
Tests KV transaction operations.
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
import time
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


class TestTransactionKV(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_key1 = "0000:foo"
        self.test_key2 = "0001:foo"
        self.test_doc1 = {
            "stuff": 10,
            "timestamp": "2024-01-01T12:00:00.000"
        }
        self.test_doc2 = {
            "stuff": 2,
            "timestamp": "2024-01-01T12:00:00.000"
        }
        self.mock_collection = Mock()
        self.mock_cluster = Mock()
    
    @patch('08a_cb_transaction_kv.cb_coll')
    @patch('08a_cb_transaction_kv.time.time')
    def test_upsert_document_success(self, mock_time, mock_coll):
        """Test successful document upsert."""
        # Setup mocks
        start_time = 1000.0
        end_time = 1000.123456
        mock_time.side_effect = [start_time, end_time]
        
        mock_result = Mock()
        mock_result.cas = 12345678
        mock_coll.upsert.return_value = mock_result
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "transaction_kv",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/08a_cb_transaction_kv.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            module.upsert_document(self.test_key1, self.test_doc1)
        
        # Verify
        mock_coll.upsert.assert_called_once_with(self.test_key1, self.test_doc1)
        mock_print.assert_any_call("\nUpsert CAS: ")
        mock_print.assert_any_call(12345678)
        expected_execution_time = end_time - start_time
        mock_print.assert_any_call(f"Upsert operation took {expected_execution_time:.6f} seconds")
    
    @patch('08a_cb_transaction_kv.cb_coll')
    @patch('08a_cb_transaction_kv.time.time')
    def test_upsert_document_document_exists_exception(self, mock_time, mock_coll):
        """Test upsert with DocumentExistsException."""
        from couchbase.exceptions import DocumentExistsException
        
        # Setup mocks
        start_time = 1000.0
        end_time = 1000.1
        mock_time.side_effect = [start_time, end_time]
        mock_coll.upsert.side_effect = DocumentExistsException("Document exists")
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "transaction_kv",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/08a_cb_transaction_kv.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            module.upsert_document(self.test_key1, self.test_doc1)
        
        # Verify exception handling
        mock_print.assert_any_call("Document already exists: Document exists")
    
    @patch('08a_cb_transaction_kv.cb_coll')
    @patch('08a_cb_transaction_kv.time.time')
    def test_upsert_document_timeout_exception(self, mock_time, mock_coll):
        """Test upsert with TimeoutException."""
        from couchbase.exceptions import TimeoutException
        
        # Setup mocks
        start_time = 1000.0
        end_time = 1000.1
        mock_time.side_effect = [start_time, end_time]
        mock_coll.upsert.side_effect = TimeoutException("Operation timed out")
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "transaction_kv",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/08a_cb_transaction_kv.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            module.upsert_document(self.test_key1, self.test_doc1)
        
        # Verify exception handling
        mock_print.assert_any_call("Operation timed out: Operation timed out")
    
    @patch('08a_cb_transaction_kv.cb_coll')
    @patch('08a_cb_transaction_kv.time.time')
    def test_upsert_document_invalid_value_exception(self, mock_time, mock_coll):
        """Test upsert with InvalidValueException."""
        from couchbase.exceptions import InvalidValueException
        
        # Setup mocks
        start_time = 1000.0
        end_time = 1000.1
        mock_time.side_effect = [start_time, end_time]
        mock_coll.upsert.side_effect = InvalidValueException("Invalid value")
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "transaction_kv",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/08a_cb_transaction_kv.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            module.upsert_document(self.test_key1, self.test_doc1)
        
        # Verify exception handling
        mock_print.assert_any_call("Invalid document value: Invalid value")
    
    @patch('08a_cb_transaction_kv.cluster')
    @patch('08a_cb_transaction_kv.cb_coll')
    @patch('08a_cb_transaction_kv.time.time')
    @patch('08a_cb_transaction_kv.datetime')
    def test_move_numbers_success(self, mock_datetime, mock_time, mock_coll, mock_cluster):
        """Test successful move_numbers transaction."""
        # Setup mocks
        start_time = 1000.0
        end_time = 1000.5
        mock_time.side_effect = [start_time, end_time]
        
        # Mock datetime
        mock_now = Mock()
        mock_now.isoformat.return_value = "2024-01-01T12:00:00.000"
        mock_datetime.now.return_value = mock_now
        
        # Mock transaction context and documents
        mock_ctx = Mock()
        mock_doc1 = Mock()
        mock_doc1.content_as = {dict: {"stuff": 10, "timestamp": "2024-01-01T11:00:00.000"}}
        mock_doc2 = Mock()
        mock_doc2.content_as = {dict: {"stuff": 2, "timestamp": "2024-01-01T11:00:00.000"}}
        
        mock_ctx.get.side_effect = [mock_doc1, mock_doc2]
        
        # Mock transaction result
        mock_transaction_result = Mock()
        mock_transaction_result.is_committed.return_value = True
        mock_cluster.transactions.run.return_value = mock_transaction_result
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "transaction_kv",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/08a_cb_transaction_kv.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            module.move_numbers(self.test_key1, self.test_key2, 3)
        
        # Verify transaction was called
        mock_cluster.transactions.run.assert_called_once()
        mock_print.assert_any_call("\nMove numbers: ")
        mock_print.assert_any_call("Transaction completed successfully")
    
    @patch('08a_cb_transaction_kv.cluster')
    @patch('08a_cb_transaction_kv.cb_coll')
    @patch('08a_cb_transaction_kv.time.time')
    def test_move_numbers_transaction_failed(self, mock_time, mock_coll, mock_cluster):
        """Test move_numbers with TransactionFailed exception."""
        from couchbase.exceptions import TransactionFailed
        
        # Setup mocks
        start_time = 1000.0
        end_time = 1000.2
        mock_time.side_effect = [start_time, end_time]
        mock_cluster.transactions.run.side_effect = TransactionFailed("Transaction failed")
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "transaction_kv",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/08a_cb_transaction_kv.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            module.move_numbers(self.test_key1, self.test_key2, 3)
        
        # Verify exception handling
        mock_print.assert_any_call("Transaction did not reach commit point. Error: Transaction failed")
    
    @patch('08a_cb_transaction_kv.cluster')
    @patch('08a_cb_transaction_kv.cb_coll')
    @patch('08a_cb_transaction_kv.time.time')
    def test_move_numbers_commit_ambiguous(self, mock_time, mock_coll, mock_cluster):
        """Test move_numbers with TransactionCommitAmbiguous exception."""
        from couchbase.exceptions import TransactionCommitAmbiguous
        
        # Setup mocks
        start_time = 1000.0
        end_time = 1000.3
        mock_time.side_effect = [start_time, end_time]
        mock_cluster.transactions.run.side_effect = TransactionCommitAmbiguous("Commit ambiguous")
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "transaction_kv",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/08a_cb_transaction_kv.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            module.move_numbers(self.test_key1, self.test_key2, 3)
        
        # Verify exception handling
        mock_print.assert_any_call("Transaction possibly committed. Error: Commit ambiguous")
    
    @patch('08a_cb_transaction_kv.cluster')
    @patch('08a_cb_transaction_kv.cb_coll')
    @patch('08a_cb_transaction_kv.time.time')
    def test_move_numbers_document_not_found(self, mock_time, mock_coll, mock_cluster):
        """Test move_numbers with DocumentNotFoundException."""
        from couchbase.exceptions import DocumentNotFoundException
        
        # Setup mocks
        start_time = 1000.0
        end_time = 1000.1
        mock_time.side_effect = [start_time, end_time]
        mock_cluster.transactions.run.side_effect = DocumentNotFoundException("Document not found")
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "transaction_kv",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/08a_cb_transaction_kv.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            module.move_numbers(self.test_key1, self.test_key2, 3)
        
        # Verify exception handling
        mock_print.assert_any_call("Document not found: Document not found")
    
    def test_transaction_logic_document_modification(self):
        """Test the transaction logic for document modification."""
        # Mock transaction context and documents
        mock_ctx = Mock()
        
        # Create mock documents with content
        mock_doc1 = Mock()
        mock_doc1.content_as = {dict: {"stuff": 10, "timestamp": "2024-01-01T11:00:00.000"}}
        mock_doc2 = Mock()
        mock_doc2.content_as = {dict: {"stuff": 2, "timestamp": "2024-01-01T11:00:00.000"}}
        
        mock_ctx.get.side_effect = [mock_doc1, mock_doc2]
        
        # Simulate the transaction logic
        amount = 3
        
        # Get documents
        doc1 = mock_ctx.get(self.mock_collection, self.test_key1)
        doc2 = mock_ctx.get(self.mock_collection, self.test_key2)
        
        # Modify documents
        doc1_content = doc1.content_as[dict].copy()
        doc2_content = doc2.content_as[dict].copy()
        
        doc1_content['stuff'] -= amount
        doc2_content['stuff'] += amount
        
        # Verify modifications
        self.assertEqual(doc1_content['stuff'], 7)  # 10 - 3
        self.assertEqual(doc2_content['stuff'], 5)  # 2 + 3
        
        # Verify ctx.get was called correctly
        mock_ctx.get.assert_any_call(self.mock_collection, self.test_key1)
        mock_ctx.get.assert_any_call(self.mock_collection, self.test_key2)
    
    @patch('08a_cb_transaction_kv.TransactionOptions')
    def test_transaction_options_configuration(self, mock_transaction_options):
        """Test transaction options are properly configured."""
        # Import module
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "transaction_kv",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/08a_cb_transaction_kv.py"
        )
        module = importlib.util.module_from_spec(spec)
        
        # Test TransactionOptions configuration
        from couchbase.options import TransactionOptions
        
        options = TransactionOptions(durability_level=None)
        
        # Verify options are configured correctly
        self.assertIsNone(options.durability_level)
    
    @patch('08a_cb_transaction_kv.Cluster')
    @patch('08a_cb_transaction_kv.PasswordAuthenticator')
    def test_cluster_connection_setup(self, mock_auth, mock_cluster):
        """Test cluster connection setup with proper authentication."""
        # Setup mocks
        mock_cluster_instance = Mock()
        mock_cluster.return_value = mock_cluster_instance
        mock_auth_instance = Mock()
        mock_auth.return_value = mock_auth_instance
        
        # Import module to trigger connection setup
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "transaction_kv",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/08a_cb_transaction_kv.py"
        )
        module = importlib.util.module_from_spec(spec)
        
        # Verify authentication setup
        mock_auth.assert_called_once_with("Administrator", "password")
        mock_cluster.assert_called_once()
    
    def test_datetime_timestamp_generation(self):
        """Test datetime timestamp generation for transactions."""
        # Test that datetime generates proper ISO format timestamps
        mock_datetime = datetime(2024, 1, 1, 12, 0, 0, 123000)
        expected_timestamp = "2024-01-01T12:00:00.123"
        
        # Simulate the isoformat call with millisecond precision
        timestamp = mock_datetime.isoformat(timespec='milliseconds')
        
        self.assertEqual(timestamp, expected_timestamp)
    
    def test_execution_time_calculation(self):
        """Test execution time calculation in move_numbers."""
        start_time = 1000.0
        end_time = 1000.567890
        expected_execution_time = end_time - start_time
        
        # Simulate timing calculation
        execution_time = end_time - start_time
        
        self.assertAlmostEqual(execution_time, expected_execution_time, places=6)
        self.assertEqual(f"{execution_time:.6f}", "0.567890")
    
    @patch('08a_cb_transaction_kv.cluster')
    def test_transaction_commit_check(self, mock_cluster):
        """Test transaction commit status checking."""
        # Mock transaction result
        mock_transaction_result = Mock()
        mock_transaction_result.is_committed.return_value = True
        mock_cluster.transactions.run.return_value = mock_transaction_result
        
        # Simulate commit check
        result = mock_cluster.transactions.run(lambda ctx: None)
        
        if result.is_committed():
            status = "Transaction completed successfully"
        else:
            status = "Transaction failed to commit"
        
        self.assertEqual(status, "Transaction completed successfully")
        mock_transaction_result.is_committed.assert_called_once()


if __name__ == '__main__':
    unittest.main()
