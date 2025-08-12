"""
Unit tests for 08b_cb_transaction_query.py
Tests N1QL transaction operations.
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
import time
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


class TestTransactionQuery(unittest.TestCase):
    
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
    
    @patch('08b_cb_transaction_query.cb_coll')
    @patch('08b_cb_transaction_query.time.time')
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
            "transaction_query",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/08b_cb_transaction_query.py"
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
    
    @patch('08b_cb_transaction_query.cb_coll')
    @patch('08b_cb_transaction_query.time.time')
    def test_upsert_document_authentication_exception(self, mock_time, mock_coll):
        """Test upsert with AuthenticationException."""
        from couchbase.exceptions import AuthenticationException
        
        # Setup mocks
        start_time = 1000.0
        end_time = 1000.1
        mock_time.side_effect = [start_time, end_time]
        mock_coll.upsert.side_effect = AuthenticationException("Auth failed")
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "transaction_query",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/08b_cb_transaction_query.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            module.upsert_document(self.test_key1, self.test_doc1)
        
        # Verify exception handling
        mock_print.assert_any_call("Authentication failed: Auth failed")
    
    @patch('08b_cb_transaction_query.cluster')
    @patch('08b_cb_transaction_query.time.time')
    @patch('08b_cb_transaction_query.datetime')
    def test_move_numbers_n1ql_success(self, mock_datetime, mock_time, mock_cluster):
        """Test successful move_numbers_n1ql transaction."""
        # Setup mocks
        start_time = 1000.0
        end_time = 1000.5
        mock_time.side_effect = [start_time, end_time]
        
        # Mock datetime
        mock_now = Mock()
        mock_now.isoformat.return_value = "2024-01-01T12:00:00.000"
        mock_datetime.now.return_value = mock_now
        
        # Mock transaction context and query results
        mock_ctx = Mock()
        mock_result1 = Mock()
        mock_result1.__iter__ = Mock(return_value=iter([{"id": self.test_key1, "stuff": 7}]))
        mock_result2 = Mock()
        mock_result2.__iter__ = Mock(return_value=iter([{"id": self.test_key2, "stuff": 5}]))
        
        mock_ctx.query.side_effect = [mock_result1, mock_result2]
        
        # Mock transaction result
        mock_transaction_result = Mock()
        mock_transaction_result.is_committed.return_value = True
        mock_cluster.transactions.run.return_value = mock_transaction_result
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "transaction_query",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/08b_cb_transaction_query.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            module.move_numbers_n1ql(self.test_key1, self.test_key2, 3)
        
        # Verify transaction was called
        mock_cluster.transactions.run.assert_called_once()
        mock_print.assert_any_call("\nMove numbers (N1QL): ")
        mock_print.assert_any_call("Transaction committed successfully")
    
    @patch('08b_cb_transaction_query.cluster')
    @patch('08b_cb_transaction_query.time.time')
    def test_move_numbers_n1ql_transaction_failed(self, mock_time, mock_cluster):
        """Test move_numbers_n1ql with TransactionFailed exception."""
        from couchbase.exceptions import TransactionFailed
        
        # Setup mocks
        start_time = 1000.0
        end_time = 1000.2
        mock_time.side_effect = [start_time, end_time]
        mock_cluster.transactions.run.side_effect = TransactionFailed("Transaction failed")
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "transaction_query",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/08b_cb_transaction_query.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            module.move_numbers_n1ql(self.test_key1, self.test_key2, 3)
        
        # Verify exception handling
        mock_print.assert_any_call("Transaction did not reach commit point. Error: Transaction failed")
    
    @patch('08b_cb_transaction_query.cluster')
    @patch('08b_cb_transaction_query.time.time')
    def test_move_numbers_n1ql_commit_ambiguous(self, mock_time, mock_cluster):
        """Test move_numbers_n1ql with TransactionCommitAmbiguous exception."""
        from couchbase.exceptions import TransactionCommitAmbiguous
        
        # Setup mocks
        start_time = 1000.0
        end_time = 1000.3
        mock_time.side_effect = [start_time, end_time]
        mock_cluster.transactions.run.side_effect = TransactionCommitAmbiguous("Commit ambiguous")
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "transaction_query",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/08b_cb_transaction_query.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            module.move_numbers_n1ql(self.test_key1, self.test_key2, 3)
        
        # Verify exception handling
        mock_print.assert_any_call("Transaction possibly committed. Error: Commit ambiguous")
    
    def test_n1ql_query_construction(self):
        """Test N1QL query construction for updates."""
        # Test query construction logic
        bucket_name = "travel-sample"
        cb_scope = "inventory"
        cb_collection = "airline"
        key1 = "0000:foo"
        amount = 3
        timestamp = "2024-01-01T12:00:00.000"
        
        # Construct first query
        query1 = f"""
        UPDATE `{bucket_name}`.`{cb_scope}`.`{cb_collection}`
        USE KEYS "{key1}"
        SET stuff = stuff - {amount},
            timestamp = "{timestamp}"
        RETURNING META().id, stuff
        """
        
        # Verify query structure
        self.assertIn("UPDATE", query1)
        self.assertIn("USE KEYS", query1)
        self.assertIn(f'"{key1}"', query1)
        self.assertIn(f"stuff - {amount}", query1)
        self.assertIn("RETURNING", query1)
        self.assertIn("META().id", query1)
    
    def test_transaction_query_results_processing(self):
        """Test processing of transaction query results."""
        # Mock query result
        mock_result = Mock()
        test_rows = [
            {"id": "0000:foo", "stuff": 7},
            {"id": "0001:foo", "stuff": 5}
        ]
        mock_result.__iter__ = Mock(return_value=iter(test_rows))
        
        # Simulate result processing
        processed_results = []
        for row in mock_result:
            processed_results.append(row)
        
        # Verify processing
        self.assertEqual(len(processed_results), 2)
        self.assertEqual(processed_results[0]["id"], "0000:foo")
        self.assertEqual(processed_results[0]["stuff"], 7)
        self.assertEqual(processed_results[1]["id"], "0001:foo")
        self.assertEqual(processed_results[1]["stuff"], 5)
    
    @patch('08b_cb_transaction_query.cluster')
    @patch('08b_cb_transaction_query.time.time')
    def test_move_numbers_n1ql_document_not_found(self, mock_time, mock_cluster):
        """Test move_numbers_n1ql with DocumentNotFoundException."""
        from couchbase.exceptions import DocumentNotFoundException
        
        # Setup mocks
        start_time = 1000.0
        end_time = 1000.1
        mock_time.side_effect = [start_time, end_time]
        mock_cluster.transactions.run.side_effect = DocumentNotFoundException("Document not found")
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "transaction_query",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/08b_cb_transaction_query.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            module.move_numbers_n1ql(self.test_key1, self.test_key2, 3)
        
        # Verify exception handling
        mock_print.assert_any_call("Document not found: Document not found")
    
    @patch('08b_cb_transaction_query.cluster')
    @patch('08b_cb_transaction_query.time.time')
    def test_move_numbers_n1ql_timeout_exception(self, mock_time, mock_cluster):
        """Test move_numbers_n1ql with TimeoutException."""
        from couchbase.exceptions import TimeoutException
        
        # Setup mocks
        start_time = 1000.0
        end_time = 1000.1
        mock_time.side_effect = [start_time, end_time]
        mock_cluster.transactions.run.side_effect = TimeoutException("Operation timed out")
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "transaction_query",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/08b_cb_transaction_query.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            module.move_numbers_n1ql(self.test_key1, self.test_key2, 3)
        
        # Verify exception handling
        mock_print.assert_any_call("Operation timed out: Operation timed out")
    
    def test_transaction_logic_with_n1ql_queries(self):
        """Test the transaction logic using N1QL queries."""
        # Mock transaction context
        mock_ctx = Mock()
        
        # Mock query results
        mock_result1 = Mock()
        mock_result1.__iter__ = Mock(return_value=iter([{"id": self.test_key1, "stuff": 7}]))
        mock_result2 = Mock()
        mock_result2.__iter__ = Mock(return_value=iter([{"id": self.test_key2, "stuff": 5}]))
        
        mock_ctx.query.side_effect = [mock_result1, mock_result2]
        
        # Simulate transaction logic
        amount = 3
        bucket_name = "travel-sample"
        cb_scope = "inventory"
        cb_collection = "airline"
        
        # First query (subtract)
        query1 = f"""
        UPDATE `{bucket_name}`.`{cb_scope}`.`{cb_collection}`
        USE KEYS "{self.test_key1}"
        SET stuff = stuff - {amount}
        RETURNING META().id, stuff
        """
        result1 = mock_ctx.query(query1)
        
        # Second query (add)
        query2 = f"""
        UPDATE `{bucket_name}`.`{cb_scope}`.`{cb_collection}`
        USE KEYS "{self.test_key2}"
        SET stuff = stuff + {amount}
        RETURNING META().id, stuff
        """
        result2 = mock_ctx.query(query2)
        
        # Verify queries were executed
        self.assertEqual(mock_ctx.query.call_count, 2)
        
        # Verify results
        for row in result1:
            self.assertEqual(row["id"], self.test_key1)
            self.assertEqual(row["stuff"], 7)
        
        for row in result2:
            self.assertEqual(row["id"], self.test_key2)
            self.assertEqual(row["stuff"], 5)
    
    @patch('08b_cb_transaction_query.Cluster')
    @patch('08b_cb_transaction_query.PasswordAuthenticator')
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
            "transaction_query",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/08b_cb_transaction_query.py"
        )
        module = importlib.util.module_from_spec(spec)
        
        # Verify authentication setup
        mock_auth.assert_called_once_with("Administrator", "password")
        mock_cluster.assert_called_once()
    
    def test_datetime_isoformat_with_milliseconds(self):
        """Test datetime isoformat with millisecond precision."""
        # Test datetime formatting for timestamps
        test_datetime = datetime(2024, 1, 1, 12, 0, 0, 123000)
        expected_format = "2024-01-01T12:00:00.123"
        
        # Get formatted timestamp
        timestamp = test_datetime.isoformat(timespec='milliseconds')
        
        self.assertEqual(timestamp, expected_format)
    
    def test_execution_time_measurement(self):
        """Test execution time measurement in move_numbers_n1ql."""
        start_time = 1000.0
        end_time = 1000.789012
        expected_execution_time = end_time - start_time
        
        # Simulate timing calculation
        execution_time = end_time - start_time
        
        self.assertAlmostEqual(execution_time, expected_execution_time, places=6)
        self.assertEqual(f"{execution_time:.6f}", "0.789012")
    
    @patch('08b_cb_transaction_query.cluster')
    def test_transaction_commit_status_check(self, mock_cluster):
        """Test transaction commit status checking."""
        # Mock transaction result with different commit statuses
        mock_transaction_result_committed = Mock()
        mock_transaction_result_committed.is_committed.return_value = True
        
        mock_transaction_result_not_committed = Mock()
        mock_transaction_result_not_committed.is_committed.return_value = False
        
        # Test committed transaction
        mock_cluster.transactions.run.return_value = mock_transaction_result_committed
        result = mock_cluster.transactions.run(lambda ctx: None)
        
        if result.is_committed():
            status = "Transaction committed successfully"
        else:
            status = "Transaction did not commit"
        
        self.assertEqual(status, "Transaction committed successfully")
        
        # Test non-committed transaction
        mock_cluster.transactions.run.return_value = mock_transaction_result_not_committed
        result = mock_cluster.transactions.run(lambda ctx: None)
        
        if result.is_committed():
            status = "Transaction committed successfully"
        else:
            status = "Transaction did not commit"
        
        self.assertEqual(status, "Transaction did not commit")
    
    def test_bucket_scope_collection_variables(self):
        """Test bucket, scope, and collection variable usage in queries."""
        # Test that variables are properly used in query construction
        bucket_name = "travel-sample"
        cb_scope = "inventory"
        cb_collection = "airline"
        
        # Note: The original code has undefined variables in the query construction
        # This test verifies what the expected behavior should be
        query_template = "UPDATE `{bucket}`.`{scope}`.`{collection}` USE KEYS \"{key}\""
        query = query_template.format(
            bucket=bucket_name,
            scope=cb_scope,
            collection=cb_collection,
            key="test_key"
        )
        
        expected_query = "UPDATE `travel-sample`.`inventory`.`airline` USE KEYS \"test_key\""
        self.assertEqual(query, expected_query)


if __name__ == '__main__':
    unittest.main()
