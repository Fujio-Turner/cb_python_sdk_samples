"""
Unit tests for 06_cb_get_retry_replica_read.py
Tests retry logic and replica read functionality.
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
from datetime import timedelta
import time

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


class TestGetRetryReplicaRead(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_key = "airline_8091"
        self.test_timeout = 5
        self.mock_collection = Mock()
        self.mock_result = Mock()
        self.mock_result.content_as = {str: '{"name": "Test Airline", "type": "airline"}'}
        self.mock_result.cas = 12345678
    
    @patch('06_cb_get_retry_replica_read.cb_coll')
    @patch('06_cb_get_retry_replica_read.time.time')
    def test_get_airline_by_key_success_first_attempt(self, mock_time, mock_coll):
        """Test successful document retrieval on first attempt."""
        # Setup mocks
        mock_time.side_effect = [1000.0, 1000.5]  # start and end times
        mock_coll.get.return_value = self.mock_result
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "retry_replica_read",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/06_cb_get_retry_replica_read.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            result = module.get_airline_by_key(self.test_key, self.test_timeout)
        
        # Verify
        mock_coll.get.assert_called_once_with(self.test_key, timeout=timedelta(seconds=self.test_timeout))
        self.assertEqual(result, self.mock_result)
        mock_print.assert_any_call("\nGet Result: ")
        mock_print.assert_any_call('{"name": "Test Airline", "type": "airline"}')
        mock_print.assert_any_call("CAS:", 12345678)
    
    @patch('06_cb_get_retry_replica_read.cb_coll')
    @patch('06_cb_get_retry_replica_read.time.time')
    def test_get_airline_by_key_document_not_found(self, mock_time, mock_coll):
        """Test DocumentNotFoundException handling."""
        from couchbase.exceptions import DocumentNotFoundException
        
        # Setup mocks
        mock_time.side_effect = [1000.0, 1000.1]
        mock_coll.get.side_effect = DocumentNotFoundException("Document not found")
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "retry_replica_read",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/06_cb_get_retry_replica_read.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            result = module.get_airline_by_key(self.test_key, self.test_timeout)
        
        # Verify
        mock_coll.get.assert_called_once_with(self.test_key, timeout=timedelta(seconds=self.test_timeout))
        self.assertIsNone(result)
        mock_print.assert_any_call("Document not found: Document not found")
    
    @patch('06_cb_get_retry_replica_read.cb_coll')
    @patch('06_cb_get_retry_replica_read.time.time')
    def test_get_airline_by_key_timeout_with_retries(self, mock_time, mock_coll):
        """Test TimeoutException with retries before success."""
        from couchbase.exceptions import TimeoutException
        
        # Setup mocks
        mock_time.side_effect = [1000.0, 1001.0]
        # First 2 attempts timeout, third succeeds
        mock_coll.get.side_effect = [
            TimeoutException("Timeout 1"),
            TimeoutException("Timeout 2"),
            self.mock_result
        ]
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "retry_replica_read",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/06_cb_get_retry_replica_read.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            result = module.get_airline_by_key(self.test_key, self.test_timeout)
        
        # Verify retry attempts
        self.assertEqual(mock_coll.get.call_count, 3)
        self.assertEqual(result, self.mock_result)
        mock_print.assert_any_call("Attempt 1 failed: Timeout 1. Retrying...")
        mock_print.assert_any_call("Attempt 2 failed: Timeout 2. Retrying...")
    
    @patch('06_cb_get_retry_replica_read.cb_coll')
    @patch('06_cb_get_retry_replica_read.time.time')
    def test_get_airline_by_key_all_retries_fail_replica_success(self, mock_time, mock_coll):
        """Test all retries fail, replica read succeeds."""
        from couchbase.exceptions import TimeoutException
        
        # Setup mocks
        mock_time.side_effect = [1000.0, 1002.0]
        # All normal attempts timeout
        mock_coll.get.side_effect = TimeoutException("Always timeout")
        # Replica read succeeds
        mock_replica_result = Mock()
        mock_replica_result.content_as = {str: '{"name": "Replica Airline", "type": "airline"}'}
        mock_replica_result.cas = 87654321
        mock_coll.get_any_replica.return_value = mock_replica_result
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "retry_replica_read",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/06_cb_get_retry_replica_read.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            result = module.get_airline_by_key(self.test_key, self.test_timeout)
        
        # Verify retry attempts and replica read
        self.assertEqual(mock_coll.get.call_count, 4)  # 3 retries + 1 initial
        mock_coll.get_any_replica.assert_called_once_with(self.test_key, timeout=timedelta(seconds=self.test_timeout))
        self.assertEqual(result, mock_replica_result)
        mock_print.assert_any_call("All 3 attempts failed. Trying replica read.")
        mock_print.assert_any_call("Replica read result:")
        mock_print.assert_any_call('{"name": "Replica Airline", "type": "airline"}')
        mock_print.assert_any_call("CAS:", 87654321)
    
    @patch('06_cb_get_retry_replica_read.cb_coll')
    @patch('06_cb_get_retry_replica_read.time.time')
    def test_get_airline_by_key_replica_read_timeout(self, mock_time, mock_coll):
        """Test replica read also times out."""
        from couchbase.exceptions import TimeoutException
        
        # Setup mocks
        mock_time.side_effect = [1000.0, 1003.0]
        # All attempts timeout including replica
        mock_coll.get.side_effect = TimeoutException("Always timeout")
        mock_coll.get_any_replica.side_effect = TimeoutException("Replica timeout")
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "retry_replica_read",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/06_cb_get_retry_replica_read.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            result = module.get_airline_by_key(self.test_key, self.test_timeout)
        
        # Verify all attempts were made
        self.assertEqual(mock_coll.get.call_count, 4)
        mock_coll.get_any_replica.assert_called_once()
        self.assertIsNone(result)
        mock_print.assert_any_call("Replica read timed out: Replica timeout")
    
    @patch('06_cb_get_retry_replica_read.cb_coll')
    @patch('06_cb_get_retry_replica_read.time.time')
    def test_get_airline_by_key_replica_document_not_found(self, mock_time, mock_coll):
        """Test replica read document not found."""
        from couchbase.exceptions import TimeoutException, DocumentNotFoundException
        
        # Setup mocks
        mock_time.side_effect = [1000.0, 1003.0]
        # All attempts timeout, replica not found
        mock_coll.get.side_effect = TimeoutException("Always timeout")
        mock_coll.get_any_replica.side_effect = DocumentNotFoundException("Not in replica")
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "retry_replica_read",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/06_cb_get_retry_replica_read.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            result = module.get_airline_by_key(self.test_key, self.test_timeout)
        
        # Verify
        mock_print.assert_any_call("Document not found in any replica: Not in replica")
        self.assertIsNone(result)
    
    @patch('06_cb_get_retry_replica_read.cb_coll')
    @patch('06_cb_get_retry_replica_read.time.time')
    def test_get_airline_by_key_couchbase_exception(self, mock_time, mock_coll):
        """Test CouchbaseException handling."""
        from couchbase.exceptions import CouchbaseException
        
        # Setup mocks
        mock_time.side_effect = [1000.0, 1000.1]
        mock_coll.get.side_effect = CouchbaseException("General Couchbase error")
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "retry_replica_read",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/06_cb_get_retry_replica_read.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            result = module.get_airline_by_key(self.test_key, self.test_timeout)
        
        # Verify exception handling
        mock_print.assert_any_call("Couchbase error: General Couchbase error")
        self.assertIsNone(result)
    
    @patch('06_cb_get_retry_replica_read.cb_coll')
    @patch('06_cb_get_retry_replica_read.time.time')
    def test_get_airline_by_key_execution_time_measurement(self, mock_time, mock_coll):
        """Test execution time measurement."""
        # Setup mocks
        start_time = 1000.0
        end_time = 1000.567890
        expected_execution_time = end_time - start_time
        mock_time.side_effect = [start_time, end_time]
        mock_coll.get.return_value = self.mock_result
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "retry_replica_read",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/06_cb_get_retry_replica_read.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            result = module.get_airline_by_key(self.test_key, self.test_timeout)
        
        # Verify execution time was printed
        mock_print.assert_any_call(f"Get operation took {expected_execution_time:.6f} seconds")
    
    @patch('06_cb_get_retry_replica_read.Cluster')
    @patch('06_cb_get_retry_replica_read.PasswordAuthenticator')
    def test_cluster_connection_setup(self, mock_auth, mock_cluster):
        """Test cluster connection setup with proper options."""
        # Setup mocks
        mock_cluster_instance = Mock()
        mock_cluster.return_value = mock_cluster_instance
        mock_auth_instance = Mock()
        mock_auth.return_value = mock_auth_instance
        
        # Import module to trigger connection setup
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "retry_replica_read",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/06_cb_get_retry_replica_read.py"
        )
        module = importlib.util.module_from_spec(spec)
        
        # Verify authentication and cluster setup
        mock_auth.assert_called_once_with("Administrator", "password")
        mock_cluster.assert_called_once()
    
    @patch('06_cb_get_retry_replica_read.Cluster')
    @patch('06_cb_get_retry_replica_read.PasswordAuthenticator')
    def test_cluster_connection_with_wan_development_profile(self, mock_auth, mock_cluster):
        """Test cluster connection applies wan_development profile."""
        # Setup mocks
        mock_cluster_instance = Mock()
        mock_cluster.return_value = mock_cluster_instance
        mock_options = Mock()
        
        with patch('06_cb_get_retry_replica_read.ClusterOptions') as mock_cluster_options:
            mock_cluster_options.return_value = mock_options
            
            # Import module
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "retry_replica_read",
                "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/06_cb_get_retry_replica_read.py"
            )
            module = importlib.util.module_from_spec(spec)
            
            # Verify wan_development profile was applied
            mock_options.apply_profile.assert_called_once_with('wan_development')


if __name__ == '__main__':
    unittest.main()
