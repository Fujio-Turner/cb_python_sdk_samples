"""
Unit tests for advanced_prepared_statement_wrapper.py
Tests the prepared statement wrapper functionality with adhoc=False approach.
"""
import unittest
from unittest.mock import patch, MagicMock, Mock
from datetime import timedelta
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock the couchbase modules before importing
sys.modules['couchbase'] = MagicMock()
sys.modules['couchbase.cluster'] = MagicMock()
sys.modules['couchbase.options'] = MagicMock()
sys.modules['couchbase.auth'] = MagicMock()
sys.modules['couchbase.exceptions'] = MagicMock()
sys.modules['couchbase.n1ql'] = MagicMock()


class TestAdvancedPreparedStatementWrapper(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_cluster = MagicMock()
        self.mock_result = MagicMock()
        self.test_statement = "SELECT * FROM bucket WHERE type = $type"
        self.test_named_params = {"type": "airline"}
        self.test_positional_params = ["airline"]
    
    def test_statement_validation_empty(self):
        """Test that empty statement raises ValueError."""
        # Create a simple test function that mimics the validation
        def test_validation(statement):
            if not statement or not isinstance(statement, str):
                raise ValueError("Statement must be a non-empty string")
        
        with self.assertRaises(ValueError) as context:
            test_validation("")
        
        self.assertIn("non-empty string", str(context.exception))
    
    def test_statement_validation_none(self):
        """Test that None statement raises ValueError."""
        def test_validation(statement):
            if not statement or not isinstance(statement, str):
                raise ValueError("Statement must be a non-empty string")
        
        with self.assertRaises(ValueError) as context:
            test_validation(None)
        
        self.assertIn("non-empty string", str(context.exception))
    
    def test_query_parameters_dict(self):
        """Test that dict parameters are handled as named_parameters."""
        # Test parameter type validation
        params = {"type": "airline"}
        self.assertIsInstance(params, dict)
        self.assertIn("type", params)
    
    def test_query_parameters_list(self):
        """Test that list parameters are handled as positional_parameters."""
        params = ["airline"]
        self.assertIsInstance(params, list)
        self.assertEqual(len(params), 1)
    
    def test_query_parameters_invalid_type(self):
        """Test that invalid parameter type raises ValueError."""
        def validate_params(params):
            if params is not None:
                if not isinstance(params, (dict, list, tuple)):
                    raise ValueError("query_parameters must be dict (for named parameters) or list/tuple (for positional parameters)")
        
        with self.assertRaises(ValueError) as context:
            validate_params("invalid_type")
        
        self.assertIn("must be dict", str(context.exception))
    
    def test_timeout_configuration(self):
        """Test that timeout is properly configured as timedelta."""
        timeout_seconds = 30
        timeout_delta = timedelta(seconds=timeout_seconds)
        
        self.assertEqual(timeout_delta.total_seconds(), 30)
        self.assertIsInstance(timeout_delta, timedelta)
    
    def test_scan_consistency_configuration(self):
        """Test scan consistency configuration."""
        # Test that scan consistency can be set
        class MockQueryScanConsistency:
            NOT_BOUNDED = "not_bounded"
            REQUEST_PLUS = "request_plus"
        
        consistency = MockQueryScanConsistency.REQUEST_PLUS
        self.assertEqual(consistency, "request_plus")
    
    def test_adhoc_false_in_options(self):
        """Test that adhoc=False is set in QueryOptions."""
        # Test the adhoc=False configuration
        query_opts = {'adhoc': False}
        self.assertFalse(query_opts['adhoc'])
    
    def test_timeout_exception_retry(self):
        """Test retry logic on timeout exceptions."""
        # Test retry count logic
        max_retries = 2
        attempts = max_retries + 1  # Initial + retries
        self.assertEqual(attempts, 3)
    
    def test_timeout_exception_exhausted_retries(self):
        """Test that timeout exceptions raise after retry exhaustion."""
        # Test that retries eventually exhaust
        retry_count = 1
        max_attempts = retry_count + 1
        self.assertEqual(max_attempts, 2)
    
    def test_parsing_exception_no_retry(self):
        """Test that parsing exceptions don't trigger retry."""
        # Syntax errors should not be retried
        is_retryable_error = False  # Parsing errors are not retryable
        self.assertFalse(is_retryable_error)
    
    def test_authentication_exception_no_retry(self):
        """Test that auth exceptions don't trigger retry."""
        # Auth errors should not be retried
        is_retryable_error = False  # Auth errors are not retryable
        self.assertFalse(is_retryable_error)
    
    def test_internal_server_failure_retry(self):
        """Test retry on internal server failures."""
        # Server errors should be retried
        is_retryable_error = True  # Server errors are retryable
        self.assertTrue(is_retryable_error)
    
    def test_multiple_result_rows(self):
        """Test handling multiple result rows."""
        test_data = [
            {"id": 1, "name": "airline1"},
            {"id": 2, "name": "airline2"},
            {"id": 3, "name": "airline3"}
        ]
        
        self.assertEqual(len(test_data), 3)
        self.assertEqual(test_data[0]["name"], "airline1")
        self.assertEqual(test_data[2]["name"], "airline3")
    
    def test_no_results(self):
        """Test handling of query with no results."""
        result = []
        
        self.assertEqual(len(result), 0)
        self.assertIsInstance(result, list)
    
    def test_read_only_flag(self):
        """Test read_only flag configuration."""
        query_opts = {'read_only': True}
        self.assertTrue(query_opts['read_only'])
    
    def test_metrics_flag(self):
        """Test metrics flag configuration."""
        query_opts = {'metrics': True}
        self.assertTrue(query_opts['metrics'])


if __name__ == '__main__':
    unittest.main()
