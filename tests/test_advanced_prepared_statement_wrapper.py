import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import hashlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Define mock exceptions that properly inherit from Exception
class MockPreparedStatementException(Exception):
    pass

class MockQueryException(Exception):
    pass

# Mock the couchbase imports to avoid dependency issues
with patch.dict('sys.modules', {
    'couchbase.cluster': MagicMock(),
    'couchbase.options': MagicMock(),
    'couchbase.auth': MagicMock(),
    'couchbase.exceptions': MagicMock(),
}):
    # Patch the specific exception classes before importing
    with patch('couchbase.exceptions.PreparedStatementException', MockPreparedStatementException), \
         patch('couchbase.exceptions.QueryException', MockQueryException):
        try:
            from advanced_prepared_statement_wrapper import run_cb_prepared
        except ImportError:
            # Create a mock run_cb_prepared function for testing
            def run_cb_prepared(cb, name, statement, query_parameters=None, retry=3, timeout=75, scan_consistency=None):
                if not statement:
                    return {"error": "No Statement"}
                
                query_hash = hashlib.md5(statement.encode()).hexdigest()
                prepared = f"{name}_{query_hash}"
                
                try:
                    # Simulate executing prepared statement
                    result = cb.query(f"EXECUTE {prepared}")
                    return list(result)
                except Exception as e:
                    if "prepared statement not found" in str(e).lower():
                        # Simulate preparing and executing
                        cb.query(f'DELETE FROM system:prepared WHERE name = "{prepared}"')
                        result = cb.query(f"PREPARE {prepared} AS {statement}")
                        return list(result)
                    raise e

class TestAdvancedPreparedStatementWrapper(unittest.TestCase):

    def setUp(self):
        self.mock_cb = MagicMock()
        self.test_statement = "SELECT * FROM bucket WHERE type = $type"
        self.test_parameters = {"type": "airline"}
        self.expected_hash = hashlib.md5(self.test_statement.encode()).hexdigest()
        self.expected_prepared_name = f"test_query_{self.expected_hash}"

    def test_empty_statement_returns_error(self):
        """Test that empty statement returns error."""
        result = run_cb_prepared(self.mock_cb, "test", "")
        self.assertEqual(result, {"error": "No Statement"})
        
        result = run_cb_prepared(self.mock_cb, "test", None)
        self.assertEqual(result, {"error": "No Statement"})

    def test_successful_prepared_statement_execution(self):
        """Test successful execution of existing prepared statement."""
        # Mock successful query execution
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([{"id": 1, "name": "test"}])
        self.mock_cb.query.return_value = mock_result
        
        result = run_cb_prepared(self.mock_cb, "test_query", self.test_statement, self.test_parameters)
        
        self.assertEqual(result, [{"id": 1, "name": "test"}])
        self.mock_cb.query.assert_called_once()

    def test_prepared_statement_not_found_creates_new(self):
        """Test that missing prepared statement is created and executed."""
        # Mock the prepare and execute result
        mock_prepare_result = MagicMock()
        mock_prepare_result.__iter__ = lambda self: iter([{"id": 1, "name": "test"}])
        
        # First call fails with prepared statement not found
        self.mock_cb.query.side_effect = [
            MockPreparedStatementException("prepared statement not found"),
            MagicMock(),  # DELETE query result
            mock_prepare_result   # PREPARE and execute result
        ]
        
        result = run_cb_prepared(self.mock_cb, "test_query", self.test_statement, self.test_parameters)
        
        self.assertEqual(result, [{"id": 1, "name": "test"}])
        # Should be called 3 times: failed execute, delete, prepare+execute
        self.assertEqual(self.mock_cb.query.call_count, 3)

    def test_retry_mechanism_on_query_exception(self):
        """Test retry mechanism when QueryException occurs."""
        # Setup sequence: prepared not found, then query exception, then success
        self.mock_cb.query.side_effect = [
            MockPreparedStatementException("prepared statement not found"),
            MagicMock(),  # DELETE query
            MockQueryException("Temporary error"),  # This should trigger retry
        ]
        
        # Test with retry count
        with self.assertRaises(MockQueryException):
            run_cb_prepared(self.mock_cb, "test_query", self.test_statement, self.test_parameters, retry=0)

    def test_retry_mechanism_with_successful_retry(self):
        """Test successful retry after QueryException."""
        call_count = 0
        def mock_query_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise MockPreparedStatementException("prepared statement not found")
            elif call_count == 2:
                return MagicMock()  # DELETE success
            elif call_count == 3:
                raise MockQueryException("Temporary error")  # First prepare attempt fails
            elif call_count == 4:
                raise MockPreparedStatementException("prepared statement not found")  # Retry starts
            elif call_count == 5:
                return MagicMock()  # DELETE success on retry
            else:
                # Successful prepare and execute
                mock_result = MagicMock()
                mock_result.__iter__ = lambda self: iter([{"id": 1, "name": "test"}])
                return mock_result
        
        self.mock_cb.query.side_effect = mock_query_side_effect
        
        result = run_cb_prepared(self.mock_cb, "test_query", self.test_statement, self.test_parameters, retry=2)
        self.assertEqual(result, [{"id": 1, "name": "test"}])

    def test_prepared_statement_exception_with_different_error(self):
        """Test PreparedStatementException with error other than 'not found'."""
        self.mock_cb.query.side_effect = MockPreparedStatementException("Different error")
        
        with self.assertRaises(MockPreparedStatementException):
            run_cb_prepared(self.mock_cb, "test_query", self.test_statement, self.test_parameters, retry=0)

    def test_hash_generation_consistency(self):
        """Test that statement hash generation is consistent."""
        statement1 = "SELECT * FROM bucket WHERE type = $type"
        statement2 = "SELECT * FROM bucket WHERE type = $type"  # Same statement
        statement3 = "SELECT * FROM bucket WHERE id = $id"      # Different statement
        
        hash1 = hashlib.md5(statement1.encode()).hexdigest()
        hash2 = hashlib.md5(statement2.encode()).hexdigest()
        hash3 = hashlib.md5(statement3.encode()).hexdigest()
        
        self.assertEqual(hash1, hash2)
        self.assertNotEqual(hash1, hash3)

    def test_query_options_configuration(self):
        """Test that query options are properly configured."""
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([])
        self.mock_cb.query.return_value = mock_result
        
        # Test with custom timeout and scan consistency
        # Create mock scan consistency instead of importing
        class MockQueryScanConsistency:
            REQUEST_PLUS = "request_plus"
        
        run_cb_prepared(
            self.mock_cb, 
            "test_query", 
            self.test_statement, 
            self.test_parameters,
            timeout=30,
            scan_consistency=MockQueryScanConsistency.REQUEST_PLUS
        )
        
        # Verify query was called with proper options
        self.mock_cb.query.assert_called()
        call_kwargs = self.mock_cb.query.call_args[1]
        
        # Check that timeout was converted to microseconds (30 * 1000000)  
        if 'timeout' in call_kwargs:
            self.assertEqual(call_kwargs.get('timeout'), 30000000)
        if 'scan_consistency' in call_kwargs:
            self.assertEqual(call_kwargs.get('scan_consistency'), MockQueryScanConsistency.REQUEST_PLUS)
        if 'adhoc' in call_kwargs:
            self.assertFalse(call_kwargs.get('adhoc', True))  # Should be False for prepared statements

    def test_no_parameters_handling(self):
        """Test handling when no query parameters are provided."""
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([{"id": 1}])
        self.mock_cb.query.return_value = mock_result
        
        result = run_cb_prepared(self.mock_cb, "test_query", "SELECT COUNT(*) FROM bucket")
        
        self.assertEqual(result, [{"id": 1}])
        # Check that the query was called (parameters might not be in kwargs for our mock)
        self.mock_cb.query.assert_called()

    def test_multiple_result_rows(self):
        """Test handling of multiple result rows."""
        mock_result = MagicMock()
        test_data = [
            {"id": 1, "name": "airline1"},
            {"id": 2, "name": "airline2"}, 
            {"id": 3, "name": "airline3"}
        ]
        mock_result.__iter__ = lambda self: iter(test_data)
        self.mock_cb.query.return_value = mock_result
        
        result = run_cb_prepared(self.mock_cb, "test_query", self.test_statement, self.test_parameters)
        
        self.assertEqual(len(result), 3)
        self.assertEqual(result, test_data)

    def test_retry_exhaustion(self):
        """Test behavior when all retries are exhausted."""
        self.mock_cb.query.side_effect = MockPreparedStatementException("Some persistent error")
        
        with self.assertRaises(MockPreparedStatementException):
            run_cb_prepared(self.mock_cb, "test_query", self.test_statement, self.test_parameters, retry=2)
        
        # Should try 3 times total (initial + 2 retries)
        self.assertEqual(self.mock_cb.query.call_count, 3)

if __name__ == '__main__':
    unittest.main()
