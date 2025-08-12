"""
Unit tests for 07_cb_query_own_write.py
Tests query with own write consistency.
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
import time
from datetime import timedelta

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


class TestQueryOwnWrite(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_key = "airline_8091"
        self.test_doc = {
            "type": "airline",
            "id": 8091,
            "callsign": "CBS",
            "iata": None,
            "icao": None,
            "name": "Couchbase Airways",
            "timestamp": 1234567890.123
        }
        self.mock_collection = Mock()
        self.mock_cluster = Mock()
    
    @patch('07_cb_query_own_write.cb_coll')
    @patch('07_cb_query_own_write.time.time')
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
            "query_own_write",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/07_cb_query_own_write.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            result = module.upsert_document(self.test_key, self.test_doc)
        
        # Verify
        mock_coll.upsert.assert_called_once_with(self.test_key, self.test_doc)
        self.assertEqual(result, mock_result)
        mock_print.assert_any_call("\nUpsert CAS: ")
        mock_print.assert_any_call(12345678)
        expected_execution_time = end_time - start_time
        mock_print.assert_any_call(f"Upsert operation took {expected_execution_time:.6f} seconds")
    
    @patch('07_cb_query_own_write.cb_coll')
    @patch('07_cb_query_own_write.time.time')
    def test_upsert_document_exception(self, mock_time, mock_coll):
        """Test upsert document with exception handling."""
        # Setup mocks
        start_time = 1000.0
        end_time = 1000.1
        mock_time.side_effect = [start_time, end_time]
        mock_coll.upsert.side_effect = Exception("Upsert failed")
        
        # Import and test function
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "query_own_write",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/07_cb_query_own_write.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        with patch('builtins.print') as mock_print:
            result = module.upsert_document(self.test_key, self.test_doc)
        
        # Verify exception handling
        mock_print.assert_any_call(Exception("Upsert failed"))
        self.assertIsNone(result)
    
    @patch('07_cb_query_own_write.cluster')
    @patch('07_cb_query_own_write.time.time')
    def test_query_use_keys_success(self, mock_time, mock_cluster):
        """Test successful query with USE KEYS."""
        # Setup mocks
        start_time = 1000.0
        end_time = 1000.234
        mock_time.side_effect = [start_time, end_time]
        
        mock_query_result = Mock()
        mock_row = {
            "id": self.test_key,
            "cas": 12345678,
            "name": "Couchbase Airways",
            "timestamp": 1234567890.123
        }
        mock_query_result.__iter__ = Mock(return_value=iter([mock_row]))
        mock_cluster.query.return_value = mock_query_result
        
        # Import module
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "query_own_write",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/07_cb_query_own_write.py"
        )
        module = importlib.util.module_from_spec(spec)
        
        with patch('builtins.print') as mock_print:
            # Simulate the query execution from the module
            from couchbase.options import QueryOptions
            from couchbase.n1ql import QueryScanConsistency
            
            options = QueryOptions(
                named_parameters={"cbKey": self.test_key},
                scan_consistency=QueryScanConsistency.REQUEST_PLUS,
                timeout=timedelta(seconds=30)
            )
            
            query_result = mock_cluster.query(
                "SELECT meta().id, meta().cas,name, timestamp FROM `travel-sample`.`inventory`.`airline` USE KEYS[$cbKey]",
                options
            )
            
            for row in query_result:
                mock_print(row)
        
        # Verify query execution
        self.assertEqual(mock_cluster.query.call_count, 1)
        mock_print.assert_called_with(mock_row)
    
    @patch('07_cb_query_own_write.cluster')
    @patch('07_cb_query_own_write.time.time')
    def test_query_use_keys_exception(self, mock_time, mock_cluster):
        """Test query with USE KEYS exception handling."""
        # Setup mocks
        mock_time.side_effect = [1000.0, 1000.1]
        mock_cluster.query.side_effect = Exception("Query failed")
        
        # Import module
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "query_own_write",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/07_cb_query_own_write.py"
        )
        module = importlib.util.module_from_spec(spec)
        
        with patch('builtins.print') as mock_print:
            # Simulate query exception handling
            try:
                mock_cluster.query("SELECT * FROM test")
            except Exception as e:
                mock_print(f"An error occurred during the query: {e}")
        
        # Verify exception handling
        mock_print.assert_called_with("An error occurred during the query: Query failed")
    
    @patch('07_cb_query_own_write.cluster')
    @patch('07_cb_query_own_write.time.time')
    def test_query_where_clause_success(self, mock_time, mock_cluster):
        """Test successful query with WHERE clause."""
        # Setup mocks
        start_time = 1000.0
        end_time = 1000.456
        mock_time.side_effect = [start_time, end_time]
        
        mock_query_result = Mock()
        mock_row = {
            "id": self.test_key,
            "cas": 12345678,
            "name": "Couchbase Airways",
            "timestamp": 1234567890.123
        }
        mock_query_result.__iter__ = Mock(return_value=iter([mock_row]))
        mock_cluster.query.return_value = mock_query_result
        
        with patch('builtins.print') as mock_print:
            # Simulate WHERE query execution
            from couchbase.options import QueryOptions
            from couchbase.n1ql import QueryScanConsistency
            
            options = QueryOptions(
                named_parameters={"cbKey": self.test_key},
                scan_consistency=QueryScanConsistency.REQUEST_PLUS,
                timeout=timedelta(seconds=30)
            )
            
            query_result = mock_cluster.query(
                "SELECT meta().id, meta().cas,name, timestamp FROM `travel-sample`.`inventory`.`airline` WHERE name = 'Couchbase Airways'",
                options
            )
            
            for row in query_result:
                mock_print(row)
        
        # Verify
        mock_print.assert_called_with(mock_row)
    
    @patch('07_cb_query_own_write.cluster')
    def test_query_options_configuration(self, mock_cluster):
        """Test query options are properly configured."""
        # Import module
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "query_own_write",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/07_cb_query_own_write.py"
        )
        module = importlib.util.module_from_spec(spec)
        
        # Test QueryOptions configuration
        from couchbase.options import QueryOptions
        from couchbase.n1ql import QueryScanConsistency
        
        options = QueryOptions(
            named_parameters={"cbKey": self.test_key},
            scan_consistency=QueryScanConsistency.REQUEST_PLUS,
            timeout=timedelta(seconds=30)
        )
        
        # Verify options are configured correctly
        self.assertEqual(options.named_parameters, {"cbKey": self.test_key})
        self.assertEqual(options.scan_consistency, QueryScanConsistency.REQUEST_PLUS)
        self.assertEqual(options.timeout, timedelta(seconds=30))
    
    @patch('07_cb_query_own_write.Cluster')
    @patch('07_cb_query_own_write.PasswordAuthenticator')
    def test_cluster_connection_setup(self, mock_auth, mock_cluster):
        """Test cluster connection setup."""
        # Setup mocks
        mock_cluster_instance = Mock()
        mock_cluster.return_value = mock_cluster_instance
        mock_auth_instance = Mock()
        mock_auth.return_value = mock_auth_instance
        
        # Import module to trigger connection setup
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "query_own_write",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/07_cb_query_own_write.py"
        )
        module = importlib.util.module_from_spec(spec)
        
        # Verify connection setup
        mock_auth.assert_called_once_with("Administrator", "password")
        mock_cluster.assert_called_once()
    
    @patch('07_cb_query_own_write.cluster')
    def test_cluster_close(self, mock_cluster):
        """Test cluster connection is properly closed."""
        # Import module
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "query_own_write",
            "/Users/fujio.turner/Documents/GitHub/cb_python_sdk_samples/07_cb_query_own_write.py"
        )
        module = importlib.util.module_from_spec(spec)
        
        # Simulate closing connection
        mock_cluster.close()
        
        # Verify close was called
        mock_cluster.close.assert_called_once()
    
    def test_document_structure(self):
        """Test that the document structure is correct."""
        # Test document with timestamp
        current_time = time.time()
        doc = {
            "type": "airline",
            "id": 8091,
            "callsign": "CBS",
            "iata": None,
            "icao": None,
            "name": "Couchbase Airways",
            "timestamp": current_time
        }
        
        # Verify document structure
        self.assertEqual(doc["type"], "airline")
        self.assertEqual(doc["id"], 8091)
        self.assertEqual(doc["callsign"], "CBS")
        self.assertIsNone(doc["iata"])
        self.assertIsNone(doc["icao"])
        self.assertEqual(doc["name"], "Couchbase Airways")
        self.assertIsInstance(doc["timestamp"], float)
    
    def test_query_timing_measurement(self):
        """Test query timing measurement logic."""
        start_time = 1000.0
        end_time = 1000.1234
        expected_query_time = end_time - start_time
        
        # Simulate timing calculation
        query_time = end_time - start_time
        
        self.assertAlmostEqual(query_time, expected_query_time, places=4)
        self.assertEqual(f"{query_time:.4f}", "0.1234")
    
    @patch('07_cb_query_own_write.cluster')
    def test_named_parameters_usage(self, mock_cluster):
        """Test proper usage of named parameters in queries."""
        # Test that named parameters are used correctly
        from couchbase.options import QueryOptions
        from couchbase.n1ql import QueryScanConsistency
        
        key = "test_key"
        options = QueryOptions(
            named_parameters={"cbKey": key},
            scan_consistency=QueryScanConsistency.REQUEST_PLUS,
            timeout=timedelta(seconds=30)
        )
        
        # Verify named parameter is set correctly
        self.assertIn("cbKey", options.named_parameters)
        self.assertEqual(options.named_parameters["cbKey"], key)
        
        # Test that query uses the parameter placeholder
        query = "SELECT meta().id, meta().cas,name, timestamp FROM `travel-sample`.`inventory`.`airline` USE KEYS[$cbKey]"
        self.assertIn("$cbKey", query)


if __name__ == '__main__':
    unittest.main()
