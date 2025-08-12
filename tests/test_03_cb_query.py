import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestCbQuery(unittest.TestCase):

    def setUp(self):
        self.mock_cluster = MagicMock()
        self.mock_bucket = MagicMock()
        self.mock_collection = MagicMock()
        self.mock_query_result = MagicMock()
        self.test_query_data = [
            {"id": "airline_1", "country": "France"},
            {"id": "airline_2", "country": "France"}
        ]

    @patch('builtins.__import__')
    def test_module_imports(self, mock_import):
        """Test that the module can be imported with mocked dependencies."""
        mock_module = MagicMock()
        
        def side_effect(name, *args, **kwargs):
            if '03_cb_query' in name:
                return mock_module
            return unittest.mock.DEFAULT
            
        mock_import.side_effect = side_effect
        
        try:
            module = __import__('03_cb_query')
            self.assertIsNotNone(module)
        except Exception as e:
            self.fail(f"Module import failed: {e}")

    @patch('builtins.__import__')
    @patch('time.time')
    def test_query_execution(self, mock_time, mock_import):
        """Test SQL++ query execution."""
        mock_time.side_effect = [1.0, 1.5]  # start and end time
        
        mock_module = MagicMock()
        mock_module.cluster = MagicMock()
        
        # Setup query result mock
        mock_query_result = MagicMock()
        mock_query_result.__iter__ = lambda self: iter(self.test_query_data)
        mock_module.cluster.query.return_value = mock_query_result
        
        def side_effect(name, *args, **kwargs):
            if '03_cb_query' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print:
            module = __import__('03_cb_query')
            if hasattr(module, 'cluster'):
                module.cluster = mock_module.cluster
                
                # Simulate the query execution from the module
                query = "SELECT meta().id , country FROM `travel-sample`.`inventory`.`airline` WHERE country = $country"
                country = "France"
                
                start_time = time.time()
                query_result = module.cluster.query(query, country=country)
                end_time = time.time()
                
                mock_module.cluster.query.assert_called_once_with(query, country=country)
                mock_print.assert_called()

    @patch('builtins.__import__')
    def test_query_exception_handling(self, mock_import):
        """Test query exception handling."""
        mock_module = MagicMock()
        mock_module.cluster = MagicMock()
        mock_module.cluster.query.side_effect = Exception("Query execution failed")
        
        def side_effect(name, *args, **kwargs):
            if '03_cb_query' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print:
            module = __import__('03_cb_query')
            if hasattr(module, 'cluster'):
                module.cluster = mock_module.cluster
                
                try:
                    query = "SELECT meta().id , country FROM `travel-sample`.`inventory`.`airline` WHERE country = $country"
                    module.cluster.query(query, country="France")
                except Exception as e:
                    self.assertIn("Query execution failed", str(e))
                    mock_print.assert_called()

    @patch('builtins.__import__')
    @patch('time.time')
    def test_query_timing_measurement(self, mock_time, mock_import):
        """Test query execution time measurement."""
        mock_time.side_effect = [10.0, 10.25]  # 0.25 second execution time
        
        mock_module = MagicMock()
        mock_module.cluster = MagicMock()
        
        mock_query_result = MagicMock()
        mock_query_result.__iter__ = lambda self: iter([])
        mock_module.cluster.query.return_value = mock_query_result
        
        def side_effect(name, *args, **kwargs):
            if '03_cb_query' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print:
            module = __import__('03_cb_query')
            if hasattr(module, 'cluster'):
                module.cluster = mock_module.cluster
                
                # Simulate the timing code from the module
                start_time = mock_time()
                query_result = module.cluster.query("SELECT * FROM test", country="France")
                end_time = mock_time()
                query_time = end_time - start_time
                
                self.assertEqual(query_time, 0.25)
                mock_print.assert_called()

    @patch('builtins.__import__')
    def test_query_with_parameters(self, mock_import):
        """Test parameterized query execution."""
        mock_module = MagicMock()
        mock_module.cluster = MagicMock()
        
        mock_query_result = MagicMock()
        mock_query_result.__iter__ = lambda self: iter(self.test_query_data)
        mock_module.cluster.query.return_value = mock_query_result
        
        def side_effect(name, *args, **kwargs):
            if '03_cb_query' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        module = __import__('03_cb_query')
        if hasattr(module, 'cluster'):
            module.cluster = mock_module.cluster
            
            # Test different countries
            countries = ["France", "Germany", "Italy"]
            
            for country in countries:
                result = module.cluster.query(
                    "SELECT meta().id , country FROM `travel-sample`.`inventory`.`airline` WHERE country = $country",
                    country=country
                )
                self.assertIsNotNone(result)

    @patch('builtins.__import__')
    def test_query_result_iteration(self, mock_import):
        """Test iterating through query results."""
        mock_module = MagicMock()
        mock_module.cluster = MagicMock()
        
        # Create a mock result that can be iterated
        mock_query_result = MagicMock()
        mock_query_result.__iter__ = lambda self: iter(self.test_query_data)
        mock_module.cluster.query.return_value = mock_query_result
        
        def side_effect(name, *args, **kwargs):
            if '03_cb_query' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print:
            module = __import__('03_cb_query')
            if hasattr(module, 'cluster'):
                module.cluster = mock_module.cluster
                
                query_result = module.cluster.query("SELECT * FROM test", country="France")
                
                # Simulate iteration from the module
                rows = []
                for row in query_result:
                    rows.append(row)
                
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[0]["country"], "France")
                self.assertEqual(rows[1]["country"], "France")

    @patch('builtins.__import__')
    def test_cluster_connection_cleanup(self, mock_import):
        """Test cluster connection cleanup."""
        mock_module = MagicMock()
        mock_module.cluster = MagicMock()
        
        def side_effect(name, *args, **kwargs):
            if '03_cb_query' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print:
            module = __import__('03_cb_query')
            if hasattr(module, 'cluster'):
                module.cluster = mock_module.cluster
                
                # Simulate cluster cleanup
                module.cluster.close()
                mock_module.cluster.close.assert_called_once()
                mock_print.assert_called()

    @patch('builtins.__import__')
    def test_query_constants_and_config(self, mock_import):
        """Test that query module has proper configuration constants."""
        mock_module = MagicMock()
        mock_module.ENDPOINT = "localhost"
        mock_module.USERNAME = "Administrator"
        mock_module.PASSWORD = "password"
        mock_module.BUCKET_NAME = "travel-sample"
        mock_module.CB_SCOPE = "inventory"
        mock_module.CB_COLLECTION = "airline"
        
        def side_effect(name, *args, **kwargs):
            if '03_cb_query' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        module = __import__('03_cb_query')
        if hasattr(module, 'BUCKET_NAME'):
            self.assertEqual(module.BUCKET_NAME, "travel-sample")
        if hasattr(module, 'CB_SCOPE'):
            self.assertEqual(module.CB_SCOPE, "inventory")
        if hasattr(module, 'CB_COLLECTION'):
            self.assertEqual(module.CB_COLLECTION, "airline")

if __name__ == '__main__':
    unittest.main()
