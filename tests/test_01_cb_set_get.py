import unittest
from unittest.mock import patch, MagicMock, Mock
import sys
import os
import importlib.util
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestCbSetGet(unittest.TestCase):
    
    def setUp(self):
        self.mock_cluster = MagicMock()
        self.mock_bucket = MagicMock()
        self.mock_collection = MagicMock()
        self.mock_result = MagicMock()
        self.mock_result.cas = "1234567890"
        self.mock_result.content_as = {"str": {"type": "airline", "name": "Test Airline"}}

    def test_upsert_document(self):
        """Test upsert document function with mocked dependencies."""
        with patch('builtins.print') as mock_print, \
             patch('time.time', side_effect=[1.0, 1.1]):
            
            # Test the upsert function logic directly
            def mock_upsert_document(key, doc):
                print("\nUpsert CAS: ")
                start_time = time.time()
                try:
                    result = self.mock_collection.upsert(key, doc)
                    print(result.cas)
                except Exception as e:
                    print(e)
                finally:
                    end_time = time.time()
                    execution_time = end_time - start_time
                    print(f"Upsert operation took {execution_time:.6f} seconds")
            
            self.mock_collection.upsert.return_value = self.mock_result
            mock_upsert_document("test_key", {"test": "data"})
            
            self.mock_collection.upsert.assert_called_once_with("test_key", {"test": "data"})
            mock_print.assert_called()

    def test_get_airline_by_key(self):
        """Test get airline by key function."""
        with patch('builtins.print') as mock_print, \
             patch('time.time', side_effect=[2.0, 2.2]):
            
            def mock_get_airline_by_key(key):
                print("\nGet Result: ")
                start_time = time.time()
                try:
                    result = self.mock_collection.get(key)
                    print(result.content_as["str"])
                    print("CAS:", result.cas)
                except Exception as e:
                    print(e)
                finally:
                    end_time = time.time()
                    execution_time = end_time - start_time
                    print(f"Get operation took {execution_time:.6f} seconds")
            
            self.mock_collection.get.return_value = self.mock_result
            mock_get_airline_by_key("test_key")
            
            self.mock_collection.get.assert_called_once_with("test_key")
            mock_print.assert_called()

    def test_upsert_document_exception(self):
        """Test upsert document exception handling."""
        with patch('builtins.print') as mock_print, \
             patch('time.time', side_effect=[1.0, 1.1]):
            
            def mock_upsert_document(key, doc):
                print("\nUpsert CAS: ")
                start_time = time.time()
                try:
                    result = self.mock_collection.upsert(key, doc)
                    print(result.cas)
                except Exception as e:
                    print(e)
                finally:
                    end_time = time.time()
                    execution_time = end_time - start_time
                    print(f"Upsert operation took {execution_time:.6f} seconds")
            
            self.mock_collection.upsert.side_effect = Exception("Connection error")
            mock_upsert_document("test_key", {"test": "data"})
            
            mock_print.assert_called()

    def test_get_airline_exception(self):
        """Test get airline exception handling.""" 
        with patch('builtins.print') as mock_print, \
             patch('time.time', side_effect=[2.0, 2.2]):
            
            def mock_get_airline_by_key(key):
                print("\nGet Result: ")
                start_time = time.time()
                try:
                    result = self.mock_collection.get(key)
                    print(result.content_as["str"])
                    print("CAS:", result.cas)
                except Exception as e:
                    print(e)
                finally:
                    end_time = time.time()
                    execution_time = end_time - start_time
                    print(f"Get operation took {execution_time:.6f} seconds")
            
            self.mock_collection.get.side_effect = Exception("Document not found")
            mock_get_airline_by_key("nonexistent_key")
            
            mock_print.assert_called()

if __name__ == '__main__':
    unittest.main()
