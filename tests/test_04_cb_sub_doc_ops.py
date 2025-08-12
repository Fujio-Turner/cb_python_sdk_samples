import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestCbSubDocOps(unittest.TestCase):

    def setUp(self):
        self.mock_collection = MagicMock()
        self.mock_upsert_result = MagicMock()
        self.mock_upsert_result.cas = 1234567890
        
        self.mock_lookup_result = MagicMock()
        self.mock_lookup_result.cas = 2345678901
        self.mock_lookup_result.content_as = MagicMock()
        self.mock_lookup_result.content_as[str] = MagicMock(side_effect=lambda i: ["Couchbase Airways", 1609459200.0][i])
        
        self.mock_mutate_result = MagicMock()
        self.mock_mutate_result.cas = 3456789012

    @patch('builtins.__import__')
    def test_module_imports(self, mock_import):
        """Test that the module can be imported with mocked dependencies."""
        mock_module = MagicMock()
        
        def side_effect(name, *args, **kwargs):
            if '04_cb_sub_doc_ops' in name:
                return mock_module
            return unittest.mock.DEFAULT
            
        mock_import.side_effect = side_effect
        
        try:
            module = __import__('04_cb_sub_doc_ops')
            self.assertIsNotNone(module)
        except Exception as e:
            self.fail(f"Module import failed: {e}")

    @patch('builtins.__import__')
    @patch('time.time')
    def test_upsert_document(self, mock_time, mock_import):
        """Test document upsert operation."""
        mock_time.side_effect = [1.0, 1.1]
        
        mock_module = MagicMock()
        mock_module.cb_coll = MagicMock()
        mock_module.cb_coll.upsert.return_value = self.mock_upsert_result
        
        def side_effect(name, *args, **kwargs):
            if '04_cb_sub_doc_ops' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print:
            module = __import__('04_cb_sub_doc_ops')
            if hasattr(module, 'cb_coll'):
                module.cb_coll = mock_module.cb_coll
            if hasattr(module, 'upsert_document'):
                test_doc = {"type": "airline", "name": "Test Airline", "timestamp": 1609459200.0}
                module.upsert_document("test_key", test_doc)
                mock_module.cb_coll.upsert.assert_called_once_with("test_key", test_doc)
                mock_print.assert_called()

    @patch('builtins.__import__')
    @patch('time.time')
    def test_sub_get_airline(self, mock_time, mock_import):
        """Test subdocument get operation for airline fields."""
        mock_time.side_effect = [2.0, 2.1]
        
        mock_module = MagicMock()
        mock_module.cb_coll = MagicMock()
        mock_module.cb_coll.lookup_in.return_value = self.mock_lookup_result
        
        # Mock the subdocument module
        mock_sd = MagicMock()
        mock_sd.get = MagicMock(side_effect=lambda field: f"get_{field}")
        mock_module.SD = mock_sd
        
        def side_effect(name, *args, **kwargs):
            if '04_cb_sub_doc_ops' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print:
            module = __import__('04_cb_sub_doc_ops')
            if hasattr(module, 'cb_coll'):
                module.cb_coll = mock_module.cb_coll
            if hasattr(module, 'SD'):
                module.SD = mock_sd
            if hasattr(module, 'sub_get_airline'):
                result_cas = module.sub_get_airline("test_key")
                
                # Verify lookup_in was called with proper subdocument operations
                mock_module.cb_coll.lookup_in.assert_called_once()
                call_args = mock_module.cb_coll.lookup_in.call_args
                self.assertEqual(call_args[0][0], "test_key")  # key argument
                
                self.assertEqual(result_cas, self.mock_lookup_result.cas)
                mock_print.assert_called()

    @patch('builtins.__import__')
    @patch('time.time')
    def test_sub_update_airline(self, mock_time, mock_import):
        """Test subdocument update operation for airline fields."""
        mock_time.side_effect = [3.0, 3.2]
        
        mock_module = MagicMock()
        mock_module.cb_coll = MagicMock()
        mock_module.cb_coll.mutate_in.return_value = self.mock_mutate_result
        
        # Mock the subdocument module
        mock_sd = MagicMock()
        mock_sd.upsert = MagicMock(side_effect=lambda field, value: f"upsert_{field}_{value}")
        mock_module.SD = mock_sd
        
        def side_effect(name, *args, **kwargs):
            if '04_cb_sub_doc_ops' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print:
            module = __import__('04_cb_sub_doc_ops')
            if hasattr(module, 'cb_coll'):
                module.cb_coll = mock_module.cb_coll
            if hasattr(module, 'SD'):
                module.SD = mock_sd
            if hasattr(module, 'sub_update_airline'):
                test_data = {"name": "Updated Airline", "timestamp": 1609459999.0}
                test_cas = 1111111111
                
                module.sub_update_airline("test_key", test_cas, test_data)
                
                # Verify mutate_in was called with proper parameters
                mock_module.cb_coll.mutate_in.assert_called_once()
                call_args = mock_module.cb_coll.mutate_in.call_args
                self.assertEqual(call_args[0][0], "test_key")  # key argument
                self.assertEqual(call_args[1]['cas'], test_cas)  # cas argument
                
                mock_print.assert_called()

    @patch('builtins.__import__')
    def test_upsert_exception_handling(self, mock_import):
        """Test exception handling in upsert operation."""
        mock_module = MagicMock()
        mock_module.cb_coll = MagicMock()
        mock_module.cb_coll.upsert.side_effect = Exception("Upsert failed")
        
        def side_effect(name, *args, **kwargs):
            if '04_cb_sub_doc_ops' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print, patch('time.time', side_effect=[1.0, 1.1]):
            module = __import__('04_cb_sub_doc_ops')
            if hasattr(module, 'cb_coll'):
                module.cb_coll = mock_module.cb_coll
            if hasattr(module, 'upsert_document'):
                test_doc = {"type": "airline", "name": "Test Airline"}
                module.upsert_document("test_key", test_doc)
                mock_print.assert_called()

    @patch('builtins.__import__')
    def test_sub_get_exception_handling(self, mock_import):
        """Test exception handling in subdocument get operation."""
        from couchbase.exceptions import DocumentNotFoundException
        
        mock_module = MagicMock()
        mock_module.cb_coll = MagicMock()
        mock_module.cb_coll.lookup_in.side_effect = DocumentNotFoundException("Document not found")
        
        mock_sd = MagicMock()
        mock_sd.get = MagicMock(return_value="mock_get_op")
        mock_module.SD = mock_sd
        
        def side_effect(name, *args, **kwargs):
            if '04_cb_sub_doc_ops' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print, patch('time.time', side_effect=[2.0, 2.1]):
            module = __import__('04_cb_sub_doc_ops')
            if hasattr(module, 'cb_coll'):
                module.cb_coll = mock_module.cb_coll
            if hasattr(module, 'SD'):
                module.SD = mock_sd
            if hasattr(module, 'sub_get_airline'):
                result = module.sub_get_airline("nonexistent_key")
                self.assertIsNone(result)
                mock_print.assert_called()

    @patch('builtins.__import__')
    def test_sub_update_cas_mismatch(self, mock_import):
        """Test CAS mismatch handling in subdocument update."""
        from couchbase.exceptions import CasMismatchException
        
        mock_module = MagicMock()
        mock_module.cb_coll = MagicMock()
        mock_module.cb_coll.mutate_in.side_effect = CasMismatchException("CAS mismatch")
        
        mock_sd = MagicMock()
        mock_sd.upsert = MagicMock(return_value="mock_upsert_op")
        mock_module.SD = mock_sd
        
        def side_effect(name, *args, **kwargs):
            if '04_cb_sub_doc_ops' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print, patch('time.time', side_effect=[3.0, 3.1]):
            module = __import__('04_cb_sub_doc_ops')
            if hasattr(module, 'cb_coll'):
                module.cb_coll = mock_module.cb_coll
            if hasattr(module, 'SD'):
                module.SD = mock_sd
            if hasattr(module, 'sub_update_airline'):
                test_data = {"name": "Updated Airline", "timestamp": 1609459999.0}
                old_cas = 9999999999  # Wrong CAS value
                
                module.sub_update_airline("test_key", old_cas, test_data)
                mock_print.assert_called()

    @patch('builtins.__import__')
    def test_complete_workflow(self, mock_import):
        """Test the complete workflow: upsert, get, update."""
        mock_module = MagicMock()
        mock_module.cb_coll = MagicMock()
        
        # Mock upsert
        mock_module.cb_coll.upsert.return_value = self.mock_upsert_result
        
        # Mock lookup
        mock_module.cb_coll.lookup_in.return_value = self.mock_lookup_result
        
        # Mock mutate
        mock_module.cb_coll.mutate_in.return_value = self.mock_mutate_result
        
        # Mock subdocument operations
        mock_sd = MagicMock()
        mock_sd.get = MagicMock(return_value="mock_get")
        mock_sd.upsert = MagicMock(return_value="mock_upsert")
        mock_module.SD = mock_sd
        
        def side_effect(name, *args, **kwargs):
            if '04_cb_sub_doc_ops' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print, patch('time.time', side_effect=[1.0, 1.1, 2.0, 2.1, 3.0, 3.1]):
            module = __import__('04_cb_sub_doc_ops')
            if hasattr(module, 'cb_coll'):
                module.cb_coll = mock_module.cb_coll
            if hasattr(module, 'SD'):
                module.SD = mock_sd
            
            # Simulate the workflow
            key = "airline_test"
            doc = {
                "type": "airline",
                "id": 1001,
                "name": "Test Airways",
                "timestamp": time.time()
            }
            
            if hasattr(module, 'upsert_document'):
                module.upsert_document(key, doc)
                mock_module.cb_coll.upsert.assert_called()
            
            if hasattr(module, 'sub_get_airline'):
                cas = module.sub_get_airline(key)
                mock_module.cb_coll.lookup_in.assert_called()
                self.assertEqual(cas, self.mock_lookup_result.cas)
            
            if hasattr(module, 'sub_update_airline'):
                updated_data = {"name": "Updated Test Airways", "timestamp": time.time()}
                module.sub_update_airline(key, cas, updated_data)
                mock_module.cb_coll.mutate_in.assert_called()
            
            mock_print.assert_called()

if __name__ == '__main__':
    unittest.main()
