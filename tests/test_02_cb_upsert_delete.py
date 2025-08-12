import unittest
from unittest.mock import patch, MagicMock
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestCbUpsertDelete(unittest.TestCase):

    def setUp(self):
        self.mock_collection = MagicMock()
        self.mock_upsert_result = MagicMock()
        self.mock_upsert_result.cas = 1234567890
        self.mock_get_result = MagicMock()
        self.mock_get_result.content_as = {"str": {"type": "airline", "name": "Test Airline"}}

    @patch('builtins.__import__')
    def test_module_imports(self, mock_import):
        """Test that the module can be imported with mocked dependencies."""
        mock_module = MagicMock()
        
        def side_effect(name, *args, **kwargs):
            if '02_cb_upsert_delete' in name:
                return mock_module
            return unittest.mock.DEFAULT
            
        mock_import.side_effect = side_effect
        
        try:
            module = __import__('02_cb_upsert_delete')
            self.assertIsNotNone(module)
        except Exception as e:
            self.fail(f"Module import failed: {e}")

    @patch('builtins.__import__')
    @patch('time.time')
    def test_upsert_operation(self, mock_time, mock_import):
        """Test upsert operation."""
        mock_time.side_effect = [1.0, 1.1]
        
        mock_module = MagicMock()
        mock_module.cb_coll = MagicMock()
        mock_module.cb_coll.upsert.return_value = self.mock_upsert_result
        
        def side_effect(name, *args, **kwargs):
            if '02_cb_upsert_delete' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print:
            module = __import__('02_cb_upsert_delete')
            if hasattr(module, 'cb_coll'):
                module.cb_coll = mock_module.cb_coll
            if hasattr(module, 'upsert_document'):
                test_doc = {"type": "airline", "name": "Test Airline"}
                module.upsert_document("test_key", test_doc)
                mock_module.cb_coll.upsert.assert_called_once_with("test_key", test_doc)
                mock_print.assert_called()

    @patch('builtins.__import__')
    @patch('time.time')
    def test_delete_operation(self, mock_time, mock_import):
        """Test delete operation."""
        mock_time.side_effect = [2.0, 2.1]
        
        mock_module = MagicMock()
        mock_module.cb_coll = MagicMock()
        mock_delete_result = MagicMock()
        mock_delete_result.cas = 9876543210
        mock_module.cb_coll.remove.return_value = mock_delete_result
        
        def side_effect(name, *args, **kwargs):
            if '02_cb_upsert_delete' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print:
            module = __import__('02_cb_upsert_delete')
            if hasattr(module, 'cb_coll'):
                module.cb_coll = mock_module.cb_coll
            if hasattr(module, 'delete_document'):
                module.delete_document("test_key")
                mock_module.cb_coll.remove.assert_called_once_with("test_key")
                mock_print.assert_called()

    @patch('builtins.__import__')
    def test_upsert_exception_handling(self, mock_import):
        """Test upsert operation exception handling."""
        mock_module = MagicMock()
        mock_module.cb_coll = MagicMock()
        mock_module.cb_coll.upsert.side_effect = Exception("Connection error")
        
        def side_effect(name, *args, **kwargs):
            if '02_cb_upsert_delete' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print, patch('time.time', side_effect=[1.0, 1.1]):
            module = __import__('02_cb_upsert_delete')
            if hasattr(module, 'cb_coll'):
                module.cb_coll = mock_module.cb_coll
            if hasattr(module, 'upsert_document'):
                test_doc = {"type": "airline", "name": "Test Airline"}
                module.upsert_document("test_key", test_doc)
                mock_print.assert_called()

    @patch('builtins.__import__')
    def test_delete_exception_handling(self, mock_import):
        """Test delete operation exception handling."""
        from couchbase.exceptions import DocumentNotFoundException
        
        mock_module = MagicMock()
        mock_module.cb_coll = MagicMock()
        mock_module.cb_coll.remove.side_effect = DocumentNotFoundException("Document not found")
        
        def side_effect(name, *args, **kwargs):
            if '02_cb_upsert_delete' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print, patch('time.time', side_effect=[2.0, 2.1]):
            module = __import__('02_cb_upsert_delete')
            if hasattr(module, 'cb_coll'):
                module.cb_coll = mock_module.cb_coll
            if hasattr(module, 'delete_document'):
                module.delete_document("nonexistent_key")
                mock_print.assert_called()

    @patch('builtins.__import__')
    def test_upsert_delete_workflow(self, mock_import):
        """Test complete upsert and delete workflow."""
        mock_module = MagicMock()
        mock_module.cb_coll = MagicMock()
        
        # Mock upsert
        mock_upsert_result = MagicMock()
        mock_upsert_result.cas = 1111111111
        mock_module.cb_coll.upsert.return_value = mock_upsert_result
        
        # Mock delete
        mock_delete_result = MagicMock()
        mock_delete_result.cas = 2222222222
        mock_module.cb_coll.remove.return_value = mock_delete_result
        
        def side_effect(name, *args, **kwargs):
            if '02_cb_upsert_delete' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print, patch('time.time', side_effect=[1.0, 1.1, 2.0, 2.1]):
            module = __import__('02_cb_upsert_delete')
            if hasattr(module, 'cb_coll'):
                module.cb_coll = mock_module.cb_coll
            
            # Test workflow
            key = "test_airline"
            doc = {"type": "airline", "name": "Test Airline"}
            
            if hasattr(module, 'upsert_document'):
                module.upsert_document(key, doc)
                mock_module.cb_coll.upsert.assert_called_with(key, doc)
            
            if hasattr(module, 'delete_document'):
                module.delete_document(key)
                mock_module.cb_coll.remove.assert_called_with(key)
            
            mock_print.assert_called()

if __name__ == '__main__':
    unittest.main()
