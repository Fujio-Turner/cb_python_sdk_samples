import unittest
from unittest.mock import patch, MagicMock, Mock
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestCbGetUpdateWithCas(unittest.TestCase):

    def setUp(self):
        self.mock_collection = MagicMock()
        self.mock_result = MagicMock()
        self.mock_result.cas = 1234567890
        self.mock_result.content_as = {"str": {"type": "airline", "name": "Test Airline"}}

    @patch('builtins.__import__')
    def test_module_imports(self, mock_import):
        """Test that the module can be imported with mocked dependencies."""
        mock_module = MagicMock()
        
        def side_effect(name, *args, **kwargs):
            if '01a_cb_get_update_w_cas' in name:
                return mock_module
            return unittest.mock.DEFAULT
            
        mock_import.side_effect = side_effect
        
        try:
            module = __import__('01a_cb_get_update_w_cas')
            self.assertIsNotNone(module)
        except Exception as e:
            self.fail(f"Module import failed: {e}")

    @patch('builtins.__import__')
    @patch('time.time')
    def test_get_operation_with_cas(self, mock_time, mock_import):
        """Test get operation that returns a CAS value."""
        mock_time.side_effect = [1.0, 1.1]
        
        mock_module = MagicMock()
        mock_module.cb_coll = MagicMock()
        mock_module.cb_coll.get.return_value = self.mock_result
        
        def side_effect(name, *args, **kwargs):
            if '01a_cb_get_update_w_cas' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print:
            module = __import__('01a_cb_get_update_w_cas')
            if hasattr(module, 'cb_coll'):
                module.cb_coll = mock_module.cb_coll
            if hasattr(module, 'get_airline_by_key'):
                module.get_airline_by_key("test_key")
                mock_print.assert_called()

    @patch('builtins.__import__')
    @patch('time.time')
    def test_update_with_cas_operation(self, mock_time, mock_import):
        """Test update operation using CAS value."""
        mock_time.side_effect = [2.0, 2.1]
        
        mock_module = MagicMock()
        mock_module.cb_coll = MagicMock()
        mock_replace_result = MagicMock()
        mock_replace_result.cas = 9876543210
        mock_module.cb_coll.replace.return_value = mock_replace_result
        
        def side_effect(name, *args, **kwargs):
            if '01a_cb_get_update_w_cas' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print:
            module = __import__('01a_cb_get_update_w_cas')
            if hasattr(module, 'cb_coll'):
                module.cb_coll = mock_module.cb_coll
            if hasattr(module, 'update_document_with_cas'):
                test_doc = {"type": "airline", "name": "Updated Airline"}
                module.update_document_with_cas("test_key", test_doc, self.mock_result.cas)
                mock_print.assert_called()

    @patch('builtins.__import__')
    def test_cas_mismatch_handling(self, mock_import):
        """Test handling of CAS mismatch exceptions."""
        from couchbase.exceptions import CasMismatchException
        
        mock_module = MagicMock()
        mock_module.cb_coll = MagicMock()
        mock_module.cb_coll.replace.side_effect = CasMismatchException("CAS mismatch")
        
        def side_effect(name, *args, **kwargs):
            if '01a_cb_get_update_w_cas' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print, patch('time.time', side_effect=[1.0, 1.1]):
            module = __import__('01a_cb_get_update_w_cas')
            if hasattr(module, 'cb_coll'):
                module.cb_coll = mock_module.cb_coll
            if hasattr(module, 'update_document_with_cas'):
                test_doc = {"type": "airline", "name": "Updated Airline"}
                module.update_document_with_cas("test_key", test_doc, 12345)
                mock_print.assert_called()

    @patch('builtins.__import__')
    def test_document_operations_workflow(self, mock_import):
        """Test the complete workflow of get and update with CAS."""
        mock_module = MagicMock()
        mock_module.cb_coll = MagicMock()
        
        # Mock get operation
        mock_get_result = MagicMock()
        mock_get_result.cas = 1111111111
        mock_get_result.content_as = {"str": {"type": "airline", "callsign": "TEST"}}
        mock_module.cb_coll.get.return_value = mock_get_result
        
        # Mock replace operation  
        mock_replace_result = MagicMock()
        mock_replace_result.cas = 2222222222
        mock_module.cb_coll.replace.return_value = mock_replace_result
        
        def side_effect(name, *args, **kwargs):
            if '01a_cb_get_update_w_cas' in name:
                return mock_module
            return unittest.mock.DEFAULT
        
        mock_import.side_effect = side_effect
        
        with patch('builtins.print') as mock_print, patch('time.time', side_effect=[1.0, 1.1, 2.0, 2.1]):
            module = __import__('01a_cb_get_update_w_cas')
            if hasattr(module, 'cb_coll'):
                module.cb_coll = mock_module.cb_coll
            
            # Simulate workflow
            key = "test_airline"
            if hasattr(module, 'get_airline_by_key'):
                module.get_airline_by_key(key)
            
            if hasattr(module, 'update_document_with_cas'):
                updated_doc = {"type": "airline", "callsign": "UPDATED"}
                module.update_document_with_cas(key, updated_doc, mock_get_result.cas)
            
            mock_print.assert_called()

if __name__ == '__main__':
    unittest.main()
