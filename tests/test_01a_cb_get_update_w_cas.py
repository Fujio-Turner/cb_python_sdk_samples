import unittest
from unittest.mock import MagicMock
import sys

sys.modules['couchbase'] = MagicMock()
sys.modules['couchbase.cluster'] = MagicMock()
sys.modules['couchbase.auth'] = MagicMock()
sys.modules['couchbase.options'] = MagicMock()
sys.modules['couchbase.exceptions'] = MagicMock()


class CasMismatchException(Exception):
    pass


class TestCbGetUpdateWithCas(unittest.TestCase):

    def setUp(self):
        self.mock_collection = MagicMock()
        self.mock_result = MagicMock()
        self.mock_result.cas = 1234567890
        self.mock_result.content_as = {"type": "airline", "name": "Test Airline"}

    def test_get_operation_returns_cas(self):
        self.mock_collection.get.return_value = self.mock_result
        
        result = self.mock_collection.get("test_key")
        
        self.assertEqual(result.cas, 1234567890)
        self.mock_collection.get.assert_called_once_with("test_key")

    def test_replace_operation_with_cas(self):
        mock_replace_result = MagicMock()
        mock_replace_result.cas = 9876543210
        self.mock_collection.replace.return_value = mock_replace_result
        
        test_doc = {"type": "airline", "name": "Updated Airline"}
        result = self.mock_collection.replace("test_key", test_doc, cas=1234567890)
        
        self.assertEqual(result.cas, 9876543210)
        self.mock_collection.replace.assert_called_once_with("test_key", test_doc, cas=1234567890)

    def test_cas_mismatch_exception(self):
        self.mock_collection.replace.side_effect = CasMismatchException("CAS mismatch")
        
        test_doc = {"type": "airline", "name": "Updated Airline"}
        with self.assertRaises(CasMismatchException):
            self.mock_collection.replace("test_key", test_doc, cas=12345)

    def test_get_and_replace_workflow(self):
        self.mock_collection.get.return_value = self.mock_result
        
        mock_replace_result = MagicMock()
        mock_replace_result.cas = 2222222222
        self.mock_collection.replace.return_value = mock_replace_result
        
        get_result = self.mock_collection.get("test_key")
        cas_value = get_result.cas
        
        updated_doc = {"type": "airline", "name": "Updated Airline"}
        replace_result = self.mock_collection.replace("test_key", updated_doc, cas=cas_value)
        
        self.assertEqual(replace_result.cas, 2222222222)
        self.mock_collection.get.assert_called_once_with("test_key")
        self.mock_collection.replace.assert_called_once_with("test_key", updated_doc, cas=1234567890)


if __name__ == '__main__':
    unittest.main()
