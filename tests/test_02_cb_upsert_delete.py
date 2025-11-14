import unittest
from unittest.mock import MagicMock
import sys

sys.modules['couchbase'] = MagicMock()
sys.modules['couchbase.cluster'] = MagicMock()
sys.modules['couchbase.auth'] = MagicMock()
sys.modules['couchbase.options'] = MagicMock()
sys.modules['couchbase.exceptions'] = MagicMock()


class DocumentNotFoundException(Exception):
    pass


class TestCbUpsertDelete(unittest.TestCase):

    def setUp(self):
        self.mock_collection = MagicMock()
        self.mock_upsert_result = MagicMock()
        self.mock_upsert_result.cas = 1234567890

    def test_upsert_operation(self):
        self.mock_collection.upsert.return_value = self.mock_upsert_result
        
        test_doc = {"type": "airline", "name": "Test Airline"}
        result = self.mock_collection.upsert("test_key", test_doc)
        
        self.assertEqual(result.cas, 1234567890)
        self.mock_collection.upsert.assert_called_once_with("test_key", test_doc)

    def test_delete_operation(self):
        mock_delete_result = MagicMock()
        mock_delete_result.cas = 9876543210
        self.mock_collection.remove.return_value = mock_delete_result
        
        result = self.mock_collection.remove("test_key")
        
        self.assertEqual(result.cas, 9876543210)
        self.mock_collection.remove.assert_called_once_with("test_key")

    def test_upsert_exception_handling(self):
        self.mock_collection.upsert.side_effect = Exception("Connection error")
        
        test_doc = {"type": "airline", "name": "Test Airline"}
        with self.assertRaises(Exception) as context:
            self.mock_collection.upsert("test_key", test_doc)
        
        self.assertIn("Connection error", str(context.exception))

    def test_delete_exception_handling(self):
        self.mock_collection.remove.side_effect = DocumentNotFoundException("Document not found")
        
        with self.assertRaises(DocumentNotFoundException):
            self.mock_collection.remove("nonexistent_key")

    def test_upsert_delete_workflow(self):
        mock_upsert_result = MagicMock()
        mock_upsert_result.cas = 1111111111
        self.mock_collection.upsert.return_value = mock_upsert_result
        
        mock_delete_result = MagicMock()
        mock_delete_result.cas = 2222222222
        self.mock_collection.remove.return_value = mock_delete_result
        
        key = "test_airline"
        doc = {"type": "airline", "name": "Test Airline"}
        
        upsert_result = self.mock_collection.upsert(key, doc)
        self.assertEqual(upsert_result.cas, 1111111111)
        
        delete_result = self.mock_collection.remove(key)
        self.assertEqual(delete_result.cas, 2222222222)
        
        self.mock_collection.upsert.assert_called_with(key, doc)
        self.mock_collection.remove.assert_called_with(key)


if __name__ == '__main__':
    unittest.main()
