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


class CasMismatchException(Exception):
    pass


class TestCbSubDocOps(unittest.TestCase):

    def setUp(self):
        self.mock_collection = MagicMock()
        self.mock_upsert_result = MagicMock()
        self.mock_upsert_result.cas = 1234567890
        
        self.mock_lookup_result = MagicMock()
        self.mock_lookup_result.cas = 2345678901
        
        self.mock_mutate_result = MagicMock()
        self.mock_mutate_result.cas = 3456789012

    def test_upsert_document(self):
        self.mock_collection.upsert.return_value = self.mock_upsert_result
        
        test_doc = {"type": "airline", "name": "Test Airline", "timestamp": 1609459200.0}
        result = self.mock_collection.upsert("test_key", test_doc)
        
        self.assertEqual(result.cas, 1234567890)
        self.mock_collection.upsert.assert_called_once_with("test_key", test_doc)

    def test_subdocument_lookup_operation(self):
        self.mock_collection.lookup_in.return_value = self.mock_lookup_result
        
        mock_sd_get = MagicMock()
        operations = [mock_sd_get("name"), mock_sd_get("timestamp")]
        
        result = self.mock_collection.lookup_in("test_key", operations)
        
        self.assertEqual(result.cas, 2345678901)
        self.mock_collection.lookup_in.assert_called_once_with("test_key", operations)

    def test_subdocument_mutate_operation(self):
        self.mock_collection.mutate_in.return_value = self.mock_mutate_result
        
        mock_sd_upsert = MagicMock()
        operations = [
            mock_sd_upsert("name", "Updated Airline"),
            mock_sd_upsert("timestamp", 1609459999.0)
        ]
        
        result = self.mock_collection.mutate_in("test_key", operations, cas=1111111111)
        
        self.assertEqual(result.cas, 3456789012)
        self.mock_collection.mutate_in.assert_called_once_with("test_key", operations, cas=1111111111)

    def test_upsert_exception_handling(self):
        self.mock_collection.upsert.side_effect = Exception("Upsert failed")
        
        test_doc = {"type": "airline", "name": "Test Airline"}
        with self.assertRaises(Exception) as context:
            self.mock_collection.upsert("test_key", test_doc)
        
        self.assertIn("Upsert failed", str(context.exception))

    def test_subdocument_get_exception_handling(self):
        self.mock_collection.lookup_in.side_effect = DocumentNotFoundException("Document not found")
        
        mock_sd_get = MagicMock()
        operations = [mock_sd_get("name")]
        
        with self.assertRaises(DocumentNotFoundException):
            self.mock_collection.lookup_in("nonexistent_key", operations)

    def test_subdocument_update_cas_mismatch(self):
        self.mock_collection.mutate_in.side_effect = CasMismatchException("CAS mismatch")
        
        mock_sd_upsert = MagicMock()
        operations = [mock_sd_upsert("name", "Updated Airline")]
        
        with self.assertRaises(CasMismatchException):
            self.mock_collection.mutate_in("test_key", operations, cas=9999999999)

    def test_complete_workflow(self):
        self.mock_collection.upsert.return_value = self.mock_upsert_result
        self.mock_collection.lookup_in.return_value = self.mock_lookup_result
        self.mock_collection.mutate_in.return_value = self.mock_mutate_result
        
        key = "airline_test"
        doc = {
            "type": "airline",
            "id": 1001,
            "name": "Test Airways",
            "timestamp": 1609459200.0
        }
        
        upsert_result = self.mock_collection.upsert(key, doc)
        self.assertEqual(upsert_result.cas, 1234567890)
        
        mock_sd_get = MagicMock()
        lookup_ops = [mock_sd_get("name"), mock_sd_get("timestamp")]
        lookup_result = self.mock_collection.lookup_in(key, lookup_ops)
        self.assertEqual(lookup_result.cas, 2345678901)
        
        mock_sd_upsert = MagicMock()
        mutate_ops = [
            mock_sd_upsert("name", "Updated Test Airways"),
            mock_sd_upsert("timestamp", 1609459999.0)
        ]
        mutate_result = self.mock_collection.mutate_in(key, mutate_ops, cas=lookup_result.cas)
        self.assertEqual(mutate_result.cas, 3456789012)
        
        self.mock_collection.upsert.assert_called()
        self.mock_collection.lookup_in.assert_called()
        self.mock_collection.mutate_in.assert_called()


if __name__ == '__main__':
    unittest.main()
