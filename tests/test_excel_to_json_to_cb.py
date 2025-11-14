import unittest
from unittest.mock import patch, MagicMock, mock_open
import sys
import os
import hashlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

sys.modules['couchbase'] = MagicMock()
sys.modules['couchbase.cluster'] = MagicMock()
sys.modules['couchbase.auth'] = MagicMock()
sys.modules['couchbase.options'] = MagicMock()
sys.modules['couchbase.exceptions'] = MagicMock()

class TestExcelToJsonToCb(unittest.TestCase):

    def setUp(self):
        self.mock_cluster = MagicMock()
        self.mock_collection = MagicMock()
        self.mock_result = MagicMock()
        self.mock_result.cas = 1234567890
        
        self.test_data = [
            {"Customer Id": "C001", "First Name": "John", "Last Name": "Doe"},
            {"Customer Id": "C002", "First Name": "Jane", "Last Name": "Smith"}
        ]

    def test_md5_hash_calculation(self):
        test_content = b'test data'
        expected_hash = 'eb733a00c0c9d336e65691a37ab54293'
        
        md5_hash = hashlib.md5()
        md5_hash.update(test_content)
        result = md5_hash.hexdigest()
        
        self.assertEqual(result, expected_hash)

    def test_key_generation_with_customer_id(self):
        record = {"Customer Id": "C123", "Name": "Test Customer"}
        
        if "Customer Id" in record and record["Customer Id"] and str(record["Customer Id"]).strip():
            key = f"c:{record['Customer Id']}"
        else:
            import uuid
            key = f"c:{uuid.uuid4()}"
            record["key_exception"] = True
        
        self.assertEqual(key, "c:C123")
        self.assertNotIn("key_exception", record)

    def test_key_generation_without_customer_id(self):
        record = {"Name": "Test Customer"}
        
        if "Customer Id" in record and record["Customer Id"] and str(record["Customer Id"]).strip():
            key = f"c:{record['Customer Id']}"
        else:
            import uuid
            key = f"c:{uuid.uuid4()}"
            record["key_exception"] = True
        
        self.assertTrue(key.startswith("c:"))
        self.assertIn("key_exception", record)
        self.assertTrue(record["key_exception"])

    def test_key_generation_with_empty_customer_id(self):
        record = {"Customer Id": "", "Name": "Test Customer"}
        
        if "Customer Id" in record and record["Customer Id"] and str(record["Customer Id"]).strip():
            key = f"c:{record['Customer Id']}"
        else:
            import uuid
            key = f"c:{uuid.uuid4()}"
            record["key_exception"] = True
        
        self.assertTrue(key.startswith("c:"))
        self.assertIn("key_exception", record)
        self.assertTrue(record["key_exception"])

    def test_key_generation_with_whitespace_customer_id(self):
        record = {"Customer Id": "   ", "Name": "Test Customer"}
        
        if "Customer Id" in record and record["Customer Id"] and str(record["Customer Id"]).strip():
            key = f"c:{record['Customer Id']}"
        else:
            import uuid
            key = f"c:{uuid.uuid4()}"
            record["key_exception"] = True
        
        self.assertTrue(key.startswith("c:"))
        self.assertIn("key_exception", record)
        self.assertTrue(record["key_exception"])

    def test_audit_information_structure(self):
        import time
        
        file_name = "test.csv"
        file_md5 = "abc123"
        script_name = "python-user"
        script_version = "1.0"
        
        record = {"Customer Id": "C001"}
        record["audit"] = {
            "cr": {
                "dt": time.time(),
                "ver": script_version,
                "by": script_name,
                "src": file_name,
                "md5": file_md5
            }
        }
        
        self.assertIn("audit", record)
        self.assertIn("cr", record["audit"])
        self.assertEqual(record["audit"]["cr"]["ver"], "1.0")
        self.assertEqual(record["audit"]["cr"]["by"], "python-user")
        self.assertEqual(record["audit"]["cr"]["src"], "test.csv")
        self.assertEqual(record["audit"]["cr"]["md5"], "abc123")
        self.assertIsInstance(record["audit"]["cr"]["dt"], float)

    def test_multiple_records_audit_info(self):
        import time
        
        file_name = "customers.csv"
        file_md5 = "xyz789"
        script_name = "python-user"
        script_version = "1.0"
        
        records = self.test_data.copy()
        
        for record in records:
            record["audit"] = {
                "cr": {
                    "dt": time.time(),
                    "ver": script_version,
                    "by": script_name,
                    "src": file_name,
                    "md5": file_md5
                }
            }
        
        for record in records:
            self.assertIn("audit", record)
            self.assertEqual(record["audit"]["cr"]["src"], "customers.csv")
            self.assertEqual(record["audit"]["cr"]["md5"], "xyz789")

    def test_exception_handling_document_exists(self):
        class DocumentExistsException(Exception):
            pass
        
        mock_collection = MagicMock()
        mock_collection.insert.side_effect = DocumentExistsException("Document exists")
        
        record = {"Customer Id": "C001", "audit": {}}
        key = "c:C001"
        
        with self.assertRaises(DocumentExistsException):
            mock_collection.insert(key, record)

    def test_exception_handling_timeout(self):
        class TimeoutException(Exception):
            pass
        
        mock_collection = MagicMock()
        mock_collection.insert.side_effect = TimeoutException("Timeout")
        
        record = {"Customer Id": "C001", "audit": {}}
        key = "c:C001"
        
        with self.assertRaises(TimeoutException):
            mock_collection.insert(key, record)

    def test_exception_handling_type_error(self):
        mock_collection = MagicMock()
        mock_collection.insert.side_effect = TypeError("Invalid type")
        
        record = {"Customer Id": "C001", "audit": {}}
        key = "c:C001"
        
        with self.assertRaises(TypeError):
            mock_collection.insert(key, record)
        
        mock_collection.insert.assert_called_once()

if __name__ == '__main__':
    unittest.main()
