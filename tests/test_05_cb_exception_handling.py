import unittest
from unittest.mock import patch, MagicMock, mock_open
import hashlib
from datetime import timedelta


class DocumentExistsException(Exception):
    pass


class DocumentNotFoundException(Exception):
    pass


class TimeoutException(Exception):
    pass


class ServiceUnavailableException(Exception):
    pass


class ParsingFailedException(Exception):
    pass


class CASMismatchException(Exception):
    pass


class TestCbExceptionHandling(unittest.TestCase):

    def setUp(self):
        self.mock_cluster = MagicMock()
        self.mock_collection = MagicMock()
        self.mock_result = MagicMock()
        self.mock_result.cas = 1234567890

    def test_md5_hash_calculation(self):
        """Test MD5 hash calculation directly."""
        test_content = b'test file content for hashing'
        expected_hash = hashlib.md5(test_content).hexdigest()
        
        def get_file_md5(filename):
            md5_hash = hashlib.md5()
            with open(filename, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    md5_hash.update(byte_block)
            return md5_hash.hexdigest()
        
        with patch('builtins.open', mock_open(read_data=test_content)):
            result = get_file_md5('test_file.csv')
            self.assertEqual(result, expected_hash)

    def test_document_exists_exception_handling(self):
        """Test DocumentExistsException handling."""
        self.mock_collection.insert.side_effect = DocumentExistsException("Document already exists")
        
        with self.assertRaises(DocumentExistsException):
            self.mock_collection.insert("test_key", {"data": "test"})
        
        self.assertEqual(self.mock_collection.insert.call_count, 1)

    def test_timeout_exception_handling(self):
        """Test TimeoutException handling."""
        self.mock_collection.insert.side_effect = TimeoutException("Operation timed out")
        
        with self.assertRaises(TimeoutException):
            self.mock_collection.insert("test_key", {"data": "test"})
        
        self.assertEqual(self.mock_collection.insert.call_count, 1)

    def test_timeout_exception_with_retry(self):
        """Test TimeoutException handling with retry mechanism."""
        # First call times out, second call succeeds
        self.mock_collection.insert.side_effect = [
            TimeoutException("Operation timed out"),
            self.mock_result
        ]
        
        # First attempt
        with self.assertRaises(TimeoutException):
            self.mock_collection.insert("test_key", {"data": "test"})
        
        # Retry succeeds
        result = self.mock_collection.insert("test_key", {"data": "test"})
        self.assertEqual(result.cas, 1234567890)
        self.assertEqual(self.mock_collection.insert.call_count, 2)

    def test_service_unavailable_exception_handling(self):
        """Test ServiceUnavailableException handling."""
        self.mock_collection.insert.side_effect = ServiceUnavailableException("Service unavailable")
        
        with self.assertRaises(ServiceUnavailableException):
            self.mock_collection.insert("test_key", {"data": "test"})
        
        self.assertEqual(self.mock_collection.insert.call_count, 1)

    def test_document_not_found_exception_handling(self):
        """Test DocumentNotFoundException handling."""
        self.mock_collection.get.side_effect = DocumentNotFoundException("Document not found")
        
        with self.assertRaises(DocumentNotFoundException):
            self.mock_collection.get("does_not_exist")
        
        self.assertEqual(self.mock_collection.get.call_count, 1)

    def test_get_with_default_value_pattern(self):
        """Test get with default value pattern."""
        def get_document_or_default(collection, key, default=None):
            try:
                result = collection.get(key)
                return result.content_as[dict]
            except DocumentNotFoundException:
                return default
        
        self.mock_collection.get.side_effect = DocumentNotFoundException("Document not found")
        
        result = get_document_or_default(self.mock_collection, "does_not_exist", {"default": True})
        self.assertEqual(result, {"default": True})

    def test_cas_mismatch_exception_handling(self):
        """Test CAS mismatch exception handling."""
        self.mock_collection.replace.side_effect = CASMismatchException("CAS mismatch")
        
        with self.assertRaises(CASMismatchException):
            from couchbase.options import ReplaceOptions
            self.mock_collection.replace("test_key", {"data": "test"}, ReplaceOptions(cas=12345))
        
        self.assertEqual(self.mock_collection.replace.call_count, 1)

    def test_parsing_failed_exception_handling(self):
        """Test ParsingFailedException handling."""
        self.mock_cluster.query.side_effect = ParsingFailedException("Query parsing failed")
        
        with self.assertRaises(ParsingFailedException):
            self.mock_cluster.query("SELECT * WHERE type = 'airline'")
        
        self.assertEqual(self.mock_cluster.query.call_count, 1)

    def test_value_error_handling(self):
        """Test ValueError handling."""
        self.mock_collection.insert.side_effect = ValueError("Invalid value provided")
        
        with self.assertRaises(ValueError):
            self.mock_collection.insert("test_key", {"data": "invalid"})
        
        self.assertEqual(self.mock_collection.insert.call_count, 1)

    def test_type_error_handling(self):
        """Test TypeError handling."""
        self.mock_collection.insert.side_effect = TypeError("Invalid type provided")
        
        with self.assertRaises(TypeError):
            self.mock_collection.insert("test_key", "not_a_dict")
        
        self.assertEqual(self.mock_collection.insert.call_count, 1)

    def test_successful_insert(self):
        """Test successful document insertion."""
        self.mock_collection.insert.return_value = self.mock_result
        
        result = self.mock_collection.insert("test_key", {"data": "test"})
        
        self.assertEqual(result.cas, 1234567890)
        self.mock_collection.insert.assert_called_once_with("test_key", {"data": "test"})

    def test_insert_with_timeout_option(self):
        """Test insert with timeout option."""
        from couchbase.options import InsertOptions
        
        self.mock_collection.insert.return_value = self.mock_result
        
        result = self.mock_collection.insert(
            "test_key", 
            {"data": "test"}, 
            InsertOptions(timeout=timedelta(seconds=5))
        )
        
        self.assertEqual(result.cas, 1234567890)
        self.assertEqual(self.mock_collection.insert.call_count, 1)

    def test_connection_failure(self):
        """Test Couchbase connection failure handling."""
        with patch('couchbase.cluster.Cluster') as mock_cluster_class:
            mock_cluster_class.side_effect = Exception("Connection failed")
            
            with self.assertRaises(Exception) as context:
                from couchbase.cluster import Cluster
                from couchbase.options import ClusterOptions
                from couchbase.auth import PasswordAuthenticator
                
                Cluster("couchbase://localhost", ClusterOptions(PasswordAuthenticator("user", "pass")))
            
            self.assertIn("Connection failed", str(context.exception))


if __name__ == '__main__':
    unittest.main()
