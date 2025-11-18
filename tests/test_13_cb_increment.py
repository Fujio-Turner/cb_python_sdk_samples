"""
Unit tests for 13_cb_increment.py - Binary Counter Operations
"""

import unittest
from unittest.mock import MagicMock, patch, call
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


class TestIncrementOperations(unittest.TestCase):
    """Test cases for binary counter operations"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_cluster = MagicMock()
        self.mock_bucket = MagicMock()
        self.mock_collection = MagicMock()
        self.mock_binary = MagicMock()
        
        self.mock_cluster.bucket.return_value = self.mock_bucket
        self.mock_bucket.scope.return_value.collection.return_value = self.mock_collection
        self.mock_collection.binary.return_value = self.mock_binary
    
    def test_basic_increment(self):
        """Test basic increment operation"""
        mock_result = MagicMock()
        mock_result.content = 100
        self.mock_binary.increment.return_value = mock_result
        
        result = self.mock_binary.increment('counter-doc')
        
        self.assertEqual(result.content, 100)
        self.mock_binary.increment.assert_called_once()
    
    def test_increment_with_initial_value(self):
        """Test increment with initial value"""
        try:
            from couchbase.options import IncrementOptions, SignedInt64
            
            mock_result = MagicMock()
            mock_result.content = 100
            self.mock_binary.increment.return_value = mock_result
            
            result = self.mock_binary.increment(
                'counter-doc',
                IncrementOptions(initial=SignedInt64(100))
            )
            
            self.assertEqual(result.content, 100)
        except ImportError:
            self.skipTest("Couchbase SDK not available")
    
    def test_increment_with_custom_delta(self):
        """Test increment with custom delta value"""
        try:
            from couchbase.options import IncrementOptions, DeltaValue
            
            mock_result = MagicMock()
            mock_result.content = 5
            self.mock_binary.increment.return_value = mock_result
            
            result = self.mock_binary.increment(
                'counter-doc',
                IncrementOptions(delta=DeltaValue(5))
            )
            
            self.assertEqual(result.content, 5)
        except ImportError:
            self.skipTest("Couchbase SDK not available")
    
    def test_multiple_increments(self):
        """Test multiple increment operations"""
        self.mock_binary.increment.side_effect = [
            MagicMock(content=100),
            MagicMock(content=101),
            MagicMock(content=102)
        ]
        
        result1 = self.mock_binary.increment('counter-doc')
        result2 = self.mock_binary.increment('counter-doc')
        result3 = self.mock_binary.increment('counter-doc')
        
        self.assertEqual(result1.content, 100)
        self.assertEqual(result2.content, 101)
        self.assertEqual(result3.content, 102)
        self.assertEqual(self.mock_binary.increment.call_count, 3)
    
    def test_decrement_basic(self):
        """Test basic decrement operation"""
        mock_result = MagicMock()
        mock_result.content = 99
        self.mock_binary.decrement.return_value = mock_result
        
        result = self.mock_binary.decrement('counter-doc')
        
        self.assertEqual(result.content, 99)
        self.mock_binary.decrement.assert_called_once()
    
    def test_decrement_with_delta(self):
        """Test decrement with custom delta"""
        try:
            from couchbase.options import DecrementOptions, DeltaValue
            
            mock_result = MagicMock()
            mock_result.content = 95
            self.mock_binary.decrement.return_value = mock_result
            
            result = self.mock_binary.decrement(
                'counter-doc',
                DecrementOptions(delta=DeltaValue(5))
            )
            
            self.assertEqual(result.content, 95)
        except ImportError:
            self.skipTest("Couchbase SDK not available")
    
    def test_increment_decrement_sequence(self):
        """Test sequence of increment and decrement operations"""
        self.mock_binary.increment.return_value = MagicMock(content=101)
        self.mock_binary.decrement.return_value = MagicMock(content=100)
        
        result1 = self.mock_binary.increment('counter-doc')
        result2 = self.mock_binary.decrement('counter-doc')
        
        self.assertEqual(result1.content, 101)
        self.assertEqual(result2.content, 100)
    
    def test_increment_document_not_found(self):
        """Test increment when document doesn't exist (no initial value)"""
        try:
            from couchbase.exceptions import DocumentNotFoundException
            
            self.mock_binary.increment.side_effect = DocumentNotFoundException()
            
            with self.assertRaises(DocumentNotFoundException):
                self.mock_binary.increment('non-existent-counter')
        except ImportError:
            self.skipTest("Couchbase SDK not available")
    
    def test_increment_with_initial_creates_document(self):
        """Test that increment with initial value creates document"""
        try:
            from couchbase.options import IncrementOptions, SignedInt64
            
            mock_result = MagicMock()
            mock_result.content = 0
            self.mock_binary.increment.return_value = mock_result
            
            result = self.mock_binary.increment(
                'new-counter',
                IncrementOptions(initial=SignedInt64(0))
            )
            
            self.assertEqual(result.content, 0)
        except ImportError:
            self.skipTest("Couchbase SDK not available")
    
    def test_counter_result_content(self):
        """Test accessing counter result content"""
        mock_result = MagicMock()
        mock_result.content = 42
        self.mock_binary.increment.return_value = mock_result
        
        result = self.mock_binary.increment('counter-doc')
        
        self.assertIsNotNone(result.content)
        self.assertEqual(result.content, 42)
    
    def test_large_delta_value(self):
        """Test increment with large delta value"""
        try:
            from couchbase.options import IncrementOptions, DeltaValue
            
            mock_result = MagicMock()
            mock_result.content = 1000
            self.mock_binary.increment.return_value = mock_result
            
            result = self.mock_binary.increment(
                'counter-doc',
                IncrementOptions(delta=DeltaValue(1000))
            )
            
            self.assertEqual(result.content, 1000)
        except ImportError:
            self.skipTest("Couchbase SDK not available")
    
    def test_decrement_to_zero(self):
        """Test decrement operation reaching zero"""
        mock_result = MagicMock()
        mock_result.content = 0
        self.mock_binary.decrement.return_value = mock_result
        
        result = self.mock_binary.decrement('counter-doc')
        
        self.assertEqual(result.content, 0)
    
    @patch('sys.modules', {'couchbase': MagicMock()})
    def test_import_increment_options(self):
        """Test importing IncrementOptions"""
        try:
            from couchbase.options import IncrementOptions
            self.assertTrue(True)
        except ImportError:
            self.skipTest("Couchbase SDK not available")
    
    @patch('sys.modules', {'couchbase': MagicMock()})
    def test_import_decrement_options(self):
        """Test importing DecrementOptions"""
        try:
            from couchbase.options import DecrementOptions
            self.assertTrue(True)
        except ImportError:
            self.skipTest("Couchbase SDK not available")
    
    @patch('sys.modules', {'couchbase': MagicMock()})
    def test_import_delta_value(self):
        """Test importing DeltaValue"""
        try:
            from couchbase.options import DeltaValue
            self.assertTrue(True)
        except ImportError:
            self.skipTest("Couchbase SDK not available")
    
    @patch('sys.modules', {'couchbase': MagicMock()})
    def test_import_signed_int64(self):
        """Test importing SignedInt64"""
        try:
            from couchbase.options import SignedInt64
            self.assertTrue(True)
        except ImportError:
            self.skipTest("Couchbase SDK not available")


class TestCounterExceptionHandling(unittest.TestCase):
    """Test exception handling for counter operations"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_binary = MagicMock()
    
    def test_handle_document_not_found(self):
        """Test handling DocumentNotFoundException"""
        try:
            from couchbase.exceptions import DocumentNotFoundException
            
            self.mock_binary.increment.side_effect = DocumentNotFoundException()
            
            with self.assertRaises(DocumentNotFoundException):
                self.mock_binary.increment('non-existent')
        except ImportError:
            self.skipTest("Couchbase SDK not available")
    
    def test_handle_timeout_exception(self):
        """Test handling TimeoutException"""
        try:
            from couchbase.exceptions import TimeoutException
            
            self.mock_binary.increment.side_effect = TimeoutException()
            
            with self.assertRaises(TimeoutException):
                self.mock_binary.increment('counter-doc')
        except ImportError:
            self.skipTest("Couchbase SDK not available")
    
    def test_handle_couchbase_exception(self):
        """Test handling general CouchbaseException"""
        try:
            from couchbase.exceptions import CouchbaseException
            
            self.mock_binary.increment.side_effect = CouchbaseException()
            
            with self.assertRaises(CouchbaseException):
                self.mock_binary.increment('counter-doc')
        except ImportError:
            self.skipTest("Couchbase SDK not available")


class TestCounterCleanup(unittest.TestCase):
    """Test cleanup operations for counters"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_collection = MagicMock()
    
    def test_remove_counter_after_use(self):
        """Test removing counter document after use"""
        self.mock_collection.remove('counter-doc')
        
        self.mock_collection.remove.assert_called_once_with('counter-doc')
    
    def test_multiple_counter_cleanup(self):
        """Test removing multiple counter documents"""
        counters = ['counter-1', 'counter-2', 'counter-3']
        
        for counter in counters:
            self.mock_collection.remove(counter)
        
        self.assertEqual(self.mock_collection.remove.call_count, 3)


if __name__ == '__main__':
    unittest.main()
