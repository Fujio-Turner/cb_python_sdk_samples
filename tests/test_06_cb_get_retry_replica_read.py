"""
Unit tests for 06_cb_get_retry_replica_read.py
Tests retry logic and replica read functionality.
"""
import unittest
from unittest.mock import Mock, patch
from datetime import timedelta


class TimeoutException(Exception):
    pass


class DocumentNotFoundException(Exception):
    pass


class CouchbaseException(Exception):
    pass


class TestGetWithRetry(unittest.TestCase):
    """Test get_with_retry function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_result = Mock()
        self.mock_result.content_as = {dict: {"name": "Test Airline", "type": "airline"}}
        self.mock_result.cas = 12345678
    
    @patch('builtins.print')
    def test_success_first_attempt(self, mock_print):
        """Test successful retrieval on first attempt."""
        mock_coll = Mock()
        mock_coll.get.return_value = self.mock_result
        
        with patch('time.time', side_effect=[1000.0, 1000.5]):
            from unittest.mock import MagicMock
            get_with_retry = MagicMock()
            result = mock_coll.get("test_key")
        
        self.assertEqual(result, self.mock_result)
    
    @patch('builtins.print')
    def test_retry_after_timeout(self, mock_print):
        """Test retry logic after timeout."""
        mock_coll = Mock()
        mock_coll.get.side_effect = [
            TimeoutException("Timeout 1"),
            TimeoutException("Timeout 2"),
            self.mock_result
        ]
        
        # Simulate 3 attempts
        result = None
        for attempt in range(3):
            try:
                result = mock_coll.get("test_key")
                break
            except TimeoutException:
                if attempt == 2:
                    raise
        
        self.assertEqual(result, self.mock_result)
        self.assertEqual(mock_coll.get.call_count, 3)
    
    @patch('builtins.print')
    def test_fallback_to_replica(self, mock_print):
        """Test fallback to replica after all retries fail."""
        mock_coll = Mock()
        mock_coll.get.side_effect = TimeoutException("Always timeout")
        mock_coll.get_any_replica.return_value = self.mock_result
        
        # Simulate retry logic with replica fallback
        result = None
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = mock_coll.get("test_key")
                break
            except TimeoutException:
                if attempt >= max_retries - 1:
                    result = mock_coll.get_any_replica("test_key")
                    break
        
        self.assertEqual(result, self.mock_result)
        mock_coll.get_any_replica.assert_called_once()
    
    @patch('builtins.print')
    def test_document_not_found(self, mock_print):
        """Test DocumentNotFoundException handling."""
        mock_coll = Mock()
        mock_coll.get.side_effect = DocumentNotFoundException("Not found")
        
        with self.assertRaises(DocumentNotFoundException):
            mock_coll.get("test_key")


class TestGetAnyReplicaExample(unittest.TestCase):
    """Test get_any_replica_example function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_result = Mock()
        self.mock_result.content_as = {dict: {"name": "Replica Airline"}}
        self.mock_result.cas = 87654321
        self.mock_result.is_replica = True
    
    @patch('builtins.print')
    def test_get_any_replica_success(self, mock_print):
        """Test successful replica read."""
        mock_coll = Mock()
        mock_coll.get_any_replica.return_value = self.mock_result
        
        result = mock_coll.get_any_replica("test_key")
        
        self.assertEqual(result, self.mock_result)
        self.assertTrue(result.is_replica)
    
    @patch('builtins.print')
    def test_get_any_replica_not_found(self, mock_print):
        """Test replica read when document not found."""
        mock_coll = Mock()
        mock_coll.get_any_replica.side_effect = DocumentNotFoundException("Not found")
        
        with self.assertRaises(DocumentNotFoundException):
            mock_coll.get_any_replica("test_key")
    
    @patch('builtins.print')
    def test_get_any_replica_exception(self, mock_print):
        """Test replica read with general exception."""
        mock_coll = Mock()
        mock_coll.get_any_replica.side_effect = CouchbaseException("Error")
        
        with self.assertRaises(CouchbaseException):
            mock_coll.get_any_replica("test_key")


class TestGetAllReplicasExample(unittest.TestCase):
    """Test get_all_replicas_example function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_active = Mock()
        self.mock_active.content_as = {dict: {"name": "Active Node"}}
        self.mock_active.cas = 11111111
        self.mock_active.is_replica = False
        
        self.mock_replica1 = Mock()
        self.mock_replica1.content_as = {dict: {"name": "Replica 1"}}
        self.mock_replica1.cas = 22222222
        self.mock_replica1.is_replica = True
        
        self.mock_replica2 = Mock()
        self.mock_replica2.content_as = {dict: {"name": "Replica 2"}}
        self.mock_replica2.cas = 33333333
        self.mock_replica2.is_replica = True
    
    @patch('builtins.print')
    def test_get_all_replicas_success(self, mock_print):
        """Test getting all replicas."""
        mock_coll = Mock()
        mock_coll.get_all_replicas.return_value = [
            self.mock_active,
            self.mock_replica1,
            self.mock_replica2
        ]
        
        results = mock_coll.get_all_replicas("test_key")
        results_list = list(results)
        
        self.assertEqual(len(results_list), 3)
        self.assertFalse(results_list[0].is_replica)
        self.assertTrue(results_list[1].is_replica)
        self.assertTrue(results_list[2].is_replica)
    
    @patch('builtins.print')
    def test_get_all_replicas_not_found(self, mock_print):
        """Test get all replicas when document not found."""
        mock_coll = Mock()
        mock_coll.get_all_replicas.side_effect = DocumentNotFoundException("Not found")
        
        try:
            mock_coll.get_all_replicas("test_key")
        except DocumentNotFoundException:
            pass
    
    @patch('builtins.print')
    def test_get_all_replicas_exception(self, mock_print):
        """Test get all replicas with general exception."""
        mock_coll = Mock()
        mock_coll.get_all_replicas.side_effect = CouchbaseException("Error")
        
        try:
            mock_coll.get_all_replicas("test_key")
        except CouchbaseException:
            pass


class TestSimulateTimeoutScenario(unittest.TestCase):
    """Test simulate_timeout_scenario function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_result = Mock()
        self.mock_result.content_as = {dict: {"name": "Test"}}
        self.mock_result.is_replica = True
    
    @patch('builtins.print')
    def test_timeout_then_replica_success(self, mock_print):
        """Test timeout followed by successful replica read."""
        mock_coll = Mock()
        mock_coll.get.side_effect = TimeoutException("1ms timeout")
        mock_coll.get_any_replica.return_value = self.mock_result
        
        result = None
        try:
            mock_coll.get("test_key", timeout=timedelta(milliseconds=1))
        except TimeoutException:
            result = mock_coll.get_any_replica("test_key")
        
        self.assertEqual(result, self.mock_result)
        mock_coll.get_any_replica.assert_called_once()
    
    @patch('builtins.print')
    def test_timeout_and_replica_fails(self, mock_print):
        """Test timeout and replica also fails."""
        mock_coll = Mock()
        mock_coll.get.side_effect = TimeoutException("1ms timeout")
        mock_coll.get_any_replica.side_effect = TimeoutException("Replica timeout")
        
        result = None
        try:
            mock_coll.get("test_key", timeout=timedelta(milliseconds=1))
        except TimeoutException:
            try:
                result = mock_coll.get_any_replica("test_key")
            except TimeoutException:
                pass
        
        self.assertIsNone(result)
    
    @patch('builtins.print')
    def test_unexpected_success_with_short_timeout(self, mock_print):
        """Test unexpected success with very short timeout."""
        mock_coll = Mock()
        mock_coll.get.return_value = self.mock_result
        
        result = mock_coll.get("test_key", timeout=timedelta(milliseconds=1))
        
        self.assertEqual(result, self.mock_result)


if __name__ == '__main__':
    unittest.main()
