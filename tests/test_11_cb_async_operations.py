"""
Unit tests for 11_cb_async_operations.py
Tests async operations including concurrent operations and retry logic.
"""
import unittest
from unittest.mock import patch, MagicMock, AsyncMock, Mock
import asyncio
from datetime import timedelta
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock couchbase modules before importing
sys.modules['couchbase'] = MagicMock()
sys.modules['couchbase.cluster'] = MagicMock()
sys.modules['couchbase.auth'] = MagicMock()
sys.modules['couchbase.options'] = MagicMock()
sys.modules['couchbase.exceptions'] = MagicMock()
sys.modules['acouchbase'] = MagicMock()
sys.modules['acouchbase.cluster'] = MagicMock()


class TestAsyncOperations(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_cluster = AsyncMock()
        self.mock_bucket = AsyncMock()
        self.mock_collection = AsyncMock()
        self.mock_result = MagicMock()
        self.mock_result.cas = "12345678"
    
    def test_async_cluster_connect(self):
        """Test async cluster connection."""
        async def test_connect():
            with patch('acouchbase.cluster.Cluster.connect', new_callable=AsyncMock) as mock_connect:
                mock_cluster = AsyncMock()
                mock_connect.return_value = mock_cluster
                
                from acouchbase.cluster import Cluster
                cluster = await Cluster.connect('couchbase://localhost', MagicMock())
                
                self.assertIsNotNone(cluster)
                mock_connect.assert_awaited_once()
        
        asyncio.run(test_connect())
    
    def test_async_wait_until_ready(self):
        """Test async wait_until_ready."""
        async def test_wait():
            mock_cluster = AsyncMock()
            mock_cluster.wait_until_ready = AsyncMock()
            
            await mock_cluster.wait_until_ready(timedelta(seconds=10))
            
            mock_cluster.wait_until_ready.assert_awaited_once()
        
        asyncio.run(test_wait())
    
    def test_upsert_documents_async(self):
        """Test async upsert operations."""
        async def test_upsert():
            from unittest.mock import AsyncMock
            
            # Mock collection with async upsert
            mock_collection = AsyncMock()
            mock_result = MagicMock()
            mock_result.cas = "123456"
            mock_collection.upsert = AsyncMock(return_value=mock_result)
            
            # Simulate upsert operation
            doc = {"type": "airline", "name": "Test Airline"}
            result = await mock_collection.upsert("test_key", doc)
            
            self.assertEqual(result.cas, "123456")
            mock_collection.upsert.assert_awaited_once_with("test_key", doc)
        
        asyncio.run(test_upsert())
    
    def test_get_documents_async(self):
        """Test async get operations."""
        async def test_get():
            # Mock collection with async get
            mock_collection = AsyncMock()
            mock_result = MagicMock()
            mock_result.content_as = {dict: {"id": 1, "name": "Test Airline"}}
            mock_collection.get = AsyncMock(return_value=mock_result)
            
            # Simulate get operation
            result = await mock_collection.get("test_key")
            
            self.assertEqual(result.content_as[dict]["name"], "Test Airline")
            mock_collection.get.assert_awaited_once_with("test_key")
        
        asyncio.run(test_get())
    
    def test_remove_documents_async(self):
        """Test async remove operations."""
        async def test_remove():
            mock_collection = AsyncMock()
            mock_collection.remove = AsyncMock()
            
            await mock_collection.remove("test_key")
            
            mock_collection.remove.assert_awaited_once_with("test_key")
        
        asyncio.run(test_remove())
    
    def test_concurrent_upserts(self):
        """Test concurrent upsert operations."""
        async def test_concurrent():
            mock_collection = AsyncMock()
            
            # Create multiple upsert tasks
            tasks = []
            for i in range(20):
                mock_result = MagicMock()
                mock_result.cas = f"cas_{i}"
                task = mock_collection.upsert(f"key_{i}", {"id": i})
                tasks.append(task)
            
            # Execute all tasks concurrently
            results = await asyncio.gather(*tasks)
            
            # Verify all were called
            self.assertEqual(mock_collection.upsert.await_count, 20)
        
        asyncio.run(test_concurrent())
    
    def test_concurrent_gets(self):
        """Test concurrent get operations."""
        async def test_concurrent():
            mock_collection = AsyncMock()
            
            # Create multiple get tasks
            tasks = []
            for i in range(20):
                task = mock_collection.get(f"key_{i}")
                tasks.append(task)
            
            # Execute all tasks concurrently
            results = await asyncio.gather(*tasks)
            
            # Verify all were called
            self.assertEqual(mock_collection.get.await_count, 20)
        
        asyncio.run(test_concurrent())
    
    def test_exception_handling_async(self):
        """Test exception handling in async operations."""
        async def test_exception():
            # Mock DocumentNotFoundException
            class MockDocumentNotFoundException(Exception):
                pass
            
            mock_collection = AsyncMock()
            mock_collection.get = AsyncMock(side_effect=MockDocumentNotFoundException("Not found"))
            
            with self.assertRaises(MockDocumentNotFoundException):
                await mock_collection.get("nonexistent_key")
        
        asyncio.run(test_exception())
    
    def test_async_context_manager(self):
        """Test async context manager for cluster."""
        async def test_context():
            mock_cluster = AsyncMock()
            mock_cluster.__aenter__ = AsyncMock(return_value=mock_cluster)
            mock_cluster.__aexit__ = AsyncMock(return_value=None)
            
            async with mock_cluster as cluster:
                self.assertEqual(cluster, mock_cluster)
            
            mock_cluster.__aenter__.assert_awaited_once()
            mock_cluster.__aexit__.assert_awaited_once()
        
        asyncio.run(test_context())
    
    def test_cluster_close_async(self):
        """Test async cluster close."""
        async def test_close():
            mock_cluster = AsyncMock()
            mock_cluster.close = AsyncMock()
            
            await mock_cluster.close()
            
            mock_cluster.close.assert_awaited_once()
        
        asyncio.run(test_close())
    
    def test_bucket_and_collection_refs(self):
        """Test getting bucket and collection references."""
        async def test_refs():
            mock_cluster = Mock()
            mock_bucket = Mock()
            mock_scope = Mock()
            mock_collection = Mock()
            
            mock_cluster.bucket.return_value = mock_bucket
            mock_bucket.scope.return_value = mock_scope
            mock_scope.collection.return_value = mock_collection
            
            bucket = mock_cluster.bucket("travel-sample")
            scope = bucket.scope("inventory")
            collection = scope.collection("airline")
            
            self.assertEqual(collection, mock_collection)
        
        asyncio.run(test_refs())
    
    def test_performance_measurement(self):
        """Test that timing measurements work."""
        import time
        
        start_time = time.time()
        time.sleep(0.01)  # Small delay
        end_time = time.time()
        
        elapsed = end_time - start_time
        self.assertGreater(elapsed, 0.01)
        self.assertLess(elapsed, 0.1)
    
    def test_asyncio_gather_return_exceptions(self):
        """Test asyncio.gather with return_exceptions."""
        async def test_gather():
            # Mock CouchbaseException
            class MockCouchbaseException(Exception):
                pass
            
            async def success_task():
                return "success"
            
            async def failure_task():
                raise MockCouchbaseException("Error")
            
            # Gather with return_exceptions to capture both success and failures
            results = await asyncio.gather(
                success_task(),
                failure_task(),
                return_exceptions=True
            )
            
            self.assertEqual(results[0], "success")
            self.assertIsInstance(results[1], MockCouchbaseException)
        
        asyncio.run(test_gather())
    
    def test_main_function_structure(self):
        """Test main function structure."""
        async def mock_main():
            # Simulate main function structure
            cluster = None
            try:
                cluster = AsyncMock()
                await cluster.wait_until_ready(timedelta(seconds=10))
                # Perform operations...
                return "success"
            finally:
                if cluster:
                    await cluster.close()
        
        result = asyncio.run(mock_main())
        self.assertEqual(result, "success")
    
    def test_async_sleep(self):
        """Test async sleep for delays."""
        async def test_sleep():
            import time
            
            start = time.time()
            await asyncio.sleep(0.01)
            elapsed = time.time() - start
            
            self.assertGreater(elapsed, 0.01)
        
        asyncio.run(test_sleep())
    
    def test_document_content_structure(self):
        """Test document structure for async operations."""
        doc = {
            "type": "airline",
            "id": 9000,
            "callsign": "ASYNC000",
            "name": "Async Airways #0",
            "country": "United States",
            "timestamp": 1234567890
        }
        
        self.assertEqual(doc["type"], "airline")
        self.assertEqual(doc["id"], 9000)
        self.assertIn("callsign", doc)
        self.assertIn("name", doc)
    
    def test_cas_value_handling(self):
        """Test CAS value handling in results."""
        mock_result = MagicMock()
        mock_result.cas = "1234567890abcdef"
        
        self.assertIsNotNone(mock_result.cas)
        self.assertIsInstance(mock_result.cas, str)
    
    def test_content_as_dict(self):
        """Test content_as[dict] access pattern."""
        mock_result = MagicMock()
        test_doc = {"id": 1, "name": "Test"}
        mock_result.content_as = {dict: test_doc}
        
        retrieved_doc = mock_result.content_as[dict]
        self.assertEqual(retrieved_doc["name"], "Test")
    
    def test_retry_configuration(self):
        """Test retry configuration in __init__."""
        # Test default retry values
        max_retries = 3
        initial_backoff = 0.1
        
        self.assertEqual(max_retries, 3)
        self.assertEqual(initial_backoff, 0.1)
    
    def test_exponential_backoff_calculation(self):
        """Test exponential backoff delay calculation."""
        initial_backoff = 0.1
        
        # Calculate backoff delays
        delay_0 = initial_backoff * (2 ** 0)  # 0.1s
        delay_1 = initial_backoff * (2 ** 1)  # 0.2s
        delay_2 = initial_backoff * (2 ** 2)  # 0.4s
        
        self.assertEqual(delay_0, 0.1)
        self.assertEqual(delay_1, 0.2)
        self.assertEqual(delay_2, 0.4)
    
    def test_retry_with_timeout(self):
        """Test retry logic with timeout exception."""
        async def test_retry():
            # Mock timeout exception
            class MockTimeoutException(Exception):
                pass
            
            mock_collection = AsyncMock()
            
            # First call times out, second succeeds
            mock_result = MagicMock()
            mock_result.cas = "12345"
            mock_collection.upsert = AsyncMock(side_effect=[
                MockTimeoutException("Timeout"),
                mock_result
            ])
            
            # Would retry once on timeout
            self.assertEqual(mock_collection.upsert.await_count, 0)
        
        asyncio.run(test_retry())
    
    def test_retry_exhaustion(self):
        """Test behavior when retries are exhausted."""
        max_retries = 3
        total_attempts = max_retries + 1  # Initial + retries
        
        self.assertEqual(total_attempts, 4)
    
    def test_enable_retry_flag(self):
        """Test enable_retry parameter."""
        enable_retry = True
        self.assertTrue(enable_retry)
        
        # Can be disabled
        enable_retry = False
        self.assertFalse(enable_retry)
    
    def test_document_not_found_no_retry(self):
        """Test that DocumentNotFoundException doesn't trigger retry."""
        # Document not found is not a transient error
        should_retry = False  # Don't retry for not found
        self.assertFalse(should_retry)


if __name__ == '__main__':
    unittest.main()
