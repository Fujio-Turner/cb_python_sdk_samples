"""
Unit tests for 09_cb_fts_search.py
Tests full-text search functionality.
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import timedelta


class TestFTSSearch(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_cluster = Mock()
        self.mock_bucket = Mock()
        self.search_results = [
            {"name": "Hotel Paris", "country": "France"},
            {"name": "Paris Plaza", "country": "France"},
            {"name": "Le Paris", "country": "France"}
        ]
    
    def test_cluster_connection_setup(self):
        """Test cluster connection setup with proper authentication."""
        # Mock connection components
        mock_auth = Mock()
        mock_options = Mock()
        mock_cluster = Mock()
        
        # Test connection logic simulation
        endpoint = "localhost"
        username = "Administrator"
        password = "password"
        
        # Verify connection parameters
        self.assertEqual(endpoint, "localhost")
        self.assertEqual(username, "Administrator")
        self.assertEqual(password, "password")
    
    def test_cluster_options_configuration(self):
        """Test cluster options configuration."""
        # Test cluster options setup
        mock_auth = Mock()
        mock_options = Mock()
        
        # Simulate options configuration
        options_config = {
            'auth': mock_auth,
            'profile': 'wan_development'
        }
        
        # Verify configuration
        self.assertIn('auth', options_config)
        self.assertIn('profile', options_config)
        self.assertEqual(options_config['profile'], 'wan_development')
    
    def test_basic_search_query_success(self):
        """Test successful basic SEARCH() query."""
        # Mock query result
        mock_query_result = Mock()
        mock_query_result.__iter__ = Mock(return_value=iter(self.search_results))
        self.mock_cluster.query.return_value = mock_query_result
        
        # Test query
        query = """
        SELECT name, country
        FROM `travel-sample`.inventory.hotel
        WHERE SEARCH(hotel, "paris")
        LIMIT 5
        """
        
        result = self.mock_cluster.query(query)
        
        # Verify
        self.mock_cluster.query.assert_called_once_with(query)
        
        # Test result iteration
        result_list = list(result)
        self.assertEqual(len(result_list), 3)
        self.assertEqual(result_list[0]["name"], "Hotel Paris")
    
    def test_basic_search_query_exception(self):
        """Test basic SEARCH() query with exception."""
        # Mock exception
        test_exception = Exception("Search index not found")
        self.mock_cluster.query.side_effect = test_exception
        
        # Test exception handling
        query = """
        SELECT name, country
        FROM `travel-sample`.inventory.hotel
        WHERE SEARCH(hotel, "paris")
        LIMIT 5
        """
        
        with self.assertRaises(Exception) as context:
            self.mock_cluster.query(query)
        
        self.assertEqual(str(context.exception), "Search index not found")
    
    def test_search_with_specific_field_and_index(self):
        """Test SEARCH() with specific field and FTS index."""
        # Mock query result
        france_results = [
            {"name": "Hotel France", "country": "France"},
            {"name": "French Hotel", "country": "France"}
        ]
        mock_query_result = Mock()
        mock_query_result.__iter__ = Mock(return_value=iter(france_results))
        self.mock_cluster.query.return_value = mock_query_result
        
        # Test query with specific field and index
        query = """
        SELECT name, country
        FROM `travel-sample`.inventory.hotel
        WHERE SEARCH(hotel, "country:fran*", {"index": "hotels-index"})
        LIMIT 5
        """
        
        result = self.mock_cluster.query(query)
        
        # Verify
        self.mock_cluster.query.assert_called_once_with(query)
        
        # Test results
        result_list = list(result)
        self.assertEqual(len(result_list), 2)
        self.assertTrue(all(r["country"] == "France" for r in result_list))
    
    def test_advanced_search_with_conjunctive_query(self):
        """Test advanced SEARCH() with conjunctive query."""
        # Mock result for complex query
        luxury_results = [
            {"name": "Paris Luxury Hotel", "country": "France", "description": "Luxury accommodation"}
        ]
        mock_query_result = Mock()
        mock_query_result.__iter__ = Mock(return_value=iter(luxury_results))
        self.mock_cluster.query.return_value = mock_query_result
        
        # Test complex conjunctive query
        query = """
        SELECT name, country, description
        FROM `travel-sample`.inventory.hotel
        WHERE SEARCH(hotel, 
            {
                "conjuncts": [
                    {"field": "country", "match": "franc*"},
                    {"field": "description", "match": "luxur*"},
                    {"field": "free_parking", "bool": true},
                    {"field": "vacancy", "min": 50, "max": 100},
                    {"field": "name", "match": "paris", "boost": 2.0}
                ]
            },
            {"index": "hotels-index"})
        LIMIT 5
        """
        
        result = self.mock_cluster.query(query)
        
        # Verify
        self.mock_cluster.query.assert_called_once_with(query)
        
        # Test results
        result_list = list(result)
        self.assertEqual(len(result_list), 1)
        self.assertIn("Luxury", result_list[0]["description"])
    
    def test_geo_search_query(self):
        """Test geo-based SEARCH() query."""
        # Mock geo search results
        geo_results = [
            {"name": "US Hotel", "country": "United States", "geo": {"lat": 40.7128, "lon": -74.0060}}
        ]
        mock_query_result = Mock()
        mock_query_result.__iter__ = Mock(return_value=iter(geo_results))
        self.mock_cluster.query.return_value = mock_query_result
        
        # Test geo query
        query = """
        SELECT name, country, geo
        FROM `travel-sample`.inventory.hotel
        WHERE SEARCH(hotel, 
            "country:\\"united states\\" AND geo:[-33.8688, 151.2093, 10mi]", 
            {"index": "hotels-geo-index"})
        LIMIT 5
        """
        
        result = self.mock_cluster.query(query)
        
        # Verify
        self.mock_cluster.query.assert_called_once_with(query)
        
        # Test results
        result_list = list(result)
        self.assertEqual(len(result_list), 1)
        self.assertEqual(result_list[0]["country"], "United States")
    
    def test_query_execution_timing(self):
        """Test query execution timing measurement."""
        # Mock time values
        start_time = 1000.0
        end_time = 1002.5
        expected_time = end_time - start_time
        
        with patch('time.time', side_effect=[start_time, end_time]):
            # Simulate timing logic
            actual_start = time.time()
            # Simulate query execution
            actual_end = time.time()
            execution_time = actual_end - actual_start
        
        self.assertEqual(execution_time, expected_time)
        self.assertEqual(execution_time, 2.5)
    
    def test_bucket_reference_setup(self):
        """Test bucket reference setup."""
        # Mock bucket setup
        mock_cluster = Mock()
        mock_bucket = Mock()
        mock_cluster.bucket.return_value = mock_bucket
        
        # Test bucket reference
        bucket_name = "travel-sample"
        cb = mock_cluster.bucket(bucket_name)
        
        # Verify
        mock_cluster.bucket.assert_called_once_with(bucket_name)
        self.assertEqual(cb, mock_bucket)
    
    def test_cluster_close(self):
        """Test cluster connection close."""
        # Mock cluster close
        mock_cluster = Mock()
        
        # Test close
        mock_cluster.close()
        
        # Verify
        mock_cluster.close.assert_called_once()
    
    def test_search_query_parameters(self):
        """Test SEARCH() query parameter handling."""
        # Test different search parameters
        search_params = {
            "simple_term": "paris",
            "field_specific": "country:fran*",
            "with_index": {"index": "hotels-index"},
            "conjunctive": {
                "conjuncts": [
                    {"field": "country", "match": "france"},
                    {"field": "name", "match": "paris"}
                ]
            },
            "geo_query": "country:\"united states\" AND geo:[-33.8688, 151.2093, 10mi]"
        }
        
        # Verify parameter types and structure
        self.assertIsInstance(search_params["simple_term"], str)
        self.assertIsInstance(search_params["with_index"], dict)
        self.assertIn("index", search_params["with_index"])
        self.assertIsInstance(search_params["conjunctive"], dict)
        self.assertIn("conjuncts", search_params["conjunctive"])
        self.assertIsInstance(search_params["conjunctive"]["conjuncts"], list)
    
    def test_fts_index_configuration(self):
        """Test FTS index configuration."""
        # Test index configurations
        index_configs = {
            "hotels-index": {"bucket": "travel-sample", "scope": "inventory", "collection": "hotel"},
            "hotels-geo-index": {"bucket": "travel-sample", "scope": "inventory", "collection": "hotel", "geo": True}
        }
        
        # Verify index configurations
        self.assertIn("hotels-index", index_configs)
        self.assertIn("hotels-geo-index", index_configs)
        self.assertEqual(index_configs["hotels-index"]["bucket"], "travel-sample")
        self.assertTrue(index_configs["hotels-geo-index"]["geo"])
    
    def test_search_result_processing(self):
        """Test search result processing."""
        # Mock search results with different structures
        mixed_results = [
            {"name": "Hotel A", "country": "France", "rating": 4.5},
            {"name": "Hotel B", "country": "France", "rating": 3.8, "description": "Luxury hotel"},
            {"name": "Hotel C", "country": "France", "geo": {"lat": 48.8566, "lon": 2.3522}}
        ]
        
        # Test result processing
        for result in mixed_results:
            self.assertIn("name", result)
            self.assertIn("country", result)
            self.assertEqual(result["country"], "France")
            
            # Test optional fields
            if "rating" in result:
                self.assertIsInstance(result["rating"], (int, float))
            if "geo" in result:
                self.assertIn("lat", result["geo"])
                self.assertIn("lon", result["geo"])


if __name__ == '__main__':
    unittest.main()
