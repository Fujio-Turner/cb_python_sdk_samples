import unittest
from unittest.mock import MagicMock, patch
import sys
import os
import importlib.util

# Add the parent directory to Python path for general imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestAiVectorSearch(unittest.TestCase):
    
    def setUp(self):
        # Create mocks for all required couchbase modules
        self.mock_cluster_module = MagicMock()
        self.mock_auth_module = MagicMock()
        self.mock_options_module = MagicMock()
        self.mock_vector_search_module = MagicMock()
        
        # Setup specific mock classes
        self.mock_cluster_class = MagicMock()
        self.mock_cluster_instance = MagicMock()
        self.mock_cluster_class.return_value = self.mock_cluster_instance
        self.mock_cluster_module.Cluster = self.mock_cluster_class
        
        self.mock_bucket = MagicMock()
        self.mock_cluster_instance.bucket.return_value = self.mock_bucket
        self.mock_scope = MagicMock()
        self.mock_bucket.scope.return_value = self.mock_scope
        self.mock_collection = MagicMock()
        self.mock_scope.collection.return_value = self.mock_collection
        
        self.mock_search_result = MagicMock()
        # Setup rows iterator
        mock_row = MagicMock()
        mock_row.id = "test_id"
        mock_row.score = 0.99
        mock_row.fields = {"name": "Test Country"}
        self.mock_search_result.rows.return_value = [mock_row]
        self.mock_scope.search.return_value = self.mock_search_result

        self.mock_cluster_module.Cluster = self.mock_cluster_class
        
        # Apply patches to sys.modules to intercept imports
        self.modules_patcher = patch.dict(sys.modules, {
            'couchbase.cluster': self.mock_cluster_module,
            'couchbase.auth': self.mock_auth_module,
            'couchbase.options': self.mock_options_module,
            'couchbase.vector_search': self.mock_vector_search_module
        })
        self.modules_patcher.start()

    def tearDown(self):
        self.modules_patcher.stop()

    def test_vector_search_script_execution(self):
        """Test that the vector search script runs and performs expected operations."""
        
        # Define the path to the script
        current_dir = os.path.dirname(os.path.abspath(__file__))
        root_dir = os.path.dirname(current_dir)
        script_path = os.path.join(root_dir, 'ai_vector_sample', '04_vector_search_using_python_sdk.py')
        
        # Verify script exists
        self.assertTrue(os.path.exists(script_path), f"Script not found at {script_path}")
        
        # Execute the script
        with open(script_path, 'r') as f:
            script_content = f.read()
            
        # We need to run it in a context where the mocks are active
        # effectively 'exec'ing the script
        
        global_vars = {}
        exec(script_content, global_vars)
        
        # Verification
        
        # 1. Verify Cluster Connection
        self.mock_cluster_class.assert_called()
        self.mock_cluster_instance.wait_until_ready.assert_called_once()
        
        # 2. Verify Bucket/Scope/Collection access
        self.mock_cluster_instance.bucket.assert_called_with("cake")
        self.mock_bucket.scope.assert_called_with("us")
        self.mock_scope.collection.assert_called_with("orders")
        
        # 3. Verify Search execution
        self.mock_scope.search.assert_called_once()
        call_args = self.mock_scope.search.call_args
        _, kwargs = call_args
        
        self.assertEqual(kwargs['index_name'], "vect")
        self.assertIn('search_request', kwargs)
        self.assertIn('options', kwargs)

if __name__ == '__main__':
    unittest.main()
