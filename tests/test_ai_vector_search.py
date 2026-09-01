import unittest
from unittest.mock import MagicMock, patch
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestAiVectorSearch(unittest.TestCase):

    def setUp(self):
        self.mock_cluster_module = MagicMock()
        self.mock_auth_module = MagicMock()
        self.mock_options_module = MagicMock()
        self.mock_vector_search_module = MagicMock()
        self.mock_search_module = MagicMock()
        self.mock_exceptions_module = MagicMock()

        self.mock_cluster_class = MagicMock()
        self.mock_cluster_instance = MagicMock()
        self.mock_cluster_class.connect.return_value = self.mock_cluster_instance
        self.mock_cluster_module.Cluster = self.mock_cluster_class

        self.mock_bucket = MagicMock()
        self.mock_cluster_instance.bucket.return_value = self.mock_bucket
        self.mock_scope = MagicMock()
        self.mock_bucket.scope.return_value = self.mock_scope
        self.mock_collection = MagicMock()
        self.mock_scope.collection.return_value = self.mock_collection

        self.mock_search_result = MagicMock()
        mock_row = MagicMock()
        mock_row.id = "test_id"
        mock_row.score = 0.99
        mock_row.fields = {"name": "Test Country"}
        self.mock_search_result.rows.return_value = [mock_row]
        self.mock_scope.search.return_value = self.mock_search_result
        mock_query_result = MagicMock()
        mock_query_result.rows.return_value = []
        self.mock_cluster_instance.query.return_value = mock_query_result

        self.modules_patcher = patch.dict(sys.modules, {
            'couchbase.cluster': self.mock_cluster_module,
            'couchbase.auth': self.mock_auth_module,
            'couchbase.options': self.mock_options_module,
            'couchbase.vector_search': self.mock_vector_search_module,
            'couchbase.search': self.mock_search_module,
            'couchbase.exceptions': self.mock_exceptions_module,
        })
        self.modules_patcher.start()

    def tearDown(self):
        self.modules_patcher.stop()

    def test_vector_search_script_execution(self):
        """Test that the vector search script connects with Cluster.connect and searches."""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        root_dir = os.path.dirname(current_dir)
        script_path = os.path.join(root_dir, 'ai_vector_sample', '04_vector_search_using_python_sdk.py')

        self.assertTrue(os.path.exists(script_path), f"Script not found at {script_path}")

        with open(script_path, 'r') as f:
            script_content = f.read()

        exec(script_content, {'__name__': '__main__'})

        self.mock_cluster_class.connect.assert_called()
        self.mock_cluster_instance.wait_until_ready.assert_called_once()
        self.mock_cluster_instance.bucket.assert_called_with("cake")
        self.mock_bucket.scope.assert_called_with("us")
        self.mock_scope.collection.assert_called_with("orders")
        self.assertGreaterEqual(self.mock_scope.search.call_count, 2)
        first_args = self.mock_scope.search.call_args_list[0][0]
        self.assertEqual(first_args[0], "vect")
        self.mock_cluster_instance.query.assert_called()
        self.mock_cluster_instance.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
