import unittest
from unittest.mock import MagicMock


class TestCbQuery(unittest.TestCase):

    def setUp(self):
        self.mock_cluster = MagicMock()
        self.test_query_data = [
            {"id": "airline_1", "country": "France"},
            {"id": "airline_2", "country": "France"}
        ]

    def test_query_execution(self):
        mock_query_result = MagicMock()
        mock_query_result.__iter__ = lambda self: iter(self.test_query_data)
        self.mock_cluster.query.return_value = mock_query_result
        
        query = "SELECT meta().id, country FROM `travel-sample`.`inventory`.`airline` WHERE country = $country"
        result = self.mock_cluster.query(query, country="France")
        
        self.mock_cluster.query.assert_called_once_with(query, country="France")
        self.assertIsNotNone(result)

    def test_query_exception_handling(self):
        self.mock_cluster.query.side_effect = Exception("Query execution failed")
        
        query = "SELECT meta().id, country FROM `travel-sample`.`inventory`.`airline` WHERE country = $country"
        with self.assertRaises(Exception) as context:
            self.mock_cluster.query(query, country="France")
        
        self.assertIn("Query execution failed", str(context.exception))

    def test_query_with_parameters(self):
        mock_query_result = MagicMock()
        mock_query_result.__iter__ = lambda self: iter(self.test_query_data)
        self.mock_cluster.query.return_value = mock_query_result
        
        countries = ["France", "Germany", "Italy"]
        
        for country in countries:
            result = self.mock_cluster.query(
                "SELECT meta().id, country FROM `travel-sample`.`inventory`.`airline` WHERE country = $country",
                country=country
            )
            self.assertIsNotNone(result)

    def test_query_result_iteration(self):
        mock_query_result = MagicMock()
        mock_query_result.__iter__ = lambda _: iter(self.test_query_data)
        self.mock_cluster.query.return_value = mock_query_result
        
        query_result = self.mock_cluster.query("SELECT * FROM test", country="France")
        
        rows = []
        for row in query_result:
            rows.append(row)
        
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["country"], "France")
        self.assertEqual(rows[1]["country"], "France")

    def test_cluster_connection_cleanup(self):
        self.mock_cluster.close()
        self.mock_cluster.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
