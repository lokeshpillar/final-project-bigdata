import unittest
from unittest.mock import MagicMock, patch

from nyc_opendata.data_ingestion import fetch_nyc_data, process_and_store_data
from nyc_opendata.database import MongoDB


class TestDataIngestion(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock(spec=MongoDB)
        self.mock_collection = MagicMock()
        self.mock_db.get_collection.return_value = self.mock_collection

    @patch("nyc_opendata.data_ingestion.requests.get")
    def test_fetch_nyc_data_success(self, mock_get):
        # Mock successful API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"crash_date": "2021-01-01"}]
        mock_get.return_value = mock_response

        result = fetch_nyc_data(0, 1)
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["crash_date"], "2021-01-01")

    @patch("nyc_opendata.data_ingestion.requests.get")
    def test_fetch_nyc_data_error(self, mock_get):
        # Mock failed API response
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        result = fetch_nyc_data(0)
        self.assertIsNone(result)

    def test_process_and_store_data(self):
        test_data = [
            {
                "crash_date": "2021-01-01",
                "number_of_persons_injured": "2",
                "number_of_persons_killed": "0",
                "number_of_pedestrians_injured": "1",
                "number_of_pedestrians_killed": "0",
                "number_of_cyclist_injured": "0",
                "number_of_cyclist_killed": "0",
                "number_of_motorist_injured": "1",
                "number_of_motorist_killed": "0",
            }
        ]

        processed_count = process_and_store_data(test_data, self.mock_collection)
        self.assertEqual(processed_count, 1)
        self.mock_collection.insert_many.assert_called_once()

    def test_process_and_store_empty_data(self):
        processed_count = process_and_store_data(None, self.mock_collection)
        self.assertEqual(processed_count, 0)
        self.mock_collection.insert_many.assert_not_called()
