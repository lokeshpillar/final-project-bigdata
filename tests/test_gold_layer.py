import unittest
from unittest.mock import patch, MagicMock
from nyc_opendata.gold_layer import DataAnalyzer, VisualizationCreator
from nyc_opendata.database import MongoDB


class TestDataAnalyzer(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock(spec=MongoDB)
        # Set _client and _db attributes on mock before creating analyzer
        self.mock_db._client = MagicMock()
        self.mock_db._db = MagicMock()
        self.analyzer = DataAnalyzer(self.mock_db)

    def test_validate_db_connection_success(self):
        # _client and _db already set in setUp
        self.analyzer._validate_db_connection()  # Should not raise error

    def test_validate_db_connection_failure(self):
        self.mock_db._client = None
        self.mock_db._db = None
        with self.assertRaises(ConnectionError):
            self.analyzer._validate_db_connection()

    def test_create_time_based_analysis(self):
        mock_collection = MagicMock()
        mock_collection.aggregate.return_value = [
            {
                "_id": {"hour": 12},
                "total_accidents": 100,
                "total_injured": 50,
                "total_killed": 2,
            }
        ]
        self.mock_db.get_collection.return_value = mock_collection

        results = self.analyzer.create_time_based_analysis()

        self.assertIsNotNone(results)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["total_accidents"], 100)

    def test_create_borough_analysis(self):
        mock_collection = MagicMock()
        mock_collection.aggregate.return_value = [
            {
                "_id": "MANHATTAN",
                "total_accidents": 200,
                "total_injured": 100,
                "total_killed": 5,
                "pedestrian_injured": 30,
                "cyclist_injured": 20,
                "motorist_injured": 50,
            }
        ]
        self.mock_db.get_collection.return_value = mock_collection

        results = self.analyzer.create_borough_analysis()

        self.assertIsNotNone(results)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["_id"], "MANHATTAN")

    def test_create_vehicle_analysis(self):
        mock_collection = MagicMock()
        mock_collection.aggregate.return_value = [
            {
                "_id": "SEDAN",
                "total_accidents": 150,
                "total_injured": 75,
                "total_killed": 3,
                "avg_injuries_per_accident": 0.5,
            }
        ]
        self.mock_db.get_collection.return_value = mock_collection

        results = self.analyzer.create_vehicle_analysis()

        self.assertIsNotNone(results)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["_id"], "SEDAN")


class TestVisualizationCreator(unittest.TestCase):
    @patch("nyc_opendata.gold_layer.os.makedirs")
    def setUp(self, mock_makedirs):
        self.viz_creator = VisualizationCreator()
        self.viz_creator.db = MagicMock(spec=MongoDB)

    @patch("nyc_opendata.gold_layer.Figure")
    @patch("nyc_opendata.gold_layer.sns.barplot")
    def test_create_time_plot(self, mock_barplot, mock_figure):
        mock_collection = MagicMock()
        mock_collection.find.return_value = [
            {"_id": {"hour": 12}, "total_accidents": 100}
        ]
        self.viz_creator.db.get_collection.return_value = mock_collection

        self.viz_creator._create_time_plot()
        mock_figure.assert_called_once()

    @patch("nyc_opendata.gold_layer.Figure")
    @patch("nyc_opendata.gold_layer.sns.barplot")
    def test_create_borough_plot(self, mock_barplot, mock_figure):
        mock_collection = MagicMock()
        mock_collection.find.return_value = [
            {"_id": "MANHATTAN", "total_accidents": 200}
        ]
        self.viz_creator.db.get_collection.return_value = mock_collection

        self.viz_creator._create_borough_plot()
        mock_figure.assert_called_once()

    @patch("nyc_opendata.gold_layer.Figure")
    @patch("nyc_opendata.gold_layer.sns.barplot")
    def test_create_vehicle_plot(self, mock_barplot, mock_figure):
        mock_collection = MagicMock()
        mock_collection.find.return_value = [{"_id": "SEDAN", "total_accidents": 150}]
        self.viz_creator.db.get_collection.return_value = mock_collection

        self.viz_creator._create_vehicle_plot()
        mock_figure.assert_called_once()
