from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from nyc_opendata.data_cleaning import DataCleaner


@pytest.fixture
def mock_db():
    """Fixture to provide a mocked database connection"""
    with patch("nyc_opendata.database.MongoDB") as mock_db:
        mock_raw_collection = Mock()
        mock_clean_collection = Mock()

        mock_db.return_value.get_collection.side_effect = lambda x: {
            "raw_vehicle_collisions": mock_raw_collection,
            "clean_vehicle_collisions": mock_clean_collection,
        }.get(x)

        yield mock_db.return_value


@pytest.fixture
def data_cleaner(mock_db):
    """Fixture to provide DataCleaner instance with mocked db"""
    cleaner = DataCleaner(batch_size=100)
    cleaner.db = mock_db
    return cleaner


@pytest.fixture
def sample_raw_doc():
    """Fixture to provide sample raw document for testing"""
    return {
        "collision_id": "123",
        "crash_date": datetime(2023, 1, 1),
        "crash_time": "12:00",
        "borough": "MANHATTAN",
        "zip_code": "10001",
        "number_of_persons_injured": "0",
        "number_of_persons_killed": "0",
        "number_of_pedestrians_injured": "0",
        "number_of_pedestrians_killed": "0",
        "number_of_cyclist_injured": "0",
        "number_of_cyclist_killed": "0",
        "number_of_motorist_injured": "0",
        "number_of_motorist_killed": "0",
    }


def test_clean_document(data_cleaner, sample_raw_doc):
    """Test document cleaning logic without database interaction"""
    cleaned_doc = data_cleaner._clean_document(sample_raw_doc)

    assert cleaned_doc is not None
    assert cleaned_doc["collision_id"] == "123"
    assert cleaned_doc["location"]["borough"] == "MANHATTAN"
    assert cleaned_doc["location"]["zip_code"] == 10001


@pytest.mark.parametrize("batch_size", [1, 2, 5])
def test_process_batch(data_cleaner, sample_raw_doc, batch_size):
    """Test batch processing with different sizes"""
    batch = [sample_raw_doc] * batch_size
    cleaned_batch = data_cleaner._process_batch(batch)

    assert len(cleaned_batch) == batch_size
    assert all(doc["collision_id"] == "123" for doc in cleaned_batch)


@patch("nyc_opendata.data_cleaning.DataCleaner._insert_batch")
def test_clean_data_integration(mock_insert, data_cleaner, mock_db):
    """Test full data cleaning process with mocked database"""

    mock_raw_docs = [
        {
            "collision_id": str(i),
            "crash_date": datetime(2023, 1, 1),
            "crash_time": "12:00",
            "borough": "MANHATTAN",
            "zip_code": "10001",
            "number_of_persons_injured": "0",
            "number_of_persons_killed": "0",
            "number_of_pedestrians_injured": "0",
            "number_of_pedestrians_killed": "0",
            "number_of_cyclist_injured": "0",
            "number_of_cyclist_killed": "0",
            "number_of_motorist_injured": "0",
            "number_of_motorist_killed": "0",
        }
        for i in range(5)
    ]

    raw_collection = mock_db.get_collection("raw_vehicle_collisions")
    raw_collection.count_documents.return_value = len(mock_raw_docs)
    raw_collection.find.return_value.skip.return_value.limit.return_value = (
        mock_raw_docs
    )

    clean_collection = mock_db.get_collection("clean_vehicle_collisions")

    data_cleaner.clean_data()

    assert raw_collection.count_documents.called
    assert raw_collection.find.called
    assert clean_collection.drop.called
    assert clean_collection.create_index.called
    assert mock_insert.called


def test_handle_missing_data(data_cleaner):
    """Test handling of missing data"""
    doc_with_missing = {
        "collision_id": "456",
        "crash_date": datetime(2023, 1, 1),
        "crash_time": "12:00",
        "borough": None,
        "zip_code": None,
        "number_of_persons_injured": "0",
        "number_of_persons_killed": "0",
        "number_of_pedestrians_injured": "0",
        "number_of_pedestrians_killed": "0",
        "number_of_cyclist_injured": "0",
        "number_of_cyclist_killed": "0",
        "number_of_motorist_injured": "0",
        "number_of_motorist_killed": "0",
    }

    cleaned_doc = data_cleaner._clean_document(doc_with_missing)

    assert cleaned_doc is not None
    assert cleaned_doc["location"]["borough"] == "Unknown"
    assert cleaned_doc["location"]["zip_code"] is None


def test_batch_insertion_error_handling(data_cleaner):
    """Test error handling during batch insertion"""
    batch = [{"collision_id": "123"}]
    collection = Mock()
    collection.insert_many.side_effect = Exception("Database error")

    data_cleaner._insert_batch(batch, collection)
    assert collection.insert_many.called


def test_verify_cleaning(data_cleaner, mock_db):
    """Test cleaning verification with mocked data"""

    raw_collection = mock_db.get_collection("raw_vehicle_collisions")
    clean_collection = mock_db.get_collection("clean_vehicle_collisions")

    raw_collection.count_documents.return_value = 100
    clean_collection.count_documents.return_value = 95
    clean_collection.find_one.return_value = {
        "collision_id": "123",
        "location": {"borough": "MANHATTAN"},
    }

    data_cleaner.verify_cleaning()

    assert raw_collection.count_documents.called
    assert clean_collection.count_documents.called
    assert clean_collection.find_one.called
