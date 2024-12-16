import pytest
from pymongo.collection import Collection
from pymongo.database import Database

from nyc_opendata.database import MongoDB


@pytest.fixture
def mongodb():
    """Fixture to provide MongoDB instance for tests"""
    db = MongoDB()
    try:
        db.connect()
        yield db
    except Exception as e:
        pytest.skip(f"MongoDB not available: {e}")
    finally:
        db.close()


def test_connection(mongodb):
    """Test MongoDB connection"""
    assert mongodb._db is not None
    assert isinstance(mongodb._db, Database)
    assert mongodb._db.name == "nyc_data"


def test_get_collection(mongodb):
    """Test getting a collection"""
    collection = mongodb.get_collection("test_collection")
    assert isinstance(collection, Collection)
    assert collection.name == "test_collection"


def test_drop_collection(mongodb):
    """Test dropping a collection"""
    # Create a test collection
    collection = mongodb.get_collection("test_collection")
    collection.insert_one({"test": "data"})

    # Drop the collection
    result = mongodb.drop_collection("test_collection")
    assert result is True

    # Verify collection is dropped
    collections = mongodb.list_collections()
    assert "test_collection" not in collections


def test_list_collections(mongodb):
    """Test listing collections"""
    # Create test collections
    mongodb.get_collection("test_collection1").insert_one({"test": "data1"})
    mongodb.get_collection("test_collection2").insert_one({"test": "data2"})

    collections = mongodb.list_collections()
    assert isinstance(collections, list)
    assert "test_collection1" in collections
    assert "test_collection2" in collections

    # Cleanup
    mongodb.drop_collection("test_collection1")
    mongodb.drop_collection("test_collection2")
