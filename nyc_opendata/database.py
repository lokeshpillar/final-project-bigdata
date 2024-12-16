from typing import Optional

from pymongo import MongoClient

from .config import MONGODB


class MongoDB:
    """A class to handle MongoDB database operations.

    This class provides methods to connect to a MongoDB database and perform various operations
    like getting collections, dropping collections, listing collections, and resetting the database.

    Attributes:
        connection_string (str): MongoDB connection string with authentication details
        database_name (str): Name of the MongoDB database
        _client (Optional[MongoClient]): MongoDB client instance
        _db (Optional[MongoClient]): MongoDB database instance
    """

    def __init__(
        self,
        host: str = MONGODB.host,
        port: int = MONGODB.port,
        username: str = MONGODB.username,
        password: str = MONGODB.password,
        database: str = MONGODB.database,
    ):
        """Initialize MongoDB connection.

        Args:
            host (str): MongoDB host address. Defaults to config value.
            port (int): MongoDB port number. Defaults to config value.
            username (str): MongoDB username. Defaults to config value.
            password (str): MongoDB password. Defaults to config value.
            database (str): Database name. Defaults to config value.
        """
        self.connection_string = f"mongodb://{username}:{password}@{host}:{port}/"
        self.database_name = database
        self._client = None
        self._db = None

    def connect(self) -> None:
        """Establish connection to MongoDB.

        Creates a new MongoDB client connection if one doesn't exist already.
        Sets up both the client and database instance attributes.
        """
        if not self._client:
            self._client = MongoClient(self.connection_string)
            self._db = self._client[self.database_name]

    def get_collection(self, collection_name: str) -> Optional[MongoClient]:
        """Get a specific collection from MongoDB.

        Attempts to connect to the database and retrieve the specified collection.
        Handles any connection errors and returns None if unsuccessful.

        Args:
            collection_name (str): Name of the collection to retrieve

        Returns:
            Optional[pymongo.collection.Collection]: MongoDB collection if successful, None otherwise

        Example:
            >>> db = MongoDB()
            >>> collection = db.get_collection("my_collection")
            >>> if collection:
            ...     # work with collection
        """
        try:
            self.connect()
            collection = self._db[collection_name]
            return collection
        except Exception as e:
            print(f"Failed to get collection {collection_name}: {e}")
            return None

    def drop_collection(self, collection_name: str) -> bool:
        """Drop a collection from MongoDB.

        Attempts to connect to the database and drop the specified collection.
        Prints status message and handles any errors that occur.

        Args:
            collection_name (str): Name of the collection to drop

        Returns:
            bool: True if collection was successfully dropped, False otherwise

        Example:
            >>> db = MongoDB()
            >>> if db.drop_collection("old_collection"):
            ...     print("Collection dropped successfully")
        """
        try:
            self.connect()
            self._db[collection_name].drop()
            print(f"Dropped collection: {collection_name}")
            return True
        except Exception as e:
            print(f"Error dropping collection {collection_name}: {e}")
            return False

    def list_collections(self) -> list:
        """List all collections in the database.

        Connects to the database and retrieves names of all collections.

        Returns:
            list: List of collection names as strings

        Example:
            >>> db = MongoDB()
            >>> collections = db.list_collections()
            >>> print("Available collections:", collections)
        """
        self.connect()
        return self._db.list_collection_names()

    def reset_database(self) -> None:
        """Clean up all data from MongoDB collections and output plots.

        Performs a complete reset of the database by:
        1. Connecting to the database
        2. Getting list of all collections
        3. Dropping each collection one by one
        4. Verifying all collections were dropped successfully

        Handles errors at both the database and collection level.
        Prints status messages throughout the process.

        Example:
            >>> db = MongoDB()
            >>> db.reset_database()
            # Output: Database reset successful - all collections deleted
        """
        try:
            self.connect()
            collections = self.list_collections()
            for collection in collections:
                try:
                    self.drop_collection(collection)
                except Exception as e:
                    print(f"Error dropping collection {collection}: {e}")

            remaining_collections = self.list_collections()
            if remaining_collections:
                print("Database reset failed - some collections remain")
            else:
                print("Database reset successful - all collections deleted")
        except Exception as e:
            print(f"Error resetting database: {e}")

    def close(self) -> None:
        """Close the MongoDB connection.

        Closes the active MongoDB client connection if one exists and
        resets both client and database instance attributes to None.

        Example:
            >>> db = MongoDB()
            >>> # ... do some database operations ...
            >>> db.close()  # Clean up connection when done
        """
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
