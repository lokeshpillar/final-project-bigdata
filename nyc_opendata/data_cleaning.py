from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from tqdm import tqdm

from .config import COLLECTIONS
from .database import MongoDB


class DataCleaner:
    """Handles cleaning and transformation of vehicle collision data."""

    def __init__(self, batch_size: int = 10000):
        """Initialize DataCleaner with batch size for processing.

        Args:
            batch_size: Number of records to process in each batch
        """
        self.db = MongoDB()
        self.batch_size = batch_size

    def clean_data(self) -> None:
        """Clean and transform raw collision data into structured format.

        Raises:
            ConnectionError: If database connection fails
            Exception: For other processing errors
        """
        try:
            self.db.connect()
            raw_collection = self.db.get_collection(COLLECTIONS.raw_vehicle_collisions)
            clean_collection = self.db.get_collection(
                COLLECTIONS.clean_vehicle_collisions
            )

            if raw_collection is None or clean_collection is None:
                raise ConnectionError("Failed to get database collections")

            clean_collection.drop()
            clean_collection.create_index("collision_id", unique=True)

            total_docs = raw_collection.count_documents({})

            for i in tqdm(range(0, total_docs, self.batch_size)):
                try:
                    batch = list(raw_collection.find().skip(i).limit(self.batch_size))
                    clean_docs = self._process_batch(batch)

                    if clean_docs:
                        self._insert_batch(clean_docs, clean_collection)

                except Exception as e:
                    print(f"Error processing batch starting at {i}: {str(e)}")
                    continue

        except Exception as e:
            print(f"Error in data cleaning process: {str(e)}")
            raise
        finally:
            self.db.close()

    def _process_batch(self, batch: List[Dict]) -> List[Dict]:
        """Process a batch of raw documents into cleaned format.

        Args:
            batch: List of raw documents to process

        Returns:
            List of cleaned documents
        """
        clean_docs = []
        for doc in batch:
            try:
                clean_doc = self._clean_document(doc)
                if clean_doc:
                    clean_docs.append(clean_doc)
            except Exception as e:
                print(
                    f"Error processing document ID {doc.get('collision_id')}: {str(e)}"
                )
                continue
        return clean_docs

    def _clean_document(self, doc: Dict) -> Optional[Dict]:
        """Clean and transform a single document.

        Args:
            doc: Raw document to clean

        Returns:
            Cleaned document or None if processing fails
        """
        try:
            borough = doc.get("borough")
            if pd.isna(borough) or not borough:
                borough = "Unknown"

            return {
                "collision_id": doc["collision_id"],
                "crash_datetime": pd.to_datetime(
                    f"{doc['crash_date'].strftime('%Y-%m-%d')} {doc['crash_time']}"
                ),
                "location": {
                    "borough": borough,
                    "zip_code": (
                        int(doc["zip_code"]) if pd.notna(doc.get("zip_code")) else None
                    ),
                    "latitude": (
                        float(doc["latitude"])
                        if pd.notna(doc.get("latitude"))
                        else None
                    ),
                    "longitude": (
                        float(doc["longitude"])
                        if pd.notna(doc.get("longitude"))
                        else None
                    ),
                    "on_street": doc.get("on_street_name", ""),
                    "cross_street": doc.get("cross_street_name", ""),
                    "off_street": doc.get("off_street_name", ""),
                },
                "casualties": {
                    "total_injured": int(doc["number_of_persons_injured"]),
                    "total_killed": int(doc["number_of_persons_killed"]),
                    "pedestrians": {
                        "injured": int(doc["number_of_pedestrians_injured"]),
                        "killed": int(doc["number_of_pedestrians_killed"]),
                    },
                    "cyclists": {
                        "injured": int(doc["number_of_cyclist_injured"]),
                        "killed": int(doc["number_of_cyclist_killed"]),
                    },
                    "motorists": {
                        "injured": int(doc["number_of_motorist_injured"]),
                        "killed": int(doc["number_of_motorist_killed"]),
                    },
                },
                "vehicles": {
                    "vehicle_1": {
                        "type": doc.get("vehicle_type_code1", "Unknown"),
                        "contributing_factor": doc.get(
                            "contributing_factor_vehicle_1", "Unknown"
                        ),
                    },
                    "vehicle_2": {
                        "type": doc.get("vehicle_type_code2", "Unknown"),
                        "contributing_factor": doc.get(
                            "contributing_factor_vehicle_2", "Unknown"
                        ),
                    },
                },
                "created_at": datetime.now(),
            }
        except Exception as e:
            print(f"Error cleaning document: {str(e)}")
            return None

    def _insert_batch(self, docs: List[Dict], collection) -> None:
        """Insert batch of documents into collection.

        Args:
            docs: List of documents to insert
            collection: MongoDB collection
        """
        try:
            collection.insert_many(docs, ordered=False)
        except Exception as e:
            print(f"Duplicate documents removed: {str(e).count('dup key')}")

    def verify_cleaning(self) -> None:
        """Verify cleaning results and print summary statistics."""
        try:
            self.db.connect()
            raw_collection = self.db.get_collection(COLLECTIONS.raw_vehicle_collisions)
            clean_collection = self.db.get_collection(
                COLLECTIONS.clean_vehicle_collisions
            )

            if raw_collection is None or clean_collection is None:
                raise ConnectionError("Failed to get collections for verification")

            raw_count = raw_collection.count_documents({})
            clean_count = clean_collection.count_documents({})

            print(f"\nOriginal documents: {raw_count}")
            print(f"Cleaned documents: {clean_count}")
            print(f"Difference: {raw_count - clean_count}")

            sample_doc = clean_collection.find_one()
            if sample_doc:
                print("\nSample cleaned document:")
                for key, value in sample_doc.items():
                    print(f"{key}: {value}")
            else:
                print("\nNo sample document found")

        except Exception as e:
            print(f"Error verifying cleaning: {str(e)}")
            raise
        finally:
            self.db.close()


def main() -> None:
    """Main execution function."""
    try:
        print("Starting data cleaning process...")
        cleaner = DataCleaner()
        cleaner.clean_data()
        cleaner.verify_cleaning()
        print("Data cleaning completed successfully")
    except Exception as e:
        print(f"Fatal error in data cleaning: {str(e)}")
        raise


if __name__ == "__main__":
    main()
