import time

import pandas as pd
import requests
from tqdm import tqdm

from .config import COLLECTIONS
from .database import MongoDB


def fetch_nyc_data(offset, limit=50000):
    """Fetch data from NYC Open Data API"""
    base_url = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"

    params = {
        "$limit": limit,
        "$offset": offset,
    }

    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        print(f"Fetched {len(data)} records")
        return data
    else:
        print(f"Error fetching data: {response.status_code}")
        return None


def process_and_store_data(data, collection):
    """Process and store data in MongoDB"""
    if not data:
        return 0

    df = pd.DataFrame(data)

    if "crash_date" in df.columns:
        df["crash_date"] = pd.to_datetime(df["crash_date"])

    numeric_columns = [
        "number_of_persons_injured",
        "number_of_persons_killed",
        "number_of_pedestrians_injured",
        "number_of_pedestrians_killed",
        "number_of_cyclist_injured",
        "number_of_cyclist_killed",
        "number_of_motorist_injured",
        "number_of_motorist_killed",
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    records = df.to_dict("records")

    if records:
        collection.insert_many(records)

    return len(records)


def main():
    db = MongoDB()
    try:
        db.connect()
        collection = db.get_collection(COLLECTIONS.raw_vehicle_collisions)
        if collection is None:
            print("Exiting due to MongoDB connection failure")
            return

        target_docs = 100000
        current_docs = 0
        offset = 0
        batch_size = 10000

        with tqdm(total=target_docs) as pbar:
            while current_docs < target_docs:
                data = fetch_nyc_data(offset, batch_size)
                if not data:
                    break

                processed_count = process_and_store_data(data, collection)
                if processed_count == 0:
                    break

                current_docs += processed_count
                offset += batch_size
                pbar.update(processed_count)

                time.sleep(1)

        final_count = collection.count_documents({})
        print(f"\nTotal documents stored: {final_count}")

        print("\nSample document structure:")
        print(collection.find_one())
    finally:
        db.close()


if __name__ == "__main__":
    main()
