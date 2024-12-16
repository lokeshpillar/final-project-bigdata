import os
import shutil
from .database import MongoDB
from .config import VIZ_CONFIG


def main():
    """Clean up all data from MongoDB collections and output plots.

    This function:
    1. Creates a MongoDB instance
    2. Calls reset_database() to clean up collections and plots
    3. Removes the entire output directory
    4. Closes the connection

    Prints status messages for each operation and any errors encountered.
    """
    db = MongoDB()
    try:
        if os.path.exists(VIZ_CONFIG.output_dir):
            try:
                shutil.rmtree(VIZ_CONFIG.output_dir)
                print(f"Removed directory: {VIZ_CONFIG.output_dir}")
            except Exception as e:
                print(f"Error removing directory {VIZ_CONFIG.output_dir}: {e}")

        db.reset_database()
    finally:
        db.close()


if __name__ == "__main__":
    main()
