import os
from typing import Any, Dict, List

import pandas as pd
import seaborn as sns
from matplotlib.figure import Figure

from .config import COLLECTIONS, VIZ_CONFIG
from .database import MongoDB


class DataAnalyzer:
    """
    Handles data analysis operations for vehicle collision data.

    This class encapsulates all analysis operations including time-based,
    borough-based, and vehicle-based analysis of collision data. It manages
    database interactions and ensures proper error handling.

    Attributes:
        db (MongoDB): Database connection instance
    """

    def __init__(self, db: MongoDB):
        """
        Initialize DataAnalyzer with database connection.

        Args:
            db (MongoDB): Connected MongoDB database instance

        Raises:
            ConnectionError: If database connection is not valid
        """
        self.db = db
        self._validate_db_connection()

    def _validate_db_connection(self) -> None:
        """
        Validate that database connection is active.

        Raises:
            ConnectionError: If database connection is not established
        """
        if self.db._client is None or self.db._db is None:
            raise ConnectionError("Database connection not established")

    def create_time_based_analysis(self) -> List[Dict[str, Any]]:
        """
        Create time-based aggregations of vehicle collision data.

        Aggregates collision data by hour of day, calculating total accidents,
        injuries, and fatalities for each hour.

        Returns:
            List[Dict[str, Any]]: Aggregation results containing hourly statistics

        Raises:
            ValueError: If required collections cannot be accessed
            Exception: For other database operation failures
        """
        try:
            clean_collection = self.db.get_collection(
                COLLECTIONS.clean_vehicle_collisions
            )
            if clean_collection is None:
                raise ValueError("Could not access clean_vehicle_collisions collection")

            time_analysis = self.db.get_collection(COLLECTIONS.gold_time_analysis)
            if time_analysis is None:
                raise ValueError("Could not access gold_time_analysis collection")

            time_analysis.drop()

            pipeline = [
                {
                    "$group": {
                        "_id": {"hour": {"$hour": "$crash_datetime"}},
                        "total_accidents": {"$sum": 1},
                        "total_injured": {"$sum": "$casualties.total_injured"},
                        "total_killed": {"$sum": "$casualties.total_killed"},
                    }
                },
                {"$sort": {"_id.hour": 1}},
            ]

            results = list(clean_collection.aggregate(pipeline))
            if not results:
                print("No results found for time-based analysis")
                return []

            time_analysis.insert_many(results)
            print("Created time-based analysis")
            return results

        except ValueError as ve:
            print(f"Collection access error: {str(ve)}")
            raise
        except Exception as e:
            print(f"Error in time-based analysis: {str(e)}")
            raise

    def create_borough_analysis(self) -> List[Dict[str, Any]]:
        """
        Create location-based aggregations by borough.

        Analyzes collision patterns across NYC boroughs, including total accidents,
        injuries by type (pedestrian, cyclist, motorist), and fatalities.

        Returns:
            List[Dict[str, Any]]: Aggregation results containing borough-wise statistics

        Raises:
            ValueError: If required collections cannot be accessed
            Exception: For other database operation failures
        """
        try:
            clean_collection = self.db.get_collection(
                COLLECTIONS.clean_vehicle_collisions
            )
            if clean_collection is None:
                raise ValueError("Could not access clean_vehicle_collisions collection")

            borough_analysis = self.db.get_collection(COLLECTIONS.gold_borough_analysis)
            if borough_analysis is None:
                raise ValueError("Could not access gold_borough_analysis collection")

            borough_analysis.drop()

            pipeline = [
                {
                    "$group": {
                        "_id": "$location.borough",
                        "total_accidents": {"$sum": 1},
                        "total_injured": {"$sum": "$casualties.total_injured"},
                        "total_killed": {"$sum": "$casualties.total_killed"},
                        "pedestrian_injured": {
                            "$sum": "$casualties.pedestrians.injured"
                        },
                        "cyclist_injured": {"$sum": "$casualties.cyclists.injured"},
                        "motorist_injured": {"$sum": "$casualties.motorists.injured"},
                    }
                },
                {"$match": {"_id": {"$ne": "Unknown"}}},
                {"$sort": {"total_accidents": -1}},
            ]

            results = list(clean_collection.aggregate(pipeline))
            if not results:
                print("No results found for borough analysis")
                return []

            borough_analysis.insert_many(results)
            print("Created borough analysis")
            return results

        except ValueError as ve:
            print(f"Collection access error: {str(ve)}")
            raise
        except Exception as e:
            print(f"Error in borough analysis: {str(e)}")
            raise

    def create_vehicle_analysis(self) -> List[Dict[str, Any]]:
        """
        Create vehicle type-based analysis.

        Analyzes collision patterns by vehicle type, including total accidents,
        injuries, fatalities, and average injuries per accident.

        Returns:
            List[Dict[str, Any]]: Aggregation results containing vehicle type statistics

        Raises:
            ValueError: If required collections cannot be accessed
            Exception: For other database operation failures
        """
        try:
            clean_collection = self.db.get_collection(
                COLLECTIONS.clean_vehicle_collisions
            )
            if clean_collection is None:
                raise ValueError("Could not access clean_vehicle_collisions collection")

            vehicle_analysis = self.db.get_collection(COLLECTIONS.gold_vehicle_analysis)
            if vehicle_analysis is None:
                raise ValueError("Could not access gold_vehicle_analysis collection")

            vehicle_analysis.drop()

            pipeline = [
                {
                    "$group": {
                        "_id": "$vehicles.vehicle_1.type",
                        "total_accidents": {"$sum": 1},
                        "total_injured": {"$sum": "$casualties.total_injured"},
                        "total_killed": {"$sum": "$casualties.total_killed"},
                        "avg_injuries_per_accident": {
                            "$avg": "$casualties.total_injured"
                        },
                    }
                },
                {"$match": {"_id": {"$ne": "Unknown"}}},
                {"$sort": {"total_accidents": -1}},
                {"$limit": VIZ_CONFIG.top_vehicles_limit},
            ]

            results = list(clean_collection.aggregate(pipeline))
            if not results:
                print("No results found for vehicle analysis")
                return []

            vehicle_analysis.insert_many(results)
            print("Created vehicle analysis")
            return results

        except ValueError as ve:
            print(f"Collection access error: {str(ve)}")
            raise
        except Exception as e:
            print(f"Error in vehicle analysis: {str(e)}")
            raise


class VisualizationCreator:
    """
    Handles creation of visualizations from analyzed data.

    This class manages the generation of various plots and charts
    based on the analyzed collision data.

    Attributes:
        db (MongoDB): Database connection for querying data
        output_dir (str): Directory where visualizations will be saved
    """

    def __init__(self):
        """Initialize VisualizationCreator with database connection."""
        self.db = MongoDB()
        self.db.connect()
        self.output_dir = VIZ_CONFIG.output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def create_all_visualizations(self) -> None:
        """
        Create all visualization plots sequentially.

        Creates three visualization plots:
        1. Hourly distribution of accidents
        2. Borough-wise accident comparison
        3. Top vehicle types involved in accidents

        Raises:
            Exception: If visualization creation fails
        """
        try:
            self._create_time_plot()
            self._create_borough_plot()
            self._create_vehicle_plot()
            print("Successfully created all visualizations")

        except Exception as e:
            print(f"Error creating visualizations: {str(e)}")
            raise
        finally:
            self.db.close()

    def _create_time_plot(self) -> None:
        """Create hourly distribution plot."""
        try:
            collection = self.db.get_collection(COLLECTIONS.gold_time_analysis)
            time_data = pd.DataFrame(list(collection.find()))

            fig = Figure(figsize=VIZ_CONFIG.figure_size_large, dpi=300)
            ax = fig.add_subplot(111)
            ax.plot(
                time_data["_id"].apply(lambda x: x["hour"]),
                time_data["total_accidents"],
                marker="o",
                linewidth=2,
            )
            ax.set_title("Distribution of Accidents by Hour of Day", pad=20)
            ax.set_xlabel("Hour of Day")
            ax.set_ylabel("Number of Accidents")
            ax.grid(True, alpha=0.3)
            fig.savefig(
                os.path.join(self.output_dir, "hourly_distribution.png"),
                bbox_inches="tight",
            )
        except Exception as e:
            print(f"Error creating time plot: {str(e)}")
            raise

    def _create_borough_plot(self) -> None:
        """Create borough comparison plot."""
        try:
            collection = self.db.get_collection(COLLECTIONS.gold_borough_analysis)
            borough_data = pd.DataFrame(list(collection.find()))

            fig = Figure(figsize=VIZ_CONFIG.figure_size_large, dpi=300)
            ax = fig.add_subplot(111)
            sns.barplot(data=borough_data, x="_id", y="total_accidents", ax=ax)
            ax.set_title("Total Accidents by Borough", pad=20)
            ax.set_xlabel("Borough")
            ax.set_ylabel("Number of Accidents")
            ax.tick_params(axis="x", rotation=45)
            fig.tight_layout()
            fig.savefig(
                os.path.join(self.output_dir, "borough_comparison.png"),
                bbox_inches="tight",
            )
        except Exception as e:
            print(f"Error creating borough plot: {str(e)}")
            raise

    def _create_vehicle_plot(self) -> None:
        """Create vehicle type analysis plot."""
        try:
            collection = self.db.get_collection(COLLECTIONS.gold_vehicle_analysis)
            vehicle_data = pd.DataFrame(list(collection.find()))

            fig = Figure(figsize=VIZ_CONFIG.figure_size_large, dpi=300)
            ax = fig.add_subplot(111)
            sns.barplot(
                data=vehicle_data.head(VIZ_CONFIG.top_vehicles_limit),
                x="total_accidents",
                y="_id",
                ax=ax,
            )
            ax.set_title("Top 10 Vehicle Types Involved in Accidents", pad=20)
            ax.set_xlabel("Number of Accidents")
            ax.set_ylabel("Vehicle Type")
            fig.tight_layout()
            fig.savefig(
                os.path.join(self.output_dir, "vehicle_analysis.png"),
                bbox_inches="tight",
            )
        except Exception as e:
            print(f"Error creating vehicle plot: {str(e)}")
            raise


def main() -> None:
    """
    Main execution function.

    Orchestrates the entire data analysis and visualization process:
    1. Establishes database connection
    2. Performs data analysis
    3. Creates visualizations
    4. Ensures proper cleanup

    Raises:
        Exception: If any step in the process fails
    """
    db = MongoDB()
    try:
        db.connect()
        print("Starting gold layer processing...")

        analyzer = DataAnalyzer(db)
        analyzer.create_time_based_analysis()
        analyzer.create_borough_analysis()
        analyzer.create_vehicle_analysis()

        print("Creating visualizations...")
        viz_creator = VisualizationCreator()
        viz_creator.create_all_visualizations()

        print("Gold layer processing completed successfully!")

    except Exception as e:
        print(f"Error in gold layer processing: {str(e)}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
