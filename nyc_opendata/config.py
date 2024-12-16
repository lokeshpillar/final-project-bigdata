from dataclasses import dataclass
from pathlib import Path


@dataclass
class MongoDBConfig:
    host: str = "localhost"
    port: int = 27017
    username: str = "admin"
    password: str = "password"
    database: str = "nyc_data"


@dataclass
class CollectionNames:
    raw_vehicle_collisions: str = "raw_vehicle_collisions"
    clean_vehicle_collisions: str = "clean_vehicle_collisions"
    gold_time_analysis: str = "gold_time_analysis"
    gold_borough_analysis: str = "gold_borough_analysis"
    gold_vehicle_analysis: str = "gold_vehicle_analysis"


@dataclass
class VisualizationConfig:
    output_dir: Path = Path("plots")
    figure_size_large: tuple[int, int] = (12, 6)
    top_vehicles_limit: int = 10


COLLECTIONS = CollectionNames()
MONGODB = MongoDBConfig()
VIZ_CONFIG = VisualizationConfig()
