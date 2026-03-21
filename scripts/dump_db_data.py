import logging
import shutil

import geopandas as gpd
import pandas as pd
from geoalchemy2 import load_spatialite
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.event import listen
from water_quality.logs import setup_logging
from water_quality.summaries.summary import (
    add_water_quality_observations_to_db,
)
from waterbodies.db import get_waterbodies_engine, get_test_waterbodies_engine
from waterbodies.db_models import WaterbodyBase
from waterbodies.surface_area_change import create_waterbodies_observations_table
from waterbodies.historical_extent import add_waterbodies_polygons_to_db, create_waterbodies_historical_extent_table
log_level = getattr(logging, "INFO")
_log = setup_logging(log_level)

engine = get_waterbodies_engine()
_log.info(f"PostgreSQL waterbodies db url: {engine.url}")


def get_custom_engine(file_db_path: str) -> Engine:
    """Get a SQLite file-based database engine."""
    engine = create_engine(
        f"sqlite+pysqlite:///{file_db_path}", echo=False, future=True
    )
    listen(engine, "connect", load_spatialite)
    # Create the required waterbodies tables in the engine
    metadata_obj = WaterbodyBase.metadata
    metadata_obj.create_all(bind=engine, checkfirst=True)
    return engine


shutil.copyfile("/tmp/sample_waterbodies.db",  "/tmp/test_waterbodies.db")
file_db_path = "/tmp/test_waterbodies.db"
file_db_engine = get_test_waterbodies_engine()
_log.info(f"File-based SQLite database  db url: {file_db_engine.url}")

# Add historical extent sample data
sample_waterbody_observations = gpd.read_file(file_db_path, layer="waterbodies_historical_extent")
add_waterbodies_polygons_to_db(sample_waterbody_observations, engine)

# Add surface area change sample data
create_waterbodies_observations_table(engine)

# Add water quality sample data
sample_water_quality_observations = pd.read_sql("SELECT * FROM waterbodies_water_quality ", con=file_db_engine)
add_water_quality_observations_to_db(sample_water_quality_observations, engine)