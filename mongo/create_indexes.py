"""Create recommended indexes for the `propiedades` collection.

Usage:
  - Set `MONGO_URI` in your environment or copy `.env.example` -> `.env` and fill.
  - Run: `python mongo/create_indexes.py`

This script is idempotent: calling create_index on an existing index is a no-op.
"""
import os
from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DATABASE = os.getenv("MONGO_DB_NAME", "ID2")
COLLECTION = os.getenv("MONGO_COLLECTION_NAME", "propiedades")


def main():
    if not MONGO_URI:
        print("MONGO_URI not set. Please set it in the environment or .env file.")
        return

    client = MongoClient(MONGO_URI)
    db = client[DATABASE]
    coll = db[COLLECTION]

    print(f"Creating indexes on {DATABASE}.{COLLECTION} ...")

    # Index for fast lookup by city (case-insensitive search recommended via collation)
    try:
        coll.create_index([("ubicacion.ciudad", ASCENDING)], name="idx_ubicacion_ciudad")
        print("Created idx_ubicacion_ciudad")
    except Exception as e:
        print("Failed to create idx_ubicacion_ciudad:", e)

    # Index for zone (zona / ubicacion.zona)
    try:
        coll.create_index([("ubicacion.zona", ASCENDING)], name="idx_ubicacion_zona")
        coll.create_index([("zona", ASCENDING)], name="idx_zona_root")
        print("Created idx_ubicacion_zona and idx_zona_root")
    except Exception as e:
        print("Failed to create zona indexes:", e)

    # Multikey index on reviews' rating
    try:
        coll.create_index([("resenas.calificacion", DESCENDING)], name="idx_resenas_calificacion")
        print("Created idx_resenas_calificacion (multikey)")
    except Exception as e:
        print("Failed to create idx_resenas_calificacion:", e)

    # Index on review date for recent reviews
    try:
        coll.create_index([("resenas.fecha", DESCENDING)], name="idx_resenas_fecha")
        print("Created idx_resenas_fecha")
    except Exception as e:
        print("Failed to create idx_resenas_fecha:", e)

    # Example geospatial index (uncomment if you have ubicacion.location GeoJSON points)
    try:
        # coll.create_index([("ubicacion.location", "2dsphere")], name="idx_ubicacion_2dsphere")
        pass
    except Exception as e:
        print("Failed to create geospatial index:", e)

    print("Indexes creation finished.")


if __name__ == "__main__":
    main()
