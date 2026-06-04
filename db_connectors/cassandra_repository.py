import os
from datetime import datetime, date, timezone, timedelta

from astrapy import DataAPIClient


class CassandraRepository:
    def __init__(self, token=None, endpoint=None, database_name=None, collection_name=None):
        self.token = token or os.getenv("ASTRA_DB_TOKEN")
        self.endpoint = endpoint or os.getenv("ASTRA_DB_ENDPOINT")
        self.database_name = database_name or os.getenv("ASTRA_DB_NAME", "airbnb")
        self.collection_name = collection_name or os.getenv("ASTRA_COLLECTION_NAME", os.getenv("ASTRA_VISITS_TABLE", "historial_vistas"))
        self.availability_table_name = os.getenv("ASTRA_AVAILABILITY_TABLE", "disponibilidad_propiedad")
        self.visits_table_name = os.getenv("ASTRA_VISITS_TABLE", "historial_vistas")
        self.client = None
        self.db = None
        self.collection = None
        self.availability_table = None
        self.visits_table = None
        self.connect()

    def connect(self):
        if not self.token or not self.endpoint:
            return False

        try:
            self.client = DataAPIClient(self.token)
            self.db = self.client.get_database(self.endpoint, keyspace=self.database_name)
            try:
                self.collection = self.db.get_collection(self.collection_name)
            except Exception:
                self.collection = self.db.create_collection(self.collection_name)

            try:
                self.availability_table = self.db.get_table(self.availability_table_name)
            except Exception:
                self.availability_table = None

            try:
                self.visits_table = self.db.get_table(self.visits_table_name)
            except Exception:
                self.visits_table = None
            return True
        except Exception:
            self.client = None
            self.db = None
            self.collection = None
            self.availability_table = None
            self.visits_table = None
            return False

    def _parse_date(self, value):
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            value = value.strip()
            try:
                return date.fromisoformat(value)
            except ValueError:
                pass
        text = str(value).strip()
        if len(text) >= 10:
            try:
                return date.fromisoformat(text[:10])
            except ValueError:
                pass
        raise ValueError(f"Fecha inválida: {value}")

    def check_property_availability(self, property_id, start_date, end_date):
        if self.availability_table is None:
            raise RuntimeError("Tabla disponibilidad_propiedad no disponible.")

        start = self._parse_date(start_date)
        end = self._parse_date(end_date)
        if start > end:
            raise ValueError("La fecha de inicio no puede ser mayor que la fecha de fin.")

        expected_days = (end - start).days + 1

        query = {
            "propiedad_id": property_id,
            "fecha": {"$gte": start, "$lte": end},
        }
        rows = list(self.availability_table.find(query))

        available_by_day = {}
        for row in rows:
            row_date = self._parse_date(row.get("fecha"))
            available_by_day[row_date] = bool(row.get("disponible"))

        missing_days = []
        unavailable_days = []
        current_day = start
        while current_day <= end:
            if current_day not in available_by_day:
                missing_days.append(current_day.isoformat())
            elif not available_by_day[current_day]:
                unavailable_days.append(current_day.isoformat())
            current_day += timedelta(days=1)

        is_available = not missing_days and not unavailable_days
        return {
            "property_id": property_id,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "available": is_available,
            "missing_days": missing_days,
            "unavailable_days": unavailable_days,
            "days_checked": expected_days,
        }

    def register_event(self, user_id, event_type, property_id=None, payload=None):
        if self.collection is None:
            raise RuntimeError("Cassandra no conectado.")

        document = {
            "user_id": user_id,
            "event_type": event_type,
            "property_id": property_id,
            "payload": payload or {},
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        return self.collection.insert_one(document)

    def get_user_events(self, user_id, limit=10):
        if self.collection is None:
            raise RuntimeError("Cassandra no conectado.")

        events = list(self.collection.find({"user_id": user_id}))
        events.sort(key=lambda item: item.get("created_at", ""), reverse=True)
        return events[: int(limit)]

    def close(self):
        if self.client and hasattr(self.client, "close"):
            self.client.close()
        self.client = None
        self.db = None
        self.collection = None
        self.availability_table = None
        self.visits_table = None
