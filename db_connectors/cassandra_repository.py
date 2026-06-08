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

    def _table_command(self, table_name, body):
        if self.db is None:
            raise RuntimeError("Cassandra no conectado.")
        return self.db.command(body, collection_name=table_name)

    def _table_find(self, table_name, filter_doc):
        response = self._table_command(table_name, {"find": {"filter": filter_doc}})
        data = response.get("data", {}) if isinstance(response, dict) else {}
        return data.get("documents", []) or []

    def _table_find_one(self, table_name, filter_doc):
        response = self._table_command(table_name, {"findOne": {"filter": filter_doc}})
        data = response.get("data", {}) if isinstance(response, dict) else {}
        return data.get("document")

    def _table_insert_one(self, table_name, row):
        return self._table_command(table_name, {"insertOne": {"document": row}})

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
                self._table_find_one(
                    self.availability_table_name,
                    {"propiedad_id": "__healthcheck__", "fecha": "1900-01-01"},
                )
                self.availability_table = self.availability_table_name
            except Exception as availability_err:
                self.availability_table = None
                print(f"✗ Error tabla Astra '{self.availability_table_name}': {availability_err}")

            try:
                self.visits_table = self.visits_table_name
            except Exception as visits_err:
                self.visits_table = None
                print(f"✗ Error tabla Astra '{self.visits_table_name}': {visits_err}")
            return True
        except Exception as conn_err:
            self.client = None
            self.db = None
            self.collection = None
            self.availability_table = None
            self.visits_table = None
            print(f"✗ Error Astra DB: {conn_err}")
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

        rows = self._table_find(self.availability_table_name, {"propiedad_id": str(property_id)})

        available_by_day = {}
        for row in rows:
            row_date = self._parse_date(row.get("fecha"))
            if start <= row_date <= end:
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

    def verificar_disponibilidad(self, propiedad_id, inicio, fin):
        """Retorna False si existe algún día con disponible=False; True en otro caso."""
        if self.availability_table is None:
            raise RuntimeError("Tabla disponibilidad_propiedad no disponible.")

        start = self._parse_date(inicio)
        end = self._parse_date(fin)
        if start > end:
            raise ValueError("La fecha de inicio no puede ser mayor que la fecha de fin.")

        rows = self._table_find(self.availability_table_name, {"propiedad_id": str(propiedad_id)})

        for row in rows:
            row_date = self._parse_date(row.get("fecha"))
            if start <= row_date <= end and row.get("disponible") is False:
                return False
        return True

    def block_dates(self, propiedad_id, inicio, fin, precio_noche=None):
        """Inserta un registro por cada día del rango en disponibilidad_propiedad marcando disponible=False."""
        if self.availability_table is None:
            raise RuntimeError("Tabla disponibilidad_propiedad no disponible en Astra DB.")

        start = self._parse_date(inicio)
        end = self._parse_date(fin)
        if start > end:
            raise ValueError("La fecha de inicio no puede ser mayor que la fecha de fin.")

        propiedad_id_str = str(propiedad_id)
        current = start
        errors = []
        while current <= end:
            try:
                print(f"Insertando fecha {current.isoformat()} para propiedad {propiedad_id_str}")
                self._table_insert_one(self.availability_table_name, {
                    "propiedad_id": propiedad_id_str,
                    "fecha": current.isoformat(),
                    "disponible": False,
                    "precio_calculated": float(precio_noche) if precio_noche is not None else None,
                })
            except Exception as e:
                errors.append(f"{current.isoformat()}: {e}")
            current += timedelta(days=1)

        return {"blocked": True, "errors": errors}

    def registrar_vista(self, usuario_id, propiedad_id):
        """Inserta un evento de vista en historial_vistas usando el schema real de la tabla."""
        try:
            usuario_id_int = int(usuario_id)
        except Exception as conv_err:
            raise ValueError(f"usuario_id inválido para historial_vistas: {usuario_id}") from conv_err

        if self.visits_table is None:
            raise RuntimeError("Tabla historial_vistas no disponible en Cassandra.")

        self._table_insert_one(self.visits_table_name, {
            "usuario_id": usuario_id_int,
            "propiedad_id": str(propiedad_id),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dispositivo": "cli",
        })

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
