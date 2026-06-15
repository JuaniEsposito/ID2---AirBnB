import os
import re
from datetime import datetime, timedelta, timezone

from bson import ObjectId
from pymongo.errors import PyMongoError
from pymongo import MongoClient


class MongoRepository:
    def __init__(self, uri=None, database_name=None, collection_name=None):
        self.uri = uri or os.getenv("MONGO_URI")
        self.database_name = database_name or os.getenv("MONGO_DB_NAME", "ID2")
        self.collection_name = collection_name or os.getenv("MONGO_COLLECTION_NAME", "propiedades")
        self.client = None
        self.db = None
        self.collection = None
        self.connect()

    def connect(self):
        if not self.uri:
            return False

        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]
            return True
        except Exception:
            self.client = None
            self.db = None
            self.collection = None
            return False

    def popular_accommodations(self):
        if self.collection is None:
            raise RuntimeError("MongoDB no conectado.")

        pipeline = [
            {
                "$addFields": {
                    "_tipo_inferido": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {"$regexMatch": {"input": {"$ifNull": ["$titulo", ""]}, "regex": "departamento|depto|dpto|apto", "options": "i"}},
                                    "then": "Departamento",
                                },
                                {
                                    "case": {"$regexMatch": {"input": {"$ifNull": ["$titulo", ""]}, "regex": "casa", "options": "i"}},
                                    "then": "Casa",
                                },
                                {
                                    "case": {"$regexMatch": {"input": {"$ifNull": ["$titulo", ""]}, "regex": "habitaci[oó]n|hab", "options": "i"}},
                                    "then": "Habitación",
                                },
                                {
                                    "case": {"$regexMatch": {"input": {"$ifNull": ["$titulo", ""]}, "regex": "caba[ñn]a", "options": "i"}},
                                    "then": "Cabaña",
                                },
                            ],
                            "default": "Sin clasificar",
                        }
                    }
                }
            },
            {"$group": {"_id": {"$ifNull": ["$tipo_propiedad", "$_tipo_inferido"]}, "total": {"$sum": 1}}},
            {"$sort": {"total": -1}},
        ]
        resultados = list(self.collection.aggregate(pipeline))

        return [
            {"tipo_propiedad": item["_id"] or "Sin clasificar", "total": item["total"]}
            for item in resultados
        ]

    def recent_properties(self, days=30):
        if self.collection is None:
            raise RuntimeError("MongoDB no conectado.")

        cutoff = datetime.now(timezone.utc) - timedelta(days=int(days))
        cutoff_object_id = ObjectId.from_datetime(cutoff.replace(tzinfo=None))
        documentos = list(self.collection.find({"_id": {"$gte": cutoff_object_id}}).sort("_id", -1))
        return documentos

    def property_review_summary(self, limit=10):
        if self.collection is None:
            raise RuntimeError("MongoDB no conectado.")

        pipeline = [
            {
                "$addFields": {
                    "_rating_value": {
                        "$convert": {
                            "input": {"$ifNull": ["$calificacion_promedio", {"$avg": {"$ifNull": ["$resenas.calificacion", []]}}]},
                            "to": "double",
                            "onError": None,
                            "onNull": None,
                        }
                    }
                    ,
                    "_tipo_inferido": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {"$regexMatch": {"input": {"$ifNull": ["$titulo", ""]}, "regex": "departamento|depto|dpto|apto", "options": "i"}},
                                    "then": "Departamento",
                                },
                                {
                                    "case": {"$regexMatch": {"input": {"$ifNull": ["$titulo", ""]}, "regex": "casa", "options": "i"}},
                                    "then": "Casa",
                                },
                                {
                                    "case": {"$regexMatch": {"input": {"$ifNull": ["$titulo", ""]}, "regex": "habitaci[oó]n|hab", "options": "i"}},
                                    "then": "Habitación",
                                },
                                {
                                    "case": {"$regexMatch": {"input": {"$ifNull": ["$titulo", ""]}, "regex": "caba[ñn]a", "options": "i"}},
                                    "then": "Cabaña",
                                },
                            ],
                            "default": {"$ifNull": ["$tipo_propiedad", "Sin clasificar"]},
                        }
                    },
                    "_anfitrion_nombre": {
                        "$ifNull": [
                            "$metadata_anfitrion.nombre",
                            {"$concat": ["Anfitrión #", {"$toString": "$anfitrion_id"}]},
                        ]
                    },
                    "_cantidad_resenas": {"$size": {"$ifNull": ["$resenas", []]}},
                }
            },
            {
                "$group": {
                    "_id": "$_id",
                    "titulo": {"$first": "$titulo"},
                    "tipo_propiedad": {"$first": "$_tipo_inferido"},
                    "ciudad": {"$first": "$ubicacion.ciudad"},
                    "anfitrion": {"$first": "$_anfitrion_nombre"},
                    "promedio_calificacion": {"$first": "$_rating_value"},
                    "cantidad_resenas": {"$first": "$_cantidad_resenas"},
                    "mejor_resena": {
                        "$first": {"usuario_id": None, "nombre_usuario": None, "fecha": None, "calificacion": None, "comentario": None}
                    },
                }
            },
            {"$sort": {"promedio_calificacion": -1, "cantidad_resenas": -1}},
            {"$limit": int(limit)},
        ]
        return list(self.collection.aggregate(pipeline))

    def create_property(self, property_doc):
        if self.collection is None:
            raise RuntimeError("MongoDB no conectado.")
        if not isinstance(property_doc, dict):
            raise ValueError("property_doc debe ser un diccionario.")
        try:
            result = self.collection.insert_one(property_doc)
            return {"inserted_id": str(result.inserted_id)}
        except PyMongoError:
            raise
        except Exception:
            raise

    def get_property_by_id(self, property_id):
        if self.collection is None:
            raise RuntimeError("MongoDB no conectado.")

        try:
            query_id = ObjectId(property_id) if ObjectId.is_valid(str(property_id)) else property_id
            return self.collection.find_one({"_id": query_id})
        except PyMongoError:
            raise
        except Exception:
            raise

    def add_review(self, property_id, review_doc):
        if self.collection is None:
            raise RuntimeError("MongoDB no conectado.")
        try:
            query_id = ObjectId(property_id) if ObjectId.is_valid(property_id) else property_id
            res = self.collection.update_one(
                {"_id": query_id},
                {"$push": {"resenas": review_doc}},
            )

            if res.matched_count:
                propiedad = self.collection.find_one(
                    {"_id": query_id},
                    {"resenas.calificacion": 1, "calificacion_promedio": 1},
                ) or {}
                resenas = propiedad.get("resenas") if isinstance(propiedad.get("resenas"), list) else []
                calificaciones = []
                for resena in resenas:
                    if not isinstance(resena, dict):
                        continue
                    calificacion = resena.get("calificacion")
                    if calificacion is None:
                        continue
                    try:
                        calificaciones.append(float(calificacion))
                    except (TypeError, ValueError):
                        continue

                promedio = round(sum(calificaciones) / len(calificaciones), 2) if calificaciones else None
                self.collection.update_one(
                    {"_id": query_id},
                    {"$set": {"calificacion_promedio": promedio, "cantidad_resenas": len(resenas)}},
                )

            return {
                "matched_count": res.matched_count,
                "modified_count": res.modified_count,
            }
        except PyMongoError:
            raise
        except Exception:
            raise

    def recent_visible_reviews(self, limit=10):
        if self.collection is None:
            raise RuntimeError("MongoDB no conectado.")

        pipeline = [
            {"$unwind": "$resenas"},
            {"$match": {"resenas.visible": True}},
            {
                "$project": {
                    "titulo": 1,
                    "tipo_propiedad": 1,
                    "ciudad": "$ubicacion.ciudad",
                    "anfitrion": "$metadata_anfitrion.nombre",
                    "resena": "$resenas",
                }
            },
            {"$sort": {"resena.fecha": -1}},
            {"$limit": int(limit)},
        ]
        return list(self.collection.aggregate(pipeline))

    def reviews_by_user_ids(self, user_ids, limit=50):
        if self.collection is None:
            raise RuntimeError("MongoDB no conectado.")

        normalized_ids = []
        for user_id in user_ids or []:
            if user_id in (None, ""):
                continue
            normalized_ids.append(user_id)
            normalized_ids.append(str(user_id))
            if isinstance(user_id, str) and user_id.isdigit():
                normalized_ids.append(int(user_id))

        seen = set()
        user_id_values = []
        for value in normalized_ids:
            key = (type(value).__name__, str(value))
            if key in seen:
                continue
            seen.add(key)
            user_id_values.append(value)

        if not user_id_values:
            return []

        pipeline = [
            {"$unwind": "$resenas"},
            {"$match": {"resenas.usuario_id": {"$in": user_id_values}}},
            {
                "$project": {
                    "titulo": 1,
                    "tipo_propiedad": 1,
                    "ciudad": "$ubicacion.ciudad",
                    "fecha": "$resenas.fecha",
                    "calificacion": "$resenas.calificacion",
                    "comentario": "$resenas.comentario",
                    "visible": "$resenas.visible",
                }
            },
            {"$sort": {"fecha": -1}},
            {"$limit": int(limit)},
        ]
        return list(self.collection.aggregate(pipeline))

    def top_hosts_by_rating(self, limit=5):
        if self.collection is None:
            raise RuntimeError("MongoDB no conectado.")

        pipeline = [
            {
                "$addFields": {
                    "_rating_value": {
                        "$convert": {
                            "input": {"$ifNull": ["$calificacion_promedio", {"$avg": {"$ifNull": ["$resenas.calificacion", []]}}]},
                            "to": "double",
                            "onError": None,
                            "onNull": None,
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$anfitrion_id",
                    "nombre": {
                        "$first": {
                            "$ifNull": [
                                "$metadata_anfitrion.nombre",
                                {"$concat": ["Anfitrión #", {"$toString": "$anfitrion_id"}]},
                            ]
                        }
                    },
                    "promedio_calificacion": {"$avg": "$_rating_value"},
                    "cantidad_resenas": {"$sum": 1},
                    "cantidad_propiedades": {"$sum": 1},
                }
            },
            {"$sort": {"promedio_calificacion": -1, "cantidad_resenas": -1}},
            {"$limit": int(limit)},
        ]
        return list(self.collection.aggregate(pipeline))

    def most_demanded_areas_by_country(self, country, limit=10):
        if self.collection is None:
            raise RuntimeError("MongoDB no conectado.")

        pipeline = [
            {"$match": {"ubicacion.pais": {"$regex": f"^{re.escape(country.strip())}$", "$options": "i"}}},
            {
                "$group": {
                    "_id": {
                        "pais": "$ubicacion.pais",
                        "provincia": "$ubicacion.provincia",
                        "ciudad": "$ubicacion.ciudad",
                    },
                    "total": {"$sum": 1},
                }
            },
            {"$sort": {"total": -1}},
            {"$limit": int(limit)},
        ]
        return list(self.collection.aggregate(pipeline))

    def properties_with_high_rating_in_center(self, min_rating=4.5, ciudad=None):
        if self.collection is None:
            raise RuntimeError("MongoDB no conectado.")

        # If a city is provided, filter by ubicacion.ciudad (exact-ish match, case-insensitive).
        # Otherwise fall back to the historical 'centro' zone behavior.
        if ciudad and ciudad.strip():
            city_regex = f"^{re.escape(ciudad.strip())}$"
            match_stage = {"$match": {"ubicacion.ciudad": {"$regex": city_regex, "$options": "i"}}}
        else:
            zone_search = "centro"
            match_stage = {
                "$match": {
                    "$or": [
                        {"zona": {"$regex": zone_search, "$options": "i"}},
                        {"ubicacion.zona": {"$regex": zone_search, "$options": "i"}},
                    ]
                }
            }

        pipeline = [
            match_stage,
            {
                "$addFields": {
                    "_rating_value": {
                        "$convert": {
                            "input": {"$ifNull": ["$calificacion_promedio", {"$avg": {"$ifNull": ["$resenas.calificacion", []]}}]},
                            "to": "double",
                            "onError": None,
                            "onNull": None,
                        }
                    }
                }
            },
            {"$match": {"_rating_value": {"$gte": float(min_rating)}}},
            {"$sort": {"_rating_value": -1}},
            {"$addFields": {"mejor_calificacion": "$_rating_value"}},
        ]

        return list(self.collection.aggregate(pipeline))

    def properties_with_many_reviews_or_touristic_zone(self, min_reviews=20):
        if self.collection is None:
            raise RuntimeError("MongoDB no conectado.")

        tourist_regex = "(tur[ií]st|playa|centro|cerca del mar|zona premium|parque|ski|monta[nñ]a)"
        pipeline = [
            {
                "$addFields": {
                    "cantidad_resenas": {"$size": {"$ifNull": ["$resenas", []]}},
                    "zona_texto": {
                        "$concat": [
                            {"$ifNull": ["$zona", ""]},
                            " ",
                            {"$ifNull": ["$ubicacion.zona", ""]},
                            " ",
                            {"$ifNull": ["$ubicacion.ciudad", ""]},
                        ]
                    },
                }
            },
            {
                "$match": {
                    "$or": [
                        {"cantidad_resenas": {"$gte": int(min_reviews)}},
                        {"zona_texto": {"$regex": tourist_regex, "$options": "i"}},
                    ]
                }
            },
            {"$sort": {"cantidad_resenas": -1}},
        ]
        return list(self.collection.aggregate(pipeline))

    def find_properties_by_type_and_rating(self, tipo_propiedad, min_rating):
        if self.collection is None:
            raise RuntimeError("MongoDB no conectado.")

        tipo_propiedad = (tipo_propiedad or "").strip()

        # Map common user inputs to likely document values (covers synonyms and abbreviations).
        aliases = {
            "apartamento": ["apartamento", "departamento", "depto", "dpto", "apto"],
            "casa": ["casa", "caba[ñn]a", "chalet", "house"],
            "habitacion": ["habitaci", "habitaci[oó]n", "hab"],
            "casa de playa": ["casa de playa", "casa playa", "cabaña playa"],
        }

        tipo_lower = tipo_propiedad.lower()
        patterns = None
        for key, vals in aliases.items():
            if tipo_lower == key or tipo_lower in vals:
                patterns = vals
                break

        if patterns is None and tipo_propiedad:
            patterns = [tipo_propiedad]
        elif patterns is None:
            patterns = []

        # Build regex alternation from patterns
        tipo_pattern = "|".join(re.escape(p) for p in patterns if p)

        pipeline = [
            {
                "$match": {
                    "$or": [
                        {"tipo_propiedad": {"$regex": tipo_pattern, "$options": "i"}},
                        {"titulo": {"$regex": tipo_pattern, "$options": "i"}},
                    ]
                }
            },
            {
                "$addFields": {
                    "_rating_value": {
                        "$convert": {
                            "input": {"$ifNull": ["$calificacion_promedio", {"$avg": {"$ifNull": ["$resenas.calificacion", []]}}]},
                            "to": "double",
                            "onError": None,
                            "onNull": None,
                        }
                    }
                }
            },
            {"$match": {"_rating_value": {"$gte": float(min_rating)}}},
            {"$sort": {"_rating_value": -1}},
            {
                "$addFields": {
                    "mejor_calificacion": "$_rating_value",
                    "reseñas_que_cumplen": [],
                }
            },
        ]

        return list(self.collection.aggregate(pipeline))

    def properties_with_high_rating_anywhere(self, min_rating=4.5, limit=50):
        if self.collection is None:
            raise RuntimeError("MongoDB no conectado.")

        pipeline = [
            {
                "$addFields": {
                    "_rating_value": {
                        "$convert": {
                            "input": {"$ifNull": ["$calificacion_promedio", {"$avg": {"$ifNull": ["$resenas.calificacion", []]}}]},
                            "to": "double",
                            "onError": None,
                            "onNull": None,
                        }
                    }
                }
            },
            {"$match": {"_rating_value": {"$gte": float(min_rating)}}},
            {"$sort": {"_rating_value": -1}},
            {
                "$addFields": {
                    "mejor_calificacion": "$_rating_value",
                    "reseñas_que_cumplen": [],
                }
            },
        ]

        if limit:
            pipeline.append({"$limit": int(limit)})

        return list(self.collection.aggregate(pipeline))

    def find_properties_by_location(self, ciudad=None, zona=None):
        if self.collection is None:
            raise RuntimeError("MongoDB no conectado.")

        filtro = {}
        if ciudad:
            filtro["ciudad"] = ciudad
        if zona:
            filtro["zona"] = zona

        return list(self.collection.find(filtro))

    def close(self):
        if self.client:
            self.client.close()
