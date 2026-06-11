import os
import hashlib
import getpass
import logging
import platform
import re
import time
import uuid
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from bson import ObjectId
from psycopg2 import Error as Psycopg2Error
from pymongo.errors import PyMongoError
from redis.exceptions import RedisError

from db_connectors import CassandraRepository, MongoRepository, PostgresRepository
from db_connectors.redis_repository import RedisRepository


# Cargar variables del archivo .env
load_dotenv()

logger = logging.getLogger(__name__)

PAUSA_EXITO_SEGUNDOS = 1.0
PAUSA_ERROR_SEGUNDOS = 1.5
PAUSA_LISTADO_SEGUNDOS = 0.5


class AirbnbOrchestrator:
    def __init__(self):
        print("\n--- INICIALIZANDO ORQUESTADOR POLÍGLOTA ---")

        self.postgres = PostgresRepository()
        self.mongo = MongoRepository()
        self.cassandra = CassandraRepository()
        self.redis = RedisRepository()
        try:
            self.cache_ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))
        except Exception:
            self.cache_ttl = 300

        self._print_connection_status("Postgres", self.postgres.connection is not None)
        self._print_connection_status("MongoDB", self.mongo.collection is not None)
        self._print_connection_status("Astra DB", self.cassandra.collection is not None)
        self._print_connection_status("Redis", getattr(self.redis, 'client', None) is not None)

        self._migrar_roles_legacy()

    def _auth_user_key(self, email):
        return f"auth:user:{(email or '').strip().casefold()}"

    def _auth_session_key(self, token):
        return f"sesion:{(token or '').strip()}"

    def _auth_session_index_key(self, email):
        return f"auth:session_index:{(email or '').strip().casefold()}"

    def _session_index_key(self):
        return "sessionindex"

    def _active_user_key(self):
        return "user"

    def _normalize_email(self, email):
        return (email or "").strip().casefold()

    def _hash_password(self, password, salt=None):
        salt = salt or uuid.uuid4().hex
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 120_000)
        return f"{salt}${digest.hex()}"

    def _verify_password(self, password, stored_value):
        try:
            salt, hashed = stored_value.split("$", 1)
        except ValueError:
            return False
        return self._hash_password(password, salt=salt) == stored_value

    def _session_ttl(self, trusted_device):
        if trusted_device:
            return 60 * 60 * 24
        return 5

    def _normalizar_tipo_propiedad(self, tipo_propiedad, titulo=None):
        tipo = (tipo_propiedad or "").strip()
        if tipo:
            return tipo

        titulo_normalizado = (titulo or "").strip().casefold()
        if any(token in titulo_normalizado for token in ["departamento", "depto", "dpto", "apto"]):
            return "Departamento"
        if "casa" in titulo_normalizado:
            return "Casa"
        if any(token in titulo_normalizado for token in ["habitación", "habitacion", "hab"]):
            return "Habitación"
        if any(token in titulo_normalizado for token in ["cabaña", "cabana"]):
            return "Cabaña"
        return "Alojamiento"

    def _normalizar_tipo_usuario(self, user_type):
        tipo = (user_type or "").strip().casefold()
        tipo_sin_tilde = (
            tipo.replace("á", "a")
            .replace("é", "e")
            .replace("í", "i")
            .replace("ó", "o")
            .replace("ú", "u")
        )

        if tipo_sin_tilde in {"anfitrion", "anfitrion/a", "host"} or "anfitr" in tipo_sin_tilde:
            return "anfitrion"
        if tipo_sin_tilde in {"huesped", "huesped/a", "guest"} or "huesp" in tipo_sin_tilde:
            return "huesped"
        if tipo_sin_tilde in {"admin", "administrador"}:
            return "admin"
        if tipo_sin_tilde == "ambos" or "amb" in tipo_sin_tilde:
            return "huesped"
        return "huesped"

    def _migrar_roles_legacy(self):
        # Migrate legacy role value "ambos" to "huesped" in Redis and Postgres.
        try:
            self.postgres.migrar_tipos_usuario_legacy(from_tipo="ambos", to_tipo="huesped")
        except Exception:
            pass

        try:
            self.postgres.asegurar_rol_admin_en_usuario()
        except Exception:
            pass

        if getattr(self.redis, 'client', None) is None:
            return

        try:
            for raw_key in self.redis.client.scan_iter(match="auth:user:*"):
                key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else raw_key
                user_doc = self.redis.get_json(key) or {}
                tipo_actual = user_doc.get("tipo")
                tipo_normalizado = self._normalizar_tipo_usuario(tipo_actual)

                if (tipo_actual or "").strip().casefold() != tipo_normalizado:
                    user_doc["tipo"] = tipo_normalizado
                    self.redis.set_json(key, user_doc)

                email = self._normalize_email(user_doc.get("email"))
                if email and tipo_normalizado in {"huesped", "anfitrion", "admin"}:
                    try:
                        self.postgres.actualizar_tipo_usuario_por_email(email, tipo_normalizado)
                    except Exception:
                        pass
        except Exception:
            pass

    def _seleccionar_tipo_usuario_cli(self):
        opciones = {
            "1": "anfitrion",
            "2": "huesped",
            "3": "admin",
        }

        while True:
            print("Tipo de usuario:")
            print("1. Anfitrión")
            print("2. Huésped")
            print("3. Admin")
            seleccion = input("Seleccione una opción (1/2/3): ").strip()
            if seleccion in opciones:
                return opciones[seleccion]
            print("⚠ Opción inválida. Intente nuevamente.")

    def _seleccionar_tipo_propiedad_cli(self):
        tipos_catalogo = []
        try:
            tipos_catalogo = self.postgres.listar_tipos_propiedad()
        except Exception:
            tipos_catalogo = []

        nombres = []
        vistos = set()
        for item in tipos_catalogo:
            nombre = (item.get("nombre") or "").strip() if isinstance(item, dict) else ""
            if not nombre:
                continue
            key = nombre.casefold()
            if key in vistos:
                continue
            vistos.add(key)
            nombres.append(nombre)

        if not nombres:
            nombres = ["Departamento", "Casa", "Habitación", "Cabaña", "Loft", "Alojamiento"]

        while True:
            print("Tipo de propiedad:")
            for idx, nombre in enumerate(nombres, start=1):
                print(f"{idx}. {nombre}")
            manual_index = len(nombres) + 1
            print(f"{manual_index}. Otro (ingresar manualmente)")

            seleccion = input(f"Seleccione una opción (1-{manual_index}): ").strip()
            if seleccion.isdigit():
                idx = int(seleccion)
                if 1 <= idx <= len(nombres):
                    return self._normalizar_tipo_propiedad(nombres[idx - 1])
                if idx == manual_index:
                    manual = input("Ingrese el tipo de propiedad: ").strip()
                    if manual:
                        return self._normalizar_tipo_propiedad(manual)
                    print("⚠ Debe ingresar un tipo válido.")
                    continue

            print("⚠ Opción inválida. Intente nuevamente.")

    def _variantes_ciudad(self, ciudad):
        valor = (ciudad or "").strip()
        if not valor:
            return []

        normalizado = (
            valor.casefold()
            .replace("á", "a")
            .replace("é", "e")
            .replace("í", "i")
            .replace("ó", "o")
            .replace("ú", "u")
        )

        aliases = {
            "caba": ["CABA", "Buenos Aires", "Ciudad Autónoma de Buenos Aires", "Capital Federal"],
            "ciudad autonoma de buenos aires": ["CABA", "Buenos Aires", "Ciudad Autónoma de Buenos Aires", "Capital Federal"],
            "buenos aires": ["CABA", "Buenos Aires", "Ciudad Autónoma de Buenos Aires", "Capital Federal"],
        }

        values = aliases.get(normalizado, [valor])
        # Preserve order while removing duplicates.
        unique = []
        seen = set()
        for item in values:
            key = item.casefold()
            if key in seen:
                continue
            seen.add(key)
            unique.append(item)
        return unique

    def signup_usuario(self, nombre_completo, email, password, user_type="huesped"):
        if getattr(self.redis, 'client', None) is None:
            return "Redis no conectado. No se puede registrar el usuario."

        nombre_completo = (nombre_completo or "").strip()
        email_normalizado = self._normalize_email(email)
        password = (password or "").strip()
        user_type = self._normalizar_tipo_usuario(user_type)

        if not nombre_completo or not email_normalizado or not password:
            return "Debe completar nombre, email y contraseña."

        user_key = self._auth_user_key(email_normalizado)
        if self.redis.get_json(user_key) is not None:
            return f"El usuario {email_normalizado} ya está registrado."

        notes = []
        postgres_user_id = None
        postgres_result = None

        try:
            postgres_result = self.postgres.register_user(nombre_completo, email_normalizado, user_type=user_type, activo=True)
            postgres_user_id = postgres_result.get("id") if isinstance(postgres_result, dict) else None
            notes.append(postgres_result.get("mensaje", "Usuario registrado en Postgres."))
        except Exception as e:
            notes.append(f"Postgres no pudo registrar el usuario: {e}")


        user_doc = {
            "nombre_completo": nombre_completo,
            "email": email_normalizado,
            "tipo": user_type,
            "password_hash": self._hash_password(password),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self.redis.set_json(user_key, user_doc)
        notes.insert(0, f"Usuario {nombre_completo} <{email_normalizado}> registrado correctamente en Redis.")
        return " ".join(notes)

    def login_usuario(self, email, password, trusted_device=False):
        if getattr(self.redis, 'client', None) is None:
            return None, "Redis no conectado. No se puede iniciar sesi\u00f3n."

        email_normalizado = self._normalize_email(email)
        password = (password or "").strip()

        if not email_normalizado or not password:
            return None, "Debe completar email y contrase\u00f1a."

        user_doc = self.redis.get_json(self._auth_user_key(email_normalizado))
        if not user_doc:
            postgres_user = None
            try:
                postgres_user = self.postgres.get_user_by_email(email_normalizado)
            except Exception:
                postgres_user = None
            if postgres_user:
                return None, "El usuario existe en Postgres, pero no tiene credenciales de sesi\u00f3n en Redis."
            return None, f"El usuario {email_normalizado} no est\u00e1 registrado."

        if not self._verify_password(password, user_doc.get("password_hash", "")):
            return None, "Contrase\u00f1a incorrecta."

        session_token = uuid.uuid4().hex
        session_ttl = self._session_ttl(bool(trusted_device))
        session_doc = {
            "email": email_normalizado,
            "nombre_completo": user_doc.get("nombre_completo", "Usuario"),
            "tipo": self._normalizar_tipo_usuario(user_doc.get("tipo", "huesped")),
            "trusted_device": bool(trusted_device),
            "session_token": session_token,
            "login_at": datetime.now(timezone.utc).isoformat(),
        }

        self.redis.set_json(self._auth_session_key(session_token), session_doc, ex=session_ttl)
        self.redis.set_json(self._auth_session_index_key(email_normalizado), {"session_token": session_token}, ex=session_ttl)
        self.redis.set_json(self._session_index_key(), {"email": email_normalizado, "session_token": session_token}, ex=session_ttl)
        self.redis.set_json(self._active_user_key(), {
            "email": email_normalizado,
            "nombre_completo": user_doc.get("nombre_completo", "Usuario"),
            "tipo": self._normalizar_tipo_usuario(user_doc.get("tipo", "huesped")),
            "last_login_at": session_doc["login_at"],
        }, ex=session_ttl)

        user_doc["last_login_at"] = session_doc["login_at"]
        user_doc["trusted_device"] = bool(trusted_device)
        self.redis.set_json(self._auth_user_key(email_normalizado), user_doc)

        return session_doc, None

    def _tipo_usuario_sesion(self, active_session=None):
        if not active_session:
            return "huesped"

        tipo = active_session.get("tipo") if isinstance(active_session, dict) else None
        if tipo:
            return self._normalizar_tipo_usuario(tipo)

        email = self._normalize_email(active_session.get("email") if isinstance(active_session, dict) else None)
        if email and getattr(self.redis, 'client', None) is not None:
            try:
                user_doc = self.redis.get_json(self._auth_user_key(email)) or {}
                if user_doc.get("tipo"):
                    return self._normalizar_tipo_usuario(user_doc.get("tipo"))
            except Exception:
                pass

        return "huesped"

    def _sesion_activa_en_redis(self, active_session=None):
        if not active_session:
            return False
        if getattr(self.redis, 'client', None) is None:
            return False

        token = (active_session.get("session_token") if isinstance(active_session, dict) else None) or ""
        token = token.strip()
        if not token:
            return False

        try:
            return self._redis_get_json(self._auth_session_key(token)) is not None
        except Exception:
            return False

    def logout_usuario(self, email, session_token=None):
        if getattr(self.redis, 'client', None) is None:
            return "Redis no conectado. No se puede cerrar sesi\u00f3n."

        email_normalizado = self._normalize_email(email)
        if not email_normalizado:
            return "Debe indicar un email v\u00e1lido para cerrar sesi\u00f3n."

        token = (session_token or "").strip()
        if not token:
            session_index = self._redis_get_json(self._auth_session_index_key(email_normalizado)) or {}
            token = session_index.get("session_token", "")
        if not token:
            session_index_global = self._redis_get_json(self._session_index_key()) or {}
            if self._normalize_email(session_index_global.get("email")) == email_normalizado:
                token = session_index_global.get("session_token", "")

        if token:
            try:
                if getattr(self.redis, 'client', None) is not None:
                    self.redis.client.delete(self._auth_session_key(token))
            except Exception:
                pass

        try:
            if getattr(self.redis, 'client', None) is not None:
                self.redis.client.delete(self._auth_session_index_key(email_normalizado))
        except Exception:
            pass

        try:
            if getattr(self.redis, 'client', None) is not None:
                self.redis.client.delete(self._session_index_key())
        except Exception:
            pass

        try:
            if getattr(self.redis, 'client', None) is not None:
                self.redis.client.delete(self._active_user_key())
        except Exception:
            pass

        return f"Sesi\u00f3n cerrada para {email_normalizado}."

    def disponibilidad_propiedad_rango(self, propiedad_id, fecha_inicio, fecha_fin):
        resolved_property_id = self._resolver_propiedad_postgres(propiedad_id)
        if resolved_property_id is None:
            return "La propiedad indicada no existe."

        propiedad_id = str(resolved_property_id).strip()
        fecha_inicio = (fecha_inicio or "").strip()
        fecha_fin = (fecha_fin or "").strip()

        if not propiedad_id:
            return "Debe ingresar un propiedad_id válido."

        cache_key = f"availability:{propiedad_id}:{fecha_inicio}:{fecha_fin}"
        cached = self._redis_get_json(cache_key)
        if cached is not None:
            return self._formatear_resultado_disponibilidad(cached, fecha_inicio, fecha_fin)

        try:
            resultado = self.cassandra.check_property_availability(propiedad_id, fecha_inicio, fecha_fin)
            if getattr(self.redis, 'client', None) is not None:
                self._redis_set_json(cache_key, resultado, ttl_seconds=300)

            return self._formatear_resultado_disponibilidad(resultado, fecha_inicio, fecha_fin)
        except Exception as e:
            return f"Error al consultar disponibilidad: {e}"

    def _formatear_resultado_disponibilidad(self, resultado, fecha_inicio, fecha_fin):
        if not isinstance(resultado, dict):
            return "No se pudo interpretar la disponibilidad de la propiedad."

        inicio = resultado.get('start_date', fecha_inicio)
        fin = resultado.get('end_date', fecha_fin)

        if resultado.get("available"):
            return f"Disponible del {inicio} al {fin}."

        unavailable_days = resultado.get("unavailable_days") or []
        if unavailable_days:
            dias_texto = ", ".join(str(day) for day in unavailable_days)
            return f"No disponible entre {inicio} y {fin}. Días ocupados: {dias_texto}."

        return f"No disponible entre {inicio} y {fin}."

    def _propiedad_existe_en_postgres(self, propiedad_id):
        if getattr(self.postgres, 'connection', None) is None or getattr(self.postgres, 'cursor', None) is None:
            return None

        try:
            self.postgres.cursor.execute(
                """
                SELECT 1
                FROM propiedad
                WHERE id = %s
                LIMIT 1;
                """,
                (int(propiedad_id),),
            )
            return self.postgres.cursor.fetchone() is not None
        except Exception:
            try:
                self.postgres.connection.rollback()
            except Exception:
                pass
            return None

    def _listar_propiedades_para_disponibilidad(self, limite=20):
        if getattr(self.mongo, 'collection', None) is None:
            return []

        try:
            docs = list(
                self.mongo.collection.find(
                    {"activa": True},
                    {"_id": 1, "propiedad_pg_id": 1, "titulo": 1, "ubicacion": 1},
                ).limit(limite)
            )
        except Exception:
            return []

        opciones = []
        for doc in docs:
            propiedad_pg_id = doc.get("propiedad_pg_id")
            if propiedad_pg_id is None:
                continue
            titulo = (doc.get("titulo") or "Sin título").strip()
            ubicacion = doc.get("ubicacion") if isinstance(doc.get("ubicacion"), dict) else {}
            ciudad = (ubicacion.get("ciudad") or "N/D").strip()
            opciones.append(
                {
                    "propiedad_id": str(propiedad_pg_id),
                    "titulo": titulo,
                    "ciudad": ciudad,
                }
            )
        return opciones

    def _seleccionar_propiedad_disponibilidad_cli(self, limite=20):
        opciones = self._listar_propiedades_para_disponibilidad(limite=limite)
        if not opciones:
            return None

        print("Propiedades disponibles para consultar:")
        for idx, item in enumerate(opciones, start=1):
            print(f"  {idx}. {item.get('titulo')} | {item.get('ciudad')} | ID {item.get('propiedad_id')}")

        while True:
            seleccion = input("Seleccione una propiedad (número) o 0 para volver: ").strip()
            if seleccion == "0":
                return None
            if not seleccion.isdigit():
                print("Opción inválida. Debe ingresar un número de la lista.")
                continue

            indice = int(seleccion)
            if 1 <= indice <= len(opciones):
                return opciones[indice - 1].get("propiedad_id")

            print("Índice fuera de rango. Intente nuevamente.")

    def registrar_login_evento(self, email, trusted_device):
        event_payload = {
            "email": self._normalize_email(email),
            "trusted_device": bool(trusted_device),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if getattr(self.redis, 'client', None) is not None:
            self.redis.lpush_json("loginevents", event_payload, max_len=2000)

        return event_payload

    # Redis connection handled by RedisRepository

    def _print_connection_status(self, label, connected):
        if connected:
            print(f"✓ {label} conectado.")
        else:
            print(f"✗ Error {label}: no se pudo establecer la conexión.")

    def _redis_get_text(self, key):
        if getattr(self.redis, 'client', None) is None:
            return None
        try:
            return self.redis.get_text(key)
        except Exception:
            return None

    def _redis_get_json(self, key):
        if getattr(self.redis, 'client', None) is None:
            return None
        try:
            return self.redis.get_json(key)
        except Exception:
            return None

    def _redis_set_text(self, key, value, ttl_seconds=300):
        if getattr(self.redis, 'client', None) is None:
            return
        try:
            ex = ttl_seconds if ttl_seconds is not None else getattr(self, 'cache_ttl', 300)
            self.redis.set_text(key, value, ex=ex)
        except Exception:
            pass

    def _redis_set_json(self, key, obj, ttl_seconds=300):
        if getattr(self.redis, 'client', None) is None:
            return
        try:
            ex = ttl_seconds if ttl_seconds is not None else getattr(self, 'cache_ttl', 300)
            self.redis.set_json(key, obj, ex=ex)
        except Exception:
            pass

    def _pausa_ui(self, tipo="exito", listado_largo=False):
        if listado_largo:
            segundos = PAUSA_LISTADO_SEGUNDOS
        elif tipo == "error":
            segundos = PAUSA_ERROR_SEGUNDOS
        else:
            segundos = PAUSA_EXITO_SEGUNDOS

        try:
            time.sleep(segundos)
        except Exception:
            pass


    def menu_casos_de_uso(self, active_session=None):
        while True:
            if active_session and not self._sesion_activa_en_redis(active_session):
                print("\nTu sesión expiró. Iniciá sesión nuevamente.")
                return "logout"

            print("\n" + "=" * 30)
            print("=== CASOS DE USO ===")
            print("=" * 30)
            print("1. ¿Cuántas reservas se realizan en una ciudad específica en el último mes? (PostgreSQL + Redis)")
            print("2. ¿Qué tipos de alojamiento son más populares entre los usuarios? (Redis + MongoDB)")
            print("3. ¿Cuántas propiedades han sido agregadas recientemente en la plataforma? (MongoDB)")
            print("4. ¿Qué anfitriones tienen las mejores calificaciones? (PostgreSQL, fallback MongoDB)")
            print("5. ¿Cuáles son las áreas más demandadas para alquileres en un país? (PostgreSQL)")
            print("6. ¿Cuántas propiedades tienen una calificación mayor a 4.5 Y están ubicadas en el centro de la ciudad? (MongoDB)")
            print("7. ¿Qué tipos de alojamientos han recibido más de 20 reseñas o están en una zona turística popular? (MongoDB)")
            print("8. Disponibilidad de propiedad en rango de fechas (Cassandra + Redis)")
            print("0. Volver")

            opc_raw = input("\nSeleccione una opción: ").strip()
            if active_session and not self._sesion_activa_en_redis(active_session):
                print("\nTu sesión expiró. Iniciá sesión nuevamente.")
                return "logout"

            # Tolerate keypad/terminal variants such as +2, 2+ or 02.
            opcion_match = re.fullmatch(r"\+?(\d+)\+?", opc_raw)
            opc = str(int(opcion_match.group(1))) if opcion_match else opc_raw
            if opc == "0":
                return
            if opc == "1":
                ciudad = input("Ingrese la ciudad a consultar (ej: 'Buenos Aires'): ")
                print(f"\n> Resultado: {self.contar_reservas_ciudad_ultimo_mes(ciudad)}")
                self._pausa_ui("exito")
            elif opc == "2":
                print("\n> Consultando agregación en MongoDB...")
                print(self.alojamiento_mas_popular())
                self._pausa_ui("exito", listado_largo=True)
            elif opc == "3":
                dias = input("Ingrese la cantidad de días para buscar propiedades recientes (ej: 30): ").strip() or "30"
                print(self.propiedades_recientes(dias=dias))
                self._pausa_ui("exito", listado_largo=True)
            elif opc == "4":
                limite = input("Ingrese cuántos anfitriones mostrar (ej: 5): ").strip() or "5"
                print(self.mejores_anfitriones(limite=int(limite)))
                self._pausa_ui("exito", listado_largo=True)
            elif opc == "5":
                pais = input("Ingrese el país a analizar (ej: Argentina): ")
                limite = input("Ingrese cuántas áreas mostrar (ej: 10): ").strip() or "10"
                print(self.areas_mas_demandadas_pais(pais, limite=int(limite)))
                self._pausa_ui("exito", listado_largo=True)
            elif opc == "6":
                print("\n> Consultando propiedades con calificación mayor a 4.5 en CABA...")
                resultados = self.mongo.properties_with_high_rating_in_center(min_rating=4.5, ciudad="CABA")
                if not resultados:
                    print("No se encontraron propiedades en CABA con calificación mayor o igual a 4.5.")
                    self._pausa_ui("error")
                else:
                    print(f"Se encontraron {len(resultados)} propiedades en CABA con calificación mayor o igual a 4.5.")
                    print(self._formatear_propiedades_mongo(resultados, titulo="PROPIEDADES EN CABA CON CALIFICACIÓN >= 4.5"))
                    self._pausa_ui("exito", listado_largo=True)
            elif opc == "7":
                min_reviews = input("Ingrese la cantidad mínima de reseñas (ej: 20): ").strip() or "20"
                print(self.propiedades_mas_resenadas_o_zona_turistica(min_reviews=int(min_reviews)))
                self._pausa_ui("exito", listado_largo=True)
            elif opc == "8":
                propiedad_id = self._seleccionar_propiedad_disponibilidad_cli(limite=20)
                if not propiedad_id:
                    print("Consulta de disponibilidad cancelada.")
                    self._pausa_ui("error")
                    continue
                fecha_inicio = input("Ingrese fecha inicio (YYYY-MM-DD): ").strip()
                fecha_fin = input("Ingrese fecha fin (YYYY-MM-DD): ").strip()
                resultado = self.disponibilidad_propiedad_rango(propiedad_id, fecha_inicio, fecha_fin)
                print("\n> Resultado de disponibilidad:")
                print(resultado)
                self._pausa_ui("exito")
            else:
                print("\n⚠ Opción no válida. Intente nuevamente.")
                self._pausa_ui("error")


    # Business-level helpers

    def publicar_propiedad_business(self, anfitrion_id, property_doc):
        try:
            property_doc = dict(property_doc or {})
            property_doc["tipo_propiedad"] = self._normalizar_tipo_propiedad(
                property_doc.get("tipo_propiedad"),
                titulo=property_doc.get("titulo"),
            )
            anfitrion_resuelto = self._resolver_usuario_postgres(anfitrion_id)
            if anfitrion_resuelto is None:
                raise ValueError(
                    "El anfitrión indicado no existe en Postgres. Use un email registrado o un ID válido de usuario."
                )
            property_doc["anfitrion_id"] = anfitrion_resuelto

            # 1) Postgres: alta en propiedad (si falla, no se continúa con Mongo).
            postgres_res = self.postgres.publicar_propiedad_maestro(anfitrion_resuelto, property_doc)
            propiedad_pg_id = postgres_res.get("id") if isinstance(postgres_res, dict) else None
            if propiedad_pg_id is None:
                raise RuntimeError("Postgres no devolvió el ID de la propiedad maestra.")

            # 2) Postgres: asegurar ubicación ciudad/país antes de Mongo.
            # Si falla aquí, se aborta y no se continúa con inserción en Mongo.
            ubicacion_doc = property_doc.get("ubicacion") if isinstance(property_doc.get("ubicacion"), dict) else {}
            ciudad = (ubicacion_doc.get("ciudad") or "").strip()
            pais = (ubicacion_doc.get("pais") or "").strip()
            ubicacion_res = self.postgres.asegurar_ubicacion(
                ciudad,
                pais,
                propiedad_id=propiedad_pg_id,
                property_doc=property_doc,
            )
            ubicacion_insert_id = ubicacion_res.get("id") if isinstance(ubicacion_res, dict) else None
            if isinstance(ubicacion_res, dict) and ubicacion_res.get("created"):
                print(f"[Postgres] Nueva ubicación creada automáticamente: {ciudad}, {pais}")

            # Campos exclusivos de MongoDB (no persistidos en Postgres).
            def _norm_text(value):
                return (
                    (value or "").strip().casefold()
                    .replace("á", "a")
                    .replace("é", "e")
                    .replace("í", "i")
                    .replace("ó", "o")
                    .replace("ú", "u")
                )

            ciudades_turisticas = {
                _norm_text(x)
                for x in [
                    "CABA",
                    "Buenos Aires",
                    "Bariloche",
                    "Mar del Plata",
                    "Pinamar",
                    "Mar de las Pampas",
                    "Mendoza",
                    "Salta",
                    "Rio de Janeiro",
                    "Florianópolis",
                    "Salvador",
                    "Santiago",
                    "Valparaíso",
                    "Torres del Paine",
                ]
            }
            barrios_centricos = {
                _norm_text(x)
                for x in [
                    "Microcentro",
                    "Recoleta",
                    "Palermo",
                    "San Telmo",
                    "Retiro",
                    "Copacabana",
                    "Ipanema",
                    "Centro",
                    "Providencia",
                    "Las Condes",
                    "Santiago Centro",
                ]
            }

            ciudad = ubicacion_doc.get("ciudad")
            barrio = property_doc.get("barrio") or ubicacion_doc.get("barrio")

            zona_turistica = _norm_text(ciudad) in ciudades_turisticas
            zona_centrica = _norm_text(barrio) in barrios_centricos if barrio else False

            # 3) Recuperar servicios confirmados desde Postgres (M:N) para desnormalizar en Mongo.
            servicios_nombres = []
            try:
                servicios_nombres = self.postgres.get_servicios_by_propiedad(propiedad_pg_id)
            except Exception:
                # Fallback: usar los nombres que vinieron en el doc original
                raw = property_doc.get("servicios")
                if isinstance(raw, list):
                    servicios_nombres = [str(s) for s in raw if s]
                elif isinstance(raw, str) and raw.strip():
                    servicios_nombres = [s.strip() for s in raw.split(",") if s.strip()]

            # 4) Mongo: insertar documento enlazado con propiedad_pg_id y servicios desnormalizados.
            mongo_doc = dict(property_doc)
            mongo_doc["propiedad_pg_id"] = propiedad_pg_id
            mongo_doc["zona_turistica"] = zona_turistica
            mongo_doc["zona_centrica"] = zona_centrica
            mongo_doc["servicios"] = servicios_nombres  # array de strings desnormalizado

            mongo_res = None
            try:
                mongo_res = self.mongo.create_property(mongo_doc)
            except Exception as mongo_err:
                logger.warning(
                    "No se pudo insertar la propiedad en Mongo para propiedad_pg_id=%s: %s",
                    propiedad_pg_id,
                    mongo_err,
                )
                mongo_res = {
                    "created": False,
                    "warning": str(mongo_err),
                }

            postgres_payload = {
                "created": bool(postgres_res.get("created")) if isinstance(postgres_res, dict) else True,
                "id": propiedad_pg_id,
                "table": postgres_res.get("table", "propiedad") if isinstance(postgres_res, dict) else "propiedad",
            }
            if ubicacion_insert_id is not None:
                postgres_payload["ubicacion_id"] = ubicacion_insert_id

            return {
                "postgres": postgres_payload,
                "mongo": mongo_res,
                "propiedad_pg_id": propiedad_pg_id,
                "zona_turistica": zona_turistica,
                "zona_centrica": zona_centrica,
            }
        except PyMongoError:
            raise
        except Exception:
            raise

    def _resolver_usuario_postgres(self, usuario_ref):
        valor = (usuario_ref or "").strip()
        if not valor:
            return None

        if valor.isdigit():
            usuario_id = int(valor)
            try:
                if getattr(self.postgres, "connection", None) is None or getattr(self.postgres, "cursor", None) is None:
                    return usuario_id
                self.postgres.cursor.execute("SELECT id FROM usuario WHERE id = %s LIMIT 1", (usuario_id,))
                row = self.postgres.cursor.fetchone()
                return usuario_id if row else None
            except Exception:
                return usuario_id

        if "@" in valor:
            try:
                usuario = self.postgres.get_user_by_email(valor)
            except Exception:
                usuario = None
            if usuario and usuario.get("id") is not None:
                return usuario.get("id")

        return valor

    def _buscar_propiedad_para_reserva(self, propiedad_id):
        valor = (propiedad_id or "").strip() if isinstance(propiedad_id, str) else propiedad_id
        if valor in (None, ""):
            return None

        try:
            propiedad = self.mongo.get_property_by_id(valor)
        except Exception:
            propiedad = None

        if propiedad:
            return propiedad

        if getattr(self.mongo, 'collection', None) is None:
            return None

        try:
            candidatos = [valor]
            if isinstance(valor, str) and valor.isdigit():
                candidatos.insert(0, int(valor))

            for candidato in candidatos:
                propiedad = self.mongo.collection.find_one({"propiedad_pg_id": candidato})
                if propiedad:
                    return propiedad
        except Exception:
            return None

        return None

    def _calcular_monto_reserva(self, propiedad_id, inicio, fin, monto_ingresado=None):
        monto_texto = (monto_ingresado or "").strip() if isinstance(monto_ingresado, str) else monto_ingresado
        if monto_texto not in (None, ""):
            return float(monto_texto)

        propiedad = self._buscar_propiedad_para_reserva(propiedad_id)

        if not propiedad:
            raise ValueError("No se encontró la propiedad para calcular el monto automáticamente.")

        precio_noche = propiedad.get("precio_por_noche")
        if precio_noche is None:
            raise ValueError("La propiedad no tiene precio_por_noche definido para calcular el monto.")

        start_date = datetime.fromisoformat(inicio).date()
        end_date = datetime.fromisoformat(fin).date()
        noches = (end_date - start_date).days
        if noches <= 0:
            raise ValueError("La fecha fin debe ser posterior a la fecha inicio.")

        return float(precio_noche) * noches

    def _obtener_precio_por_noche_propiedad(self, propiedad_id):
        propiedad = self._buscar_propiedad_para_reserva(propiedad_id)

        if not propiedad:
            return None

        precio_noche = propiedad.get("precio_por_noche")
        return float(precio_noche) if precio_noche is not None else None

    def _resolver_propiedad_postgres(self, propiedad_ref):
        valor = (propiedad_ref or "").strip() if isinstance(propiedad_ref, str) else propiedad_ref
        if valor in (None, ""):
            return None

        if isinstance(valor, str) and valor.isdigit():
            valor = int(valor)

        if isinstance(valor, int):
            existe = self._propiedad_existe_en_postgres(valor)
            if existe is False:
                return None
            if existe is True:
                return valor

            # Fallback cuando no se puede validar en Postgres: buscar mapping en Mongo.
            propiedad_fallback = self._buscar_propiedad_para_reserva(str(valor))
            if propiedad_fallback and propiedad_fallback.get("propiedad_pg_id") is not None:
                return int(propiedad_fallback.get("propiedad_pg_id")) if str(propiedad_fallback.get("propiedad_pg_id")).isdigit() else propiedad_fallback.get("propiedad_pg_id")
            return None

        propiedad = self._buscar_propiedad_para_reserva(valor)
        if not propiedad:
            return None

        propiedad_pg_id = propiedad.get("propiedad_pg_id")
        if propiedad_pg_id is None:
            return None
        propiedad_pg_id = int(propiedad_pg_id) if str(propiedad_pg_id).isdigit() else propiedad_pg_id

        if isinstance(propiedad_pg_id, int):
            existe = self._propiedad_existe_en_postgres(propiedad_pg_id)
            if existe is False:
                return None

        return propiedad_pg_id

    def realizar_reserva_business(self, usuario_id, propiedad_id, inicio, fin, monto=None, metodo_pago_id=None):
        cassandra_check_warning = None
        # 1) Insert reserva en Postgres (fuente de verdad)
        try:
            resolved_user_id = self._resolver_usuario_postgres(usuario_id)
            if resolved_user_id is None:
                raise ValueError("Debe indicar un usuario válido para la reserva.")

            resolved_property_id = self._resolver_propiedad_postgres(propiedad_id)
            if resolved_property_id is None:
                raise ValueError("Debe indicar una propiedad válida para la reserva.")

            # 0) Verificación de disponibilidad en Cassandra antes de tocar Postgres.
            disponibilidad_propiedad_id = str(resolved_property_id)
            try:
                disponible = self.cassandra.verificar_disponibilidad(disponibilidad_propiedad_id, inicio, fin)
            except Exception as cassandra_err:
                print(f"Error Cassandra en verificación de disponibilidad: {cassandra_err}")
                raise ValueError(
                    "Error: No se pudo validar la disponibilidad en este momento. "
                    "Intente nuevamente en unos minutos."
                ) from cassandra_err
            if not disponible:
                raise ValueError("Error: La propiedad no está disponible en las fechas seleccionadas")

            monto_calculado = self._calcular_monto_reserva(propiedad_id, inicio, fin, monto_ingresado=monto)
            pg_res = self.postgres.create_reservation(
                resolved_user_id,
                resolved_property_id,
                inicio,
                fin,
                amount=monto_calculado,
            )
        except Psycopg2Error:
            raise
        except Exception as e:
            raise

        reserva_id = pg_res.get('id') if isinstance(pg_res, dict) else None
        tabla_reserva = pg_res.get('table') if isinstance(pg_res, dict) else 'reserva'
        warnings = []
        if cassandra_check_warning:
            warnings.append(cassandra_check_warning)
        precio_por_noche = self._obtener_precio_por_noche_propiedad(propiedad_id)
        disponibilidad_propiedad_id = str(resolved_property_id)

        # 1.b) Ajustar estados iniciales
        pago_result = None
        import psycopg2 
        try:
            conn2 = psycopg2.connect(self.postgres.dsn, sslmode="require", connect_timeout=10)
            cur2 = conn2.cursor()
            cur2.execute("UPDATE reserva SET estado_id = 1 WHERE id = %s;", (reserva_id,))
            if metodo_pago_id is not None:
                cur2.execute(
                    "INSERT INTO pago (reserva_id, monto, fecha_pago, metodo_pago_id, estado_pago_id) VALUES (%s, %s, NOW(), %s, 1)",
                    (reserva_id, monto_calculado, int(metodo_pago_id))
                )
                pago_result = {'created': True}
            conn2.commit()
            cur2.close()
            conn2.close()
        except Exception as e:
            print(f"ERROR PAGO: {e}")
        # 2) Marcar disponibilidad en Cassandra por cada día del rango, y caché Redis.
        try:
            if getattr(self.cassandra, 'availability_table', None) is not None:
                self.cassandra.block_dates(
                    disponibilidad_propiedad_id,
                    inicio,
                    fin,
                    precio_noche=precio_por_noche,
                )
            else:
                warnings.append("No se pudo registrar la disponibilidad en Cassandra.")
        except Exception as cass_err:
            warnings.append("No se pudo registrar la disponibilidad en Cassandra.")

        # Caché Redis por día (best-effort, no bloquea el flujo)
        try:
            if getattr(self.redis, 'client', None) is not None:
                from datetime import date as _date, timedelta as _td
                _start = datetime.fromisoformat(inicio).date()
                _end = datetime.fromisoformat(fin).date()
                _cur = _start
                while _cur <= _end:
                    try:
                        key = f"disp:{disponibilidad_propiedad_id}:{_cur.isoformat()}"
                        self.redis.set_text(key, 'false', ex=60 * 60 * 24)
                    except Exception:
                        pass
                    _cur += timedelta(days=1)
        except Exception:
            pass

        result = {
            'postgres': pg_res,
            'reserva_id': reserva_id,
            'monto_calculado': monto_calculado,
            'pago': pago_result,
        }
        if warnings:
            result['warning'] = " | ".join(warnings)
        return result

    def confirmar_pago(self, reserva_id):
        if getattr(self.postgres, 'connection', None) is None or getattr(self.postgres, 'cursor', None) is None:
            raise RuntimeError("Conexión a Postgres no disponible.")

        try:
            reserva_id_int = int(reserva_id)
        except (TypeError, ValueError):
            return {"ok": False, "mensaje": "reserva_id inválido."}

        cursor = self.postgres.cursor
        connection = self.postgres.connection

        try:
            cursor.execute(
                """
                UPDATE pago
                SET estado_pago_id = 2
                WHERE reserva_id = %s;
                """,
                (reserva_id_int,),
            )
            pagos_actualizados = cursor.rowcount or 0

            cursor.execute(
                """
                UPDATE reserva
                SET estado_id = 2
                WHERE id = %s;
                """,
                (reserva_id_int,),
            )
            reservas_actualizadas = cursor.rowcount or 0

            connection.commit()
            return {
                "ok": True,
                "mensaje": "Pago confirmado y reserva actualizada a confirmada.",
                "pagos_actualizados": int(pagos_actualizados),
                "reservas_actualizadas": int(reservas_actualizadas),
            }
        except Exception as e:
            connection.rollback()
            return {"ok": False, "mensaje": f"Error al confirmar pago: {e}"}

    def cancelar_reserva(self, reserva_id, usuario_id=None):
        if getattr(self.postgres, 'connection', None) is None or getattr(self.postgres, 'cursor', None) is None:
            raise RuntimeError("Conexión a Postgres no disponible.")

        try:
            reserva_id_int = int(reserva_id)
        except (TypeError, ValueError):
            return {"ok": False, "mensaje": "reserva_id inválido."}

        cursor = self.postgres.cursor
        connection = self.postgres.connection
        usuario_id_int = None
        if usuario_id is not None:
            try:
                usuario_id_int = int(usuario_id)
            except (TypeError, ValueError):
                return {"ok": False, "mensaje": "usuario_id inválido para validar titularidad de reserva."}

        try:
            cursor.execute(
                """
                SELECT estado_id, usuario_id
                FROM reserva
                WHERE id = %s
                LIMIT 1;
                """,
                (reserva_id_int,),
            )
            row = cursor.fetchone()
            if not row:
                connection.rollback()
                return {"ok": False, "mensaje": f"No existe la reserva con id {reserva_id_int}."}

            estado_actual = row[0]
            usuario_reserva = row[1]

            if usuario_id_int is not None:
                try:
                    usuario_reserva_int = int(usuario_reserva)
                except (TypeError, ValueError):
                    connection.rollback()
                    return {"ok": False, "mensaje": "No se pudo validar el titular de la reserva."}

                if usuario_reserva_int != usuario_id_int:
                    connection.rollback()
                    return {"ok": False, "mensaje": "La reserva no pertenece al usuario autenticado."}

            if int(estado_actual) == 3:
                connection.rollback()
                return {"ok": False, "mensaje": "La reserva ya está cancelada."}

            cursor.execute(
                """
                UPDATE reserva
                SET estado_id = 3
                WHERE id = %s;
                """,
                (reserva_id_int,),
            )
            reservas_actualizadas = cursor.rowcount or 0

            cursor.execute(
                """
                UPDATE pago
                SET estado_pago_id = 3
                WHERE reserva_id = %s;
                """,
                (reserva_id_int,),
            )
            pagos_actualizados = cursor.rowcount or 0

            connection.commit()
            return {
                "ok": True,
                "mensaje": "Reserva cancelada y pago marcado como rechazado.",
                "reservas_actualizadas": int(reservas_actualizadas),
                "pagos_actualizados": int(pagos_actualizados),
            }
        except Exception as e:
            connection.rollback()
            return {"ok": False, "mensaje": f"Error al cancelar reserva: {e}"}

    def dejar_resena_business(
        self,
        usuario_id,
        propiedad_id,
        texto,
        rating,
        puntaje_limpieza=None,
        puntaje_comunicacion=None,
        puntaje_ubicacion=None,
    ):
        resolved_user_id = self._resolver_usuario_postgres(usuario_id)
        resolved_property_id = self._resolver_propiedad_postgres(propiedad_id)

        if resolved_user_id is None:
            raise ValueError("Debe indicar un usuario válido para la reseña.")
        if resolved_property_id is None:
            raise ValueError("Debe indicar una propiedad válida para la reseña.")

        propiedad_doc = self._buscar_propiedad_para_reserva(propiedad_id)
        if not propiedad_doc:
            raise ValueError("No se encontró la propiedad en Mongo para registrar la reseña.")

        mongo_property_id = str(propiedad_doc.get("_id")) if propiedad_doc.get("_id") is not None else str(propiedad_id)

        comentario = texto.get('comentario', '') if isinstance(texto, dict) else texto
        comentario = (comentario or '').strip()
        calificacion = float(rating) if rating is not None else None

        if calificacion is None or calificacion < 0 or calificacion > 5:
            raise ValueError("La calificación general debe estar entre 0 y 5.")

        def _normalizar_puntaje(valor, nombre_campo):
            if valor is None:
                return None
            puntaje_valor = float(valor)
            if puntaje_valor < 0 or puntaje_valor > 5:
                raise ValueError(f"{nombre_campo} debe estar entre 0 y 5.")
            return puntaje_valor

        puntaje_limpieza = _normalizar_puntaje(puntaje_limpieza, "Puntaje limpieza")
        puntaje_comunicacion = _normalizar_puntaje(puntaje_comunicacion, "Puntaje comunicación")
        puntaje_ubicacion = _normalizar_puntaje(puntaje_ubicacion, "Puntaje ubicación")

        review = {
            'usuario_id': resolved_user_id,
            'nombre_usuario': None,
            'calificacion': calificacion,
            'comentario': comentario,
            'fecha': datetime.now(timezone.utc).isoformat(),
            'visible': True,
        }
        if puntaje_limpieza is not None:
            review['puntaje_limpieza'] = puntaje_limpieza
        if puntaje_comunicacion is not None:
            review['puntaje_comunicacion'] = puntaje_comunicacion
        if puntaje_ubicacion is not None:
            review['puntaje_ubicacion'] = puntaje_ubicacion

        # 1) SQL primero; si falla, no se intenta Mongo.
        try:
            sql_review_res = self.postgres.create_review(
                autor_id=resolved_user_id,
                propiedad_id=resolved_property_id,
                puntaje_general=calificacion,
                puntaje_limpieza=puntaje_limpieza,
                puntaje_comunicacion=puntaje_comunicacion,
                puntaje_ubicacion=puntaje_ubicacion,
                comentario=comentario,
                visible=True,
            )
        except Psycopg2Error:
            raise
        except Exception:
            raise

        # 2) Mongo; si falla, se compensa eliminando la reseña SQL recién creada.
        try:
            mongo_res = self.mongo.add_review(mongo_property_id, review)
        except Exception as mongo_err:
            try:
                if (
                    isinstance(sql_review_res, dict)
                    and sql_review_res.get('id') is not None
                    and sql_review_res.get('table')
                    and getattr(self.postgres, 'connection', None) is not None
                    and getattr(self.postgres, 'cursor', None) is not None
                ):
                    table_name = str(sql_review_res.get('table'))
                    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", table_name):
                        self.postgres.cursor.execute(f"DELETE FROM {table_name} WHERE id = %s", (sql_review_res.get('id'),))
                        self.postgres.connection.commit()
            except Exception:
                try:
                    if getattr(self.postgres, 'connection', None) is not None:
                        self.postgres.connection.rollback()
                except Exception:
                    pass

            raise RuntimeError(
                f"No se pudo guardar la reseña en Mongo; se intentó revertir SQL para mantener consistencia: {mongo_err}"
            )

        # Recalcular promedio en SQL tomando todas las reseñas de la propiedad.
        try:
            self.postgres.actualizar_promedio_propiedad_desde_resenias(resolved_property_id)
        except Psycopg2Error:
            warning_text = 'La reseña se guardó en SQL y Mongo pero no se pudo actualizar el promedio en Postgres.'
            return {'mongo': mongo_res, 'sql_resenia': sql_review_res, 'warning': warning_text}
        except Exception:
            warning_text = 'La reseña se guardó en SQL y Mongo pero no se pudo sincronizar el promedio en Postgres.'
            return {'mongo': mongo_res, 'sql_resenia': sql_review_res, 'warning': warning_text}

        return {'mongo': mongo_res, 'sql_resenia': sql_review_res}

    # Helper wrappers to call repository create operations
    def create_property(self, property_doc):
        try:
            return self.mongo.create_property(property_doc)
        except Exception as e:
            raise

    def create_review(self, property_id, review_doc):
        try:
            return self.mongo.add_review(property_id, review_doc)
        except Exception as e:
            raise

    def create_reservation(self, user_id, property_id, start_date, end_date, amount=None):
        try:
            return self.postgres.create_reservation(user_id, property_id, start_date, end_date, amount=amount)
        except Exception as e:
            raise

    def create_payment(self, amount, reserva_id, status='pendiente', method=None, ciudad=None, referencia_externa=None):
        try:
            metodo_param = method
            if method not in (None, "") and str(method).strip().isdigit() and getattr(self.postgres, 'cursor', None) is not None:
                self.postgres.cursor.execute(
                    """
                    SELECT nombre
                    FROM metodo_pago
                    WHERE id = %s AND activo = TRUE
                    LIMIT 1;
                    """,
                    (int(str(method).strip()),),
                )
                metodo_row = self.postgres.cursor.fetchone()
                if not metodo_row:
                    raise ValueError("Método de pago inválido o inactivo.")
                metodo_param = metodo_row[0]

            return self.postgres.create_payment(
                amount,
                reserva_id,
                status=status,
                method=metodo_param,
                ciudad=ciudad,
                referencia_externa=referencia_externa,
            )
        except Exception as e:
            raise

    def _mostrar_reservas_menu(self, solo_pendientes=True, usuario_id=None, titulo_personalizado=None, anfitrion_id=None):
        if getattr(self.postgres, 'connection', None) is None or getattr(self.postgres, 'cursor', None) is None:
            print("No hay conexión a Postgres para listar reservas.")
            return []

        cursor = self.postgres.cursor
        titulo = titulo_personalizado or ("Reservas pendientes:" if solo_pendientes else "Reservas no canceladas:")

        try:
            where_clauses = ["r.estado_id = 1" if solo_pendientes else "r.estado_id <> 3"]
            params = []
            if usuario_id is not None:
                where_clauses.append("r.usuario_id = %s")
                params.append(int(usuario_id))

            join_clause = ""
            if anfitrion_id is not None:
                join_clause = "JOIN propiedad p ON p.id = r.propiedad_id"
                where_clauses.append("p.anfitrion_id = %s")
                params.append(int(anfitrion_id))

            query = f"""
                SELECT r.id, r.propiedad_id, r.fecha_inicio, r.fecha_fin, r.monto_total
                FROM reserva r
                {join_clause}
                WHERE {' AND '.join(where_clauses)}
                ORDER BY r.id DESC
            """

            cursor.execute(query, tuple(params))
            rows = cursor.fetchall() or []

            print(f"\n{titulo}")
            print("ID  | Propiedad | Fecha inicio | Fecha fin  | Monto")
            print("--------------------------------------------------")
            if not rows:
                print("(sin resultados)")
                return []

            for row in rows:
                reserva_id, propiedad_id, fecha_inicio, fecha_fin, monto = row
                fecha_inicio_texto = fecha_inicio.isoformat() if hasattr(fecha_inicio, 'isoformat') else str(fecha_inicio)
                fecha_fin_texto = fecha_fin.isoformat() if hasattr(fecha_fin, 'isoformat') else str(fecha_fin)
                print(f"{str(reserva_id):<3} | {str(propiedad_id):>9} | {fecha_inicio_texto:<11} | {fecha_fin_texto:<10} | {monto}")
            return rows
        except Exception as e:
            try:
                self.postgres.connection.rollback()
            except Exception:
                pass
            print(f"No se pudieron listar reservas: {e}")
            return []

    def _nombre_estado_reserva(self, estado_id):
        estado_id_texto = str(estado_id) if estado_id is not None else "N/D"
        fallback = {
            1: "Pendiente",
            2: "Confirmada",
            3: "Cancelada",
        }

        try:
            estado_id_int = int(estado_id)
        except (TypeError, ValueError):
            return estado_id_texto

        if getattr(self.postgres, 'cursor', None) is None:
            return fallback.get(estado_id_int, estado_id_texto)

        try:
            self.postgres.cursor.execute(
                """
                SELECT nombre
                FROM estado_reserva
                WHERE id = %s
                LIMIT 1
                """,
                (estado_id_int,),
            )
            row = self.postgres.cursor.fetchone()
            if row and row[0]:
                return str(row[0])
        except Exception:
            try:
                if getattr(self.postgres, 'connection', None) is not None:
                    self.postgres.connection.rollback()
            except Exception:
                pass

        return fallback.get(estado_id_int, estado_id_texto)

    def _mostrar_reservas_anfitrion(self, active_session=None):
        if getattr(self.postgres, 'connection', None) is None or getattr(self.postgres, 'cursor', None) is None:
            print("No hay conexión a Postgres para listar reservas del anfitrión.")
            return

        email = self._normalize_email(active_session.get('email') if isinstance(active_session, dict) else None)
        if not email:
            print("No se encontró email de sesión para identificar al anfitrión.")
            return

        try:
            usuario = self.postgres.get_user_by_email(email)
        except Exception as e:
            print(f"No se pudo resolver anfitrión por email: {e}")
            return

        anfitrion_id = usuario.get('id') if isinstance(usuario, dict) else None
        if anfitrion_id is None:
            print("No existe un usuario anfitrión en Postgres para el email de la sesión activa.")
            return

        cursor = self.postgres.cursor
        try:
            cursor.execute(
                """
                SELECT r.id, r.usuario_id, r.propiedad_id, r.fecha_inicio, r.fecha_fin, r.monto_total, r.estado_id
                FROM reserva r
                WHERE r.propiedad_id IN (SELECT id FROM propiedad WHERE anfitrion_id = %s)
                ORDER BY r.id DESC
                """,
                (int(anfitrion_id),),
            )
            rows = cursor.fetchall() or []

            print("\nReservas de mis propiedades:")
            print("ID  | Usuario | Propiedad | Fecha inicio | Fecha fin  | Monto | Estado")
            print("-----------------------------------------------------------------------")
            if not rows:
                print("(sin resultados)")
                return

            for row in rows:
                reserva_id, usuario_id, propiedad_id, fecha_inicio, fecha_fin, monto, estado_id = row
                fecha_inicio_texto = fecha_inicio.isoformat() if hasattr(fecha_inicio, 'isoformat') else str(fecha_inicio)
                fecha_fin_texto = fecha_fin.isoformat() if hasattr(fecha_fin, 'isoformat') else str(fecha_fin)
                estado_nombre = self._nombre_estado_reserva(estado_id)
                print(
                    f"{str(reserva_id):<3} | {str(usuario_id):>7} | {str(propiedad_id):>9} | "
                    f"{fecha_inicio_texto:<11} | {fecha_fin_texto:<10} | {monto} | {estado_nombre}"
                )
        except Exception as e:
            try:
                self.postgres.connection.rollback()
            except Exception:
                pass
            print(f"No se pudieron listar reservas del anfitrión: {e}")

    def _mostrar_reservas_huesped(self, active_session=None):
        if getattr(self.postgres, 'connection', None) is None or getattr(self.postgres, 'cursor', None) is None:
            print("No hay conexión a Postgres para listar reservas del huésped.")
            return

        email = self._normalize_email(active_session.get('email') if isinstance(active_session, dict) else None)
        if not email:
            print("No se encontró email de sesión para identificar al huésped.")
            return

        try:
            usuario = self.postgres.get_user_by_email(email)
        except Exception as e:
            print(f"No se pudo resolver huésped por email: {e}")
            return

        huesped_id = usuario.get('id') if isinstance(usuario, dict) else None
        if huesped_id is None:
            print("No existe un usuario huésped en Postgres para el email de la sesión activa.")
            return

        cursor = self.postgres.cursor
        try:
            cursor.execute(
                """
                SELECT r.id, r.propiedad_id, r.fecha_inicio, r.fecha_fin, r.monto_total, r.estado_id
                FROM reserva r
                WHERE r.usuario_id = %s
                ORDER BY r.id DESC
                """,
                (int(huesped_id),),
            )
            rows = cursor.fetchall() or []

            print("\nMis reservas:")
            print("ID  | Propiedad | Fecha inicio | Fecha fin  | Monto | Estado")
            print("---------------------------------------------------------------")
            if not rows:
                print("(sin resultados)")
                return

            for row in rows:
                reserva_id, propiedad_id, fecha_inicio, fecha_fin, monto, estado_id = row
                fecha_inicio_texto = fecha_inicio.isoformat() if hasattr(fecha_inicio, 'isoformat') else str(fecha_inicio)
                fecha_fin_texto = fecha_fin.isoformat() if hasattr(fecha_fin, 'isoformat') else str(fecha_fin)
                estado_nombre = self._nombre_estado_reserva(estado_id)
                print(
                    f"{str(reserva_id):<3} | {str(propiedad_id):>9} | {fecha_inicio_texto:<11} | "
                    f"{fecha_fin_texto:<10} | {monto} | {estado_nombre}"
                )
        except Exception as e:
            try:
                self.postgres.connection.rollback()
            except Exception:
                pass
            print(f"No se pudieron listar reservas del huésped: {e}")

    def _tipo_menu_requerimientos(self, active_session=None):
        raw_tipo = (active_session.get('tipo') if isinstance(active_session, dict) else None)
        tipo = (raw_tipo or '').strip().casefold()
        if tipo in {'huesped', 'anfitrion', 'ambos'}:
            return tipo

        normalizado = self._tipo_usuario_sesion(active_session)
        if normalizado in {'huesped', 'anfitrion'}:
            return normalizado
        return 'huesped'

    def menu_requerimientos(self, active_session=None):
        while True:
            if active_session and not self._sesion_activa_en_redis(active_session):
                print("\nTu sesión expiró. Iniciá sesión nuevamente.")
                return "logout"

            tipo_usuario = self._tipo_menu_requerimientos(active_session)

            if tipo_usuario == 'huesped':
                opciones = [
                    ("1", "Hacer reserva (Postgres + Cassandra + Redis)"),
                    ("2", "Cancelar mi reserva (Postgres)"),
                    ("3", "Dejar reseña (Mongo + Postgres)"),
                    ("4", "Ver mis reservas (Postgres)"),
                ]
            elif tipo_usuario == 'anfitrion':
                opciones = [
                    ("1", "Publicar propiedad (MongoDB + Postgres)"),
                    ("2", "Ver reservas de mis propiedades (Postgres)"),
                    ("3", "Confirmar pago de una reserva (Postgres)"),
                    ("4", "Cancelar reserva (Postgres)"),
                ]
            else:
                opciones = [
                    ("1", "Hacer reserva (Postgres + Cassandra + Redis)"),
                    ("2", "Cancelar mi reserva (Postgres)"),
                    ("3", "Dejar reseña (Mongo + Postgres)"),
                    ("4", "Ver mis reservas (Postgres)"),
                    ("5", "Publicar propiedad (MongoDB + Postgres)"),
                    ("6", "Ver reservas de mis propiedades (Postgres)"),
                    ("7", "Confirmar pago de una reserva (Postgres)"),
                    ("8", "Cancelar reserva (Postgres)"),
                ]

            print("\n" + "=" * 30)
            print("=== REQUERIMIENTOS DEL SISTEMA ===")
            print("=" * 30)
            print(f"Usuario: {active_session.get('nombre_completo', 'Usuario') if active_session else 'Usuario'} | Tipo: {tipo_usuario}")
            for key, label in opciones:
                print(f"{key}. {label}")
            print("0. Volver")

            opc = input("\nSeleccione una opción: ").strip()
            if active_session and not self._sesion_activa_en_redis(active_session):
                print("\nTu sesión expiró. Iniciá sesión nuevamente.")
                return "logout"

            if opc == "0":
                return

            if tipo_usuario == 'huesped':
                accion = {
                    "1": "hacer_reserva",
                    "2": "cancelar_mi_reserva",
                    "3": "dejar_resena",
                    "4": "ver_mis_reservas",
                }.get(opc)
            elif tipo_usuario == 'anfitrion':
                accion = {
                    "1": "publicar_propiedad",
                    "2": "ver_reservas_anfitrion",
                    "3": "confirmar_pago",
                    "4": "cancelar_reserva",
                }.get(opc)
            else:
                accion = {
                    "1": "hacer_reserva",
                    "2": "cancelar_mi_reserva",
                    "3": "dejar_resena",
                    "4": "ver_mis_reservas",
                    "5": "publicar_propiedad",
                    "6": "ver_reservas_anfitrion",
                    "7": "confirmar_pago",
                    "8": "cancelar_reserva",
                }.get(opc)

            if accion == "publicar_propiedad":
                print("\n--- Publicar propiedad (MongoDB) ---")
                anfitrion = active_session.get('email') if active_session else None
                if not anfitrion:
                    print("No se pudo identificar al anfitrión de la sesión activa.")
                    continue
                titulo = input("Título: ").strip()
                descripcion = input("Descripción: ").strip()
                tipo_propiedad = self._seleccionar_tipo_propiedad_cli()
                precio = input("Precio por noche: ").strip() or None
                moneda = input("Moneda (ej: USD): ").strip() or "USD"
                huespedes_max = input("Huéspedes máximos: ").strip() or None
                cant_habitaciones = input("Cantidad de habitaciones: ").strip() or None
                cant_banios = input("Cantidad de baños: ").strip() or None
                ciudad = input("Ciudad: ").strip()
                barrio = input("Barrio (opcional): ").strip() or None
                pais = input("País: ").strip()
                latitud = input("Latitud (opcional): ").strip() or None
                longitud = input("Longitud (opcional): ").strip() or None

                # Selección de servicios desde la tabla 'servicio' (M:N)
                servicio_ids = []
                try:
                    if getattr(self.postgres, 'cursor', None) is not None:
                        self.postgres.cursor.execute("SELECT id, nombre FROM servicio ORDER BY nombre;")
                        servicios_catalogo = self.postgres.cursor.fetchall() or []
                    else:
                        servicios_catalogo = []
                except Exception:
                    try:
                        if getattr(self.postgres, 'connection', None) is not None:
                            self.postgres.connection.rollback()
                    except Exception:
                        pass
                    servicios_catalogo = []

                if servicios_catalogo:
                    print("\nServicios disponibles:")
                    for sid, snombre in servicios_catalogo:
                        print(f"  {sid}. {snombre}")
                    ids_input = input("Ingrese IDs de servicios separados por coma (Enter para ninguno): ").strip()
                    if ids_input:
                        for token in ids_input.split(','):
                            token = token.strip()
                            if token.isdigit() and any(int(token) == row[0] for row in servicios_catalogo):
                                servicio_ids.append(int(token))
                            elif token:
                                print(f"  ⚠ ID '{token}' no válido, ignorado.")
                else:
                    print("⚠ No se pudo cargar el catálogo de servicios desde Postgres.")

                fotos = input("Fotos (URLs separadas por coma, opcional): ").strip() or ''
                doc = {
                    'titulo': titulo,
                    'descripcion': descripcion,
                    'tipo_propiedad': tipo_propiedad,
                    'precio_por_noche': float(precio) if precio else None,
                    'moneda': moneda,
                    'huespedes_max': int(huespedes_max) if huespedes_max else None,
                    'cant_habitaciones': int(cant_habitaciones) if cant_habitaciones else None,
                    'cant_banios': int(cant_banios) if cant_banios else None,
                    'ubicacion': {'ciudad': ciudad, 'pais': pais, 'barrio': barrio},
                    'barrio': barrio,
                    'calificacion_promedio': None,
                    'activa': True,
                    'servicio_ids': servicio_ids,
                    'fotos': [{'url': url.strip(), 'orden': index + 1} for index, url in enumerate([item for item in fotos.split(',') if item.strip()])],
                }
                if latitud or longitud:
                    doc['ubicacion']['coordenadas'] = {
                        'type': 'Point',
                        'coordinates': [float(longitud) if longitud else None, float(latitud) if latitud else None],
                    }
                try:
                    res = self.publicar_propiedad_business(anfitrion, doc)
                    mongo_info = res.get('mongo') if isinstance(res.get('mongo'), dict) else {}
                    ubicacion_doc = doc.get('ubicacion') if isinstance(doc.get('ubicacion'), dict) else {}
                    ubicacion_texto = ", ".join(
                        [
                            parte
                            for parte in [
                                ubicacion_doc.get('barrio') or barrio,
                                ubicacion_doc.get('ciudad') or ciudad,
                                ubicacion_doc.get('pais') or pais,
                            ]
                            if parte
                        ]
                    ) or "Ubicación no informada"
                    print("\n" + "=" * 50)
                    print("✓ ¡Propiedad publicada exitosamente!")
                    print("=" * 50)
                    print(f"  Título        : {titulo or 'Sin título'}")
                    print(f"  Descripción   : {(descripcion or 'Sin descripción')[:120]}{'...' if len(descripcion or '') > 120 else ''}")
                    print(f"  Tipo          : {tipo_propiedad or 'No especificado'}")
                    print(f"  Ubicación     : {ubicacion_texto}")
                    print(f"  Precio/noche  : {precio if precio else 'N/D'} {moneda if moneda else ''}".rstrip())
                    print(f"  Huéspedes máx : {huespedes_max if huespedes_max else 'N/D'}")
                    print(f"  Habitaciones  : {cant_habitaciones if cant_habitaciones else 'N/D'}")
                    print(f"  Baños         : {cant_banios if cant_banios else 'N/D'}")
                    print(f"  Zona turística: {'Sí' if res.get('zona_turistica') else 'No'}")
                    print(f"  Zona céntrica : {'Sí' if res.get('zona_centrica') else 'No'}")
                    if mongo_info.get('warning'):
                        print(f"  ⚠ Aviso Mongo    : {mongo_info.get('warning')}")
                    print("=" * 50)
                    self._pausa_ui("exito", listado_largo=True)
                except Exception as e:
                    print(f"✗ Error publicando propiedad: {e}")
                    self._pausa_ui("error")
            elif accion == "hacer_reserva":
                print("\n--- Realizar reserva (Postgres + Cassandra + Redis) ---")

                if getattr(self.mongo, 'collection', None) is None:
                    print("MongoDB no está disponible para buscar propiedades.")
                    continue

                pais = input("¿En qué país? (Argentina/Brasil/Chile, Enter para todos): ").strip() or None
                ciudad = input("¿En qué ciudad? (Enter para todas): ").strip() or None
                tipo = input("¿Tipo de propiedad? (Departamento/Casa/Cabaña/Loft/Habitación, Enter para cualquiera): ").strip() or None
                precio_max = input("¿Precio máximo por noche en USD? (Enter para cualquiera): ").strip() or None
                checkin_busqueda = input("Check-in para búsqueda (YYYY-MM-DD, Enter para omitir): ").strip() or "sinfecha"
                checkout_busqueda = input("Check-out para búsqueda (YYYY-MM-DD, Enter para omitir): ").strip() or "sinfecha"

                query = {"activa": True}
                if pais:
                    query["ubicacion.pais"] = {"$regex": pais, "$options": "i"}
                if ciudad:
                    query["ubicacion.ciudad"] = {"$regex": ciudad, "$options": "i"}
                if tipo:
                    query["tipo_propiedad"] = {"$regex": tipo, "$options": "i"}
                if precio_max:
                    try:
                        query["precio_por_noche"] = {"$lte": float(precio_max)}
                    except ValueError:
                        print("Precio máximo inválido.")
                        continue

                ciudad_cache = (ciudad or "todas").strip().casefold() or "todas"
                checkin_cache = checkin_busqueda
                checkout_cache = checkout_busqueda
                cache_key_busqueda = f"busqueda:{ciudad_cache}:{checkin_cache}:{checkout_cache}"

                resultados = None
                cached_search = self._redis_get_json(cache_key_busqueda)
                if isinstance(cached_search, list):
                    resultados = cached_search

                if resultados is None:
                    try:
                        resultados = list(self.mongo.collection.find(query).limit(10))
                    except Exception as e:
                        print(f"Error consultando propiedades en MongoDB: {e}")
                        continue

                    try:
                        cache_docs = []
                        for doc in resultados:
                            ubicacion_cache = doc.get("ubicacion") if isinstance(doc.get("ubicacion"), dict) else {}
                            cache_docs.append({
                                "propiedad_pg_id": doc.get("propiedad_pg_id"),
                                "tipo_propiedad": doc.get("tipo_propiedad"),
                                "barrio": doc.get("barrio"),
                                "ubicacion": {
                                    "ciudad": ubicacion_cache.get("ciudad"),
                                    "pais": ubicacion_cache.get("pais"),
                                },
                                "precio_por_noche": doc.get("precio_por_noche"),
                                "huespedes_max": doc.get("huespedes_max"),
                                "cant_habitaciones": doc.get("cant_habitaciones"),
                                "cant_banios": doc.get("cant_banios"),
                                "servicios": doc.get("servicios"),
                                "calificacion_promedio": doc.get("calificacion_promedio"),
                                "zona_centrica": doc.get("zona_centrica"),
                            })
                        self._redis_set_json(cache_key_busqueda, cache_docs, ttl_seconds=300)
                    except Exception:
                        pass

                # Ranking por ciudad (Sorted Set en Redis, TTL 1h).
                try:
                    if getattr(self.redis, 'client', None) is not None and ciudad:
                        scores = {}
                        for doc in resultados:
                            prop_id = doc.get("propiedad_pg_id")
                            rating = doc.get("calificacion_promedio")
                            if prop_id is None:
                                continue
                            try:
                                score = float(rating) if rating is not None else 0.0
                            except Exception:
                                score = 0.0
                            scores[str(prop_id)] = score
                        if scores:
                            self.redis.cache_top_properties_by_city(ciudad, scores, ttl_seconds=3600)
                except Exception:
                    pass

                if not resultados:
                    print("No se encontraron propiedades con esos filtros")
                    continue

                print("\nPropiedades encontradas:")
                print("─" * 45)
                for idx, prop in enumerate(resultados, start=1):
                    ubicacion = prop.get("ubicacion") if isinstance(prop.get("ubicacion"), dict) else {}
                    tipo_propiedad = prop.get("tipo_propiedad") or "Propiedad"
                    referencia = prop.get("barrio") or ubicacion.get("ciudad") or "Ubicación sin detalle"
                    ciudad_prop = ubicacion.get("ciudad") or "N/D"
                    pais_prop = ubicacion.get("pais") or "N/D"
                    precio_noche = prop.get("precio_por_noche")
                    huespedes = prop.get("huespedes_max") if prop.get("huespedes_max") is not None else "N/D"
                    habitaciones = prop.get("cant_habitaciones") if prop.get("cant_habitaciones") is not None else "N/D"
                    banios = prop.get("cant_banios") if prop.get("cant_banios") is not None else "N/D"
                    servicios_raw = prop.get("servicios")
                    if isinstance(servicios_raw, list):
                        servicios = ", ".join(str(item) for item in servicios_raw[:4]) if servicios_raw else "N/D"
                    elif isinstance(servicios_raw, str):
                        servicios = servicios_raw or "N/D"
                    else:
                        servicios = "N/D"
                    rating = prop.get("calificacion_promedio")
                    rating_texto = f"{float(rating):.1f}" if isinstance(rating, (int, float)) else "N/D"
                    zona_centrica = "Sí" if bool(prop.get("zona_centrica")) else "No"
                    precio_texto = f"${precio_noche}/noche" if precio_noche is not None else "Precio no disponible"

                    print(f"{idx}. {tipo_propiedad} en {referencia}")
                    print(f"   Ciudad: {ciudad_prop}, {pais_prop} | Precio: {precio_texto}")
                    print(f"   Huéspedes: {huespedes} | Habitaciones: {habitaciones} | Baños: {banios}")
                    print(f"   Servicios: {servicios}")
                    print(f"   Rating: {rating_texto} ⭐ | Zona céntrica: {zona_centrica}")
                    print("─" * 45)

                seleccion_valida = None
                while True:
                    seleccion = input("Seleccione una propiedad (número) o 0 para volver: ").strip()
                    if seleccion == "0":
                        break
                    if not seleccion.isdigit():
                        print("Selección inválida. Debe ingresar un número.")
                        continue

                    indice = int(seleccion)
                    if indice < 1 or indice > len(resultados):
                        print("Selección inválida. El número está fuera de rango.")
                        continue

                    seleccion_valida = resultados[indice - 1]
                    break

                if seleccion_valida is None:
                    continue

                propiedad_real_id = seleccion_valida.get("propiedad_pg_id")
                if propiedad_real_id is None:
                    print("Esta propiedad no está disponible para reservar")
                    continue

                # Asegurar siempre el ID real de la propiedad en Postgres (nunca el número de opción del menú).
                propiedad_real_id = int(propiedad_real_id) if str(propiedad_real_id).isdigit() else propiedad_real_id

                usuario_email = (active_session.get('email') if active_session else None)
                if not usuario_email:
                    usuario_email = input("Usuario ID o email: ").strip() or None
                if not usuario_email:
                    print("No se pudo resolver el usuario para la reserva.")
                    continue

                # Registrar vista en historial_vistas (Cassandra)
                try:
                    if getattr(self.cassandra, 'availability_table', None) is not None or getattr(self.cassandra, 'visits_table', None) is not None or getattr(self.cassandra, 'collection', None) is not None:
                        usuario_pg_id_vista = self._resolver_usuario_postgres(usuario_email)
                        if isinstance(usuario_pg_id_vista, int) or (isinstance(usuario_pg_id_vista, str) and usuario_pg_id_vista.isdigit()):
                            self.cassandra.registrar_vista(
                                usuario_id=int(usuario_pg_id_vista),
                                propiedad_id=str(propiedad_real_id),
                            )
                except Exception:
                    pass  # No bloquear el flujo si Cassandra falla

                # Incrementar contador de vistas en Redis (INCR crea la clave desde 0 si no existe).
                try:
                    if getattr(self.redis, 'client', None) is not None:
                        self.redis.incr_property_views(propiedad_real_id)
                except Exception:
                    pass  # No bloquear el flujo si Redis falla

                inicio = input("Fecha inicio (YYYY-MM-DD): ").strip()
                fin = input("Fecha fin (YYYY-MM-DD): ").strip()

                if getattr(self.postgres, 'cursor', None) is None:
                    print("No hay conexión a Postgres para consultar métodos de pago.")
                    continue

                try:
                    self.postgres.cursor.execute("SELECT id, nombre FROM metodo_pago WHERE activo = TRUE")
                    metodos = self.postgres.cursor.fetchall() or []
                except Exception as e:
                    try:
                        if getattr(self.postgres, 'connection', None) is not None:
                            self.postgres.connection.rollback()
                    except Exception:
                        pass
                    print(f"No se pudieron consultar métodos de pago: {e}")
                    continue

                if not metodos:
                    print("No hay métodos de pago activos para continuar con la reserva.")
                    continue

                print("\n¿Con qué método querés pagar?")
                for m in metodos:
                    print(f"{m[0]}. {m[1]}")

                metodo_id = input("Seleccione método de pago: ").strip()
                metodos_validos = {str(m[0]) for m in metodos}
                if metodo_id not in metodos_validos:
                    print("Método de pago inválido.")
                    continue

                try:
                    res = self.realizar_reserva_business(
                        usuario_email,
                        propiedad_real_id,
                        inicio,
                        fin,
                        metodo_pago_id=metodo_id,
                    )
                    print("\n" + "=" * 50)
                    print("✓ ¡Reserva realizada exitosamente!")
                    print("=" * 50)
                    print(f"  Monto total: ${res.get('monto_calculado', 0):.2f}")
                    postgres_info = res.get('postgres', {})
                    if postgres_info.get('created'):
                        print("  Estado inicial: Pendiente")
                        pago_info = res.get('pago', {}) if isinstance(res, dict) else {}
                        if isinstance(pago_info, dict) and pago_info.get('created'):
                            print("  Pago inicial: Pendiente")
                        print(f"  Check-in: {inicio}")
                        print(f"  Check-out: {fin}")
                    else:
                        print(f"  Estado: {postgres_info.get('mensaje', 'Pendiente')}")
                    if isinstance(res, dict) and res.get('warning'):
                        print(f"  Aviso: {res.get('warning')}")
                    print("=" * 50)
                    self._pausa_ui("exito", listado_largo=True)
                except Exception as e:
                    error_text = str(e)
                    if "La propiedad no está disponible en las fechas seleccionadas" in error_text:
                        print("Error: La propiedad no está disponible en las fechas seleccionadas")
                    elif "No se pudo validar la disponibilidad en este momento" in error_text:
                        print("Error: No se pudo validar la disponibilidad en este momento. Intente nuevamente en unos minutos.")
                    elif "No pudimos validar la disponibilidad en este momento" in error_text:
                        print("No pudimos validar la disponibilidad en este momento. Por favor intentá nuevamente en unos minutos.")
                    else:
                        print(f"Error realizando reserva: {e}")
                    self._pausa_ui("error")
            elif accion == "cancelar_mi_reserva":
                print("\n--- Cancelar mi reserva (Postgres) ---")
                usuario_sesion = self._resolver_usuario_postgres(active_session.get('email') if active_session else None)
                if usuario_sesion is None:
                    print("No se pudo resolver el usuario de sesión en Postgres.")
                    continue

                try:
                    self.postgres.cursor.execute(
                        """
                        SELECT
                            r.id,
                            COALESCE(NULLIF(TRIM(p.titulo), ''), 'Propiedad sin título') AS titulo_propiedad,
                            r.fecha_inicio,
                            r.fecha_fin,
                            r.monto_total
                        FROM reserva r
                        LEFT JOIN propiedad p ON p.id = r.propiedad_id
                        WHERE r.estado_id <> 3
                          AND r.usuario_id = %s
                        ORDER BY r.id DESC
                        """,
                        (int(usuario_sesion),),
                    )
                    reservas_usuario = self.postgres.cursor.fetchall() or []
                except Exception as e:
                    try:
                        if getattr(self.postgres, 'connection', None) is not None:
                            self.postgres.connection.rollback()
                    except Exception:
                        pass
                    print(f"No se pudieron listar reservas: {e}")
                    continue

                print("\nMis reservas no canceladas:")
                print("N° | Título propiedad                  | Fecha inicio | Fecha fin  | Monto")
                print("--------------------------------------------------------------------------")
                if not reservas_usuario:
                    print("(sin resultados)")
                    continue

                for idx, row in enumerate(reservas_usuario, start=1):
                    _, titulo_propiedad, fecha_inicio, fecha_fin, monto = row
                    fecha_inicio_texto = fecha_inicio.isoformat() if hasattr(fecha_inicio, 'isoformat') else str(fecha_inicio)
                    fecha_fin_texto = fecha_fin.isoformat() if hasattr(fecha_fin, 'isoformat') else str(fecha_fin)
                    titulo_texto = str(titulo_propiedad or "Propiedad sin título")[:32]
                    print(f"{idx:<2} | {titulo_texto:<32} | {fecha_inicio_texto:<11} | {fecha_fin_texto:<10} | {monto}")

                seleccion_reserva = input("Seleccione el número de reserva a cancelar (0 para volver): ").strip()
                if not seleccion_reserva or seleccion_reserva == "0":
                    continue
                if not seleccion_reserva.isdigit():
                    print("Error: debe ingresar un número válido.")
                    continue

                indice_reserva = int(seleccion_reserva)
                if indice_reserva < 1 or indice_reserva > len(reservas_usuario):
                    print("Error: número fuera de rango.")
                    continue

                reserva_id = reservas_usuario[indice_reserva - 1][0]

                try:
                    res = self.cancelar_reserva(int(reserva_id), usuario_id=usuario_sesion)
                    if res.get("ok"):
                        print("\n" + "=" * 50)
                        print("✓ ¡Reserva cancelada exitosamente!")
                        print("=" * 50)
                        print(f"  Reservas actualizadas: {res.get('reservas_actualizadas', 0)}")
                        print(f"  Pagos actualizados: {res.get('pagos_actualizados', 0)}")
                        print("=" * 50)
                        self._pausa_ui("exito")
                    else:
                        print(f"Error cancelando reserva: {res.get('mensaje', 'Error desconocido')}")
                        self._pausa_ui("error")
                except Exception as e:
                    print(f"Error cancelando reserva: {e}")
                    self._pausa_ui("error")
            elif accion == "dejar_resena":
                print("\n--- Dejar reseña (Mongo + Postgres update) ---")
                usuario_email = (active_session.get('email') if active_session else None)
                usuario_pg_id = self._resolver_usuario_postgres(usuario_email)

                # Listar propiedades que el usuario reservó (confirmadas o cualquier estado)
                propiedades_reservadas = []
                if usuario_pg_id is not None and getattr(self.postgres, 'cursor', None) is not None:
                    try:
                        self.postgres.cursor.execute(
                            """
                            SELECT DISTINCT r.propiedad_id
                            FROM reserva r
                            WHERE r.usuario_id = %s
                            ORDER BY r.propiedad_id;
                            """,
                            (int(usuario_pg_id),),
                        )
                        propiedad_ids_pg = [row[0] for row in (self.postgres.cursor.fetchall() or [])]
                    except Exception as e:
                        try:
                            self.postgres.connection.rollback()
                        except Exception:
                            pass
                        print(f"No se pudieron obtener reservas: {e}")
                        propiedad_ids_pg = []

                    # Buscar cada propiedad en Mongo para obtener _id y título
                    for pid in propiedad_ids_pg:
                        try:
                            doc = self.mongo.collection.find_one(
                                {"propiedad_pg_id": pid},
                                {"_id": 1, "titulo": 1, "descripcion": 1, "tipo_propiedad": 1, "ubicacion": 1},
                            )
                            if doc:
                                propiedades_reservadas.append(doc)
                        except Exception:
                            pass

                if not propiedades_reservadas:
                    print("No tenés propiedades reservadas para reseñar.")
                    continue

                print("\nPropiedades que reservaste:")
                print("─" * 45)
                for idx, prop in enumerate(propiedades_reservadas, start=1):
                    titulo = prop.get("titulo") or "Sin título"
                    tipo = prop.get("tipo_propiedad") or "Propiedad"
                    ubicacion = prop.get("ubicacion") if isinstance(prop.get("ubicacion"), dict) else {}
                    ciudad = ubicacion.get("ciudad") or "N/D"
                    print(f"{idx}. {tipo}: {titulo} | {ciudad}")
                print("─" * 45)

                propiedad_mongo_id = None
                propiedad_seleccionada = None
                while True:
                    sel = input("Seleccione una propiedad (número) o 0 para volver: ").strip()
                    if sel == "0":
                        break
                    if not sel.isdigit():
                        print("Selección inválida.")
                        continue
                    idx_sel = int(sel)
                    if idx_sel < 1 or idx_sel > len(propiedades_reservadas):
                        print("Número fuera de rango.")
                        continue
                    propiedad_seleccionada = propiedades_reservadas[idx_sel - 1]
                    propiedad_mongo_id = str(propiedad_seleccionada["_id"])
                    break

                if propiedad_mongo_id is None:
                    continue

                propiedad = propiedad_mongo_id
                usuario = usuario_email
                rating = input("Calificación (0-5): ").strip() or '5'
                comentario = input("Comentario: ").strip() or ''
                puntaje_limpieza = input("Puntaje limpieza (0-5, Enter para omitir): ").strip() or None
                puntaje_comunicacion = input("Puntaje comunicación (0-5, Enter para omitir): ").strip() or None
                puntaje_ubicacion = input("Puntaje ubicación (0-5, Enter para omitir): ").strip() or None

                try:
                    calificacion_general = float(rating)
                except ValueError:
                    print("Calificación inválida. Debe ser un número entre 0 y 5.")
                    continue
                if calificacion_general < 0 or calificacion_general > 5:
                    print("Calificación inválida. Debe estar entre 0 y 5.")
                    continue

                def _parse_score_input(raw_value, label):
                    if raw_value is None:
                        return None
                    try:
                        parsed = float(raw_value)
                    except ValueError:
                        raise ValueError(f"{label} inválido. Debe ser un número entre 0 y 5.")
                    if parsed < 0 or parsed > 5:
                        raise ValueError(f"{label} inválido. Debe estar entre 0 y 5.")
                    return parsed

                try:
                    puntaje_limpieza_val = _parse_score_input(puntaje_limpieza, "Puntaje limpieza")
                    puntaje_comunicacion_val = _parse_score_input(puntaje_comunicacion, "Puntaje comunicación")
                    puntaje_ubicacion_val = _parse_score_input(puntaje_ubicacion, "Puntaje ubicación")
                except ValueError as validation_error:
                    print(str(validation_error))
                    continue

                review = {
                    'calificacion': calificacion_general,
                    'comentario': comentario,
                }
                if puntaje_limpieza_val is not None:
                    review['puntaje_limpieza'] = puntaje_limpieza_val
                if puntaje_comunicacion_val is not None:
                    review['puntaje_comunicacion'] = puntaje_comunicacion_val
                if puntaje_ubicacion_val is not None:
                    review['puntaje_ubicacion'] = puntaje_ubicacion_val
                try:
                    res = self.dejar_resena_business(
                        usuario,
                        propiedad,
                        review['comentario'],
                        review['calificacion'],
                        puntaje_limpieza=review.get('puntaje_limpieza'),
                        puntaje_comunicacion=review.get('puntaje_comunicacion'),
                        puntaje_ubicacion=review.get('puntaje_ubicacion'),
                    )
                    warning = res.get('warning') if isinstance(res, dict) else None
                    titulo_propiedad = (propiedad_seleccionada or {}).get("titulo") or "Sin título"
                    descripcion_propiedad = (propiedad_seleccionada or {}).get("descripcion") or "Sin descripción"
                    print("\n" + "=" * 50)
                    print("✓ ¡Reseña publicada exitosamente!")
                    print("=" * 50)
                    print(f"  Título : {titulo_propiedad}")
                    print(f"  Descripción: {descripcion_propiedad[:120]}{'...' if len(descripcion_propiedad) > 120 else ''}")
                    print(f"  Calificación : {rating} / 5")
                    if comentario:
                        print(f"  Comentario: {comentario[:80]}{'...' if len(comentario) > 80 else ''}")
                    if warning:
                        print(f"  ⚠ Aviso   : {warning}")
                    print("=" * 50)
                    self._pausa_ui("exito", listado_largo=True)
                except Exception as e:
                    print(f"Error dejando reseña: {e}")
                    self._pausa_ui("error")
            elif accion == "ver_mis_reservas":
                print("\n--- Ver mis reservas (Postgres) ---")
                self._mostrar_reservas_huesped(active_session)
                self._pausa_ui("exito", listado_largo=True)
            elif accion == "ver_reservas_anfitrion":
                print("\n--- Ver reservas de mis propiedades (Postgres) ---")
                self._mostrar_reservas_anfitrion(active_session)
                self._pausa_ui("exito", listado_largo=True)
            elif accion == "confirmar_pago":
                print("\n--- Confirmar pago de una reserva (Postgres) ---")
                try:
                    usuario = self.postgres.get_user_by_email(active_session.get('email') if active_session else None)
                except Exception as e:
                    print(f"No se pudo resolver el anfitrión de la sesión activa: {e}")
                    continue
                anfitrion_id = usuario.get('id') if isinstance(usuario, dict) else None
                if anfitrion_id is None:
                    print("No se pudo resolver el anfitrión de la sesión activa.")
                    continue

                reservas_visibles = self._mostrar_reservas_menu(
                    solo_pendientes=True,
                    anfitrion_id=anfitrion_id,
                    titulo_personalizado="Reservas pendientes de mis propiedades:",
                )
                reserva_id = input("Reserva ID: ").strip()
                if not reserva_id:
                    print("Error: debe ingresar una reserva_id válida.")
                    continue
                if not reserva_id.isdigit():
                    print("Error: la reserva_id debe ser numérica.")
                    continue

                reserva_id_int = int(reserva_id)

                reservas_permitidas = {int(row[0]) for row in reservas_visibles if row and row[0] is not None}
                if reservas_permitidas and reserva_id_int not in reservas_permitidas:
                    print("Error: la reserva seleccionada no pertenece a propiedades del anfitrión logueado.")
                    continue

                try:
                    res = self.confirmar_pago(reserva_id_int)
                    if res.get("ok"):
                        print("\n" + "=" * 50)
                        print("✓ ¡Pago confirmado exitosamente!")
                        print("=" * 50)
                        print(f"  Reserva ID: {reserva_id}")
                        print(f"  Pagos actualizados: {res.get('pagos_actualizados', 0)}")
                        print(f"  Reservas actualizadas: {res.get('reservas_actualizadas', 0)}")
                        print("=" * 50)
                        self._pausa_ui("exito")
                    else:
                        print(f"Error confirmando pago: {res.get('mensaje', 'Error desconocido')}")
                        self._pausa_ui("error")
                except Exception as e:
                    print(f"Error confirmando pago: {e}")
                    self._pausa_ui("error")
            elif accion == "cancelar_reserva":
                print("\n--- Cancelar reserva (Postgres) ---")
                try:
                    usuario = self.postgres.get_user_by_email(active_session.get('email') if active_session else None)
                except Exception as e:
                    print(f"No se pudo resolver el anfitrión de la sesión activa: {e}")
                    continue
                anfitrion_id = usuario.get('id') if isinstance(usuario, dict) else None
                if anfitrion_id is None:
                    print("No se pudo resolver el anfitrión de la sesión activa.")
                    continue

                reservas_visibles = self._mostrar_reservas_menu(
                    solo_pendientes=False,
                    anfitrion_id=anfitrion_id,
                    titulo_personalizado="Reservas no canceladas de mis propiedades:",
                )
                reserva_id = input("Reserva ID: ").strip()
                if not reserva_id:
                    print("Error: debe ingresar una reserva_id válida.")
                    continue
                if not reserva_id.isdigit():
                    print("Error: la reserva_id debe ser numérica.")
                    continue

                reserva_id_int = int(reserva_id)

                reservas_permitidas = {int(row[0]) for row in reservas_visibles if row and row[0] is not None}
                if reservas_permitidas and reserva_id_int not in reservas_permitidas:
                    print("Error: la reserva seleccionada no pertenece a propiedades del anfitrión logueado.")
                    continue

                try:
                    res = self.cancelar_reserva(reserva_id_int)
                    if res.get("ok"):
                        print("\n" + "=" * 50)
                        print("✓ ¡Reserva cancelada exitosamente!")
                        print("=" * 50)
                        print(f"  Reserva ID: {reserva_id}")
                        print(f"  Reservas actualizadas: {res.get('reservas_actualizadas', 0)}")
                        print(f"  Pagos actualizados: {res.get('pagos_actualizados', 0)}")
                        print("=" * 50)
                        self._pausa_ui("exito")
                    else:
                        print(f"Error cancelando reserva: {res.get('mensaje', 'Error desconocido')}")
                        self._pausa_ui("error")
                except Exception as e:
                    print(f"Error cancelando reserva: {e}")
                    self._pausa_ui("error")
            else:
                print("\n⚠ Opción no válida para el tipo de usuario actual. Intente nuevamente.")
                self._pausa_ui("error")

    def clear_redis_keys(self, pattern):
        """Delete keys matching pattern from Redis. Returns message with number deleted."""
        if getattr(self.redis, 'client', None) is None:
            return "Redis no conectado."
        try:
            deleted = 0
            # scan_iter yields bytes or str depending on redis client
            for k in self.redis.client.scan_iter(match=pattern):
                try:
                    self.redis.client.delete(k)
                    deleted += 1
                except Exception:
                    pass
            return f"Se eliminaron {deleted} claves que coinciden con '{pattern}'."
        except Exception as e:
            return f"Error al limpiar cache Redis: {e}"

    def contar_reservas_ciudad(self, ciudad):
        try:
            resultado = self.postgres.count_reservations_by_city(ciudad)
            return f"{resultado} reservas encontradas en {ciudad}."
        except Exception as e:
            return f"Error al consultar Postgres: {e}"

    def contar_reservas_ciudad_ultimo_mes(self, ciudad):
        try:
            ciudad_normalizada = (ciudad or "").strip()
            if not ciudad_normalizada:
                return "Debe ingresar una ciudad válida."

            cache_key = f"reservas:last_month:v3:{ciudad_normalizada.casefold()}"
            cached_result = self._redis_get_text(cache_key)
            if cached_result is not None:
                return f"{cached_result} reservas encontradas en {ciudad_normalizada} durante el último mes. (cache Redis)"

            resultado = self.postgres.count_reservations_by_city_last_month(ciudad)
            if int(resultado) == 0 and getattr(self.mongo, 'collection', None) is not None:
                city_variants = self._variantes_ciudad(ciudad_normalizada)
                city_filters = [
                    {"ubicacion.ciudad": {"$regex": f"^{re.escape(item)}$", "$options": "i"}}
                    for item in city_variants
                ]

                property_ids = []
                if city_filters:
                    docs = self.mongo.collection.find({"$or": city_filters}, {"_id": 1})
                    property_ids = [str(doc.get("_id")) for doc in docs if doc.get("_id") is not None]

                if property_ids:
                    resultado = self.postgres.count_reservations_last_month_by_property_ids(property_ids)

            self._redis_set_text(cache_key, resultado, ttl_seconds=300)
            return f"{resultado} reservas encontradas en {ciudad} durante el último mes."
        except Exception as e:
            return f"Error al consultar Postgres: {e}"

    def alojamiento_mas_popular(self):
        try:
            cache_key = "popular_accommodations"
            cached = self._redis_get_json(cache_key)
            if cached is not None:
                resultados = cached
                source_note = " (cache Redis)"
            else:
                resultados = self.mongo.popular_accommodations()
                if resultados:
                    self._redis_set_json(cache_key, resultados, ttl_seconds=300)
                source_note = ""

            if not resultados:
                return "No hay datos en la colección 'propiedades'."

            texto_formateado = f"\nTipo de Alojamiento | Cantidad{source_note}\n------------------------------"
            for item in resultados:
                texto_formateado += f"\n{item['tipo_propiedad']:<20} | {item['total']}"
            return texto_formateado
        except Exception as e:
            return f"Error consultando mejores anfitriones: {e}"

    def propiedades_recientes(self, dias=30):
        try:
            resultados = self.mongo.recent_properties(days=dias)
            if not resultados:
                return f"No hay propiedades agregadas en los últimos {dias} días."
            return self._formatear_propiedades_mongo(resultados, titulo=f"PROPIEDADES AGREGADAS EN LOS ÚLTIMOS {dias} DÍAS")
        except Exception as e:
            return f"Error en MongoDB: {e}"

    def mejores_anfitriones(self, limite=5):
        try:
            # SQL es la fuente principal para este caso de uso (tabla resenia).
            resultados_sql = self.postgres.top_hosts_by_reviews(limit=limite)
            if resultados_sql:
                return self._formatear_top_hosts(resultados_sql)

            # Fallback a Mongo si SQL no tiene datos aún.
            cache_key = f"top_hosts:{int(limite)}"
            cached = self._redis_get_json(cache_key)
            if cached is not None:
                resultados = cached
                source_note = " (cache Redis)"
            else:
                resultados = self.mongo.top_hosts_by_rating(limit=limite)
                if resultados:
                    self._redis_set_json(cache_key, resultados, ttl_seconds=300)
                source_note = ""

            if not resultados:
                return "No hay anfitriones con reseñas suficientes."
            formatted = self._formatear_top_hosts(resultados)
            return formatted + ("\nNota: datos desde cache Redis" if source_note else "")
        except Exception as e:
            return f"Error en MongoDB: {e}"

    def areas_mas_demandadas_pais(self, pais, limite=10):
        try:
            resultados = self.postgres.most_demanded_areas_by_country(pais, limit=limite)
            if not resultados:
                return f"No se encontraron áreas demandadas en {pais}."
            return self._formatear_areas(resultados, pais)
        except Exception as e:
            return f"Error en Postgres: {e}"

    def propiedades_rating_alto_en_centro(self, min_rating=4.5, ciudad=None):
        try:
            resultados = self.mongo.properties_with_high_rating_in_center(min_rating=min_rating, ciudad=ciudad)
            city_display = ciudad or "centro"
            if not resultados:
                return f"No se encontraron propiedades en {city_display} con calificación mayor o igual a {min_rating}."
            return self._formatear_propiedades_mongo(
                resultados,
                titulo=f"PROPIEDADES EN {city_display.upper()} CON CALIFICACIÓN >= {min_rating}",
            )
        except Exception as e:
            return f"Error en MongoDB: {e}"

    def propiedades_rating_alto_anywhere(self, min_rating=4.5, limit=50):
        try:
            resultados = self.mongo.properties_with_high_rating_anywhere(min_rating=min_rating, limit=limit)
            if not resultados:
                return f"No se encontraron propiedades con calificación mayor o igual a {min_rating} en el catálogo."
            return self._formatear_propiedades_mongo(
                resultados,
                titulo=f"PROPIEDADES EN TODO EL CATÁLOGO CON CALIFICACIÓN >= {min_rating}",
            )
        except Exception as e:
            return f"Error en MongoDB: {e}"

    def propiedades_mas_resenadas_o_zona_turistica(self, min_reviews=20):
        try:
            resultados = self.mongo.properties_with_many_reviews_or_touristic_zone(min_reviews=min_reviews)
            if not resultados:
                return f"No se encontraron propiedades con más de {min_reviews} reseñas o en zona turística."
            return self._formatear_propiedades_mongo(
                resultados,
                titulo=f"PROPIEDADES CON MÁS DE {min_reviews} RESEÑAS O EN ZONA TURÍSTICA",
            )
        except Exception as e:
            return f"Error en MongoDB: {e}"

    def resumen_reseñas_propiedades(self, limite=10):
        try:
            resultados = self.mongo.property_review_summary(limit=limite)
            if not resultados:
                return "No se encontraron reseñas para resumir."
            return self._formatear_resumen_reseñas(resultados)
        except Exception as e:
            return f"Error en MongoDB: {e}"

    def reseñas_recientes_visibles(self, limite=10):
        try:
            resultados = self.mongo.recent_visible_reviews(limit=limite)
            if not resultados:
                return "No se encontraron reseñas visibles recientes."
            return self._formatear_reseñas_recientes(resultados)
        except Exception as e:
            return f"Error en MongoDB: {e}"

    def resumen_pagos_transacciones(self, ciudad=None):
        try:
            resumen = self.postgres.payment_summary_last_month(city=ciudad)
            return self._formatear_pagos(resumen, ciudad=ciudad)
        except Exception as e:
            return f"Error al consultar pagos/transacciones: {e}"

    def buscar_propiedades_calificacion(self, tipo_propiedad, min_rating):
        try:
            resultados = self.mongo.find_properties_by_type_and_rating(tipo_propiedad, min_rating)
            if not resultados:
                return f"No se encontraron propiedades de tipo '{tipo_propiedad}' con más de {min_rating}."
            return self._formatear_propiedades_mongo(resultados)
        except Exception as e:
            return f"Error en MongoDB: {e}"

    def _formatear_propiedades_mongo(self, propiedades, titulo="RESULTADOS ENCONTRADOS"):
        bloques = [f"\n{titulo}", "=" * 72]

        for indice, propiedad in enumerate(propiedades, start=1):
            ubicacion = propiedad.get("ubicacion", {}) or {}
            anfitrion = propiedad.get("metadata_anfitrion", {}) or {}
            servicios = propiedad.get("servicios", []) or []
            rating = propiedad.get("mejor_calificacion", propiedad.get("calificacion_promedio"))
            resenas = propiedad.get("reseñas_que_cumplen", []) or []
            if isinstance(resenas, dict):
                resenas = [resenas]
            moneda = propiedad.get("id_moneda", propiedad.get("moneda", ""))
            tipo_propiedad = propiedad.get("tipo_propiedad", propiedad.get("categoria", "Sin tipo"))
            anfitrion_nombre = anfitrion.get("nombre") or f"Anfitrión #{propiedad.get('anfitrion_id', 'N/D')}"
            anfitrion_id = anfitrion.get("anfitrion_id", propiedad.get("anfitrion_id", "N/D"))

            bloques.append(f"{indice}. {propiedad.get('titulo', 'Sin título')}")
            bloques.append(f"   Tipo: {tipo_propiedad}")
            bloques.append(
                f"   Ubicación: {ubicacion.get('ciudad', 'N/D')}, {ubicacion.get('provincia', 'N/D')}, {ubicacion.get('pais', 'N/D')}"
            )
            bloques.append(f"   Precio por noche: {propiedad.get('precio_por_noche', 'N/D')} {moneda}".rstrip())
            bloques.append(f"   Anfitrión: {anfitrion_nombre} (ID {anfitrion_id})")
            bloques.append(f"   Mejor calificación: {rating if rating is not None else 'N/D'}")
            bloques.append(f"   Servicios: {', '.join(servicios) if servicios else 'N/D'}")

            if resenas:
                reseña = resenas[0]
                comentario = reseña.get("comentario", "Sin comentario")
                bloques.append(
                    f"   Reseña destacada: {reseña.get('nombre_usuario', 'N/D')} ({reseña.get('calificacion', 'N/D')}/5) - {comentario}"
                )

            bloques.append("-" * 72)

        return "\n".join(bloques)

    def _formatear_top_hosts(self, hosts):
        bloques = ["\nMEJORES ANFITRIONES", "=" * 72]
        for indice, host in enumerate(hosts, start=1):
            promedio = host.get('promedio_calificacion', 'N/D')
            cantidad_propiedades = host.get('cantidad_propiedades', host.get('cantidad_resenas', 0))
            bloques.append(
                f"{indice}. {host.get('nombre', 'N/D')} | ID {host.get('_id', 'N/D')} | Promedio {promedio:.2f} | Props {cantidad_propiedades}"
                if isinstance(promedio, (int, float))
                else f"{indice}. {host.get('nombre', 'N/D')} | ID {host.get('_id', 'N/D')} | Promedio {promedio} | Props {cantidad_propiedades}"
            )
            bloques.append("-" * 72)
        return "\n".join(bloques)

    def _formatear_areas(self, areas, pais):
        bloques = [f"\nBARRIOS MÁS DEMANDADOS EN {pais.upper()}", "=" * 72]
        for indice, area in enumerate(areas, start=1):
            barrio = area.get("barrio") or area.get("ciudad") or "N/D"
            bloques.append(f"{indice}. {barrio} | {area.get('total', 0)} reservas")
            bloques.append("-" * 72)
        return "\n".join(bloques)

    def _formatear_resumen_reseñas(self, resumenes):
        bloques = ["\nRESUMEN DE RESEÑAS POR PROPIEDAD", "=" * 72]
        for indice, item in enumerate(resumenes, start=1):
            bloques.append(f"{indice}. {item.get('titulo', 'Sin título')}")
            bloques.append(f"   Tipo: {item.get('tipo_propiedad', 'N/D')} | Ciudad: {item.get('ciudad', 'N/D')}")
            bloques.append(f"   Anfitrión: {item.get('anfitrion', 'N/D')}")
            promedio = item.get('promedio_calificacion', 'N/D')
            if isinstance(promedio, (int, float)):
                bloques.append(f"   Promedio: {promedio:.2f}/5 | Reseñas: {item.get('cantidad_resenas', 0)}")
            else:
                bloques.append(f"   Promedio: {promedio} | Reseñas: {item.get('cantidad_resenas', 0)}")

            mejor = item.get('mejor_resena', {}) or {}
            if mejor:
                bloques.append(
                    f"   Reseña destacada: {mejor.get('nombre_usuario', 'N/D')} ({mejor.get('calificacion', 'N/D')}/5) - {mejor.get('comentario', 'Sin comentario')}"
                )
            bloques.append("-" * 72)
        return "\n".join(bloques)

    def _formatear_reseñas_recientes(self, reseñas):
        bloques = ["\nRESEÑAS RECIENTES VISIBLES", "=" * 72]
        for indice, item in enumerate(reseñas, start=1):
            resena = item.get("resena", {}) or {}
            bloques.append(f"{indice}. {item.get('titulo', 'Sin título')} | {item.get('tipo_propiedad', 'N/D')} | {item.get('ciudad', 'N/D')}")
            bloques.append(f"   Anfitrión: {item.get('anfitrion', 'N/D')}")
            bloques.append(
                f"   Usuario: {resena.get('nombre_usuario', 'N/D')} ({resena.get('calificacion', 'N/D')}/5) | Fecha: {resena.get('fecha', 'N/D')}"
            )
            bloques.append(f"   Comentario: {resena.get('comentario', 'Sin comentario')}")
            bloques.append("-" * 72)
        return "\n".join(bloques)

    def _formatear_pagos(self, resumen, ciudad=None):
        bloques = ["\nPAGOS Y TRANSACCIONES", "=" * 72]
        if ciudad:
            bloques.append(f"Ciudad filtrada: {ciudad}")
        bloques.append(f"Tabla analizada: {resumen.get('tabla', 'N/D')}")
        bloques.append(f"Total de registros: {resumen.get('total_registros', 0)}")
        bloques.append(f"Monto total: {resumen.get('monto_total', 0)}")
        bloques.append(f"Estado dominante: {resumen.get('estado_dominante', 'N/D')}")
        bloques.append(f"Método dominante: {resumen.get('metodo_dominante', 'N/D')}")
        if resumen.get("mensaje"):
            bloques.append(f"Nota: {resumen['mensaje']}")
        bloques.append("-" * 72)
        return "\n".join(bloques)


    def registrar_evento_telemetria(self, user_id, event_type, property_id=None, payload=None):
        try:
            self.cassandra.register_event(user_id, event_type, property_id=property_id, payload=payload)
            return "Evento de telemetría registrado correctamente."
        except Exception as e:
            return f"Error en Cassandra: {e}"

    def obtener_eventos_usuario(self, user_id, limit=10):
        try:
            eventos = self.cassandra.get_user_events(user_id, limit=limit)
            if not eventos:
                return f"No hay eventos para el usuario {user_id}."
            return eventos
        except Exception as e:
            return f"Error en Cassandra: {e}"

    def cerrar_conexiones(self):
        self.postgres.close()
        self.mongo.close()
        self.cassandra.close()
        print("\nTodas las conexiones fueron cerradas de forma segura.")


def menu():
    orch = AirbnbOrchestrator()
    active_session = None

    while True:
        if active_session and not orch._sesion_activa_en_redis(active_session):
            print("\nTu sesión expiró. Iniciá sesión nuevamente.")
            active_session = None

        if not active_session:
            print("\n" + "=" * 30)
            print("=== AIRBNB POLÍGLOTA CLI ===")
            print("=" * 30)
            print("1. Sign up")
            print("2. Log in")
            print("0. Salir")

            opc = input("\nSeleccione una opción: ").strip()

            if opc == "1":
                print("\n--- Registro de usuario ---")
                nombre = input("Ingrese su nombre completo: ").strip()
                email = input("Ingrese su email: ").strip()
                password = getpass.getpass("Ingrese su contraseña: ").strip()
                tipo = orch._seleccionar_tipo_usuario_cli()
                print(orch.signup_usuario(nombre, email, password, user_type=tipo))
                continue

            elif opc == "2":
                print("\n--- Inicio de sesión ---")
                email = input("Ingrese su email: ").strip()
                password = getpass.getpass("Ingrese su contraseña: ").strip()
                trusted = ""
                while trusted not in {"0", "1"}:
                    trusted = input("¿El dispositivo es de confianza? (1=sí, 0=no): ").strip()
                    if trusted not in {"0", "1"}:
                        print("⚠ Opción inválida. Ingrese solo 1 o 0.")
                trusted_device = trusted == "1"
                session, error = orch.login_usuario(email, password, trusted_device)
                if error:
                    print(error)
                    orch._pausa_ui("error")
                    continue
                active_session = session
                orch.registrar_login_evento(email, trusted_device)
                tipo_sesion = orch._tipo_usuario_sesion(active_session)
                print(f"\nBienvenido/a {session.get('nombre_completo', 'Usuario')} <{session.get('email', email)}> | Tipo: {tipo_sesion}.")
                print(f"Dispositivo de confianza: {'Sí' if trusted_device else 'No'}")
                orch._pausa_ui("exito")
                continue

            elif opc == "0":
                print("\nSaliendo del sistema...")
                orch.cerrar_conexiones()
                break

            else:
                print("\n⚠ Opción no válida. Intente nuevamente.")
                orch._pausa_ui("error")
                continue

        print("\n" + "=" * 30)
        print("=== AIRBNB POLÍGLOTA CLI ===")
        print("=" * 30)
        tipo_sesion = orch._tipo_usuario_sesion(active_session)
        print(f"Sesión activa: {active_session.get('nombre_completo', 'Usuario')} <{active_session.get('email', 'N/D')}> | Tipo: {tipo_sesion} | Confiable: {'Sí' if active_session.get('trusted_device') else 'No'}")

        if tipo_sesion == "admin":
            print("1. Casos de uso implementados (modo admin)")
            print("2. Analítica rápida (reservas por ciudad / propiedades populares)")
            print("0. Cerrar sesión y salir del sistema")
        else:
            print("1. Requerimientos del sistema")
            print("0. Cerrar sesión y salir del sistema")

        opc = input("\nSeleccione una opción: ").strip()

        if active_session and not orch._sesion_activa_en_redis(active_session):
            print("\nTu sesión expiró. Iniciá sesión nuevamente.")
            active_session = None
            continue

        if opc == "1" and tipo_sesion == "admin":
            result = orch.menu_casos_de_uso(active_session=active_session)
            if result == "logout":
                active_session = None
                print("\nApagando orquestador...")
                orch.cerrar_conexiones()
                break
        elif opc == "2" and tipo_sesion == "admin":
            print("\n--- Analítica rápida (reservas por ciudad / propiedades populares) ---")
            try:
                ciudades_disponibles = orch.postgres.list_available_cities() if hasattr(orch.postgres, 'list_available_cities') else []
            except Exception:
                ciudades_disponibles = []

            if ciudades_disponibles:
                print("Ciudades disponibles para filtrar:")
                for idx, ciudad_item in enumerate(ciudades_disponibles, start=1):
                    print(f"  {idx}. {ciudad_item}")

            ciudad = None
            if ciudades_disponibles:
                while True:
                    entrada_ciudad = input("Ingrese el número de la ciudad para analítica (opcional, Enter para omitir): ").strip()
                    if not entrada_ciudad:
                        break
                    if not entrada_ciudad.isdigit():
                        print("Opción inválida. Debe ingresar un número de la lista.")
                        continue

                    indice_ciudad = int(entrada_ciudad)
                    if 1 <= indice_ciudad <= len(ciudades_disponibles):
                        ciudad = ciudades_disponibles[indice_ciudad - 1]
                        break

                    print("Índice de ciudad fuera de rango. Intente nuevamente.")
            else:
                ciudad = input("Ingrese la ciudad a filtrar para analítica (opcional, Enter para omitir): ").strip() or None

            if ciudad:
                try:
                    if ciudades_disponibles:
                        print(f"Filtrando por ciudad: {ciudad}")
                    print(orch.contar_reservas_ciudad(ciudad))
                except Exception as e:
                    print(f"Error analítica: {e}")
            print("\nPropiedades populares (cache/aggregation):")
            try:
                print(orch.alojamiento_mas_popular())
            except Exception as e:
                print(f"Error analítica: {e}")
        elif opc == "1" and tipo_sesion != "admin":
            result = orch.menu_requerimientos(active_session=active_session)
            if result == "logout":
                active_session = None
                continue
        elif opc == "0":
            print(orch.logout_usuario(active_session.get('email'), session_token=active_session.get('session_token')))
            orch._pausa_ui("exito")
            active_session = None
            print("\nApagando orquestador...")
            orch.cerrar_conexiones()
            break
        else:
            print("\n⚠ Opción no válida. Intente nuevamente.")
            orch._pausa_ui("error")


if __name__ == "__main__":
    menu()