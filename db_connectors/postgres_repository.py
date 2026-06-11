import os

import psycopg2
from psycopg2 import Error as Psycopg2Error


class PostgresRepository:
    def __init__(self, dsn=None):
        self.dsn = dsn or os.getenv("POSTGRES_URI")
        self.connection = None
        self.cursor = None
        self.connect()

    def connect(self):
        if not self.dsn:
            return False

        try:
            self.connection = psycopg2.connect(
                self.dsn,
                sslmode="require",
                connect_timeout=10,
            )
            self.cursor = self.connection.cursor()
            return True
        except KeyboardInterrupt:
            self.connection = None
            self.cursor = None
            return False
        except Exception:
            self.connection = None
            self.cursor = None
            return False

    def _table_has_columns(self, table_name, required_columns):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        self.cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s;
            """,
            (table_name,),
        )
        existing_columns = {row[0] for row in self.cursor.fetchall()}
        return all(column_name in existing_columns for column_name in required_columns)

    def _table_columns(self, table_name):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        self.cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s;
            """,
            (table_name,),
        )
        return {row[0] for row in self.cursor.fetchall()}

    def _column_metadata(self, table_name, column_name):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        self.cursor.execute(
            """
            SELECT data_type, udt_name, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s AND column_name = %s
            LIMIT 1;
            """,
            (table_name, column_name),
        )
        row = self.cursor.fetchone()
        if not row:
            return None
        return {
            "data_type": row[0],
            "udt_name": row[1],
            "is_nullable": row[2] == "YES",
        }

    def _first_existing_table(self, candidates):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        for table_name in candidates:
            try:
                self.cursor.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = %s LIMIT 1;",
                    (table_name,),
                )
                if self.cursor.fetchone():
                    return table_name
            except Exception:
                continue
        return None

    def _lookup_id_by_nombre(self, table_name, nombre):
        if not nombre:
            return None
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        columns = self._table_columns(table_name)
        if "id" not in columns or "nombre" not in columns:
            return None

        self.cursor.execute(
            f"SELECT id FROM {table_name} WHERE LOWER(nombre) = LOWER(%s) LIMIT 1;",
            (str(nombre).strip(),),
        )
        row = self.cursor.fetchone()
        return row[0] if row else None

    def _split_full_name(self, full_name):
        parts = [part for part in (full_name or "").strip().split() if part]
        if not parts:
            return "", ""
        if len(parts) == 1:
            return parts[0], ""
        return parts[0], " ".join(parts[1:])

    def _normalize_user_type(self, user_type):
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

    def actualizar_tipo_usuario_por_email(self, email, user_type):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        email_normalizado = (email or "").strip().casefold()
        if not email_normalizado:
            return {"updated": 0}

        if not self._table_has_columns("usuario", ["tipo", "email"]):
            return {"updated": 0}

        tipo_normalizado = self._normalize_user_type(user_type)

        try:
            self.cursor.execute(
                """
                UPDATE usuario
                SET tipo = %s
                WHERE LOWER(TRIM(COALESCE(email, ''))) = %s
                  AND LOWER(TRIM(COALESCE(tipo, ''))) <> %s;
                """,
                (tipo_normalizado, email_normalizado, tipo_normalizado),
            )
            updated = self.cursor.rowcount or 0
            self.connection.commit()
            return {"updated": int(updated)}
        except Psycopg2Error:
            self.connection.rollback()
            raise
        except Exception:
            self.connection.rollback()
            raise

    def _sync_identity_sequence(self, table_name, id_column="id"):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        self.cursor.execute(
            "SELECT pg_get_serial_sequence(%s, %s);",
            (table_name, id_column),
        )
        sequence_row = self.cursor.fetchone()
        sequence_name = sequence_row[0] if sequence_row else None
        if not sequence_name:
            return

        self.cursor.execute(f'SELECT COALESCE(MAX({id_column}), 0) FROM {table_name};')
        max_row = self.cursor.fetchone()
        max_id = (max_row[0] if max_row else 0) or 0
        next_value = max_id if max_id > 0 else 1
        self.cursor.execute(
            "SELECT setval(%s, %s, %s);",
            (sequence_name, next_value, max_id > 0),
        )

    def _build_property_field_map(self, property_doc, anfitrion_id):
        normalized = dict(property_doc or {})
        normalized.setdefault("anfitrion_id", anfitrion_id)

        ubicacion = normalized.get("ubicacion") if isinstance(normalized.get("ubicacion"), dict) else {}
        coordinates = ubicacion.get("coordenadas") if isinstance(ubicacion.get("coordenadas"), dict) else {}
        fotos = normalized.get("fotos")
        servicios = normalized.get("servicios")

        coords = coordinates.get("coordinates") if isinstance(coordinates.get("coordinates"), list) else []

        flat = {
            "anfitrion_id": normalized.get("anfitrion_id"),
            "titulo": normalized.get("titulo"),
            "descripcion": normalized.get("descripcion"),
            "precio_por_noche": normalized.get("precio_por_noche"),
            "moneda": normalized.get("moneda"),
            "huespedes_max": normalized.get("huespedes_max"),
            "cant_habitaciones": normalized.get("cant_habitaciones"),
            "cant_banios": normalized.get("cant_banios"),
            "calificacion_promedio": normalized.get("calificacion_promedio"),
            "activa": normalized.get("activa"),
            "tipo_propiedad": normalized.get("tipo_propiedad"),
            "ciudad": ubicacion.get("ciudad"),
            "pais": ubicacion.get("pais"),
            "provincia": ubicacion.get("provincia"),
            "calle": ubicacion.get("calle"),
            "numero": ubicacion.get("numero"),
            "codigo_postal": ubicacion.get("codigo_postal"),
            "latitud": ubicacion.get("latitud") if ubicacion.get("latitud") is not None else (coords[1] if len(coords) >= 2 else None),
            "longitud": ubicacion.get("longitud") if ubicacion.get("longitud") is not None else (coords[0] if len(coords) >= 2 else None),
        }

        if servicios is not None:
            flat["servicios"] = servicios if isinstance(servicios, str) else ", ".join(str(item) for item in servicios)
        if fotos is not None:
            flat["fotos"] = fotos

        return normalized, flat

    def _ensure_tipo_propiedad_id(self, tipo_propiedad):
        if not tipo_propiedad:
            return None

        table = self._first_existing_table(["tipo_propiedad", "tipos_propiedad"])
        if table is None:
            return None

        columns = self._table_columns(table)
        if "id" not in columns or "nombre" not in columns:
            return None

        self.cursor.execute(
            f"SELECT id FROM {table} WHERE LOWER(nombre) = LOWER(%s) LIMIT 1;",
            (str(tipo_propiedad),),
        )
        found = self.cursor.fetchone()
        if found:
            return found[0]

        insert_columns = ["nombre"]
        insert_values = [str(tipo_propiedad)]
        if "descripcion" in columns:
            insert_columns.append("descripcion")
            insert_values.append(f"Tipo de propiedad {tipo_propiedad}")

        placeholders = ", ".join(["%s"] * len(insert_values))
        query = f"INSERT INTO {table} ({', '.join(insert_columns)}) VALUES ({placeholders}) RETURNING id;"
        self.cursor.execute(query, tuple(insert_values))
        return self.cursor.fetchone()[0]

    def listar_tipos_propiedad(self):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        table = self._first_existing_table(["tipo_propiedad", "tipos_propiedad"])
        if table is None:
            return []

        columns = self._table_columns(table)
        if "nombre" not in columns:
            return []

        id_expr = "id" if "id" in columns else "NULL"
        self.cursor.execute(f"SELECT {id_expr} AS id, nombre FROM {table} ORDER BY nombre ASC;")
        rows = self.cursor.fetchall() or []
        return [{"id": row[0], "nombre": row[1]} for row in rows if row and row[1]]

    def registrar_tipo_propiedad_si_no_existe(self, tipo_propiedad):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        normalized = (tipo_propiedad or "").strip()
        if not normalized:
            return {"created": False, "id": None, "nombre": None, "table": None}

        table = self._first_existing_table(["tipo_propiedad", "tipos_propiedad"])
        if table is None:
            return {"created": False, "id": None, "nombre": normalized, "table": None, "warning": "No existe tabla de catálogo de tipo_propiedad."}

        try:
            existing_id = self._lookup_id_by_nombre(table, normalized)
            if existing_id is not None:
                self.connection.commit()
                return {"created": False, "id": existing_id, "nombre": normalized, "table": table}

            inserted_id = self._ensure_tipo_propiedad_id(normalized)
            self.connection.commit()
            return {"created": True, "id": inserted_id, "nombre": normalized, "table": table}
        except Psycopg2Error:
            self.connection.rollback()
            raise
        except Exception:
            self.connection.rollback()
            raise

    def _insert_ubicacion(self, propiedad_id, property_doc):
        table = self._first_existing_table(["ubicacion"])
        if table is None:
            return None

        columns = self._table_columns(table)
        if "propiedad_id" not in columns:
            return None

        ubicacion = (property_doc or {}).get("ubicacion") if isinstance((property_doc or {}).get("ubicacion"), dict) else {}
        coordinates = ubicacion.get("coordenadas") if isinstance(ubicacion.get("coordenadas"), dict) else {}
        coords = coordinates.get("coordinates") if isinstance(coordinates.get("coordinates"), list) else []

        candidate_values = {
            "propiedad_id": propiedad_id,
            "calle": ubicacion.get("calle"),
            "numero": ubicacion.get("numero"),
            "ciudad": ubicacion.get("ciudad"),
            "provincia": ubicacion.get("provincia"),
            "pais": ubicacion.get("pais"),
            "codigo_postal": ubicacion.get("codigo_postal"),
            "latitud": ubicacion.get("latitud") if ubicacion.get("latitud") is not None else (coords[1] if len(coords) >= 2 else None),
            "longitud": ubicacion.get("longitud") if ubicacion.get("longitud") is not None else (coords[0] if len(coords) >= 2 else None),
        }

        cols = [column for column in candidate_values if column in columns and candidate_values[column] is not None]
        if not cols:
            return None

        vals = [candidate_values[column] for column in cols]
        query = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(vals))}) RETURNING id;"
        self.cursor.execute(query, tuple(vals))
        return self.cursor.fetchone()[0] if "id" in columns else None

    def asegurar_ubicacion(self, ciudad, pais, propiedad_id=None, property_doc=None):
        """Garantiza que exista al menos un registro de ubicación para ciudad/pais en Postgres."""
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        table = self._first_existing_table(["ubicacion"])
        if table is None:
            raise RuntimeError("No se encontró la tabla ubicacion en Postgres.")

        ciudad_norm = (ciudad or "").strip()
        pais_norm = (pais or "").strip()
        if not ciudad_norm or not pais_norm:
            raise ValueError("Ciudad y país son obligatorios para asegurar ubicación.")

        columns = self._table_columns(table)
        if "ciudad" not in columns or "pais" not in columns:
            raise RuntimeError("La tabla ubicacion no contiene columnas ciudad/pais.")

        try:
            if "id" in columns:
                self.cursor.execute(
                    f"""
                    SELECT id
                    FROM {table}
                    WHERE LOWER(TRIM(ciudad)) = LOWER(TRIM(%s))
                      AND LOWER(TRIM(pais)) = LOWER(TRIM(%s))
                    LIMIT 1;
                    """,
                    (ciudad_norm, pais_norm),
                )
                existing = self.cursor.fetchone()
                if existing:
                    return {"created": False, "id": existing[0], "table": table, "ciudad": ciudad_norm, "pais": pais_norm}
            else:
                self.cursor.execute(
                    f"""
                    SELECT 1
                    FROM {table}
                    WHERE LOWER(TRIM(ciudad)) = LOWER(TRIM(%s))
                      AND LOWER(TRIM(pais)) = LOWER(TRIM(%s))
                    LIMIT 1;
                    """,
                    (ciudad_norm, pais_norm),
                )
                if self.cursor.fetchone():
                    return {"created": False, "id": None, "table": table, "ciudad": ciudad_norm, "pais": pais_norm}

            ubicacion = (property_doc or {}).get("ubicacion") if isinstance((property_doc or {}).get("ubicacion"), dict) else {}
            coordinates = ubicacion.get("coordenadas") if isinstance(ubicacion.get("coordenadas"), dict) else {}
            coords = coordinates.get("coordinates") if isinstance(coordinates.get("coordinates"), list) else []

            candidate_values = {
                "ciudad": ciudad_norm,
                "pais": pais_norm,
                "propiedad_id": propiedad_id,
                "provincia": ubicacion.get("provincia"),
                "calle": ubicacion.get("calle"),
                "numero": ubicacion.get("numero"),
                "codigo_postal": ubicacion.get("codigo_postal"),
                "latitud": ubicacion.get("latitud") if ubicacion.get("latitud") is not None else (coords[1] if len(coords) >= 2 else None),
                "longitud": ubicacion.get("longitud") if ubicacion.get("longitud") is not None else (coords[0] if len(coords) >= 2 else None),
            }

            insert_columns = []
            insert_values = []
            for column_name, value in candidate_values.items():
                if column_name not in columns or value is None:
                    continue
                insert_columns.append(column_name)
                insert_values.append(value)

            # Si propiedad_id existe y es NOT NULL, se vuelve obligatorio al insertar.
            if "propiedad_id" in columns and propiedad_id is None:
                metadata = self._column_metadata(table, "propiedad_id")
                if metadata and not metadata.get("is_nullable"):
                    raise RuntimeError("ubicacion.propiedad_id es obligatorio y no se recibió un propiedad_id válido.")

            if not insert_columns:
                raise RuntimeError("No se encontraron columnas válidas para insertar ubicación en Postgres.")

            placeholders = ", ".join(["%s"] * len(insert_values))
            returning = " RETURNING id" if "id" in columns else ""
            query = f"INSERT INTO {table} ({', '.join(insert_columns)}) VALUES ({placeholders}){returning};"

            if "id" in columns:
                try:
                    self._sync_identity_sequence(table)
                except Exception:
                    pass

            self.cursor.execute(query, tuple(insert_values))
            inserted_id = self.cursor.fetchone()[0] if "id" in columns else None
            self.connection.commit()
            return {"created": True, "id": inserted_id, "table": table, "ciudad": ciudad_norm, "pais": pais_norm}
        except Psycopg2Error:
            self.connection.rollback()
            raise
        except Exception:
            self.connection.rollback()
            raise

    def _split_full_name(self, full_name):
        parts = [part for part in (full_name or "").strip().split() if part]
        if not parts:
            return "", ""
        if len(parts) == 1:
            return parts[0], ""
        return parts[0], " ".join(parts[1:])

    def _sync_identity_sequence(self, table_name, id_column="id"):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        self.cursor.execute(
            "SELECT pg_get_serial_sequence(%s, %s);",
            (table_name, id_column),
        )
        sequence_name = self.cursor.fetchone()[0]
        if not sequence_name:
            return

        self.cursor.execute(f'SELECT COALESCE(MAX({id_column}), 0) FROM {table_name};')
        max_id = self.cursor.fetchone()[0] or 0
        next_value = max_id if max_id > 0 else 1
        self.cursor.execute(
            "SELECT setval(%s, %s, %s);",
            (sequence_name, next_value, max_id > 0),
        )

    def register_user(self, nombre_completo, email, user_type="ambos", activo=True):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        if not self._table_has_columns("usuario", ["nombre", "apellido", "email"]):
            raise RuntimeError("La tabla usuario no tiene la estructura mínima esperada.")

        nombre, apellido = self._split_full_name(nombre_completo)
        user_type = self._normalize_user_type(user_type)

        try:
            self.cursor.execute(
                """
                SELECT id
                FROM usuario
                WHERE email = %s
                LIMIT 1;
                """,
                (email,),
            )
            existing = self.cursor.fetchone()
            if existing:
                return {"created": False, "id": existing[0], "mensaje": "El usuario ya existía en Postgres."}

            columns = ["nombre", "apellido", "email"]
            values = [nombre, apellido, email]

            if self._table_has_columns("usuario", ["fecha_alta"]):
                columns.append("fecha_alta")
                values.append("CURRENT_TIMESTAMP")
            if self._table_has_columns("usuario", ["tipo"]):
                columns.append("tipo")
                values.append(user_type)
            if self._table_has_columns("usuario", ["activo"]):
                columns.append("activo")
                values.append(bool(activo))

            sql_columns = ", ".join(columns)
            placeholders = []
            query_params = []
            for value in values:
                if value == "CURRENT_TIMESTAMP":
                    placeholders.append(value)
                else:
                    placeholders.append("%s")
                    query_params.append(value)
            returning = " RETURNING id" if self._table_has_columns("usuario", ["id"]) else ""

            query = f"""
                INSERT INTO usuario ({sql_columns})
                VALUES ({', '.join(placeholders)}){returning};
            """

            try:
                self._sync_identity_sequence("usuario")
                self.cursor.execute(query, tuple(query_params))
                inserted_id = self.cursor.fetchone()[0] if returning else None
            except Psycopg2Error as exc:
                if getattr(exc, "pgcode", None) != "23505":
                    raise
                self.connection.rollback()
                self._sync_identity_sequence("usuario")
                self.cursor.execute(query, tuple(query_params))
                inserted_id = self.cursor.fetchone()[0] if returning else None
            self.connection.commit()
            return {"created": True, "id": inserted_id, "mensaje": "Usuario registrado en Postgres."}
        except Psycopg2Error:
            self.connection.rollback()
            raise
        except Exception:
            self.connection.rollback()
            raise

    def get_user_by_email(self, email):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        try:
            self.cursor.execute(
                """
                SELECT *
                FROM usuario
                WHERE email = %s
                LIMIT 1;
                """,
                ((email or "").strip(),),
            )
            row = self.cursor.fetchone()
            if not row:
                return None

            columns = [desc[0] for desc in self.cursor.description]
            return dict(zip(columns, row))
        except Psycopg2Error:
            self.connection.rollback()
            raise
        except Exception:
            self.connection.rollback()
            raise

    def migrar_tipos_usuario_legacy(self, from_tipo="ambos", to_tipo="huesped"):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        normalized_from = (from_tipo or "").strip().casefold()
        normalized_to = (to_tipo or "").strip().casefold()
        if not normalized_from or not normalized_to:
            return {"updated": 0}

        if not self._table_has_columns("usuario", ["tipo"]):
            return {"updated": 0}

        try:
            self.cursor.execute(
                """
                UPDATE usuario
                SET tipo = %s
                WHERE LOWER(TRIM(COALESCE(tipo, ''))) = %s;
                """,
                (normalized_to, normalized_from),
            )
            updated = self.cursor.rowcount or 0
            self.connection.commit()
            return {"updated": int(updated)}
        except Psycopg2Error:
            self.connection.rollback()
            raise
        except Exception:
            self.connection.rollback()
            raise

    def asegurar_rol_admin_en_usuario(self):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        if not self._table_has_columns("usuario", ["tipo"]):
            return {"updated": False, "reason": "usuario.tipo no existe"}

        try:
            self.cursor.execute(
                """
                SELECT pg_get_constraintdef(pg_constraint.oid)
                FROM pg_constraint
                WHERE conrelid = 'usuario'::regclass
                  AND contype = 'c'
                  AND conname = 'usuario_tipo_check'
                LIMIT 1;
                """
            )
            row = self.cursor.fetchone()
            definition = (row[0] if row else "") or ""
            if "admin" in definition.casefold():
                self.connection.commit()
                return {"updated": False, "reason": "constraint ya permite admin"}

            self.cursor.execute(
                """
                UPDATE usuario
                SET tipo = 'huesped'
                WHERE LOWER(TRIM(COALESCE(tipo, ''))) = 'ambos';
                """
            )
            migrated = self.cursor.rowcount or 0

            self.cursor.execute("ALTER TABLE usuario DROP CONSTRAINT IF EXISTS usuario_tipo_check;")
            self.cursor.execute(
                """
                ALTER TABLE usuario
                ADD CONSTRAINT usuario_tipo_check
                CHECK (LOWER(TRIM(COALESCE(tipo, ''))) IN ('huesped', 'anfitrion', 'admin'));
                """
            )
            self.connection.commit()
            return {"updated": True, "migrated_ambos": int(migrated)}
        except Psycopg2Error:
            self.connection.rollback()
            raise
        except Exception:
            self.connection.rollback()
            raise

    def create_reservation(self, user_id, property_id, start_date, end_date, status='confirmada', amount=None, method=None):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")
        # Try common table names
        candidates = ['reserva', 'reservas']
        chosen_table = None
        for t in candidates:
            try:
                self.cursor.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = %s LIMIT 1;",
                    (t,)
                )
                if self.cursor.fetchone():
                    chosen_table = t
                    break
            except Exception:
                continue

        if chosen_table is None:
            raise RuntimeError('No se encontró tabla de reservas en Postgres.')

        try:
            self._sync_identity_sequence(chosen_table)
        except Exception:
            pass

        cols = []
        vals = []
        if self._table_has_columns(chosen_table, ['usuario_id']):
            cols.append('usuario_id'); vals.append(user_id)
        if self._table_has_columns(chosen_table, ['propiedad_id']):
            cols.append('propiedad_id'); vals.append(property_id)
        date_cols = ['fecha_inicio', 'start_date', 'fecha_reserva', 'check_in']
        for dc in date_cols:
            if self._table_has_columns(chosen_table, [dc]):
                cols.append(dc); vals.append(start_date); break
        end_cols = ['fecha_fin', 'end_date', 'check_out', 'fecha_salida']
        for ec in end_cols:
            if self._table_has_columns(chosen_table, [ec]):
                cols.append(ec); vals.append(end_date); break
        if self._table_has_columns(chosen_table, ['estado_id']):
            cols.append('estado_id'); vals.append(1)
        for ac in ['monto_total', 'monto', 'total_pagado', 'monto_pagado']:
            if amount is not None and self._table_has_columns(chosen_table, [ac]):
                cols.append(ac)
                vals.append(amount)
                break

        placeholders = ','.join(['%s'] * len(vals)) if vals else ''
        sql_cols = ','.join(cols)
        query = f"INSERT INTO {chosen_table} ({sql_cols}) VALUES ({placeholders}) RETURNING id;" if placeholders else None
        try:
            if query:
                self.cursor.execute(query, tuple(vals))
                inserted_row = self.cursor.fetchone()
                inserted = inserted_row[0] if inserted_row else None
                self.connection.commit()
                return {'created': True, 'id': inserted, 'table': chosen_table}
            else:
                raise RuntimeError('No hay columnas compatibles para insertar reserva.')
        except Psycopg2Error:
            self.connection.rollback()
            try:
                self._sync_identity_sequence(chosen_table)
                if query:
                    self.cursor.execute(query, tuple(vals))
                    inserted_row = self.cursor.fetchone()
                    inserted = inserted_row[0] if inserted_row else None
                    self.connection.commit()
                    return {'created': True, 'id': inserted, 'table': chosen_table}
            except Psycopg2Error:
                self.connection.rollback()
                raise
            except Exception:
                self.connection.rollback()
                raise
        except Exception:
            self.connection.rollback()
            raise

    def has_overlapping_reservation(self, property_id, start_date, end_date):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        table = self._first_existing_table(['reserva', 'reservas'])
        if table is None:
            return False

        columns = self._table_columns(table)
        if 'propiedad_id' not in columns:
            return False

        start_col = next((c for c in ['fecha_inicio', 'start_date', 'fecha_reserva', 'check_in'] if c in columns), None)
        end_col = next((c for c in ['fecha_fin', 'end_date', 'check_out', 'fecha_salida'] if c in columns), None)
        if start_col is None or end_col is None:
            return False

        where_parts = [
            'propiedad_id = %s',
            f'NOT ({end_col} < %s OR {start_col} > %s)',
        ]
        params = [property_id, start_date, end_date]

        if 'estado_id' in columns:
            where_parts.append('COALESCE(estado_id, 1) <> 3')
        elif 'estado' in columns:
            where_parts.append("LOWER(COALESCE(estado, '')) NOT IN ('cancelada', 'cancelado', 'cancelled')")
        elif 'status' in columns:
            where_parts.append("LOWER(COALESCE(status, '')) NOT IN ('cancelada', 'cancelado', 'cancelled')")

        query = f"SELECT 1 FROM {table} WHERE {' AND '.join(where_parts)} LIMIT 1;"
        self.cursor.execute(query, tuple(params))
        return self.cursor.fetchone() is not None

    def create_review(
        self,
        autor_id,
        propiedad_id,
        puntaje_general,
        puntaje_limpieza=None,
        puntaje_comunicacion=None,
        puntaje_ubicacion=None,
        comentario=None,
        visible=True,
    ):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        table = self._first_existing_table(['resenia', 'resenias', 'review', 'reviews'])
        if table is None:
            raise RuntimeError('No se encontró tabla compatible para reseñas en Postgres.')

        columns = self._table_columns(table)
        required = {'autor_id', 'propiedad_id', 'puntaje_general'}
        if not required.issubset(columns):
            raise RuntimeError('La tabla de reseñas no tiene las columnas mínimas esperadas: autor_id, propiedad_id, puntaje_general.')

        puntaje = float(puntaje_general)
        if puntaje < 0 or puntaje > 5:
            raise ValueError('puntaje_general debe estar entre 0 y 5.')
        puntaje = int(round(puntaje))

        insert_columns = ['autor_id', 'propiedad_id', 'puntaje_general']
        insert_values = [autor_id, propiedad_id, puntaje]

        score_inputs = {
            'puntaje_limpieza': puntaje_limpieza,
            'puntaje_comunicacion': puntaje_comunicacion,
            'puntaje_ubicacion': puntaje_ubicacion,
        }
        for score_col in ['puntaje_limpieza', 'puntaje_comunicacion', 'puntaje_ubicacion']:
            if score_col in columns:
                score_value = score_inputs.get(score_col)
                if score_value is None:
                    score_to_insert = puntaje
                else:
                    score_to_insert = float(score_value)
                    if score_to_insert < 0 or score_to_insert > 5:
                        raise ValueError(f'{score_col} debe estar entre 0 y 5.')
                    score_to_insert = int(round(score_to_insert))
                insert_columns.append(score_col)
                insert_values.append(score_to_insert)

        if 'comentario' in columns:
            insert_columns.append('comentario')
            insert_values.append(comentario)
        if 'fecha' in columns:
            insert_columns.append('fecha')
            insert_values.append('CURRENT_DATE')
        if 'visible' in columns:
            insert_columns.append('visible')
            insert_values.append(bool(visible))

        placeholders = []
        query_params = []
        for value in insert_values:
            if value == 'CURRENT_DATE':
                placeholders.append(value)
            else:
                placeholders.append('%s')
                query_params.append(value)

        returning_clause = ' RETURNING id' if 'id' in columns else ''
        query = (
            f"INSERT INTO {table} ({', '.join(insert_columns)}) "
            f"VALUES ({', '.join(placeholders)}){returning_clause};"
        )

        try:
            if 'id' in columns:
                try:
                    self._sync_identity_sequence(table)
                except Exception:
                    pass
            self.cursor.execute(query, tuple(query_params))
            inserted = self.cursor.fetchone()[0] if 'id' in columns else None
            self.connection.commit()
            return {'created': True, 'id': inserted, 'table': table}
        except Psycopg2Error:
            self.connection.rollback()
            raise
        except Exception:
            self.connection.rollback()
            raise

    def top_hosts_by_reviews(self, limit=5):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        table_resenia = self._first_existing_table(['resenia', 'resenias', 'review', 'reviews'])
        table_propiedad = self._first_existing_table(['propiedad', 'propiedades'])
        table_usuario = self._first_existing_table(['usuario', 'usuarios'])

        if not table_resenia or not table_propiedad or not table_usuario:
            return []

        cols_resenia = self._table_columns(table_resenia)
        cols_propiedad = self._table_columns(table_propiedad)
        cols_usuario = self._table_columns(table_usuario)

        if not {'id', 'propiedad_id', 'puntaje_general'}.issubset(cols_resenia):
            return []
        if not {'id', 'anfitrion_id'}.issubset(cols_propiedad):
            return []
        if 'id' not in cols_usuario:
            return []

        visible_clause = f"WHERE COALESCE(r.visible, TRUE) = TRUE" if 'visible' in cols_resenia else ''
        name_expr = (
            "TRIM(COALESCE(u.nombre, '') || ' ' || COALESCE(u.apellido, ''))"
            if {'nombre', 'apellido'}.issubset(cols_usuario)
            else "COALESCE(u.email, CONCAT('Anfitrión #', u.id::text))"
        )

        query = f"""
            SELECT
                u.id AS host_id,
                {name_expr} AS host_nombre,
                ROUND(AVG(r.puntaje_general)::numeric, 2) AS promedio_calificacion,
                COUNT(r.id) AS cantidad_resenas,
                COUNT(DISTINCT p.id) AS cantidad_propiedades
            FROM {table_resenia} r
            INNER JOIN {table_propiedad} p ON p.id = r.propiedad_id
            INNER JOIN {table_usuario} u ON u.id = p.anfitrion_id
            {visible_clause}
            GROUP BY u.id, host_nombre
            ORDER BY promedio_calificacion DESC, cantidad_resenas DESC, host_id ASC
            LIMIT %s;
        """

        try:
            self.cursor.execute(query, (int(limit),))
            rows = self.cursor.fetchall() or []
            return [
                {
                    '_id': row[0],
                    'nombre': row[1] or f"Anfitrión #{row[0]}",
                    'promedio_calificacion': float(row[2]) if row[2] is not None else 0.0,
                    'cantidad_resenas': int(row[3]) if row[3] is not None else 0,
                    'cantidad_propiedades': int(row[4]) if row[4] is not None else 0,
                }
                for row in rows
            ]
        except Psycopg2Error:
            self.connection.rollback()
            raise
        except Exception:
            self.connection.rollback()
            raise

    def create_payment(self, amount, reserva_id, status='pendiente', method=None, ciudad=None, referencia_externa=None):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")
        table = self._first_existing_table(['pago', 'pagos', 'payment', 'payments', 'transaccion', 'transacciones'])
        if table is None:
            raise RuntimeError('No se encontró tabla compatible para pagos.')

        table_columns = self._table_columns(table)
        if 'reserva_id' not in table_columns:
            raise RuntimeError('La tabla de pagos no tiene columna reserva_id.')
        if 'monto' not in table_columns:
            raise RuntimeError('La tabla de pagos no tiene columna monto.')

        estado_pago_id = None
        metodo_pago_id = None
        if 'estado_pago_id' in table_columns:
            estado_pago_id = self._lookup_id_by_nombre('estado_pago', status)
            if estado_pago_id is None:
                estado_pago_id = self._lookup_id_by_nombre('estado_pago', 'pendiente')
            if estado_pago_id is None:
                raise RuntimeError('No se pudo resolver estado_pago_id desde el catálogo estado_pago.')
        if method and 'metodo_pago_id' in table_columns:
            metodo_pago_id = self._lookup_id_by_nombre('metodo_pago', method)
            if metodo_pago_id is None:
                raise RuntimeError('No se pudo resolver metodo_pago_id desde el catálogo metodo_pago.')

        cols = ['reserva_id', 'monto']
        vals = [reserva_id, amount]
        if 'fecha_pago' in table_columns:
            cols.append('fecha_pago')
            vals.append('CURRENT_TIMESTAMP')
        if referencia_externa and 'referencia_externa' in table_columns:
            cols.append('referencia_externa'); vals.append(referencia_externa)
        if metodo_pago_id is not None:
            cols.append('metodo_pago_id'); vals.append(metodo_pago_id)
        if estado_pago_id is not None:
            cols.append('estado_pago_id'); vals.append(estado_pago_id)
        if ciudad and 'ciudad' in table_columns:
            cols.append('ciudad'); vals.append(ciudad)

        placeholders = []
        query_params = []
        for value in vals:
            if value == 'CURRENT_TIMESTAMP':
                placeholders.append(value)
            else:
                placeholders.append('%s')
                query_params.append(value)

        sql_cols = ','.join(cols)
        returning_clause = " RETURNING id" if 'id' in table_columns else ""
        query = f"INSERT INTO {table} ({sql_cols}) VALUES ({','.join(placeholders)}){returning_clause};"
        try:
            if 'id' in table_columns:
                self._sync_identity_sequence(table)
            self.cursor.execute(query, tuple(query_params))
            inserted = self.cursor.fetchone()[0] if 'id' in table_columns else None
            self.connection.commit()
            return {'created': True, 'id': inserted}
        except Psycopg2Error as exc:
            self.connection.rollback()
            if getattr(exc, "pgcode", None) == "23505" and 'id' in table_columns:
                try:
                    self._sync_identity_sequence(table)
                    self.cursor.execute(query, tuple(query_params))
                    inserted = self.cursor.fetchone()[0] if 'id' in table_columns else None
                    self.connection.commit()
                    return {'created': True, 'id': inserted}
                except Psycopg2Error:
                    self.connection.rollback()
                    raise
                except Exception:
                    self.connection.rollback()
                    raise
            raise
        except Exception:
            self.connection.rollback()
            raise

    def publicar_propiedad_maestro(self, anfitrion_id, property_doc=None, estado='publicada'):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        normalized_doc, flat_doc = self._build_property_field_map(property_doc, anfitrion_id)

        candidates = ['propiedad', 'propiedades', 'alojamiento', 'anuncio', 'anuncios']
        chosen = None
        chosen_score = -1
        for table_name in candidates:
            try:
                if self._table_has_columns(table_name, ['id']):
                    table_columns = self._table_columns(table_name)
                    score = sum(1 for column in flat_doc if flat_doc.get(column) is not None and column in table_columns)
                    if score > chosen_score:
                        chosen = table_name
                        chosen_score = score
            except Exception:
                continue

        if chosen is None or chosen_score <= 0:
            raise RuntimeError(
                'No se encontraron columnas compatibles para tabla de propiedades en Postgres. '
                'La publicación necesita una tabla maestra como propiedad/propiedades/anuncio con columnas compatibles con el documento recibido.'
            )

        table_columns = self._table_columns(chosen)
        insert_columns = []
        insert_values = []

        if 'anfitrion_id' in table_columns and flat_doc.get('anfitrion_id') is not None:
            insert_columns.append('anfitrion_id')
            insert_values.append(flat_doc.get('anfitrion_id'))

        tipo_propiedad_id = None
        tipo_propiedad = flat_doc.get('tipo_propiedad')
        if tipo_propiedad and any(col in table_columns for col in ['tipo_propiedad_id', 'id_tipo_propiedad', 'tipo_id']):
            tipo_propiedad_id = self._ensure_tipo_propiedad_id(tipo_propiedad)
            for fk_column in ['tipo_propiedad_id', 'id_tipo_propiedad', 'tipo_id']:
                if fk_column in table_columns:
                    insert_columns.append(fk_column)
                    insert_values.append(tipo_propiedad_id)
                    break
        elif tipo_propiedad and 'tipo_propiedad' in table_columns:
            insert_columns.append('tipo_propiedad')
            insert_values.append(tipo_propiedad)

        for column in ['titulo', 'descripcion', 'precio_por_noche', 'moneda', 'huespedes_max', 'cant_habitaciones', 'cant_banios', 'calificacion_promedio', 'activa', 'estado']:
            if column in table_columns and flat_doc.get(column) is not None and column not in insert_columns:
                insert_columns.append(column)
                insert_values.append(estado if column == 'estado' and flat_doc.get(column) is None else flat_doc.get(column))

        if 'estado' in table_columns and 'estado' not in insert_columns:
            insert_columns.append('estado')
            insert_values.append(estado)

        # Allow tables that store flat location columns directly.
        for column in ['ciudad', 'provincia', 'pais', 'calle', 'numero', 'codigo_postal', 'latitud', 'longitud']:
            if column in table_columns and flat_doc.get(column) is not None and column not in insert_columns:
                insert_columns.append(column)
                insert_values.append(flat_doc.get(column))

        if not insert_columns:
            raise RuntimeError(f'No se encontraron columnas compatibles en la tabla {chosen} para el documento recibido.')

        has_id_column = 'id' in table_columns
        query = f"INSERT INTO {chosen} ({', '.join(insert_columns)}) VALUES ({', '.join(['%s'] * len(insert_values))}) RETURNING id;"
        try:
            self.cursor.execute(query, tuple(insert_values))
            inserted_row = self.cursor.fetchone() if has_id_column else None
            inserted = inserted_row[0] if inserted_row else None
            self.connection.commit()
            ubicacion_id = None
            try:
                ubicacion_id = self._insert_ubicacion(inserted, normalized_doc)
            except Exception:
                ubicacion_id = None

            # Insertar servicios en tabla intermedia si se pasaron IDs
            servicio_ids = normalized_doc.get('servicio_ids') or []
            if inserted and servicio_ids:
                try:
                    self.insertar_propiedad_servicios(inserted, servicio_ids)
                except Exception:
                    pass  # No bloquear la publicación si falla la M:N

            return {
                'created': True,
                'id': inserted,
                'table': chosen,
                'tipo_propiedad_id': tipo_propiedad_id,
                'ubicacion_id': ubicacion_id,
            }
        except Psycopg2Error:
            self.connection.rollback()
            raise
        except Exception:
            self.connection.rollback()
            raise

    def get_servicios_by_propiedad(self, propiedad_id):
        """Devuelve lista de nombres de servicios para una propiedad usando la tabla intermedia."""
        if not self.connection or not self.cursor:
            return []
        try:
            self.cursor.execute(
                """
                SELECT s.nombre
                FROM servicio s
                JOIN propiedad_servicio ps ON ps.servicio_id = s.id
                WHERE ps.propiedad_id = %s
                ORDER BY s.nombre;
                """,
                (int(propiedad_id),),
            )
            rows = self.cursor.fetchall() or []
            return [row[0] for row in rows if row and row[0]]
        except Exception:
            try:
                self.connection.rollback()
            except Exception:
                pass
            return []

    def insertar_propiedad_servicios(self, propiedad_id, servicio_ids):
        """Inserta filas en propiedad_servicio para la lista de servicio_ids dada."""
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")
        if not servicio_ids:
            return {"inserted": 0}
        try:
            inserted = 0
            for sid in servicio_ids:
                self.cursor.execute(
                    """
                    INSERT INTO propiedad_servicio (propiedad_id, servicio_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING;
                    """,
                    (int(propiedad_id), int(sid)),
                )
                inserted += self.cursor.rowcount or 0
            self.connection.commit()
            return {"inserted": inserted}
        except Psycopg2Error:
            self.connection.rollback()
            raise
        except Exception:
            self.connection.rollback()
            raise

    def registrar_ubicacion_desde_mongo(self, propiedad_ref, property_doc=None):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        ubicacion_table = self._first_existing_table(["ubicacion"])
        if not ubicacion_table:
            raise RuntimeError("No se encontró la tabla ubicacion en Postgres.")

        ubicacion_data = {}
        if isinstance(property_doc, dict) and isinstance(property_doc.get("ubicacion"), dict):
            ubicacion_data = dict(property_doc.get("ubicacion") or {})

        coordinates = ubicacion_data.get("coordenadas") if isinstance(ubicacion_data.get("coordenadas"), dict) else {}
        coords = coordinates.get("coordinates") if isinstance(coordinates.get("coordinates"), list) else []

        candidate_values = {
            "propiedad_id": propiedad_ref,
            "calle": ubicacion_data.get("calle"),
            "numero": ubicacion_data.get("numero"),
            "ciudad": ubicacion_data.get("ciudad"),
            "provincia": ubicacion_data.get("provincia"),
            "pais": ubicacion_data.get("pais"),
            "codigo_postal": ubicacion_data.get("codigo_postal"),
            "latitud": ubicacion_data.get("latitud") if ubicacion_data.get("latitud") is not None else (coords[1] if len(coords) >= 2 else None),
            "longitud": ubicacion_data.get("longitud") if ubicacion_data.get("longitud") is not None else (coords[0] if len(coords) >= 2 else None),
        }

        columns = self._table_columns(ubicacion_table)
        insert_columns = []
        insert_values = []

        for column_name, raw_value in candidate_values.items():
            if column_name not in columns or raw_value is None:
                continue

            value = raw_value
            if column_name == "propiedad_id":
                metadata = self._column_metadata(ubicacion_table, "propiedad_id")
                integer_types = {
                    "smallint",
                    "integer",
                    "bigint",
                    "int2",
                    "int4",
                    "int8",
                    "serial",
                    "bigserial",
                }
                if metadata and (metadata.get("data_type") in integer_types or metadata.get("udt_name") in integer_types):
                    try:
                        value = int(str(raw_value))
                    except Exception:
                        if metadata.get("is_nullable"):
                            continue
                        raise RuntimeError(
                            "La columna ubicacion.propiedad_id requiere entero y la propiedad publicada en Mongo tiene un id no numérico."
                        )

            insert_columns.append(column_name)
            insert_values.append(value)

        if not insert_columns:
            raise RuntimeError("No se pudieron mapear columnas para insertar la ubicación en SQL.")

        query = (
            f"INSERT INTO {ubicacion_table} ({', '.join(insert_columns)}) "
            f"VALUES ({', '.join(['%s'] * len(insert_values))}) "
            + ("RETURNING id;" if "id" in columns else ";")
        )

        try:
            if "id" in columns:
                try:
                    self._sync_identity_sequence(ubicacion_table)
                except Exception:
                    pass
            self.cursor.execute(query, tuple(insert_values))
            inserted_id = self.cursor.fetchone()[0] if "id" in columns else None
            self.connection.commit()
            return {
                "created": True,
                "table": ubicacion_table,
                "id": inserted_id,
                "propiedad_ref": str(propiedad_ref) if propiedad_ref is not None else None,
            }
        except Psycopg2Error as exc:
            self.connection.rollback()
            if getattr(exc, "pgcode", None) == "23505" and "id" in columns:
                try:
                    self._sync_identity_sequence(ubicacion_table)
                    self.cursor.execute(query, tuple(insert_values))
                    inserted_id = self.cursor.fetchone()[0] if "id" in columns else None
                    self.connection.commit()
                    return {
                        "created": True,
                        "table": ubicacion_table,
                        "id": inserted_id,
                        "propiedad_ref": str(propiedad_ref) if propiedad_ref is not None else None,
                    }
                except Psycopg2Error:
                    self.connection.rollback()
                    raise
                except Exception:
                    self.connection.rollback()
                    raise
            raise
        except Exception:
            self.connection.rollback()
            raise

    def actualizar_promedio_propiedad_desde_mongo(self, propiedad_maestro_id, nuevo_promedio):
        # Intenta actualizar una tabla de metadatos si existe
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")
        possible_tables = ['propiedades_meta', 'propiedad_meta', 'propiedades']
        for t in possible_tables:
            try:
                if self._table_has_columns(t, ['propiedad_id', 'calificacion_promedio']):
                    self.cursor.execute(f"UPDATE {t} SET calificacion_promedio = %s WHERE propiedad_id = %s;", (float(nuevo_promedio), propiedad_maestro_id))
                    self.connection.commit()
                    return {'updated': True, 'table': t}
            except Psycopg2Error:
                self.connection.rollback()
                raise
            except Exception:
                self.connection.rollback()
                continue
        return {'updated': False, 'mensaje': 'No se encontró tabla de metadatos para actualizar promedio.'}

    def actualizar_promedio_propiedad_desde_resenias(self, propiedad_id):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        table_resenia = self._first_existing_table(['resenia', 'resenias', 'review', 'reviews'])
        table_propiedad = self._first_existing_table(['propiedad', 'propiedades'])

        if not table_resenia or not table_propiedad:
            return {'updated': False, 'mensaje': 'No se encontraron tablas compatibles de reseñas/propiedades.'}

        cols_resenia = self._table_columns(table_resenia)
        cols_propiedad = self._table_columns(table_propiedad)

        if 'propiedad_id' not in cols_resenia or 'puntaje_general' not in cols_resenia:
            return {'updated': False, 'mensaje': 'La tabla de reseñas no tiene columnas esperadas.'}

        if 'calificacion_promedio' not in cols_propiedad:
            return {'updated': False, 'mensaje': 'La tabla de propiedades no tiene columna calificacion_promedio.'}

        propiedad_pk = 'id' if 'id' in cols_propiedad else ('propiedad_id' if 'propiedad_id' in cols_propiedad else None)
        if propiedad_pk is None:
            return {'updated': False, 'mensaje': 'No se pudo resolver la PK de la tabla de propiedades.'}

        visible_clause = " AND COALESCE(visible, TRUE) = TRUE" if 'visible' in cols_resenia else ""

        try:
            self.cursor.execute(
                f"""
                SELECT AVG(puntaje_general)::numeric
                FROM {table_resenia}
                WHERE propiedad_id = %s{visible_clause};
                """,
                (propiedad_id,),
            )
            row = self.cursor.fetchone()
            promedio = row[0] if row else None

            self.cursor.execute(
                f"""
                UPDATE {table_propiedad}
                SET calificacion_promedio = %s
                WHERE {propiedad_pk} = %s;
                """,
                (float(promedio) if promedio is not None else None, propiedad_id),
            )
            updated_rows = self.cursor.rowcount or 0
            self.connection.commit()

            return {
                'updated': updated_rows > 0,
                'rows': int(updated_rows),
                'table': table_propiedad,
                'promedio': float(promedio) if promedio is not None else None,
            }
        except Psycopg2Error:
            self.connection.rollback()
            raise
        except Exception:
            self.connection.rollback()
            raise

    def count_reservations_by_city(self, city):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        try:
            self.cursor.execute(
                """
                SELECT COUNT(*)
                FROM reserva r
                INNER JOIN ubicacion u ON u.propiedad_id = r.propiedad_id
                WHERE u.ciudad = %s;
                """,
                (city,),
            )
            row = self.cursor.fetchone()
            return int(row[0]) if row else 0
        except Exception:
            self.connection.rollback()
            raise

    def list_available_cities(self):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        try:
            if not self._table_has_columns("ubicacion", ["ciudad"]):
                return []

            self.cursor.execute(
                """
                SELECT DISTINCT TRIM(ciudad) AS ciudad
                FROM ubicacion
                WHERE TRIM(COALESCE(ciudad, '')) <> ''
                ORDER BY ciudad ASC;
                """
            )
            rows = self.cursor.fetchall() or []
            return [row[0] for row in rows if row and row[0]]
        except Exception:
            self.connection.rollback()
            raise

    def count_reservations_by_city_last_month(self, city):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        try:
            query = """
                SELECT count(r.id)
                FROM reserva r
                JOIN propiedad p ON r.propiedad_id = p.id
                JOIN ubicacion u ON p.id = u.propiedad_id
                WHERE TRIM(LOWER(u.ciudad)) = TRIM(LOWER(%s))
                AND r.fecha_creacion >= CURRENT_DATE - INTERVAL '1 month';
            """
            self.cursor.execute(query, (city,))
            row = self.cursor.fetchone()
            return int(row[0]) if row else 0
        except Exception:
            self.connection.rollback()
            raise

    def count_reservations_last_month_by_property_ids(self, property_ids):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        ids = [str(item).strip() for item in (property_ids or []) if str(item).strip()]
        if not ids:
            return 0

        reserva_table = self._first_existing_table(["reserva", "reservas"])
        if not reserva_table:
            return 0

        columns = self._table_columns(reserva_table)
        prop_col = next((c for c in ["propiedad_id", "property_id", "id_propiedad"] if c in columns), None)
        if not prop_col:
            return 0

        date_col = next(
            (c for c in ["fecha_inicio", "check_in", "fecha_reserva", "fecha", "created_at", "fecha_creacion"] if c in columns),
            None,
        )
        if not date_col:
            return 0

        try:
            query = f"""
                SELECT COUNT(*)
                FROM {reserva_table} r
                WHERE CAST(r.{prop_col} AS TEXT) = ANY(%s)
                  AND r.{date_col} IS NOT NULL
                  AND r.{date_col} >= CURRENT_DATE - INTERVAL '1 month';
            """
            self.cursor.execute(query, (ids,))
            row = self.cursor.fetchone()
            return int(row[0]) if row else 0
        except Exception:
            self.connection.rollback()
            raise

    def most_demanded_areas_by_country(self, country, limit=10):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        try:
            ubicacion_cols = self._table_columns("ubicacion")
            propiedad_table = self._first_existing_table(["propiedad", "propiedades"])
            propiedad_cols = self._table_columns(propiedad_table) if propiedad_table else set()

            join_propiedad = ""
            if "barrio" in ubicacion_cols:
                area_expr = "COALESCE(NULLIF(TRIM(u.barrio), ''), 'N/D')"
            elif propiedad_table and "barrio" in propiedad_cols:
                join_propiedad = f" INNER JOIN {propiedad_table} p ON p.id = r.propiedad_id"
                area_expr = "COALESCE(NULLIF(TRIM(p.barrio), ''), 'N/D')"
            else:
                # Fallback técnico para no romper si el schema aún no tiene barrio persistido.
                area_expr = "COALESCE(NULLIF(TRIM(u.ciudad), ''), 'N/D')"

            query = f"""
                SELECT
                    {area_expr} AS barrio,
                    COUNT(*) AS total
                FROM reserva r
                INNER JOIN ubicacion u ON u.propiedad_id = r.propiedad_id
                {join_propiedad}
                WHERE TRIM(LOWER(COALESCE(u.pais, ''))) = TRIM(LOWER(%s))
                GROUP BY {area_expr}
                ORDER BY total DESC, barrio ASC
                LIMIT %s;
            """

            self.cursor.execute(query, (country, int(limit)))
            rows = self.cursor.fetchall() or []
            return [
                {"barrio": row[0], "total": int(row[1])}
                for row in rows
            ]
        except Exception:
            self.connection.rollback()
            raise

    def payment_summary_last_month(self, city=None):
        if not self.connection or not self.cursor:
            raise RuntimeError("Conexión a Postgres no disponible.")

        candidates = [
            ("pagos", "monto", "estado", "created_at", "metodo_pago"),
            ("transacciones", "monto", "estado", "created_at", "metodo_pago"),
            ("reservas", "monto_pagado", "estado_transaccion", "fecha_reserva", "metodo_pago"),
            ("reservas", "total_pagado", "estado_pago", "fecha_reserva", "metodo_pago"),
        ]

        chosen_table = None
        amount_column = None
        status_column = None
        date_column = None
        method_column = None
        for table_name, amount_col, status_col, date_col, method_col in candidates:
            if self._table_has_columns(table_name, [amount_col, status_col, date_col]):
                chosen_table = table_name
                amount_column, status_column, date_column = amount_col, status_col, date_col
                method_column = method_col if self._table_has_columns(table_name, [method_col]) else None
                break

        if chosen_table is None:
            return {
                "total_registros": 0,
                "monto_total": 0,
                "estado_dominante": "N/D",
                "metodo_dominante": "N/D",
                "mensaje": "No se encontraron tablas/columnas compatibles para pagos o transacciones.",
            }

        filters = []
        params = []
        if city and self._table_has_columns(chosen_table, ["ciudad"]):
            filters.append("ciudad = %s")
            params.append(city)
        filters.append(f"{date_column} >= CURRENT_DATE - INTERVAL '1 month'")

        method_select = f", COALESCE(MODE() WITHIN GROUP (ORDER BY {method_column}), 'N/D') AS metodo_dominante" if method_column else ", 'N/D' AS metodo_dominante"

        query = f"""
            SELECT
                COUNT(*) AS total_registros,
                COALESCE(SUM({amount_column}), 0) AS monto_total,
                COALESCE(MODE() WITHIN GROUP (ORDER BY {status_column}), 'N/D') AS estado_dominante
                {method_select}
            FROM {chosen_table}
            WHERE {' AND '.join(filters)};
        """

        try:
            self.cursor.execute(query, tuple(params))
            row = self.cursor.fetchone() or (0, 0, "N/D", "N/D")
            return {
                "tabla": chosen_table,
                "total_registros": int(row[0] or 0),
                "monto_total": float(row[1] or 0),
                "estado_dominante": row[2] or "N/D",
                "metodo_dominante": row[3] or "N/D",
            }
        except Exception:
            self.connection.rollback()
            raise

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
