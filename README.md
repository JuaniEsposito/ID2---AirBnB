# Airbnb Data Orchestrator

Breve descripción: Herramienta CLI para consultar datos de ejemplo (Postgres, MongoDB, Cassandra, Redis) sobre un dataset tipo Airbnb.

## Casos de Uso implementados y cómo obtener la información

| Caso de uso | Base de datos | Dónde está implementado (módulo / función) | CLI |
|---|---:|---|---|
| Reservas en una ciudad (total) | Postgres | `db_connectors/postgres_repository.py` → `count_reservations_by_city` | Opción 1
| Reservas en la ciudad último mes | Postgres (+ Redis cache) | `db_connectors/postgres_repository.py` → `count_reservations_by_city_last_month` (cache en `db_connectors/redis_repository.py`) | Opción 2
| Tipos de alojamiento más populares | MongoDB | `db_connectors/mongo_repository.py` → `popular_accommodations` | Opción 3
| Propiedades agregadas recientemente | MongoDB | `db_connectors/mongo_repository.py` → `recent_properties` | Opción 4
| Mejores anfitriones por calificación | MongoDB (+ Redis opcional) | `db_connectors/mongo_repository.py` → `top_hosts_by_rating` | Opción 5
| Áreas más demandadas por país | MongoDB + agregación | `db_connectors/mongo_repository.py` → `most_demanded_areas_by_country` | Opción 6
| Propiedades con >20 reseñas O en zona turística | MongoDB | `db_connectors/mongo_repository.py` → `properties_with_many_reviews_or_touristic_zone` | Opción 7
| Propiedades con rating alto (todo el catálogo) | MongoDB | `db_connectors/mongo_repository.py` → `properties_with_high_rating_anywhere` | Opción 7a
| Búsqueda por ciudad / centro con rating alto | MongoDB | `db_connectors/mongo_repository.py` → `properties_with_high_rating_in_center(min_rating, ciudad=None)` | (función disponible; usar API directa o agregar opción personalizada)
| Resumen de reseñas por propiedad | MongoDB | `db_connectors/mongo_repository.py` → `property_review_summary` | Opción 10
| Reseñas recientes visibles | MongoDB | `db_connectors/mongo_repository.py` → `recent_visible_reviews` | Opción 11
| Resumen de pagos y transacciones | Postgres | `db_connectors/postgres_repository.py` → `payment_summary_last_month` | Opción 12
| Telemetría / eventos por usuario | Cassandra | `db_connectors/cassandra_repository.py` → `register_event` / `get_user_events` | Opción 13 (submenú)

## Notas y recomendaciones
- Asegurarse de llenar `.env` con `MONGO_URI`, `POSTGRES_URI`, etc., antes de ejecutar la CLI.
- Para Cassandra/Astra, revisá `ASTRA_DB_NAME` y `ASTRA_COLLECTION_NAME`; por defecto se usa `historial_vistas`, que es la colección/tabla existente para telemetría.
- Para colecciones grandes en Mongo se recomiendan índices: `ubicacion.ciudad`, `ubicacion.zona`, y considerar índices sobre `resenas.calificacion` o campos derivados.
- Si querés, puedo agregar scripts en `mongo/create_indexes.py` y un `postgres/create_indexes.sql` con los índices recomendados.

## Scripts de índices
Incluí scripts para crear índices recomendados:

- `mongo/create_indexes.py` — crea índices en la colección `propiedades` (ciudad, zona, `resenas.calificacion`, `resenas.fecha`). Ejecutar:

```powershell
python mongo/create_indexes.py
```

- `postgres/create_indexes.sql` — sentencias SQL con índices sugeridos para tablas `reserva`, `ubicacion`, `pagos`. Ejecutar con `psql`:

```powershell
psql "$POSTGRES_URI" -f postgres/create_indexes.sql
```

Revisá los nombres de tablas/columnas en tu esquema antes de ejecutar; los scripts son recomendados pero pueden necesitar ajustes.

## Normalización de Mongo
Si tus documentos nuevos vienen con el esquema de ejemplo que me pasaste (`anfitrion_id`, `titulo`, `ubicacion`, `moneda`, `calificacion_promedio`), podés materializar `tipo_propiedad`, `metadata_anfitrion` e `id_moneda` en los documentos existentes con:

```powershell
python mongo/backfill_tipo_propiedad.py
```

El script infiere `tipo_propiedad` desde el título y completa campos derivados para que las consultas actuales del orquestador trabajen con el esquema nuevo y el anterior.

## Signup y persistencia
El `signup` del CLI ahora escribe de forma best-effort en:

- Redis: usuario de auth y sesión (`sesion:{token}`)
- Postgres: tabla `usuario`

Si Postgres falla, el registro en Redis se mantiene para no romper el alta de sesión.

## Disponibilidad de propiedad
La opción `8` del menú consulta disponibilidad de una propiedad en un rango de fechas usando Cassandra (`disponibilidad_propiedad`) y cachea el resultado en Redis.

Formato de consulta:

```powershell
propiedad_id, fecha_inicio, fecha_fin
```

La tabla Cassandra no necesita guardar `fecha_inicio` y `fecha_fin`: el rango se pasa como parámetro de consulta y se valida sobre la columna diaria `fecha`.

## Cassandra / Astra
Para la parte de Cassandra, el mapeo de referencia es el keyspace `airbnb` con las tablas:

- `disponibilidad_propiedad(propiedad_id, fecha, disponible, reserva_id, precio_dia)`
- `historial_vistas(propiedad_id, fecha_hora, usuario_id, origen)`

El CLI actual usa Astra Data API para eventos de usuario (`ASTRA_COLLECTION_NAME=eventos_por_usuario`), pero este mapeo queda documentado por si querés sumar flujos CQL más adelante.

## Cómo ejecutar
1. Crear un entorno virtual e instalar dependencias:

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

2. Copiar `.env.example` → `.env` y completar valores.
3. Ejecutar la CLI:

```powershell
python orquestador.py
```
# Airbnb Data Orchestrator

CLI de consultas para el proyecto Airbnb poliglota.

## Uso

```bash
python orquestador.py
```

## Estructura

- `orquestador.py`: orquestador principal.
- `db_connectors/`: conectores a Postgres, Mongo y Cassandra.
