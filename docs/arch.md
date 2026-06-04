# Arquitectura y Mapeo de Código

Resumen rápido
- Orquestador CLI: `orquestador.py` — punto de entrada y coordinación entre conectores.
- Conectores por motor: `db_connectors/*.py` — cada archivo encapsula acceso y consultas:
  - `postgres_repository.py` — consultas `reserva`, `pagos`, agregaciones (opciones 1,2,12).
  - `mongo_repository.py` — agregaciones y búsquedas documentales (opciones 3,4,5,6,7,7a,8,9,10,11).
  - `redis_repository.py` — cache (Upstash-compatible) usado por orquestador.
  - `cassandra_repository.py` — telemetría / eventos por usuario.

Casos de uso → ruta en el código
- Reservas por ciudad / último mes: `AirbnbOrchestrator.contar_reservas_ciudad` / `contar_reservas_ciudad_ultimo_mes` → `PostgresRepository.count_reservations_by_city(_last_month)` (opciones 1 y 2). Redis cache en la opción 2.
- Tipos de alojamiento más populares: `AirbnbOrchestrator.alojamiento_mas_popular` → `MongoRepository.popular_accommodations` (opción 3). Cache Redis aplicada.
- Propiedades recientes: `MongoRepository.recent_properties` (opción 4).
- Mejores anfitriones: `MongoRepository.top_hosts_by_rating` → `AirbnbOrchestrator.mejores_anfitriones` (opción 5). Cache Redis aplicada por límite.
- Áreas más demandadas por país: `MongoRepository.most_demanded_areas_by_country` (opción 6).
- Propiedades >20 reseñas O en zona turística: `MongoRepository.properties_with_many_reviews_or_touristic_zone` (opción 7).
- Propiedades con rating alto (global / anywhere): `MongoRepository.properties_with_high_rating_anywhere` (opción 7a).
- Búsqueda ciudad/centro con rating: `MongoRepository.properties_with_high_rating_in_center(min_rating, ciudad=None)` (función disponible desde el orquestador).
- Telemetría: `CassandraRepository.register_event` / `get_user_events` (submenú opción 13 si Cassandra disponible).
- Pagos / transacciones resumen: `PostgresRepository.payment_summary_last_month` (opción 12).

Flujo de datos y notas operacionales
- `orquestador.py` actúa como coordinador: llama a métodos de repositorio, aplica formateo y caching. Redis es opcional (fallback a DB si no está disponible).
- Caché: claves usadas `popular_accommodations`, `top_hosts:{limit}`; TTL configurable con `CACHE_TTL_SECONDS` (por defecto 300s). Herramientas para limpiar cache en submenú Redis.
- Índices: ver scripts `mongo/create_indexes.py` y `postgres/create_indexes.sql` incluidos en el repo. Recomendados antes de ejecutar consultas en dataset grande.
- Consigna resumida: el sistema gestiona propiedades, usuarios, reservas, pagos y reseñas, distribuyendo la información entre los motores disponibles según el patrón de acceso.
- Normalización Mongo: el script `mongo/backfill_tipo_propiedad.py` materializa `tipo_propiedad`, `metadata_anfitrion` e `id_moneda` en documentos existentes cuando el esquema real trae `anfitrion_id`, `titulo`, `ubicacion`, `moneda` y `calificacion_promedio`.
- Signup: Redis mantiene auth/sesión; Postgres persiste el usuario maestro.
- Disponibilidad: opción 8 usa Cassandra `disponibilidad_propiedad` con rango de fechas y Redis como caché de resultado.


Mapeo de Cassandra / Astra
- Keyspace de referencia: `airbnb`
- Tablas dadas por la consigna:
  - `disponibilidad_propiedad(propiedad_id, fecha, disponible, reserva_id, precio_dia)`
  - `historial_vistas(propiedad_id, fecha_hora, usuario_id, origen)`
- El CLI actual usa Astra Data API con colección `eventos_por_usuario` para telemetría; estas tablas quedan documentadas para una futura capa CQL.
- El `fecha_inicio`/`fecha_fin` de disponibilidad no se almacenan en Cassandra; se usan como parámetros de consulta sobre la columna diaria `fecha`.

Verificación de cumplimiento actualizada
- Signup multi-base: Redis + Postgres.
- Disponibilidad de propiedad: Cassandra + Redis.

Verificación contra la consigna
| Caso de uso | Estado actual | Observación |
|---|---|---|
| Reservas en una ciudad en el último mes | Cumplido | Implementado en Postgres (`count_reservations_by_city_last_month`). |
| Tipos de alojamiento más populares | Parcial | Hoy se resuelve con Mongo + Redis; la consigna pedía Redis + PostgreSQL. |
| Propiedades agregadas recientemente | Parcial | Hoy se resuelve con Mongo usando `_id`/ObjectId; la consigna lo asocia a PostgreSQL. |
| Anfitriones con mejores calificaciones | Parcial | Hoy se resuelve con Mongo + Redis; la consigna lo asocia a Redis + PostgreSQL. |
| Áreas más demandadas por país | Parcial | Hoy se resuelve con Mongo; la consigna lo asocia a Cassandra + PostgreSQL. |
| Propiedades con rating > 4.5 en el centro | Cumplido | Implementado en Mongo con fallback por ciudad/centro. |
| Alojamientos con más de 20 reseñas O en zona turística | Cumplido | Implementado en Mongo. |
| Recomendaciones personalizadas | No implementado | Funcionalidad de recomendaciones no incluida en el proyecto. |
| Disponibilidad de propiedad en rango de fechas | Pendiente | La consigna la asocia a Cassandra + Redis; aún no hay una ruta de menú específica para esta consulta. |

Resumen
- La base del flujo está cubierta, pero hay 4 casos de uso que quedaron implementados con un backend distinto al indicado por la consigna.
- Si querés cumplir la consigna al 100%, los siguientes pasos son reubicar esos 4 casos al stack sugerido o documentar formalmente la justificación del desvío.

Variables de entorno importantes
- `MONGO_URI`, `MONGO_DB_NAME`, `MONGO_COLLECTION_NAME`
- `POSTGRES_URI` / `POSTGRES_DB` (según tu configuración en `postgres_repository.py`)
- `UPSTASH_REDIS_REST_URL`, `UPSTASH_REDIS_REST_TOKEN` (si usás Upstash)
- `ASTRA_DB_TOKEN`, `ASTRA_DB_ENDPOINT` (Cassandra/Astra)
- `CACHE_TTL_SECONDS` (TTL por defecto para cache Redis)

Cómo ejecutar
1. Copiar `.env.example` → `.env` y completar variables.
2. Crear entorno e instalar deps:
```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```
3. (Opcional) Crear índices:
```powershell
python mongo/create_indexes.py
psql "$POSTGRES_URI" -f postgres/create_indexes.sql
```
4. Ejecutar CLI:
```powershell
python orquestador.py
```

Próximos pasos sugeridos
- Añadir `docs/arch.md` (este archivo) — completado.
- Crear scripts de seed y pruebas unitarias para validar queries (pendiente).
- Opcional: instrumentar invalidación de cache en escrituras/seed para mantener consistencia.
