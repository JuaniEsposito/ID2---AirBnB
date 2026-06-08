# Airbnb Data Orchestrator

CLI para el proyecto Airbnb poliglota con persistencia en múltiples bases de datos.

## Casos de Uso implementados

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

## Estructura

- `orquestador.py`: orquestador principal.
- `db_connectors/`: conectores a Postgres, Mongo, Cassandra y Redis.
