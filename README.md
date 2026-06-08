# Airbnb Data Orchestrator

CLI para el proyecto Airbnb poliglota con persistencia en múltiples bases de datos.

## Casos de Uso implementados

| Caso de uso | Base de datos | Dónde está implementado (módulo / función) | CLI |
|---|---:|---|---|
| ¿Cuántas reservas se realizan en una ciudad específica en el último mes? | Postgres (+ Redis cache) | `orquestador.py` → `contar_reservas_ciudad_ultimo_mes` / `db_connectors/postgres_repository.py` → `count_reservations_by_city_last_month` | Opción 1
| Tipos de alojamiento más populares | MongoDB (+ Redis cache) | `orquestador.py` → `alojamiento_mas_popular` / `db_connectors/mongo_repository.py` → `popular_accommodations` | Opción 2
| Propiedades agregadas recientemente | MongoDB | `orquestador.py` → `propiedades_recientes` / `db_connectors/mongo_repository.py` → `recent_properties` | Opción 3
| Mejores anfitriones | Postgres (principal) + MongoDB/Redis (fallback) | `orquestador.py` → `mejores_anfitriones` / `db_connectors/postgres_repository.py` → `top_hosts_by_reviews` | Opción 4
| Barrios más demandados por país | Postgres | `orquestador.py` → `areas_mas_demandadas_pais` / `db_connectors/postgres_repository.py` → `most_demanded_areas_by_country` | Opción 5
| Propiedades con calificación mayor a 4.5 en CABA | MongoDB | `db_connectors/mongo_repository.py` → `properties_with_high_rating_in_center(min_rating, ciudad)` | Opción 6
| Propiedades con +20 reseñas o en zona turística | MongoDB | `orquestador.py` → `propiedades_mas_resenadas_o_zona_turistica` / `db_connectors/mongo_repository.py` → `properties_with_many_reviews_or_touristic_zone` | Opción 7
| Disponibilidad de propiedad en rango de fechas | Cassandra + Redis | `orquestador.py` → `disponibilidad_propiedad_rango` / `db_connectors/cassandra_repository.py` → `check_property_availability` | Opción 8


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
