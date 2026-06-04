# Airbnb Data Orchestrator

Proyecto de Ingeniería de Datos II. Implementación de persistencia políglota.

## Configuración
1. Instalar dependencias: `pip install pymongo astrapy redis psycopg2-binary python-dotenv`
2. Crear un archivo `.env` en la raíz con las credenciales (ver `.env.example`).
3. Ejecutar: `python orquestador.py`

## Motores Utilizados
- **Postgres (Supabase):** Transaccional.
- **MongoDB:** Catálogo.
- **Cassandra:** Historial de eventos.
- **Redis:** Caché de performance.