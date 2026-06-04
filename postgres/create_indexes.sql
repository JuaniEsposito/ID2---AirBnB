-- Recommended indexes for Postgres tables used by the Orchestrator
-- Run with psql (example):
-- psql "$POSTGRES_URI" -f postgres/create_indexes.sql

-- 1) Index for reservations by city and date
CREATE INDEX IF NOT EXISTS idx_reserva_ciudad_fecha ON reserva (ciudad, fecha);

-- 2) Partial index for recent reservations (last 30 days)
-- Adjust the WHERE clause to the column name used for reservation datetime.
-- Example assumes `fecha` is a timestamp column.
CREATE INDEX IF NOT EXISTS idx_reserva_recent ON reserva (ciudad)
  WHERE fecha >= now() - interval '30 days';

-- 3) Indexes for location lookup if using a separate ubicacion table
CREATE INDEX IF NOT EXISTS idx_ubicacion_ciudad ON ubicacion (ciudad);
CREATE INDEX IF NOT EXISTS idx_ubicacion_provincia ON ubicacion (provincia);

-- 4) Indexes to accelerate payment aggregations by city/date
CREATE INDEX IF NOT EXISTS idx_pagos_ciudad_fecha ON pagos (ciudad, fecha);

-- 5) Trigram index for case-insensitive/fuzzy city searches (requires pg_trgm)
-- Enable extension first (run as superuser):
-- CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- Then create GIN trigram index:
-- CREATE INDEX IF NOT EXISTS idx_ubicacion_ciudad_trgm ON ubicacion USING gin (lower(ciudad) gin_trgm_ops);

-- Notes:
--  - Creating indexes will improve read/query performance but adds overhead to writes.
--  - Review column names and table names in your schema before running.
