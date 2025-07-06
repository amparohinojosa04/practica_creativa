#!/usr/bin/env python3
"""
Carga la colección `origin_dest_distances` de MongoDB
en la tabla `flights.origin_dest_distances` de Cassandra.
"""

import sys
import time
import socket
from cassandra.cluster import Cluster
from pymongo import MongoClient

# ---------- 1. Esperar a Cassandra ----------
MAX_TRIES = 30
HOST, PORT = "cassandra", 9042
print(f"Intentando conectar a Cassandra en {HOST}:{PORT}…")

for attempt in range(1, MAX_TRIES + 1):
    try:
        socket.create_connection((HOST, PORT), timeout=2).close()
        print("Cassandra responde.")
        break
    except OSError:
        print(f"  · intento {attempt}/{MAX_TRIES} fallido; reintento en 2 s.")
        time.sleep(2)
else:
    print("No se pudo contactar con Cassandra, abortando migración.")
    sys.exit(1)

# ---------- 2. Preparar keyspace y tabla ----------
cluster = Cluster([HOST])
session = cluster.connect()

print("Creando keyspace y tabla (si no existen)…")
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS flights
    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
""")
session.set_keyspace("flights")

session.execute("""
    CREATE TABLE IF NOT EXISTS origin_dest_distances (
        origin   text,
        dest     text,
        distance double,
        PRIMARY KEY ((origin), dest)
    );
""")

# ---------- 3. Leer desde Mongo ----------
print("Conectando a MongoDB …")
mongo_client = MongoClient("mongodb://mongodb:27017")
source_coll = mongo_client["agile_data_science"]["origin_dest_distances"]

# ---------- 4. Insertar en Cassandra ----------
print("Migrando documentos…")
rows_inserted = 0
prepared = session.prepare(
    "INSERT INTO origin_dest_distances (origin, dest, distance) VALUES (?, ?, ?)"
)

for doc in source_coll.find({}, {"_id": 0, "Origin": 1, "Dest": 1, "Distance": 1}):
    session.execute(prepared, (
        doc.get("Origin"),
        doc.get("Dest"),
        float(doc.get("Distance", 0.0))
    ))
    rows_inserted += 1

print(f"Migración terminada: {rows_inserted} registros copiados.")
