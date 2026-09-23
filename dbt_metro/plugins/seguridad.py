"""Plugin de dbt-duckdb: carga el secreto de seudonimización en memoria.

El secreto viene de la variable de entorno PSEUDONIMO_SECRETO y vive en una base en memoria adjunta a la
instancia de DuckDB (la comparten todos los hilos de dbt). Nunca se escribe en el SQL compilado, en los logs de
dbt, en el manifest ni en el archivo warehouse.duckdb. Las macros lo leen con `pseudonimo(...)`.
"""
import os

from dbt.adapters.duckdb.plugins import BasePlugin


class Plugin(BasePlugin):
    def configure_connection(self, conn):
        secreto = os.environ.get("PSEUDONIMO_SECRETO")
        if not secreto:
            raise RuntimeError("Define la variable de entorno PSEUDONIMO_SECRETO (ver .env.example)")
        conn.execute("ATTACH ':memory:' AS seguridad")
        conn.execute("CREATE OR REPLACE TABLE seguridad.main.clave AS SELECT ? AS secreto", [secreto])
