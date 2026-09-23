"""Ejecutor de dbt (Silver) y utilidades de conteo.

    python -m flows.dbt_runner build        # crea las vistas de Bronze y corre dbt build (semillas + modelos + pruebas)
    python -m flows.dbt_runner build --sin-unificar-identidad
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

import duckdb

from bronze.config import RAIZ, Config, cargar_env
from bronze.conciliacion import crear_vistas

DBT_DIR = RAIZ / "dbt_metro"

# tabla -> columna llave (para calcular una huella del contenido, no solo el conteo)
TABLAS_STAGING = ["stg_estaciones", "stg_cdc_padron", "stg_tm_validaciones", "stg_tu_transacciones",
                  "stg_am_boardings", "stg_mr_viajes"]
TABLAS_SILVER = {
    "silver_tm_validaciones": "evento_sk", "silver_tu_transacciones": "evento_sk",
    "silver_am_boardings": "evento_sk", "silver_mr_viajes": "evento_sk",
    "silver_cuarentena": "cuarentena_sk", "silver_padron_scd2": "padron_sk",
    "silver_padron_vigente": "padron_sk", "silver_usuario": "usuario_sk", "silver_estacion": "estacion_sk",
    "silver_llaves_transurbano": "llave", "silver_llaves_metroriel": "llave", "silver_llaves_aerometro": "llave",
    "silver_dq_conteo_por_regla": "regla",
}
TABLAS_GOLD_DIM = ["dim_transporte", "dim_fecha", "dim_hora", "dim_estacion", "dim_usuario"]
TABLAS_GOLD_FACT = {"fact_abordaje": "evento_sk", "fact_viaje": "evento_sk"}


def dbt(cfg: Config, *args: str, variables: Optional[dict] = None) -> None:
    """Ejecuta dbt en un subproceso (así libera su conexión a DuckDB al terminar).
    Lanza excepción si hay errores; las advertencias no hacen fallar la ejecución."""
    cargar_env()
    if not os.environ.get("PSEUDONIMO_SECRETO"):
        raise RuntimeError(
            "Falta PSEUDONIMO_SECRETO. Es el secreto con el que se seudonimizan las llaves de usuario antes de Gold.\n"
            "  Copia .env.example a .env y define un valor largo y aleatorio, o en PowerShell:\n"
            "  $env:PSEUDONIMO_SECRETO = \"<valor largo y aleatorio>\"   (no lo subas a Git)")
    env = {**os.environ, "LAKE_DIR": str(cfg.lake_dir.resolve()), "DBT_PLUGINS_DIR": str(DBT_DIR / "plugins")}
    cmd = [sys.executable, "-c", "import sys; from dbt.cli.main import cli; sys.exit(cli())",
           *args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR),
           "--log-path", str(cfg.lake_dir / "dbt_logs"), "--target-path", str(cfg.lake_dir / "dbt_target")]
    if variables:
        cmd += ["--vars", json.dumps(variables)]
    res = subprocess.run(cmd, env=env, cwd=str(DBT_DIR))
    if res.returncode != 0:
        raise RuntimeError(f"dbt {' '.join(args)} terminó con código {res.returncode}; revisa la salida de dbt")


def construir_silver(cfg: Config, variables: Optional[dict] = None) -> None:
    crear_vistas(cfg)              # vistas de solo lectura sobre los Parquet de Bronze
    dbt(cfg, "build", variables=variables)


def _con_warehouse(cfg: Config):
    return duckdb.connect(str(cfg.lake_dir / "warehouse.duckdb"), read_only=True)


def contar_capas(cfg: Config) -> list[dict]:
    """Conteo (y huella del contenido) de cada tabla de Bronze, staging y Silver. Sirve como evidencia de idempotencia."""
    filas: list[dict] = []
    vistas = duckdb.connect(str(cfg.lake_dir / "bronze_vistas.duckdb"), read_only=True)
    for (nombre,) in vistas.execute(
            "select table_name from information_schema.tables where table_schema = 'bronze' order by 1").fetchall():
        n = vistas.execute(f"select count(*) from bronze.{nombre}").fetchone()[0]
        h = vistas.execute(f"select coalesce(bit_xor(hash(_file_hash || ':' || cast(_source_line_number as varchar))), 0) "
                           f"from bronze.{nombre}").fetchone()[0]
        filas.append({"capa": "bronze", "tabla": nombre, "filas": n, "huella": str(h)})
    vistas.close()
    wh = _con_warehouse(cfg)
    for t in TABLAS_STAGING:
        n = wh.execute(f"select count(*) from staging.{t}").fetchone()[0]
        filas.append({"capa": "staging", "tabla": t, "filas": n, "huella": ""})
    for t, llave in TABLAS_SILVER.items():
        n = wh.execute(f"select count(*) from silver.{t}").fetchone()[0]
        h = wh.execute(f"select coalesce(bit_xor(hash({llave})), 0) from silver.{t}").fetchone()[0]
        filas.append({"capa": "silver", "tabla": t, "filas": n, "huella": str(h)})
    for t in TABLAS_GOLD_DIM:
        existe = wh.execute(
            "select count(*) from information_schema.tables where table_schema='gold' and table_name=?", [t]
        ).fetchone()[0]
        if existe:
            n = wh.execute(f"select count(*) from gold.{t}").fetchone()[0]
            filas.append({"capa": "gold", "tabla": t, "filas": n, "huella": ""})
    for t, llave in TABLAS_GOLD_FACT.items():
        existe = wh.execute(
            "select count(*) from information_schema.tables where table_schema='gold' and table_name=?", [t]
        ).fetchone()[0]
        if existe:
            n = wh.execute(f"select count(*) from gold.{t}").fetchone()[0]
            h = wh.execute(f"select coalesce(bit_xor(hash({llave})), 0) from gold.{t}").fetchone()[0]
            filas.append({"capa": "gold", "tabla": t, "filas": n, "huella": str(h)})
    wh.close()
    return filas


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("comando", choices=["build", "docs"],
                    help="build: semillas + modelos + pruebas. docs: genera el grafo de linaje (lake/dbt_target/static_index.html)")
    ap.add_argument("--sin-unificar-identidad", action="store_true")
    a = ap.parse_args()
    cfg_ = Config.desde_entorno()
    if a.comando == "docs":
        crear_vistas(cfg_)
        dbt(cfg_, "docs", "generate", "--static")
        print(f"Linaje: abre {cfg_.lake_dir / 'dbt_target' / 'static_index.html'} en el navegador")
    else:
        construir_silver(cfg_, {"unificar_identidad_numerica": False} if a.sin_unificar_identidad else None)
