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
from bronze.fuentes import FUENTES

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
    "silver_dq_conteo_por_regla": "regla", "silver_evento_unificado": "evento_sk",
}
TABLAS_FASE2 = {"features.features_usuario": "persona_id", "gold.agg_cobertura_zona": "zona_id",
                "gold.agg_caso_metroriel": "zona_id", "gold.agg_usuarios_por_combinacion": "combinacion"}
TABLAS_GOLD_DIM = ["dim_transporte", "dim_fecha", "dim_hora", "dim_estacion", "dim_usuario"]
TABLAS_GOLD_FACT = {"fact_abordaje": "evento_sk", "fact_viaje": "evento_sk"}



def verificar_bronze_completo(cfg: Config) -> None:
    """Falla con un mensaje que dice qué hacer si falta alguna tabla de Bronze, en vez del error críptico de dbt
    ("Table with name ... does not exist") repetido decenas de veces."""
    requeridas = {f"{f.operador}_{f.entidad}": f for f in FUENTES.values()}
    ruta = cfg.lake_dir / "bronze_vistas.duckdb"
    presentes: set = set()
    if ruta.exists():
        con = duckdb.connect(str(ruta), read_only=True)
        try:
            presentes = {r[0] for r in con.execute(
                "select table_name from information_schema.tables where table_schema = 'bronze'").fetchall()}
        finally:
            con.close()
    faltan = sorted(set(requeridas) - presentes)
    if faltan:
        streaming = [n for n in faltan if requeridas[n].via == "streaming"]
        pistas = []
        if streaming:
            pistas.append("Streaming (Kafka): python -m bronze.cli consume ambos   (si el topic está vacío: python -m bronze.cli produce ambos --pausa 0 primero)")
        if [n for n in faltan if n not in streaming]:
            pistas.append("Batch y CDC: python -m bronze.cli batch   y   python -m bronze.cli cdc")
        raise RuntimeError(
            "Bronze está incompleto; faltan estas tablas: " + ", ".join(faltan) + ".\n  "
            + "\n  ".join(pistas) + f"\n  Revisa también que LAKE_DIR apunte al lake correcto (ahora: {cfg.lake_dir}).")


def dbt(cfg: Config, *args: str, variables: Optional[dict] = None) -> None:
    """Ejecuta dbt en un subproceso (así libera su conexión a DuckDB al terminar).
    Lanza excepción si hay errores; las advertencias no hacen fallar la ejecución."""
    cargar_env()
    if not os.environ.get("PSEUDONIMO_SECRETO"):
        raise RuntimeError(
            "Falta PSEUDONIMO_SECRETO. Es el secreto con el que se seudonimizan las llaves de usuario antes de Gold.\n"
            "  Copia .env.example a .env y define un valor largo y aleatorio, o en PowerShell:\n"
            "  $env:PSEUDONIMO_SECRETO = \"<valor largo y aleatorio>\"   (no lo subas a Git)")
    if args and args[0] == "build":
        verificar_bronze_completo(cfg)
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
    for t, llave in TABLAS_FASE2.items():
        esq, nom = t.split(".")
        if wh.execute("select count(*) from information_schema.tables where table_schema=? and table_name=?",
                      [esq, nom]).fetchone()[0]:
            n = wh.execute(f"select count(*) from {t}").fetchone()[0]
            h = wh.execute(f"select coalesce(bit_xor(hash({llave})), 0) from {t}").fetchone()[0]
            filas.append({"capa": esq, "tabla": nom, "filas": n, "huella": str(h)})
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
