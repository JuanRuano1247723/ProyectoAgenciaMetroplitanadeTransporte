"""Exporta las tablas de Gold que consume Tableau a CSV (UTF-8), por si el conector DuckDB no está disponible.

    python -m flows.exportar_tableau                    # agregados y dimensiones (pequeños, rápidos)
    python -m flows.exportar_tableau --incluir-hechos   # además fact_abordaje y fact_viaje (para trazar hasta el crudo)

Escribe en salidas/tableau/. Solo exporta el esquema gold: nunca silver (zona restringida) ni llaves crudas.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

from bronze.config import RAIZ, Config

AGREGADOS = ["agg_demanda_zona_hora", "agg_cobertura_zona", "agg_usuarios_por_combinacion",
             "agg_transbordo_temporal", "agg_caso_metroriel", "agg_od_metroriel"]
DIMENSIONES = ["dim_fecha", "dim_hora", "dim_transporte", "dim_estacion"]
HECHOS = ["fact_abordaje", "fact_viaje"]


def exportar(cfg: Config, destino: Path, hechos: bool = False) -> list[tuple[str, int]]:
    destino.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(cfg.lake_dir / "warehouse.duckdb"), read_only=True)
    salida = []
    try:
        for t in AGREGADOS + DIMENSIONES + (HECHOS if hechos else []):
            existe = con.execute("select count(*) from information_schema.tables where table_schema='gold' and table_name=?",
                                 [t]).fetchone()[0]
            if not existe:
                print(f"  (omitida: gold.{t} no existe; corre primero python -m flows.dbt_runner build)")
                continue
            ruta = destino / f"{t}.csv"
            con.execute(f"copy (select * from gold.{t}) to '{ruta.as_posix()}' (header, delimiter ',')")
            salida.append((t, con.execute(f"select count(*) from gold.{t}").fetchone()[0]))
    finally:
        con.close()
    return salida


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--incluir-hechos", action="store_true")
    ap.add_argument("--destino", default=str(RAIZ / "salidas" / "tableau"))
    a = ap.parse_args()
    for nombre, n in exportar(Config.desde_entorno(), Path(a.destino), a.incluir_hechos):
        print(f"  {nombre}.csv  {n:,} filas")
    sys.exit(0)
