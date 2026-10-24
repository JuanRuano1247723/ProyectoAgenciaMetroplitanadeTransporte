"""Ejecuta un archivo .sql con directivas `.print` (formato de la CLI de DuckDB) desde Python.

Sirve en Windows/PowerShell, donde `duckdb base.duckdb < archivo.sql` no funciona (PowerShell no admite `<`)
y `pip install duckdb` no instala el ejecutable `duckdb`.

    python -m flows.consultas sql/entregables_silver.sql          # usa lake/warehouse.duckdb
    python -m flows.consultas sql/verificacion_post_bronze.sql    # usa lake/bronze_vistas.duckdb
    python -m flows.consultas sql/entregables_silver.sql --db otra_base.duckdb
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import duckdb

from bronze.config import Config


def _tabla(columnas: list[str], filas: list[tuple], ancho_max: int = 60) -> str:
    def celda(v) -> str:
        t = "NULL" if v is None else str(v)
        return t if len(t) <= ancho_max else t[: ancho_max - 1] + "…"

    datos = [[celda(v) for v in fila] for fila in filas]
    anchos = [max([len(c)] + [len(f[i]) for f in datos]) for i, c in enumerate(columnas)]
    linea = lambda vals: " | ".join(v.ljust(anchos[i]) for i, v in enumerate(vals))
    salida = [linea(columnas), "-+-".join("-" * a for a in anchos)]
    salida += [linea(f) for f in datos]
    if not datos:
        salida.append("(sin filas)")
    return "\n".join(salida)


def ejecutar(sql_path: Path, db_path: Path) -> int:
    texto = sql_path.read_text(encoding="utf-8")
    bloques = re.split(r"\n(?=\.print)", texto[texto.index(".print"):])
    con = duckdb.connect(str(db_path), read_only=True)
    errores = 0
    for b in bloques:
        titulo = re.match(r"\.print '(.*)'", b).group(1)
        consulta = "\n".join(l for l in re.sub(r"^\.print '.*'\n", "", b).splitlines()
                             if not l.strip().startswith("--")).strip().rstrip(";")
        print(f"\n{titulo}")
        try:
            cur = con.execute(consulta)
            print(_tabla([d[0] for d in cur.description], cur.fetchall()))
        except Exception as e:                       # una consulta que falla no debe ocultar las demás
            errores += 1
            print(f"  ERROR: {e}")
    con.close()
    return errores


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("archivo_sql", type=Path)
    ap.add_argument("--db", type=Path)
    a = ap.parse_args()
    cfg = Config.desde_entorno()
    db = a.db or (cfg.lake_dir / ("warehouse.duckdb" if "silver" in a.archivo_sql.name else "bronze_vistas.duckdb"))
    if not db.exists():
        sys.exit(f"No existe {db}. Corre primero Bronze y `python -m flows.dbt_runner build`.")
    sys.exit(1 if ejecutar(a.archivo_sql, db) else 0)
