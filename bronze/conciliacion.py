"""Conciliación por archivo: origen = Bronze + malformadas.

El conteo de origen es independiente de la ingesta (líneas físicas del archivo, menos el
encabezado) y el de Bronze se toma leyendo los Parquet, filtrando por el hash del archivo actual.
Así detecta cargas incompletas, archivos borrados y descuadres, en lugar de repetir el conteo
que la propia carga reportó.
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Optional

import duckdb

from .common import contar_lineas_origen, sha256_archivo
from .config import Config
from .fuentes import FUENTES


def _hay_parquet(d: Path) -> bool:
    return d.exists() and any(d.glob("**/*.parquet"))


def _contar(con, d: Path, file_hash: str) -> int:
    if not _hay_parquet(d):
        return 0
    return con.execute("SELECT count(*) FROM read_parquet(?, union_by_name=true) WHERE _file_hash = ?",
                       [str(d / "**" / "*.parquet"), file_hash]).fetchone()[0]


def conciliar(cfg: Config, claves: Optional[list[str]] = None) -> list[dict]:
    con = duckdb.connect()
    filas = []
    for f in FUENTES.values():
        if claves and f.clave not in claves:
            continue
        ruta = cfg.data_dir / f.archivo
        if not ruta.exists():
            filas.append({"fuente": f.clave, "via": f.via, "archivo": f.archivo, "estado": "SIN_ORIGEN"})
            continue
        h = sha256_archivo(ruta)
        origen = contar_lineas_origen(ruta, f.formato == "csv")
        tabla_dir = cfg.bronze_dir / f.operador / f.entidad
        bronze = _contar(con, tabla_dir, h)
        malf = _contar(con, cfg.malformed_dir / f.operador / f.entidad, h)
        dif = origen - bronze - malf
        if dif == 0:
            estado = "OK"
        elif bronze == 0 and malf == 0:
            estado = "NO_INGERIDO"
        else:
            estado = "DESCUADRE"
        filas.append({
            "fuente": f.clave, "via": f.via, "archivo": f.archivo,
            "filas_origen": origen, "filas_bronze": bronze, "filas_malformadas": malf,
            "diferencia": dif,
            "particiones": len(list(tabla_dir.glob("_partition_date=*"))) if tabla_dir.exists() else 0,
            "estado": estado,
        })
    return filas


def a_markdown(filas: list[dict]) -> str:
    cols = ["fuente", "via", "archivo", "filas_origen", "filas_bronze", "filas_malformadas",
            "diferencia", "particiones", "estado"]
    enc = "| " + " | ".join(cols) + " |\n|" + "|".join("---" for _ in cols) + "|\n"
    cuerpo = ""
    for r in filas:
        vals = [f"{r.get(c):,}" if isinstance(r.get(c), int) else str(r.get(c, "")) for c in cols]
        cuerpo += "| " + " | ".join(vals) + " |\n"
    return enc + cuerpo


def exportar_csv(filas: list[dict], destino: Path) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    cols = ["fuente", "via", "archivo", "filas_origen", "filas_bronze", "filas_malformadas",
            "diferencia", "particiones", "estado"]
    with open(destino, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(filas)


def crear_vistas(cfg: Config, destino: Optional[Path] = None) -> Path:
    """Base DuckDB con una vista por tabla Bronze (y por tabla de malformadas), para consultas y dbt."""
    destino = destino or (cfg.lake_dir / "bronze_vistas.duckdb")
    destino.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(destino))
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    for f in FUENTES.values():
        for prefijo, base in (("", cfg.bronze_dir), ("malformadas_", cfg.malformed_dir)):
            d = base / f.operador / f.entidad
            if _hay_parquet(d):
                con.execute(
                    f"CREATE OR REPLACE VIEW bronze.{prefijo}{f.operador}_{f.entidad} AS "
                    f"SELECT * FROM read_parquet('{d / '**' / '*.parquet'}', hive_partitioning=true, "
                    f"union_by_name=true)")
    con.close()
    return destino
