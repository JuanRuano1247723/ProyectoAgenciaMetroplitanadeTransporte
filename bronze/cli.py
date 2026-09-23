"""Línea de comandos de la capa Bronze.

  python -m bronze.cli topics
  python -m bronze.cli batch [--fuentes tm_estaciones metroriel_viajes ...]
  python -m bronze.cli cdc
  python -m bronze.cli produce transmetro_validaciones --tamano-rafaga 500 --pausa 2
  python -m bronze.cli consume transmetro_validaciones --modo drain
  python -m bronze.cli reiniciar-consumidor ambos --confirmar     (repara duplicados de transporte)
  python -m bronze.cli conciliar --csv salidas/conteos_bronze.csv
  python -m bronze.cli vistas
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .common import nuevo_run_id
from .config import Config
from .conciliacion import a_markdown, conciliar, crear_vistas, exportar_csv
from .fuentes import BATCH, CDC, FUENTES, STREAMING
from .ingesta_archivos import ingerir_archivo


def _claves_stream(nombre: str) -> list[str]:
    return [f.clave for f in STREAMING] if nombre == "ambos" else [nombre]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="bronze", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("topics", help="crea los topics de Kafka")
    p = sub.add_parser("batch", help="catálogos, Transurbano y MetroRiel")
    p.add_argument("--fuentes", nargs="*", choices=[f.clave for f in BATCH])
    sub.add_parser("cdc", help="log de cambios del padrón")
    p = sub.add_parser("produce", help="publica un CSV en Kafka")
    p.add_argument("fuente", choices=[f.clave for f in STREAMING] + ["ambos"])
    p.add_argument("--tamano-rafaga", type=int, default=500)
    p.add_argument("--pausa", type=float, default=2.0)
    p.add_argument("--max-filas", type=int, default=None)
    p = sub.add_parser("consume", help="consume un topic hacia Bronze")
    p.add_argument("fuente", choices=[f.clave for f in STREAMING] + ["ambos"])
    p.add_argument("--modo", choices=["drain", "continuo"], default="drain")
    p = sub.add_parser("reiniciar-consumidor", help="borra la tabla Bronze de streaming y su estado; se reconstruye con consume")
    p.add_argument("fuente", choices=[f.clave for f in STREAMING] + ["ambos"])
    p.add_argument("--confirmar", action="store_true", help="sin esta bandera solo muestra qué se borraría")
    p.add_argument("--incluir-productor", action="store_true", help="también olvida lo publicado (si los topics se recrearon)")
    p = sub.add_parser("conciliar", help="tabla de conteos por archivo")
    p.add_argument("--csv", type=Path)
    sub.add_parser("vistas", help="crea lake/bronze_vistas.duckdb")
    a = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg, run_id = Config.desde_entorno(), nuevo_run_id()

    if a.cmd == "topics":
        from .streaming import asegurar_topics
        asegurar_topics(cfg, [f.topic for f in STREAMING])
    elif a.cmd == "batch":
        for f in BATCH:
            if not a.fuentes or f.clave in a.fuentes:
                ingerir_archivo(cfg, f, run_id)
    elif a.cmd == "cdc":
        for f in CDC:
            ingerir_archivo(cfg, f, run_id)
    elif a.cmd == "produce":
        from .streaming import publicar_archivo
        for c in _claves_stream(a.fuente):
            publicar_archivo(cfg, FUENTES[c], tamano_rafaga=a.tamano_rafaga, pausa=a.pausa,
                             max_filas=a.max_filas, run_id=run_id)
    elif a.cmd == "consume":
        from .streaming import ConsumidorBronze
        for c in _claves_stream(a.fuente):
            ConsumidorBronze(cfg, FUENTES[c]).ejecutar(modo=a.modo, run_id=run_id)
    elif a.cmd == "reiniciar-consumidor":
        from .streaming import reiniciar_consumidor
        for cl in _claves_stream(a.fuente):
            r = reiniciar_consumidor(cfg, FUENTES[cl], confirmar=a.confirmar, incluir_productor=a.incluir_productor)
            print(r)
        if not a.confirmar:
            print("(simulación: agrega --confirmar para borrar; luego corre `consume`)")
    elif a.cmd == "conciliar":
        filas = conciliar(cfg)
        print(a_markdown(filas))
        if a.csv:
            exportar_csv(filas, a.csv)
        return 0 if all(r["estado"] == "OK" for r in filas) else 1
    elif a.cmd == "vistas":
        print(crear_vistas(cfg))
    return 0


if __name__ == "__main__":
    sys.exit(main())
