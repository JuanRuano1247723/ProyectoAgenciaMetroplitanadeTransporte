"""Builds a DuckDB warehouse from synthetic data for CI.

Usage:
    python -m tests.build_ci_warehouse --out .ci/warehouse.duckdb
"""
from __future__ import annotations

import argparse
from pathlib import Path

from bronze.common import nuevo_run_id
from bronze.config import Config
from bronze.conciliacion import crear_vistas
from bronze.fuentes import BATCH, CDC, FUENTES, STREAMING
from bronze.ingesta_archivos import ingerir_archivo
from bronze.streaming import ConsumidorBronze, publicar_archivo
from tests.datos_sinteticos import generar
from tests.fake_kafka import BrokerFake, ConsumidorFake, ProductorFake


def main(out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    work = out.parent / "work"
    work.mkdir(parents=True, exist_ok=True)

    # 1. synthetic source files
    generar(work / "datos_red")

    # 2. Config pointing at the temp dirs
    cfg = Config(
        data_dir=work / "datos_red",
        lake_dir=work / "lake",
        kafka_particiones=3,
        microlote_filas=400,
        microlote_segundos=5.0,
        chunk_filas=1000,
    )

    # 3. Batch + CDC
    run = nuevo_run_id()
    for f in BATCH + CDC:
        ingerir_archivo(cfg, f, run)

    # 4. Streaming (in-memory broker, no real Kafka)
    broker = BrokerFake(cfg.kafka_particiones)
    for f in STREAMING:
        publicar_archivo(cfg, f, cliente=ProductorFake(broker),
                         tamano_rafaga=500, pausa=0, dormir=lambda s: None)
        ConsumidorBronze(cfg, f, cliente=ConsumidorFake(broker)).ejecutar()

    # 5. Materialize views into the DuckDB file dbt will read
    ruta = crear_vistas(cfg)

    # 6. Move/copy the views DB to the requested output path
    if Path(ruta).resolve() != out.resolve():
        out.write_bytes(Path(ruta).read_bytes())
    print(f"CI warehouse ready at {out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=Path(".ci/warehouse.duckdb"))
    args = ap.parse_args()
    main(args.out)
