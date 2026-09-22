"""Configuración central. Todo puede sobreescribirse con variables de entorno."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Config:
    data_dir: Path
    lake_dir: Path
    kafka_bootstrap: str = "localhost:9092"
    kafka_particiones: int = 3
    kafka_grupo: str = "bronze-consumer"
    microlote_filas: int = 10_000        # micro-lote: N filas ...
    microlote_segundos: float = 5.0      # ... o T segundos, lo que ocurra primero
    chunk_filas: int = 200_000           # tope de filas en memoria en cargas batch
    compresion: str = "zstd"

    # ---- estructura del lake -------------------------------------------------
    @property
    def landing_dir(self) -> Path:
        return self.lake_dir / "landing"

    @property
    def bronze_dir(self) -> Path:
        return self.lake_dir / "bronze"

    @property
    def malformed_dir(self) -> Path:
        return self.bronze_dir / "_malformed"

    @property
    def control_dir(self) -> Path:
        return self.bronze_dir / "_control"

    @property
    def log_dir(self) -> Path:
        return self.control_dir / "ingest_log"

    @property
    def commits_dir(self) -> Path:
        return self.control_dir / "kafka_commits"

    @property
    def checkpoints_dir(self) -> Path:
        return self.control_dir / "producer_checkpoints"

    @classmethod
    def desde_entorno(cls) -> "Config":
        return cls(
            data_dir=Path(os.getenv("DATA_DIR", RAIZ / "datos_red")),
            lake_dir=Path(os.getenv("LAKE_DIR", RAIZ / "lake")),
            kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
            kafka_particiones=int(os.getenv("KAFKA_PARTICIONES", 3)),
            kafka_grupo=os.getenv("KAFKA_GRUPO", "bronze-consumer"),
            microlote_filas=int(os.getenv("MICROLOTE_FILAS", 10_000)),
            microlote_segundos=float(os.getenv("MICROLOTE_SEGUNDOS", 5)),
            chunk_filas=int(os.getenv("CHUNK_FILAS", 200_000)),
        )
