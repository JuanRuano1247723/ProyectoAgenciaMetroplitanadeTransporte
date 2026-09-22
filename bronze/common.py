"""Utilidades compartidas por todas las vías de ingesta.

Reglas que este módulo hace cumplir:
  * Bronze es solo *append*: los archivos Parquet se escriben con nombre determinista y de forma
    atómica (archivo temporal + rename). Reejecutar una carga reemplaza el mismo archivo, nunca
    lo duplica.
  * Todas las columnas de datos son STRING tal como llegaron. Solo los metadatos (`_ingested_at`,
    offsets, número de línea) tienen tipo.
  * La bitácora de ingesta es un dataset Parquet de solo-append (un archivo pequeño por evento).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from .config import Config
from .fuentes import Fuente

log = logging.getLogger("bronze")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def nuevo_run_id() -> str:
    return utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


# ------------------------------------------------------------------ archivos de origen
def sha256_archivo(ruta: Path, bloque: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        while chunk := f.read(bloque):
            h.update(chunk)
    return h.hexdigest()


def contar_lineas_origen(ruta: Path, con_encabezado: bool) -> int:
    """Conteo independiente de la ingesta: líneas físicas del archivo (sin encabezado)."""
    n = 0
    with open(ruta, "rb") as f:
        for _ in f:
            n += 1
    return n - 1 if con_encabezado else n


def aterrizar(cfg: Config, fuente: Fuente, ruta: Path, file_hash: str, fecha_ingesta: date) -> Path:
    """Copia cruda del archivo original (zona *landing*), verificada por hash. Idempotente."""
    destino_dir = cfg.landing_dir / fuente.operador / fuente.entidad / f"ingest_date={fecha_ingesta.isoformat()}"
    destino = destino_dir / f"{file_hash[:16]}__{ruta.name}"
    existentes = list((cfg.landing_dir / fuente.operador / fuente.entidad).glob(f"**/{file_hash[:16]}__*"))
    if existentes:
        return existentes[0]
    destino_dir.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_name(f".tmp-{uuid.uuid4().hex}")
    shutil.copyfile(ruta, tmp)
    if sha256_archivo(tmp) != file_hash:
        tmp.unlink(missing_ok=True)
        raise IOError(f"La copia en landing de {ruta.name} no coincide con el hash de origen")
    os.replace(tmp, destino)
    return destino


# ------------------------------------------------------------------ fecha de partición
_ISO = re.compile(r"^(\d{4}-\d{2}-\d{2})")
_DMY = re.compile(r"^(\d{2})/(\d{2})/(\d{4})")


def fecha_particion(valor: Optional[str], regla: str, fecha_ingesta: str) -> tuple[str, bool]:
    """Devuelve (YYYY-MM-DD, usó_valor_por_defecto). Solo manipula texto: no interpreta fechas."""
    if regla == "iso" and valor:
        m = _ISO.match(valor.strip())
        if m:
            return m.group(1), False
    elif regla == "dmy" and valor:
        m = _DMY.match(valor.strip())
        if m:
            d, mes, anio = m.groups()
            return f"{anio}-{mes}-{d}", False
    return fecha_ingesta, regla != "ingesta"


# ------------------------------------------------------------------ esquemas
_TS_US = pa.timestamp("us", tz="UTC")
_TS_MS = pa.timestamp("ms", tz="UTC")
STRUCT_PUNTO = pa.struct([("station", pa.string()), ("ts", pa.string())])


def _campos_meta(fuente: Fuente, kafka: bool) -> list[pa.Field]:
    campos = [
        pa.field("_ingested_at", _TS_US),
        pa.field("_batch_id", pa.string()),
        pa.field("_source_file", pa.string()),
        pa.field("_file_hash", pa.string()),
        pa.field("_source_line_number", pa.int64()),
    ]
    if fuente.formato == "jsonl":
        campos.append(pa.field("_raw_json", pa.string()))
    if kafka:
        campos += [
            pa.field("_kafka_topic", pa.string()),
            pa.field("_kafka_partition", pa.int32()),
            pa.field("_kafka_offset", pa.int64()),
            pa.field("_kafka_timestamp", _TS_MS),
        ]
    return campos


def esquema_bronze(fuente: Fuente) -> pa.Schema:
    kafka = fuente.via == "streaming"
    if fuente.clave == "metroriel_viajes":
        datos = [
            pa.field("trip_id", pa.string()), pa.field("card", pa.string()),
            pa.field("entry", STRUCT_PUNTO), pa.field("exit", STRUCT_PUNTO),
            pa.field("fare_gtq", pa.string()), pa.field("duration_s", pa.string()),
        ]
    else:
        datos = [pa.field(c, pa.string()) for c in fuente.columnas]
    return pa.schema(datos + _campos_meta(fuente, kafka))


def esquema_malformadas(fuente: Fuente) -> pa.Schema:
    kafka = fuente.via == "streaming"
    campos = [
        pa.field("_raw_line", pa.string()),
        pa.field("_motivo", pa.string()),
        pa.field("_ingested_at", _TS_US),
        pa.field("_batch_id", pa.string()),
        pa.field("_source_file", pa.string()),
        pa.field("_file_hash", pa.string()),
        pa.field("_source_line_number", pa.int64()),
    ]
    if kafka:
        campos += [
            pa.field("_kafka_topic", pa.string()),
            pa.field("_kafka_partition", pa.int32()),
            pa.field("_kafka_offset", pa.int64()),
            pa.field("_kafka_timestamp", _TS_MS),
        ]
    return pa.schema(campos)


# ------------------------------------------------------------------ escritura Parquet
def escribir_parquet(tabla: pa.Table, destino: Path, compresion: str) -> None:
    """Escritura atómica: archivo temporal en el mismo directorio y rename."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_name(f".tmp-{uuid.uuid4().hex}.parquet")
    try:
        pq.write_table(tabla, tmp, compression=compresion)
        os.replace(tmp, destino)
    finally:
        tmp.unlink(missing_ok=True)


class Acumulador:
    """Buffer columnar por fecha de partición."""

    def __init__(self, esquema: pa.Schema):
        self.esquema = esquema
        self.nombres = esquema.names
        self.total = 0
        self._buf: dict[str, dict[str, list]] = {}

    def agregar(self, fecha: Optional[str], fila: dict) -> None:
        cols = self._buf.get(fecha)
        if cols is None:
            cols = self._buf[fecha] = {n: [] for n in self.nombres}
        for n in self.nombres:
            cols[n].append(fila[n])
        self.total += 1

    def extraer(self) -> dict[str, dict[str, list]]:
        buf, self._buf, self.total = self._buf, {}, 0
        return buf


def escribir_buffers(cfg: Config, tabla_dir: Path, esquema: pa.Schema,
                     buffers: dict, nombre_archivo: str, particionado: bool = True) -> int:
    """Escribe un archivo Parquet por partición. Devuelve el número de archivos escritos."""
    n = 0
    for fecha, cols in buffers.items():
        tabla = pa.Table.from_pydict(cols, schema=esquema)
        destino = (tabla_dir / f"_partition_date={fecha}" / nombre_archivo) if particionado \
            else (tabla_dir / nombre_archivo)
        escribir_parquet(tabla, destino, cfg.compresion)
        n += 1
    return n


def contar_particiones(tabla_dir: Path) -> int:
    return len([d for d in tabla_dir.glob("_partition_date=*") if d.is_dir()]) if tabla_dir.exists() else 0


# ------------------------------------------------------------------ bitácora de ingesta
LOG_SCHEMA = pa.schema([
    ("run_id", pa.string()), ("etapa", pa.string()), ("clave", pa.string()),
    ("operador", pa.string()), ("entidad", pa.string()),
    ("source_file", pa.string()), ("file_hash", pa.string()), ("encabezado_original", pa.string()),
    ("filas_leidas_origen", pa.int64()), ("filas_escritas_bronze", pa.int64()),
    ("filas_malformadas", pa.int64()), ("filas_particion_por_defecto", pa.int64()),
    ("particiones", pa.int32()), ("archivos_parquet", pa.int32()), ("ultima_linea", pa.int64()),
    ("estado", pa.string()), ("observaciones", pa.string()),
    ("iniciado_en", _TS_US), ("terminado_en", _TS_US), ("duracion_s", pa.float64()),
])


def registrar_log(cfg: Config, registro: dict) -> Path:
    fila = {c: registro.get(c) for c in LOG_SCHEMA.names}
    tabla = pa.Table.from_pylist([fila], schema=LOG_SCHEMA)
    h16 = (registro.get("file_hash") or "na")[:16]
    nombre = f"{registro['etapa']}__{registro['clave']}__{h16}__{registro['run_id']}.parquet"
    destino = cfg.log_dir / nombre
    escribir_parquet(tabla, destino, cfg.compresion)
    return destino


def leer_log(cfg: Config) -> list[dict]:
    if not cfg.log_dir.exists():
        return []
    filas: list[dict] = []
    for p in sorted(cfg.log_dir.glob("*.parquet")):
        filas += pq.read_table(p).to_pylist()
    return sorted(filas, key=lambda r: r["iniciado_en"])


def ya_ingerido(cfg: Config, etapa: str, clave: str, file_hash: str) -> Optional[str]:
    """Devuelve el run_id de la corrida OK que ya cargó este hash, o None."""
    if not cfg.log_dir.exists():
        return None
    for p in cfg.log_dir.glob(f"{etapa}__{clave}__{file_hash[:16]}__*.parquet"):
        for r in pq.read_table(p).to_pylist():
            if r["estado"] == "OK" and r["file_hash"] == file_hash:
                return r["run_id"]
    return None


def nuevo_registro(run_id: str, etapa: str, fuente: Fuente, **kw) -> dict:
    base = dict(run_id=run_id, etapa=etapa, clave=fuente.clave, operador=fuente.operador,
                entidad=fuente.entidad, filas_leidas_origen=0, filas_escritas_bronze=0,
                filas_malformadas=0, filas_particion_por_defecto=0, particiones=0, archivos_parquet=0)
    base.update(kw)
    return base


# ------------------------------------------------------------------ checkpoints simples (JSON)
def escribir_json_atomico(destino: Path, contenido: dict) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_name(f".tmp-{uuid.uuid4().hex}")
    tmp.write_text(json.dumps(contenido, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, destino)
