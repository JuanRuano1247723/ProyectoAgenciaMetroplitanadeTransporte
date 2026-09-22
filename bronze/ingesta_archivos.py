"""Ingesta batch y CDC hacia Bronze.

Cubre las vías que leen un archivo completo: los cuatro catálogos, Transurbano, MetroRiel (batch)
y el log de cambios del padrón (CDC). Cada línea física del archivo termina en una de dos partes:
la tabla Bronze o la tabla de malformadas. Nunca se descarta una línea.

Idempotencia: si el hash del archivo ya tiene una corrida OK en la bitácora, se omite. Si una
corrida anterior murió a medias (archivos escritos, sin registro OK), se revierten sus archivos
(nombre `part-<hash16>-*`) y se reescribe: los nombres son deterministas, así que jamás hay
duplicados.
"""
from __future__ import annotations

import csv
import json
import logging
import time
from collections import Counter
from pathlib import Path
from typing import Callable, Optional

import pyarrow as pa

from .common import (Acumulador, aterrizar, escribir_buffers, escribir_parquet, esquema_bronze,
                     esquema_malformadas, fecha_particion, nuevo_registro, registrar_log,
                     sha256_archivo, utcnow, ya_ingerido)
from .config import Config
from .fuentes import FUENTES, Fuente

log = logging.getLogger("bronze.archivos")


class EsquemaInesperado(Exception):
    """El encabezado del archivo no coincide con el esperado."""


class Malformado(Exception):
    """La línea no se pudo interpretar; se guarda en la tabla de malformadas."""


# ------------------------------------------------------------------ parsers por formato
def _txt(v) -> Optional[str]:
    if v is None or isinstance(v, str):
        return v
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def parser_csv(fuente: Fuente) -> Callable[[str], tuple[dict, Optional[str]]]:
    n = len(fuente.columnas)
    idx = fuente.indice_particion

    def parse(raw: str):
        if raw == "":
            raise Malformado("línea vacía")
        if '"' in raw:
            try:
                campos = next(csv.reader([raw]))
            except (csv.Error, StopIteration) as e:
                raise Malformado(f"CSV inválido: {e}")
        else:
            campos = raw.split(",")
        if len(campos) != n:
            raise Malformado(f"{len(campos)} campos; se esperaban {n}")
        valores = [c if c != "" else None for c in campos]   # CSV no distingue vacío de nulo
        return dict(zip(fuente.columnas, valores)), (valores[idx] if idx is not None else None)

    return parse


_CAMPOS_JSON = {"trip_id", "card", "entry", "exit", "fare_gtq", "duration_s"}


def parser_metroriel(extras: Counter) -> Callable[[str], tuple[dict, Optional[str]]]:
    def punto(v, nombre):
        if v is None:
            return None
        if not isinstance(v, dict):
            raise Malformado(f"'{nombre}' no es un objeto")
        for k in v.keys() - {"station", "ts"}:
            extras[f"{nombre}.{k}"] += 1
        return {"station": _txt(v.get("station")), "ts": _txt(v.get("ts"))}

    def parse(raw: str):
        if not raw.strip():
            raise Malformado("línea vacía")
        try:
            # parse_int/parse_float=str conservan el texto exacto de los números (12.50 sigue siendo "12.50")
            obj = json.loads(raw, parse_float=str, parse_int=str)
        except json.JSONDecodeError as e:
            raise Malformado(f"JSON inválido: {e.msg}")
        if not isinstance(obj, dict):
            raise Malformado("el registro no es un objeto JSON")
        for k in obj.keys() - _CAMPOS_JSON:
            extras[k] += 1
        fila = {
            "trip_id": _txt(obj.get("trip_id")), "card": _txt(obj.get("card")),
            "entry": punto(obj.get("entry"), "entry"), "exit": punto(obj.get("exit"), "exit"),
            "fare_gtq": _txt(obj.get("fare_gtq")), "duration_s": _txt(obj.get("duration_s")),
            "_raw_json": raw,
        }
        return fila, (fila["entry"]["ts"] if fila["entry"] else None)

    return parse


# ------------------------------------------------------------------ limpieza de intentos sin confirmar
def revertir_intento_previo(cfg: Config, fuente: Fuente, h16: str) -> int:
    """Borra archivos de un intento anterior que no llegó a registrarse como OK."""
    n = 0
    for base in (cfg.bronze_dir / fuente.operador / fuente.entidad,
                 cfg.malformed_dir / fuente.operador / fuente.entidad):
        if base.exists():
            for p in base.glob(f"**/part-{h16}-*.parquet"):
                p.unlink()
                n += 1
    return n


# ------------------------------------------------------------------ ingesta de un archivo
def ingerir_archivo(cfg: Config, fuente: Fuente, run_id: str, ruta: Optional[Path] = None) -> dict:
    ruta = ruta or (cfg.data_dir / fuente.archivo)
    if not ruta.exists():
        raise FileNotFoundError(ruta)
    etapa = fuente.via                                  # "batch" | "cdc"
    t0, iniciado = time.monotonic(), utcnow()
    file_hash = sha256_archivo(ruta)
    h16 = file_hash[:16]

    previo = ya_ingerido(cfg, etapa, fuente.clave, file_hash)
    if previo:
        reg = nuevo_registro(run_id, etapa, fuente, source_file=ruta.name, file_hash=file_hash,
                             estado="SKIPPED", observaciones=f"hash ya ingerido en la corrida {previo}",
                             iniciado_en=iniciado, terminado_en=utcnow(),
                             duracion_s=round(time.monotonic() - t0, 3))
        registrar_log(cfg, reg)
        log.info("%s: omitido (hash ya ingerido en %s)", fuente.clave, previo)
        return reg

    encabezado_original: Optional[str] = None
    try:
        aterrizar(cfg, fuente, ruta, file_hash, iniciado.date())
        revertidos = revertir_intento_previo(cfg, fuente, h16)
        if revertidos:
            log.warning("%s: se revirtieron %d archivos de un intento sin confirmar", fuente.clave, revertidos)

        fecha_ingesta = iniciado.date().isoformat()
        esquema, esquema_mal = esquema_bronze(fuente), esquema_malformadas(fuente)
        tabla_dir = cfg.bronze_dir / fuente.operador / fuente.entidad
        mal_dir = cfg.malformed_dir / fuente.operador / fuente.entidad
        acum, malos = Acumulador(esquema), []
        extras: Counter = Counter()
        parse = parser_metroriel(extras) if fuente.formato == "jsonl" else parser_csv(fuente)
        leidas = escritas = por_defecto = archivos = 0
        fechas: set[str] = set()
        chunk = 0

        def vaciar():
            nonlocal archivos, chunk
            buf = acum.extraer()
            if buf:
                archivos += escribir_buffers(cfg, tabla_dir, esquema, buf, f"part-{h16}-{chunk:05d}.parquet")
                chunk += 1

        with open(ruta, encoding="utf-8-sig", newline="", errors="replace") as f:
            primera_linea = 1
            if fuente.formato == "csv":
                encabezado_original = f.readline().rstrip("\r\n")
                cols = ([c.strip().lower() for c in next(csv.reader([encabezado_original]))]
                        if encabezado_original else [])
                if cols != list(fuente.columnas):
                    raise EsquemaInesperado(
                        f"{ruta.name}: encabezado {cols} distinto del esperado {list(fuente.columnas)}")
                primera_linea = 2

            for n, linea in enumerate(f, start=primera_linea):
                leidas += 1
                raw = linea.rstrip("\r\n")
                meta = {"_ingested_at": iniciado, "_batch_id": run_id, "_source_file": ruta.name,
                        "_file_hash": file_hash, "_source_line_number": n}
                try:
                    fila, valor_part = parse(raw)
                except Malformado as e:
                    malos.append({"_raw_line": raw, "_motivo": str(e), **meta})
                    continue
                fecha, defecto = fecha_particion(valor_part, fuente.regla_particion, fecha_ingesta)
                por_defecto += defecto
                fechas.add(fecha)
                acum.agregar(fecha, {**fila, **meta})
                escritas += 1
                if acum.total >= cfg.chunk_filas:
                    vaciar()
            vaciar()

        if malos:
            tabla = pa.Table.from_pylist(malos, schema=esquema_mal)
            escribir_parquet(tabla, mal_dir / f"part-{h16}-00000.parquet", cfg.compresion)

        if leidas != escritas + len(malos):     # invariante de conciliación
            raise AssertionError(f"Descuadre: leídas={leidas} escritas={escritas} malformadas={len(malos)}")

        obs = json.dumps({"campos_no_esperados": dict(extras)}, ensure_ascii=False) if extras else None
        reg = nuevo_registro(run_id, etapa, fuente, source_file=ruta.name, file_hash=file_hash,
                             encabezado_original=encabezado_original, filas_leidas_origen=leidas,
                             filas_escritas_bronze=escritas, filas_malformadas=len(malos),
                             filas_particion_por_defecto=por_defecto, particiones=len(fechas),
                             archivos_parquet=archivos, ultima_linea=primera_linea - 1 + leidas,
                             estado="OK", observaciones=obs, iniciado_en=iniciado,
                             terminado_en=utcnow(), duracion_s=round(time.monotonic() - t0, 3))
        registrar_log(cfg, reg)        # el registro OK es el "commit" de la carga
        log.info("%s: %d filas en Bronze, %d malformadas, %d particiones (%.1fs)",
                 fuente.clave, escritas, len(malos), len(fechas), reg["duracion_s"])
        return reg

    except Exception as e:
        reg = nuevo_registro(run_id, etapa, fuente, source_file=ruta.name, file_hash=file_hash,
                             encabezado_original=encabezado_original, estado="FAILED",
                             observaciones=f"{type(e).__name__}: {e}", iniciado_en=iniciado,
                             terminado_en=utcnow(), duracion_s=round(time.monotonic() - t0, 3))
        registrar_log(cfg, reg)
        log.error("%s: FALLÓ (%s)", fuente.clave, e)
        raise


def ingerir(cfg: Config, clave: str, run_id: str) -> dict:
    return ingerir_archivo(cfg, FUENTES[clave], run_id)
