"""Vía streaming: productor que simula el flujo y consumidor que escribe micro-lotes a Bronze.

Diseño de garantías
-------------------
* Productor: publica el CSV línea por línea (el mensaje es la línea cruda; el encabezado no se
  publica). Guarda un checkpoint tras cada ráfaga confirmada; si se interrumpe, retoma desde la
  última ráfaga confirmada (semántica *at-least-once*: como máximo se repite una ráfaga).
* Consumidor: asigna las particiones manualmente y arranca en (último offset confirmado + 1).
  Cada micro-lote se escribe en Parquet con nombre `part-k<partición>-<primer>-<último>` y SOLO
  después se escribe el marcador de confirmación. Si el proceso muere entre ambos pasos, al
  reiniciar se revierten los archivos sin marcador y el micro-lote se repite: exactly-once hacia
  Bronze sin duplicados. El commit de offsets en Kafka es solo informativo.
* Micro-lote: N filas o T segundos, lo que ocurra primero (la cola final no queda sin escribir).

Los clientes de Kafka se inyectan (`cliente=`), lo que permite probar la lógica sin un broker.
"""
from __future__ import annotations

import csv
import logging
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

import pyarrow as pa
from confluent_kafka import TIMESTAMP_NOT_AVAILABLE, TopicPartition

from .common import (aterrizar, escribir_buffers, escribir_json_atomico, escribir_parquet,
                     esquema_bronze, esquema_malformadas, fecha_particion, nuevo_registro,
                     nuevo_run_id, registrar_log, sha256_archivo, utcnow, ya_ingerido)
from .config import Config
from .fuentes import Fuente
from .ingesta_archivos import EsquemaInesperado, Malformado, parser_csv

log = logging.getLogger("bronze.streaming")


# ====================================================================== clientes reales
def crear_productor(cfg: Config):
    from confluent_kafka import Producer
    return Producer({"bootstrap.servers": cfg.kafka_bootstrap, "enable.idempotence": True,
                     "acks": "all", "linger.ms": 20, "compression.type": "zstd"})


def crear_consumidor(cfg: Config):
    from confluent_kafka import Consumer
    return Consumer({"bootstrap.servers": cfg.kafka_bootstrap, "group.id": cfg.kafka_grupo,
                     "enable.auto.commit": False, "auto.offset.reset": "earliest"})


def asegurar_topics(cfg: Config, topics: list[str], admin=None) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = admin or AdminClient({"bootstrap.servers": cfg.kafka_bootstrap})
    futuros = admin.create_topics([NewTopic(t, num_partitions=cfg.kafka_particiones, replication_factor=1)
                                   for t in topics])
    for topic, fut in futuros.items():
        try:
            fut.result()
            log.info("topic creado: %s", topic)
        except Exception as e:                       # ya existe
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                raise


# ====================================================================== productor
def _ruta_checkpoint(cfg: Config, fuente: Fuente, h16: str) -> Path:
    return cfg.checkpoints_dir / f"{fuente.clave}__{h16}.json"


def _leer_checkpoint(cfg: Config, fuente: Fuente, h16: str) -> int:
    import json
    p = _ruta_checkpoint(cfg, fuente, h16)
    return json.loads(p.read_text())["ultima_linea"] if p.exists() else 1   # 1 = solo el encabezado


def _llave_mensaje(raw: str, fuente: Fuente) -> Optional[bytes]:
    campos = next(csv.reader([raw])) if '"' in raw else raw.split(",")
    if len(campos) != len(fuente.columnas):
        return None
    valor = campos[fuente.indice_llave]
    return valor.encode() if valor else None


def publicar_archivo(cfg: Config, fuente: Fuente, *, cliente=None, tamano_rafaga: int = 500,
                     pausa: float = 2.0, max_filas: Optional[int] = None, run_id: Optional[str] = None,
                     dormir: Callable[[float], None] = time.sleep) -> dict:
    """Publica el CSV en ráfagas de `tamano_rafaga` filas con `pausa` segundos entre ráfagas."""
    run_id = run_id or nuevo_run_id()
    ruta = cfg.data_dir / fuente.archivo
    t0, iniciado = time.monotonic(), utcnow()
    file_hash = sha256_archivo(ruta)
    h16 = file_hash[:16]

    previo = ya_ingerido(cfg, "stream_produce", fuente.clave, file_hash)
    if previo:
        reg = nuevo_registro(run_id, "stream_produce", fuente, source_file=ruta.name, file_hash=file_hash,
                             estado="SKIPPED", observaciones=f"archivo ya publicado por la corrida {previo}",
                             iniciado_en=iniciado, terminado_en=utcnow(), duracion_s=0.0)
        registrar_log(cfg, reg)
        return reg

    cliente = cliente or crear_productor(cfg)
    aterrizar(cfg, fuente, ruta, file_hash, iniciado.date())
    desde = _leer_checkpoint(cfg, fuente, h16)               # última línea ya confirmada
    errores: list = []
    publicadas = en_rafaga = 0
    ultima_confirmada = desde
    encabezado_original: Optional[str] = None
    completo = True

    def on_delivery(err, msg):
        if err is not None:
            errores.append(err)

    def confirmar(n_linea: int) -> None:
        cliente.flush()
        if errores:
            raise RuntimeError(f"{len(errores)} mensajes no entregados; primer error: {errores[0]}")
        escribir_json_atomico(_ruta_checkpoint(cfg, fuente, h16), {"ultima_linea": n_linea})

    try:
        with open(ruta, encoding="utf-8-sig", newline="", errors="replace") as f:
            encabezado_original = f.readline().rstrip("\r\n")
            cols = [c.strip().lower() for c in next(csv.reader([encabezado_original]))]
            if cols != list(fuente.columnas):
                raise EsquemaInesperado(f"{ruta.name}: encabezado {cols} distinto del esperado")
            ultima_publicada = desde
            for n, linea in enumerate(f, start=2):
                if n <= desde:
                    continue
                raw = linea.rstrip("\r\n")
                cliente.produce(fuente.topic, key=_llave_mensaje(raw, fuente), value=raw.encode("utf-8"),
                                headers=[("source_file", ruta.name.encode()),
                                         ("file_hash", file_hash.encode()), ("line", str(n).encode())],
                                on_delivery=on_delivery)
                publicadas += 1
                en_rafaga += 1
                ultima_publicada = n
                tope = bool(max_filas) and publicadas >= max_filas
                if en_rafaga >= tamano_rafaga or tope:
                    confirmar(n)
                    ultima_confirmada, en_rafaga = n, 0
                    if tope:
                        completo = False
                        break
                    dormir(pausa)
            else:
                if en_rafaga:
                    confirmar(ultima_publicada)
                    ultima_confirmada = ultima_publicada
    except Exception as e:
        registrar_log(cfg, nuevo_registro(
            run_id, "stream_produce", fuente, source_file=ruta.name, file_hash=file_hash,
            encabezado_original=encabezado_original, filas_leidas_origen=publicadas,
            ultima_linea=ultima_confirmada, estado="FAILED", observaciones=f"{type(e).__name__}: {e}",
            iniciado_en=iniciado, terminado_en=utcnow(), duracion_s=round(time.monotonic() - t0, 3)))
        raise

    reg = nuevo_registro(run_id, "stream_produce", fuente, source_file=ruta.name, file_hash=file_hash,
                         encabezado_original=encabezado_original, filas_leidas_origen=publicadas,
                         ultima_linea=ultima_confirmada, estado="OK" if completo else "PARCIAL",
                         iniciado_en=iniciado, terminado_en=utcnow(),
                         duracion_s=round(time.monotonic() - t0, 3),
                         observaciones=f"tamano_rafaga={tamano_rafaga} pausa={pausa}s")
    registrar_log(cfg, reg)
    log.info("%s: %d mensajes publicados (%s)", fuente.clave, publicadas, reg["estado"])
    return reg


# ====================================================================== marcadores de confirmación
def _dir_marcadores(cfg: Config, topic: str, particion: int) -> Path:
    return cfg.commits_dir / topic / f"p{particion:02d}"


def ultimo_offset_confirmado(cfg: Config, topic: str, particion: int) -> int:
    d = _dir_marcadores(cfg, topic, particion)
    if not d.exists():
        return -1
    return max((int(p.stem) for p in d.glob("*.json")), default=-1)


def escribir_marcador(cfg: Config, topic: str, particion: int, primero: int, ultimo: int, filas: int) -> None:
    escribir_json_atomico(_dir_marcadores(cfg, topic, particion) / f"{ultimo:012d}.json",
                          {"primero": primero, "ultimo": ultimo, "filas": filas,
                           "confirmado_en": utcnow().isoformat()})


_PATRON_PART = re.compile(r"part-k(\d+)-(\d+)-(\d+)\.parquet$")


def revertir_no_confirmados(cfg: Config, fuente: Fuente, particion: int, ultimo_confirmado: int) -> int:
    """Elimina archivos de micro-lotes que se escribieron pero nunca se confirmaron."""
    n = 0
    for base in (cfg.bronze_dir / fuente.operador / fuente.entidad,
                 cfg.malformed_dir / fuente.operador / fuente.entidad):
        if not base.exists():
            continue
        for p in base.glob(f"**/part-k{particion}-*.parquet"):
            m = _PATRON_PART.search(p.name)
            if m and int(m.group(2)) > ultimo_confirmado:
                p.unlink()
                n += 1
    return n


# ====================================================================== consumidor
class ConsumidorBronze:
    def __init__(self, cfg: Config, fuente: Fuente, *, cliente=None,
                 reloj: Callable[[], float] = time.monotonic, max_ocioso_s: float = 60.0):
        self.cfg, self.fuente = cfg, fuente
        self.cliente = cliente or crear_consumidor(cfg)
        self.reloj, self.max_ocioso_s = reloj, max_ocioso_s
        self.esquema = esquema_bronze(fuente)
        self.esquema_mal = esquema_malformadas(fuente)
        self.parse = parser_csv(fuente)
        self.tabla_dir = cfg.bronze_dir / fuente.operador / fuente.entidad
        self.mal_dir = cfg.malformed_dir / fuente.operador / fuente.entidad
        self.particiones = list(range(cfg.kafka_particiones))

    # ---- procesamiento de un mensaje ------------------------------------------------
    def _meta(self, msg, headers: dict, run_id: str, ingestado) -> dict:
        tipo_ts, ts = msg.timestamp()
        kts = (datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
               if tipo_ts != TIMESTAMP_NOT_AVAILABLE and ts and ts > 0 else None)
        linea = headers.get("line")
        return {
            "_ingested_at": ingestado, "_batch_id": run_id,
            "_source_file": headers["source_file"].decode() if "source_file" in headers else None,
            "_file_hash": headers["file_hash"].decode() if "file_hash" in headers else None,
            "_source_line_number": int(linea) if linea else None,
            "_kafka_topic": msg.topic(), "_kafka_partition": msg.partition(),
            "_kafka_offset": msg.offset(), "_kafka_timestamp": kts,
        }

    def _procesar(self, msg, run_id: str, ingestado, fecha_ingesta: str):
        headers = dict(msg.headers() or [])
        meta = self._meta(msg, headers, run_id, ingestado)
        valor = msg.value() or b""
        try:
            raw = valor.decode("utf-8")
        except UnicodeDecodeError:
            return None, {"_raw_line": valor.decode("utf-8", errors="replace"),
                          "_motivo": "bytes no UTF-8", **meta}, None
        try:
            fila, valor_part = self.parse(raw)
        except Malformado as e:
            return None, {"_raw_line": raw, "_motivo": str(e), **meta}, None
        fecha, defecto = fecha_particion(valor_part, self.fuente.regla_particion, fecha_ingesta)
        return {**fila, **meta}, None, (fecha, defecto)

    # ---- escritura de un micro-lote --------------------------------------------------
    def _vaciar(self, buffer: list, run_id: str, stats: dict) -> None:
        if not buffer:
            return
        por_particion: dict[int, list] = defaultdict(list)
        for item in buffer:
            por_particion[item["particion"]].append(item)

        for p, items in por_particion.items():
            offsets = [i["offset"] for i in items]
            primero, ultimo = min(offsets), max(offsets)
            nombre = f"part-k{p}-{primero:012d}-{ultimo:012d}.parquet"
            buffers: dict[str, dict[str, list]] = {}
            malos = []
            for i in items:
                if i["fila"] is not None:
                    fecha = i["fecha"]
                    cols = buffers.setdefault(fecha, {n: [] for n in self.esquema.names})
                    for n in self.esquema.names:
                        cols[n].append(i["fila"][n])
                else:
                    malos.append(i["malo"])
            stats["archivos"] += escribir_buffers(self.cfg, self.tabla_dir, self.esquema, buffers, nombre)
            if malos:
                escribir_parquet(pa.Table.from_pylist(malos, schema=self.esquema_mal),
                                 self.mal_dir / nombre, self.cfg.compresion)
            stats["escritas"] += sum(1 for i in items if i["fila"] is not None)
            stats["malformadas"] += len(malos)
            stats["fechas"].update(buffers.keys())
            stats["por_defecto"] += sum(1 for i in items if i["defecto"])
            # confirmación: siempre DESPUÉS de escribir los datos
            escribir_marcador(self.cfg, self.fuente.topic, p, primero, ultimo, len(items))
            try:
                self.cliente.commit(offsets=[TopicPartition(self.fuente.topic, p, ultimo + 1)],
                                    asynchronous=False)
            except Exception as e:      # el commit en Kafka es informativo
                log.warning("commit de offsets en Kafka falló (no afecta a Bronze): %s", e)

    # ---- bucle principal --------------------------------------------------------------
    def ejecutar(self, modo: str = "drain", run_id: Optional[str] = None) -> dict:
        """modo='drain': consume hasta el final actual del topic y termina.
           modo='continuo': sigue consumiendo hasta que se interrumpa (Ctrl-C)."""
        run_id = run_id or nuevo_run_id()
        cfg, fuente, topic = self.cfg, self.fuente, self.fuente.topic
        t0, iniciado = time.monotonic(), utcnow()
        fecha_ingesta = iniciado.date().isoformat()
        stats = {"consumidos": 0, "escritas": 0, "malformadas": 0, "archivos": 0, "por_defecto": 0,
                 "fechas": set()}

        inicio = {}
        for p in self.particiones:
            ultimo = ultimo_offset_confirmado(cfg, topic, p)
            rev = revertir_no_confirmados(cfg, fuente, p, ultimo)
            if rev:
                log.warning("%s p%d: se revirtieron %d archivos de un micro-lote sin confirmar", topic, p, rev)
            inicio[p] = ultimo + 1
        self.cliente.assign([TopicPartition(topic, p, inicio[p]) for p in self.particiones])

        marca_alta = {p: self.cliente.get_watermark_offsets(TopicPartition(topic, p), timeout=10,
                                                            cached=False)[1] for p in self.particiones}
        pendientes = {p for p in self.particiones if inicio[p] < marca_alta[p]}
        buffer: list = []
        t_lote: Optional[float] = None
        ultimo_mensaje = self.reloj()

        try:
            while True:
                if modo == "drain" and not pendientes:
                    break
                msg = self.cliente.poll(1.0)
                ahora = self.reloj()
                if msg is None:
                    if buffer and ahora - t_lote >= cfg.microlote_segundos:
                        self._vaciar(buffer, run_id, stats)
                        buffer, t_lote = [], None
                    if modo == "drain" and ahora - ultimo_mensaje > self.max_ocioso_s:
                        raise TimeoutError(f"{topic}: sin mensajes por {self.max_ocioso_s}s con datos pendientes")
                    continue
                if msg.error():
                    from confluent_kafka import KafkaException
                    raise KafkaException(msg.error())
                ultimo_mensaje = ahora
                p, off = msg.partition(), msg.offset()
                if off < inicio[p]:
                    continue
                fila, malo, info = self._procesar(msg, run_id, iniciado, fecha_ingesta)
                buffer.append({"particion": p, "offset": off, "fila": fila, "malo": malo,
                               "fecha": info[0] if info else None, "defecto": bool(info and info[1])})
                stats["consumidos"] += 1
                if t_lote is None:
                    t_lote = ahora
                if off + 1 >= marca_alta[p]:
                    pendientes.discard(p)
                if len(buffer) >= cfg.microlote_filas or ahora - t_lote >= cfg.microlote_segundos:
                    self._vaciar(buffer, run_id, stats)
                    buffer, t_lote = [], None
        except KeyboardInterrupt:
            log.info("%s: interrupción solicitada; se escribe el micro-lote pendiente", topic)
        finally:
            self._vaciar(buffer, run_id, stats)

        reg = nuevo_registro(run_id, "stream_consume", fuente, source_file=topic,
                             filas_leidas_origen=stats["consumidos"], filas_escritas_bronze=stats["escritas"],
                             filas_malformadas=stats["malformadas"], filas_particion_por_defecto=stats["por_defecto"],
                             particiones=len(stats["fechas"]), archivos_parquet=stats["archivos"],
                             estado="OK", observaciones=f"modo={modo} inicio_offsets={inicio}",
                             iniciado_en=iniciado, terminado_en=utcnow(),
                             duracion_s=round(time.monotonic() - t0, 3))
        registrar_log(cfg, reg)
        log.info("%s: %d mensajes -> %d filas Bronze + %d malformadas", topic, stats["consumidos"],
                 stats["escritas"], stats["malformadas"])
        try:
            self.cliente.close()
        except Exception:
            pass
        return reg
