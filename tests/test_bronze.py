import itertools
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import bronze.streaming as st
from bronze.common import (esquema_bronze, fecha_particion, leer_log, nuevo_run_id, sha256_archivo)
from bronze.conciliacion import conciliar, crear_vistas
from bronze.fuentes import BATCH, CDC, FUENTES, STREAMING
from bronze.ingesta_archivos import EsquemaInesperado, ingerir_archivo
from bronze.streaming import ConsumidorBronze, publicar_archivo
from tests.fake_kafka import BrokerFake, ConsumidorFake, ProductorFake


# ----------------------------------------------------------------------- utilidades
def q(cfg, clave, sql_where="1=1", cols="*", malformadas=False):
    f = FUENTES[clave]
    base = cfg.malformed_dir if malformadas else cfg.bronze_dir
    glob = str(base / f.operador / f.entidad / "**" / "*.parquet")
    if not any((base / f.operador / f.entidad).glob("**/*.parquet")):
        return [(0,)] if cols.startswith("count(") else []
    return duckdb.connect().execute(
        f"SELECT {cols} FROM read_parquet(?, hive_partitioning=true, union_by_name=true) WHERE {sql_where}",
        [glob]).fetchall()


def n(cfg, clave, **kw):
    return q(cfg, clave, cols="count(*)", **kw)[0][0]


def cargar_todo_batch(cfg):
    run = nuevo_run_id()
    for f in BATCH + CDC:
        ingerir_archivo(cfg, f, run)


def stream_completo(cfg, clave, broker, **kw):
    f = FUENTES[clave]
    publicar_archivo(cfg, f, cliente=ProductorFake(broker), tamano_rafaga=kw.get("rafaga", 300),
                     pausa=0, dormir=lambda s: None)
    ConsumidorBronze(cfg, f, cliente=ConsumidorFake(broker)).ejecutar()


# ----------------------------------------------------------------------- reglas de partición
@pytest.mark.parametrize("valor,regla,esperado", [
    ("2026-06-01 04:50:57", "iso", ("2026-06-01", False)),
    ("2026-06-01T22:04:15Z", "iso", ("2026-06-01", False)),
    ("17/07/2027", "dmy", ("2027-07-17", False)),
    ("basura", "iso", ("2030-01-01", True)),
    (None, "dmy", ("2030-01-01", True)),
    ("2026-06-01", "ingesta", ("2030-01-01", False)),
])
def test_fecha_particion(valor, regla, esperado):
    assert fecha_particion(valor, regla, "2030-01-01") == esperado


# ----------------------------------------------------------------------- batch
def test_batch_conteos_y_conciliacion(entorno):
    cfg, esp = entorno
    cargar_todo_batch(cfg)
    for clave in [f.clave for f in BATCH + CDC]:
        assert n(cfg, clave) == esp[clave]["filas"], clave
        assert n(cfg, clave, malformadas=True) == esp[clave]["malformadas"], clave
    filas = {r["fuente"]: r for r in conciliar(cfg, [f.clave for f in BATCH + CDC])}
    assert all(r["estado"] == "OK" and r["diferencia"] == 0 for r in filas.values())


def test_todas_las_columnas_de_datos_son_string(entorno):
    for f in FUENTES.values():
        esquema = esquema_bronze(f)
        for campo in esquema:
            if not campo.name.startswith("_"):
                assert pa.types.is_string(campo.type) or pa.types.is_struct(campo.type), (f.clave, campo.name)


def test_transurbano_preserva_ceros_particiona_dmy_y_vacios_son_nulos(entorno):
    cfg, _ = entorno
    ingerir_archivo(cfg, FUENTES["transurbano_transacciones"], nuevo_run_id())
    assert n(cfg, "transurbano_transacciones", sql_where="length(num_tarjeta) = 10") == \
        n(cfg, "transurbano_transacciones")
    fechas = {r[0] for r in q(cfg, "transurbano_transacciones", cols="DISTINCT CAST(_partition_date AS VARCHAR)")}
    assert "2026-06-01" in fechas and any(f.startswith("2027-07") for f in fechas)   # anomalía visible, no oculta
    assert n(cfg, "transurbano_transacciones", sql_where="cod_parada IS NULL") > 0
    # la fecha original no se toca
    assert n(cfg, "transurbano_transacciones", sql_where="fecha LIKE '__/__/____'") == \
        n(cfg, "transurbano_transacciones")


def test_lineas_malformadas_se_conservan_con_motivo(entorno):
    cfg, _ = entorno
    ingerir_archivo(cfg, FUENTES["transurbano_transacciones"], nuevo_run_id())
    mal = q(cfg, "transurbano_transacciones", malformadas=True, cols="_raw_line, _motivo, _source_line_number")
    assert sorted(r[0] for r in mal) == ["", "x,y"]
    assert all(r[1] for r in mal) and all(r[2] > 1 for r in mal)


def test_metroriel_struct_texto_exacto_y_exit_nulo(entorno):
    cfg, esp = entorno
    ingerir_archivo(cfg, FUENTES["metroriel_viajes"], nuevo_run_id())
    esquema = pq.read_schema(next((cfg.bronze_dir / "metroriel" / "viajes").glob("**/*.parquet")))
    assert pa.types.is_struct(esquema.field("entry").type)
    # los números conservan su texto original (12.50 no se vuelve 12.5)
    assert n(cfg, "metroriel_viajes", sql_where="fare_gtq = '12.50'") > 0
    assert n(cfg, "metroriel_viajes", sql_where="fare_gtq = '12.5'") == 0
    assert n(cfg, "metroriel_viajes", sql_where="\"exit\" IS NULL") == esp["metroriel_viajes"]["filas"] // 25
    assert n(cfg, "metroriel_viajes", sql_where="\"exit\" IS NULL AND duration_s IS NULL") > 0
    # _raw_json es el registro original y parsea igual que las columnas
    fila = q(cfg, "metroriel_viajes", sql_where="trip_id = '10'", cols="_raw_json, card, entry.ts")[0]
    assert json.loads(fila[0])["card"] == fila[1] and json.loads(fila[0])["entry"]["ts"] == fila[2]
    # campo nuevo detectado en la bitácora
    reg = [r for r in leer_log(cfg) if r["clave"] == "metroriel_viajes"][-1]
    assert "extra_field" in reg["observaciones"]
    assert n(cfg, "metroriel_viajes", malformadas=True) == 2


def test_cdc_conserva_log_completo_y_delete_sin_cuerpo(entorno):
    cfg, esp = entorno
    ingerir_archivo(cfg, FUENTES["cdc_padron_usuarios"], nuevo_run_id())
    assert n(cfg, "cdc_padron_usuarios") == esp["cdc_padron_usuarios"]["filas"]      # sin aplicar operaciones
    assert n(cfg, "cdc_padron_usuarios", sql_where="op = 'DELETE' AND perfil IS NULL AND estado IS NULL") == \
        n(cfg, "cdc_padron_usuarios", sql_where="op = 'DELETE'") > 0
    assert n(cfg, "cdc_padron_usuarios", sql_where="tarjeta = 'SIN-TARJETA'") > 0      # centinela conservado
    seqs = [r[0] for r in q(cfg, "cdc_padron_usuarios", cols="seq")]
    assert len(set(seqs)) == len(seqs)


def test_encabezado_inesperado_falla_y_queda_en_bitacora(entorno):
    cfg, _ = entorno
    p = cfg.data_dir / "tu_paradas.csv"
    p.write_text(p.read_text().replace("cod_parada", "codigo_parada", 1))
    with pytest.raises(EsquemaInesperado):
        ingerir_archivo(cfg, FUENTES["tu_paradas"], nuevo_run_id())
    assert [r["estado"] for r in leer_log(cfg)] == ["FAILED"]
    assert not (cfg.bronze_dir / "transurbano" / "paradas").exists()          # no se guardó nada desalineado


def test_encabezado_en_mayusculas_se_normaliza(entorno):
    cfg, esp = entorno
    p = cfg.data_dir / "tu_paradas.csv"
    lineas = p.read_text().splitlines()
    lineas[0] = lineas[0].upper()
    p.write_text("\n".join(lineas) + "\n")
    reg = ingerir_archivo(cfg, FUENTES["tu_paradas"], nuevo_run_id())
    assert reg["encabezado_original"] == "COD_PARADA,DESCRIPCION,RUTA,SECTOR"       # el original queda en la bitácora
    assert n(cfg, "tu_paradas") == esp["tu_paradas"]["filas"]


# ----------------------------------------------------------------------- idempotencia
def test_reejecutar_no_duplica(entorno):
    cfg, esp = entorno
    cargar_todo_batch(cfg)
    antes = {f.clave: n(cfg, f.clave) for f in BATCH + CDC}
    datos = lambda: sorted(str(p) for p in cfg.bronze_dir.glob("**/*.parquet") if "_control" not in p.parts)
    archivos = datos()
    cargar_todo_batch(cfg)
    assert {f.clave: n(cfg, f.clave) for f in BATCH + CDC} == antes
    assert datos() == archivos                                    # ni un archivo nuevo, ni uno modificado de nombre
    estados = [r["estado"] for r in leer_log(cfg)]
    assert estados.count("SKIPPED") == len(BATCH + CDC)


def test_recuperacion_de_carga_batch_interrumpida(entorno, monkeypatch):
    """Archivos escritos pero sin registro OK: la reejecución revierte y reescribe sin duplicar."""
    cfg, esp = entorno
    f = FUENTES["transurbano_transacciones"]
    import bronze.ingesta_archivos as ia
    original = ia.registrar_log

    def explota(cfg_, reg):
        if reg["estado"] == "OK":
            raise RuntimeError("caída antes de registrar el OK")
        return original(cfg_, reg)

    monkeypatch.setattr(ia, "registrar_log", explota)
    with pytest.raises(RuntimeError):
        ingerir_archivo(cfg, f, nuevo_run_id())
    assert n(cfg, "transurbano_transacciones") > 0            # quedaron archivos sin confirmar
    monkeypatch.setattr(ia, "registrar_log", original)
    ingerir_archivo(cfg, f, nuevo_run_id())
    assert n(cfg, "transurbano_transacciones") == esp["transurbano_transacciones"]["filas"]


def test_archivo_nuevo_con_otro_hash_se_agrega_como_snapshot(entorno):
    cfg, esp = entorno
    f = FUENTES["tm_estaciones"]
    ingerir_archivo(cfg, f, nuevo_run_id())
    p = cfg.data_dir / f.archivo
    p.write_text(p.read_text() + "TM-L1-99,Nueva,L1,Zona 4,14.4,-90.5\n")
    ingerir_archivo(cfg, f, nuevo_run_id())
    assert n(cfg, "tm_estaciones") == 6 + 7                    # el snapshot anterior no se modifica
    assert len(q(cfg, "tm_estaciones", cols="DISTINCT _file_hash")) == 2


# ----------------------------------------------------------------------- streaming
@pytest.mark.parametrize("clave", ["transmetro_validaciones", "aerometro_boardings"])
def test_streaming_completo_conserva_duplicados_y_cuadra(entorno, clave):
    cfg, esp = entorno
    broker = BrokerFake(cfg.kafka_particiones)
    stream_completo(cfg, clave, broker)
    assert n(cfg, clave) == esp[clave]["filas"]                # incluye las 1,115 (aquí 30) duplicadas exactas
    distintos = q(cfg, clave, cols="count(DISTINCT (_kafka_partition, _kafka_offset))")[0][0]
    assert distintos == esp[clave]["filas"]
    # metadatos completos
    assert n(cfg, clave, sql_where="_kafka_topic IS NULL OR _file_hash IS NULL OR _source_line_number IS NULL") == 0
    assert [r["estado"] for r in conciliar(cfg, [clave])] == ["OK"]
    assert q(cfg, clave, cols="min(_source_line_number)")[0][0] == 2          # el encabezado no entra como dato


def test_transmetro_duplicados_exactos_estan_en_bronze(entorno):
    cfg, esp = entorno
    stream_completo(cfg, "transmetro_validaciones", BrokerFake(cfg.kafka_particiones))
    dup = q(cfg, "transmetro_validaciones",
            cols="count(*) - count(DISTINCT (validacion_id, tarjeta, estacion_id, linea, fecha_hora, tarifa, tipo))")[0][0]
    assert dup == 30


def test_aerometro_particion_por_fecha_utc_y_hora_sin_convertir(entorno):
    cfg, _ = entorno
    stream_completo(cfg, "aerometro_boardings", BrokerFake(cfg.kafka_particiones))
    assert n(cfg, "aerometro_boardings", sql_where="timestamp_utc LIKE '____-__-__T__:__:__Z'") == \
        n(cfg, "aerometro_boardings")
    # partición = fecha del propio string (UTC), aunque en hora local sea otro día
    assert n(cfg, "aerometro_boardings",
             sql_where="CAST(_partition_date AS VARCHAR) <> substr(timestamp_utc, 1, 10)") == 0


def test_microlote_por_tamano_y_cola_final(entorno):
    cfg, esp = entorno
    broker = BrokerFake(cfg.kafka_particiones)
    f = FUENTES["aerometro_boardings"]
    publicar_archivo(cfg, f, cliente=ProductorFake(broker), tamano_rafaga=500, pausa=0, dormir=lambda s: None)
    cfg2 = type(cfg)(**{**cfg.__dict__, "microlote_filas": 200, "microlote_segundos": 999})
    reg = ConsumidorBronze(cfg2, f, cliente=ConsumidorFake(broker)).ejecutar()
    assert reg["archivos_parquet"] >= esp["aerometro_boardings"]["filas"] // 200      # varios micro-lotes
    assert n(cfg2, "aerometro_boardings") == esp["aerometro_boardings"]["filas"]     # la cola final se escribió


def test_microlote_por_tiempo(entorno):
    cfg, esp = entorno
    broker = BrokerFake(cfg.kafka_particiones)
    f = FUENTES["aerometro_boardings"]
    publicar_archivo(cfg, f, cliente=ProductorFake(broker), tamano_rafaga=500, pausa=0, dormir=lambda s: None)
    cfg2 = type(cfg)(**{**cfg.__dict__, "microlote_filas": 10 ** 9, "microlote_segundos": 0.5})
    reloj = itertools.count(0, 0.01)                                                  # cada llamada avanza 10 ms
    reg = ConsumidorBronze(cfg2, f, cliente=ConsumidorFake(broker), reloj=lambda: next(reloj)).ejecutar()
    assert reg["archivos_parquet"] > 3
    assert n(cfg2, "aerometro_boardings") == esp["aerometro_boardings"]["filas"]


def test_consumidor_retoma_tras_caida_sin_duplicar(entorno):
    cfg, esp = entorno
    clave = "transmetro_validaciones"
    f = FUENTES[clave]
    broker = BrokerFake(cfg.kafka_particiones)
    publicar_archivo(cfg, f, cliente=ProductorFake(broker), tamano_rafaga=500, pausa=0, dormir=lambda s: None)
    with pytest.raises(RuntimeError):
        ConsumidorBronze(cfg, f, cliente=ConsumidorFake(broker, fallar_tras=900)).ejecutar()
    parcial = n(cfg, clave)
    assert 0 < parcial < esp[clave]["filas"]
    ConsumidorBronze(cfg, f, cliente=ConsumidorFake(broker)).ejecutar()
    assert n(cfg, clave) == esp[clave]["filas"]
    assert q(cfg, clave, cols="count(DISTINCT (_kafka_partition, _kafka_offset))")[0][0] == esp[clave]["filas"]


def test_caida_dura_entre_datos_y_marcador_revierte_el_microlote(entorno, monkeypatch):
    """Muere tras escribir Parquet pero antes de confirmar: al reiniciar no quedan duplicados."""
    cfg, esp = entorno
    clave = "aerometro_boardings"
    f = FUENTES[clave]
    broker = BrokerFake(cfg.kafka_particiones)
    publicar_archivo(cfg, f, cliente=ProductorFake(broker), tamano_rafaga=500, pausa=0, dormir=lambda s: None)

    def sin_marcador(*a, **k):
        raise RuntimeError("kill -9 simulado antes del marcador")

    monkeypatch.setattr(st, "escribir_marcador", sin_marcador)
    with pytest.raises(RuntimeError):
        ConsumidorBronze(cfg, f, cliente=ConsumidorFake(broker)).ejecutar()
    assert n(cfg, clave) > 0                                  # hay archivos sin marcador
    monkeypatch.undo()
    ConsumidorBronze(cfg, f, cliente=ConsumidorFake(broker)).ejecutar()
    assert n(cfg, clave) == esp[clave]["filas"]
    assert q(cfg, clave, cols="count(DISTINCT (_kafka_partition, _kafka_offset))")[0][0] == esp[clave]["filas"]


def test_consumidor_reejecutado_sin_mensajes_nuevos_no_agrega_nada(entorno):
    cfg, esp = entorno
    clave = "aerometro_boardings"
    broker = BrokerFake(cfg.kafka_particiones)
    stream_completo(cfg, clave, broker)
    ConsumidorBronze(cfg, FUENTES[clave], cliente=ConsumidorFake(broker)).ejecutar()
    assert n(cfg, clave) == esp[clave]["filas"]


def test_mensaje_malformado_va_a_malformadas(entorno):
    cfg, esp = entorno
    clave = "aerometro_boardings"
    f = FUENTES[clave]
    broker = BrokerFake(cfg.kafka_particiones)
    prod = ProductorFake(broker)
    prod.produce(f.topic, key=b"k", value=b"1,solo,tres", headers=[("source_file", b"x.csv"), ("file_hash", b"h"), ("line", b"2")])
    prod.produce(f.topic, key=b"k", value=b"\xff\xfe\x00", headers=[("source_file", b"x.csv"), ("file_hash", b"h"), ("line", b"3")])
    prod.flush()
    ConsumidorBronze(cfg, f, cliente=ConsumidorFake(broker)).ejecutar()
    assert n(cfg, clave) == 0
    motivos = sorted(r[0] for r in q(cfg, clave, cols="_motivo", malformadas=True))
    assert motivos == ["3 campos; se esperaban 7", "bytes no UTF-8"]


# ----------------------------------------------------------------------- productor
def test_productor_es_idempotente(entorno):
    cfg, esp = entorno
    clave = "aerometro_boardings"
    f = FUENTES[clave]
    broker = BrokerFake(cfg.kafka_particiones)
    publicar_archivo(cfg, f, cliente=ProductorFake(broker), tamano_rafaga=400, pausa=0, dormir=lambda s: None)
    assert broker.total(f.topic) == esp[clave]["filas"]
    reg = publicar_archivo(cfg, f, cliente=ProductorFake(broker), tamano_rafaga=400, pausa=0, dormir=lambda s: None)
    assert reg["estado"] == "SKIPPED" and broker.total(f.topic) == esp[clave]["filas"]


def test_productor_parcial_y_retoma_sin_repetir(entorno):
    cfg, esp = entorno
    clave = "aerometro_boardings"
    f = FUENTES[clave]
    broker = BrokerFake(cfg.kafka_particiones)
    r1 = publicar_archivo(cfg, f, cliente=ProductorFake(broker), tamano_rafaga=100, pausa=0, max_filas=250,
                          dormir=lambda s: None)
    assert r1["estado"] == "PARCIAL" and broker.total(f.topic) == 250
    r2 = publicar_archivo(cfg, f, cliente=ProductorFake(broker), tamano_rafaga=100, pausa=0, dormir=lambda s: None)
    assert r2["estado"] == "OK" and broker.total(f.topic) == esp[clave]["filas"]


def test_productor_caida_repite_como_maximo_una_rafaga(entorno):
    cfg, esp = entorno
    clave = "aerometro_boardings"
    f = FUENTES[clave]
    broker = BrokerFake(cfg.kafka_particiones)
    with pytest.raises(RuntimeError):
        publicar_archivo(cfg, f, cliente=ProductorFake(broker, fallar_en_flush=3), tamano_rafaga=100,
                         pausa=0, dormir=lambda s: None)
    publicar_archivo(cfg, f, cliente=ProductorFake(broker), tamano_rafaga=100, pausa=0, dormir=lambda s: None)
    total = broker.total(f.topic)
    assert esp[clave]["filas"] <= total <= esp[clave]["filas"] + 100        # at-least-once acotado
    ConsumidorBronze(cfg, f, cliente=ConsumidorFake(broker)).ejecutar()
    lineas = {r[0] for r in q(cfg, clave, cols="_source_line_number")}
    assert lineas == set(range(2, esp[clave]["filas"] + 2))                   # ninguna línea se perdió


def test_productor_respeta_pausa_entre_rafagas(entorno):
    cfg, esp = entorno
    pausas = []
    f = FUENTES["aerometro_boardings"]
    publicar_archivo(cfg, f, cliente=ProductorFake(BrokerFake(3)), tamano_rafaga=500, pausa=2.0,
                     dormir=pausas.append)
    assert pausas and all(p == 2.0 for p in pausas) and len(pausas) == esp["aerometro_boardings"]["filas"] // 500


# ----------------------------------------------------------------------- conciliación y vistas
def test_conciliacion_detecta_archivo_borrado(entorno):
    cfg, _ = entorno
    cargar_todo_batch(cfg)
    victima = next((cfg.bronze_dir / "metroriel" / "viajes").glob("**/*.parquet"))
    victima.unlink()
    estados = {r["fuente"]: r["estado"] for r in conciliar(cfg)}
    assert estados["metroriel_viajes"] == "DESCUADRE"
    assert estados["transmetro_validaciones"] == "NO_INGERIDO"           # aún no se ha cargado el streaming


def test_vistas_duckdb(entorno):
    cfg, esp = entorno
    cargar_todo_batch(cfg)
    ruta = crear_vistas(cfg)
    con = duckdb.connect(str(ruta), read_only=True)
    assert con.execute("SELECT count(*) FROM bronze.transurbano_transacciones").fetchone()[0] == \
        esp["transurbano_transacciones"]["filas"]
    assert con.execute("SELECT count(*) FROM bronze.malformadas_metroriel_viajes").fetchone()[0] == 2
    assert "_partition_date" in [c[0] for c in con.execute("DESCRIBE bronze.transmetro_padron_cdc").fetchall()]


def test_landing_conserva_copia_identica(entorno):
    cfg, _ = entorno
    cargar_todo_batch(cfg)
    copias = list(cfg.landing_dir.glob("**/*__metroriel_viajes.jsonl"))
    assert len(copias) == 1
    assert sha256_archivo(copias[0]) == sha256_archivo(cfg.data_dir / "metroriel_viajes.jsonl")
