"""Pruebas de Silver: corren dbt sobre un Bronze sintético con anomalías conocidas y comparan contra los CSV de origen."""
import csv
import json
import re
import shutil
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pytest

pytest.importorskip("dbt.cli.main", reason="dbt-duckdb no está instalado")

from bronze.common import nuevo_run_id
from bronze.config import Config
from bronze.fuentes import BATCH, CDC, FUENTES
from bronze.ingesta_archivos import ingerir_archivo
from bronze.streaming import ConsumidorBronze, publicar_archivo
from flows.dbt_runner import construir_silver, contar_capas
from tests.datos_sinteticos import generar
from tests.fake_kafka import BrokerFake, ConsumidorFake, ProductorFake


@pytest.fixture(scope="module")
def silver(tmp_path_factory):
    base = tmp_path_factory.mktemp("silver")
    generar(base / "datos_red")
    cfg = Config(data_dir=base / "datos_red", lake_dir=base / "lake")
    run = nuevo_run_id()
    for f in BATCH + CDC:
        ingerir_archivo(cfg, f, run)
    broker = BrokerFake(3)
    for c in ("transmetro_validaciones", "aerometro_boardings"):
        publicar_archivo(cfg, FUENTES[c], cliente=ProductorFake(broker), tamano_rafaga=1000, pausa=0,
                         dormir=lambda s: None)
        ConsumidorBronze(cfg, FUENTES[c], cliente=ConsumidorFake(broker)).ejecutar()
    construir_silver(cfg)
    return cfg


def wh(cfg):
    return duckdb.connect(str(cfg.lake_dir / "warehouse.duckdb"), read_only=True)


def filas_csv(cfg, nombre):
    with open(cfg.data_dir / nombre, newline="", encoding="utf-8") as f:
        return list(csv.reader(f))[1:]


# ----------------------------------------------------------------------- reglas de calidad
def test_conteos_por_regla_coinciden_con_las_anomalias_del_origen(silver):
    cfg = silver
    dq = {(f, r): (a, c) for f, r, a, c in wh(cfg).execute(
        "select fuente, regla, registros_afectados, registros_en_cuarentena from silver.silver_dq_conteo_por_regla").fetchall()}
    tu = [r for r in filas_csv(cfg, "transurbano_transacciones.csv") if len(r) == 7]
    cdc = filas_csv(cfg, "cdc_padron_usuarios.csv")
    tm = filas_csv(cfg, "transmetro_validaciones.csv")

    assert dq[("transurbano_transacciones", "fecha_futura")] == (sum(r[0].endswith("/2027") for r in tu),) * 2
    assert dq[("transurbano_transacciones", "cod_parada_nulo")] == (sum(r[3] == "" for r in tu),) * 2
    assert dq[("cdc_padron_usuarios", "llave_centinela")] == (sum(r[3] == "SIN-TARJETA" for r in cdc),) * 2
    assert dq[("cdc_padron_usuarios", "llave_formato_no_transmetro")] == \
        (sum(bool(re.match(r"^MR\d{7}$", r[3])) for r in cdc),) * 2
    assert dq[("transmetro_validaciones", "duplicado_torniquete")] == (len(tm) - len({r[0] for r in tm}),) * 2
    con_sin_salida = sum('"exit": null' in l for l in open(cfg.data_dir / "metroriel_viajes.jsonl"))
    assert dq[("metroriel_viajes", "viaje_sin_salida")] == (con_sin_salida,) * 2


def test_toda_regla_del_catalogo_aparece_aunque_tenga_cero(silver):
    n_reglas = wh(silver).execute("select count(*) from silver.reglas_calidad").fetchone()[0]
    assert wh(silver).execute("select count(*) from silver.silver_dq_conteo_por_regla").fetchone()[0] == n_reglas


def test_conciliacion_bronze_igual_silver_mas_cuarentena(silver):
    con = wh(silver)
    bronze = duckdb.connect(str(silver.lake_dir / "bronze_vistas.duckdb"), read_only=True)
    pares = {"transmetro_validaciones": ("transmetro_validaciones", "silver_tm_validaciones"),
             "transurbano_transacciones": ("transurbano_transacciones", "silver_tu_transacciones"),
             "aerometro_boardings": ("aerometro_boardings", "silver_am_boardings"),
             "metroriel_viajes": ("metroriel_viajes", "silver_mr_viajes")}
    for fuente, (vista, tabla) in pares.items():
        b = bronze.execute(f"select count(*) from bronze.{vista}").fetchone()[0]
        s = con.execute(f"select count(*) from silver.{tabla}").fetchone()[0]
        q = con.execute("select count(*) from silver.silver_cuarentena where fuente = ?", [fuente]).fetchone()[0]
        assert b == s + q, fuente


def test_cuarentena_conserva_la_fila_original_y_su_origen(silver):
    filas = wh(silver).execute(
        "select fila_original, _source_line_number, motivo from silver.silver_cuarentena "
        "where fuente = 'metroriel_viajes' limit 5").fetchall()
    assert filas
    for original, linea, motivo in filas:
        d = json.loads(original)
        assert "card" in d and d["_source_line_number"] == linea and motivo


def test_duplicado_de_torniquete_conserva_la_primera_lectura(silver):
    con = wh(silver)
    dup = con.execute("select _source_line_number from silver.silver_cuarentena "
                      "where regla_principal = 'duplicado_torniquete' limit 1").fetchone()[0]
    original = json.loads(con.execute(
        "select fila_original from silver.silver_cuarentena where regla_principal = 'duplicado_torniquete' "
        "and _source_line_number = ?", [dup]).fetchone()[0])
    en_silver = con.execute("select _source_line_number from silver.silver_tm_validaciones where validacion_id = ?",
                            [int(original["validacion_id"])]).fetchone()[0]
    assert en_silver < dup


# ----------------------------------------------------------------------- formatos
def test_aerometro_pasa_de_utc_a_hora_local(silver):
    filas = wh(silver).execute("select ts_utc, ts_local from silver.silver_am_boardings").fetchall()
    assert filas and all(loc == utc - timedelta(hours=6) for utc, loc in filas)


def test_montos_en_quetzales(silver):
    con = wh(silver)
    assert {float(r[0]) for r in con.execute("select distinct monto_gtq from silver.silver_tu_transacciones").fetchall()} \
        == {0.65, 1.30}
    assert con.execute("select max(tarifa_gtq) from silver.silver_am_boardings").fetchone()[0] == 3.5


def test_fechas_de_transurbano_se_leen_como_dia_mes_anio(silver):
    fechas = {str(r[0]) for r in wh(silver).execute("select distinct fecha from silver.silver_tu_transacciones").fetchall()}
    assert fechas <= {f"2026-06-0{d}" for d in range(1, 6)}


def test_zonas_conformadas(silver):
    con = wh(silver)
    z = {(r[0], r[1]): r[2] for r in con.execute(
        "select operador, zona_original, zona_id from silver.silver_estacion").fetchall()}
    assert z[("transmetro", "Zona 5")] == 5            # 'Zona N'
    assert z[("transurbano", "Z5")] == 5               # 'ZN'
    assert z[("aerometro", "Mixco")] == 101            # municipio
    assert z[("aerometro", "Zona 30")] == -1           # no está en dim_zona -> sin mapear (advertencia de dbt)
    assert z[("transmetro", "Zona 5")] == z[("transurbano", "Z5")]     # misma zona en operadores distintos


# ----------------------------------------------------------------------- padrón SCD2
def scd2_referencia(cfg):
    """Implementación independiente en Python del SCD2 sobre los eventos CDC válidos."""
    eventos = []
    for r in filas_csv(cfg, "cdc_padron_usuarios.csv"):
        seq, ts, op, tarjeta, perfil, zona, estado = [c if c != "" else None for c in r]
        if tarjeta == "SIN-TARJETA" or not re.match(r"^TC-\d{8}$", tarjeta or ""):
            continue
        eventos.append((int(seq), op, tarjeta, perfil, zona, estado))
    versiones = defaultdict(list)          # tarjeta -> [(attrs, activa, op)]
    for seq, op, t, perfil, zona, estado in sorted(eventos):
        prev = versiones[t][-1] if versiones[t] else None
        attrs = (prev[0] if prev else (None, None, None)) if op == "DELETE" else (perfil, zona, estado)
        nuevo = (attrs, op != "DELETE", op)
        if prev is None or (nuevo[0], nuevo[1]) != (prev[0], prev[1]):
            versiones[t].append(nuevo)
    return versiones


def test_scd2_coincide_con_implementacion_de_referencia(silver):
    ref = scd2_referencia(silver)
    con = wh(silver)
    assert con.execute("select count(*) from silver.silver_padron_scd2").fetchone()[0] == sum(len(v) for v in ref.values())
    activas = sum(v[-1][1] for v in ref.values())
    c = dict(zip(*[[d[0] for d in con.execute("select * from silver.silver_cdc_conteos").description],
                   con.execute("select * from silver.silver_cdc_conteos").fetchone()]))
    assert c["tarjetas_antes_de_borrados"] == len(ref)
    assert c["tarjetas_activas_despues"] == activas
    assert c["tarjetas_dadas_de_baja"] == len(ref) - activas
    assert c["tarjetas_sin_insert_previo"] == sum(v[0][2] != "INSERT" for v in ref.values())
    assert c["tarjetas_reactivadas"] == sum(any(b[1] and not a[1] for a, b in zip(v, v[1:])) for v in ref.values())
    assert c["tarjetas_antes_de_borrados"] == c["tarjetas_activas_despues"] + c["tarjetas_dadas_de_baja"]


def test_delete_marca_inactiva_pero_conserva_atributos_y_no_borra_la_tarjeta(silver):
    ref = scd2_referencia(silver)
    baja = next(t for t, v in ref.items() if not v[-1][1] and v[-1][0] != (None, None, None))
    v = wh(silver).execute("select perfil, activa, es_vigente from silver.silver_padron_scd2 where tarjeta = ? "
                           "order by version desc limit 1", [baja]).fetchone()
    assert v[1] is False and v[2] is True and v[0] == ref[baja][-1][0][0]


def test_una_sola_version_vigente_y_sin_traslape(silver):
    con = wh(silver)
    assert con.execute("select count(*) from (select tarjeta from silver.silver_padron_scd2 group by tarjeta "
                       "having count(*) filter (where es_vigente) <> 1)").fetchone()[0] == 0
    assert con.execute("select count(*) from silver.silver_padron_scd2 "
                       "where valido_hasta_seq is not null and valido_hasta_seq <= valido_desde_seq").fetchone()[0] == 0


# ----------------------------------------------------------------------- identidad y catálogos de llaves
def test_identidad_numerica_unifica_transmetro_transurbano_metroriel_y_no_aerometro(silver):
    con = wh(silver)
    comunes = con.execute("""
        select persona_id, count(distinct operador) as operadores from silver.silver_usuario
        where regla_identidad = 'numerica_compartida' group by 1 having count(distinct operador) >= 2 limit 1""").fetchone()
    assert comunes and comunes[1] >= 2
    assert con.execute("select count(*) from silver.silver_usuario where operador = 'aerometro' "
                       "and (regla_identidad <> 'solo_operador' or persona_id <> usuario_sk)").fetchone()[0] == 0
    assert con.execute("select count(*) from silver.silver_usuario "
                       "where operador = 'transmetro' and llave_original not like 'TC-%'").fetchone()[0] == 0


def test_catalogos_de_llaves_solo_tienen_la_llave_y_cuentan_lo_correcto(silver):
    con = wh(silver)
    for tabla in ("silver_llaves_transurbano", "silver_llaves_metroriel", "silver_llaves_aerometro"):
        assert [d[0] for d in con.execute(f"select * from silver.{tabla}").description] == ["llave"]
    tu = {r[2] for r in filas_csv(silver, "transurbano_transacciones.csv") if len(r) == 7}
    assert con.execute("select count(*) from silver.silver_llaves_transurbano").fetchone()[0] == len(tu)
    mr = set()
    for linea in open(silver.data_dir / "metroriel_viajes.jsonl"):
        try:
            mr.add(json.loads(linea)["card"])
        except (json.JSONDecodeError, KeyError):
            pass
    assert con.execute("select count(*) from silver.silver_llaves_metroriel").fetchone()[0] == len(mr)


def test_variable_de_identidad_puede_apagarse(silver, tmp_path):
    copia = Config(data_dir=silver.data_dir, lake_dir=tmp_path / "lake")
    shutil.copytree(silver.lake_dir / "bronze", copia.bronze_dir)
    construir_silver(copia, {"unificar_identidad_numerica": False})
    n = duckdb.connect(str(copia.lake_dir / "warehouse.duckdb"), read_only=True).execute(
        "select count(*) from silver.silver_usuario where persona_id <> usuario_sk or regla_identidad <> 'solo_operador'"
    ).fetchone()[0]
    assert n == 0


# ----------------------------------------------------------------------- idempotencia (entregable 1.5)
def test_reconstruir_silver_no_cambia_ni_conteos_ni_huellas(silver):
    antes = contar_capas(silver)
    construir_silver(silver)
    despues = contar_capas(silver)
    assert antes == despues
    assert all(r["filas"] > 0 for r in antes)                       # ninguna tabla quedó vacía
    assert {r["capa"] for r in antes} == {"bronze", "staging", "silver"}


def test_bronze_con_lineas_repetidas_detiene_silver_antes_de_construir_basura(tmp_path):
    """Si Bronze trae la misma línea de origen dos veces, la alarma de dbt falla y no se construye Silver."""
    generar(tmp_path / "datos_red", n_tm=300, n_tu=200, n_am=150, n_mr=100, n_cdc=80)
    cfg = Config(data_dir=tmp_path / "datos_red", lake_dir=tmp_path / "lake")
    run = nuevo_run_id()
    for f in BATCH + CDC:
        ingerir_archivo(cfg, f, run)
    broker = BrokerFake(3)
    for c in ("transmetro_validaciones", "aerometro_boardings"):
        publicar_archivo(cfg, FUENTES[c], cliente=ProductorFake(broker), tamano_rafaga=1000, pausa=0, dormir=lambda s: None)
        ConsumidorBronze(cfg, FUENTES[c], cliente=ConsumidorFake(broker)).ejecutar()
    un_parquet = next((cfg.bronze_dir / "transmetro" / "validaciones").glob("**/part-k*.parquet"))
    shutil.copy(un_parquet, un_parquet.with_name("part-k9-000000000000-000000000001.parquet"))

    with pytest.raises(RuntimeError):
        construir_silver(cfg)
    con = duckdb.connect(str(cfg.lake_dir / "warehouse.duckdb"), read_only=True)
    construidas = {r[0] for r in con.execute("select table_name from information_schema.tables").fetchall()}
    assert "stg_tm_validaciones" not in construidas and "silver_cuarentena" not in construidas
    log = (cfg.lake_dir / "dbt_logs" / "dbt.log").read_text(encoding="utf-8", errors="ignore")
    assert "source_llave_tecnica_unica_bronze_transmetro_validaciones" in log
