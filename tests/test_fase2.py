"""Fase 2: features desde Silver, agregados de Gold (cobertura, transbordo, caso MetroRiel), linaje e idempotencia."""
import json
from contextlib import closing
from datetime import date

import duckdb
import pytest

pytest.importorskip("dbt.cli.main", reason="dbt-duckdb no está instalado")

from flows.dbt_runner import construir_silver, contar_capas
from flows.verificar_linaje import (violaciones_datos_personales, violaciones_features, violaciones_linaje)
from tests.test_silver import silver  # noqa: F401  (fixture: Bronze sintético + dbt build, incluidas las pruebas de dbt)


def wh(cfg):
    """Conexión de solo lectura; se usa siempre con `with wh(cfg) as con` para soltar el candado antes de reconstruir."""
    return closing(duckdb.connect(str(cfg.lake_dir / "warehouse.duckdb"), read_only=True))


def test_evento_unificado_no_pierde_ni_inventa_eventos(silver):
    with wh(silver) as con:
        n_unif = con.execute("select count(*) from silver.silver_evento_unificado").fetchone()[0]
        n_orig = sum(con.execute(f"select count(*) from silver.{t}").fetchone()[0] for t in
                     ("silver_tm_validaciones", "silver_tu_transacciones", "silver_am_boardings", "silver_mr_viajes"))
        assert n_unif == n_orig > 0


def test_features_una_fila_por_persona_y_conteos_independientes(silver):
    with wh(silver) as con:
        corte = con.execute("select max(fecha_corte) from features.features_usuario").fetchone()[0]
        assert corte == con.execute("select max(fecha) from silver.silver_evento_unificado").fetchone()[0]
        # total de viajes = eventos que no son TRANSBORDO de Transmetro, calculado directo sobre las tablas de origen
        esperado = (con.execute("select count(*) from silver.silver_tm_validaciones where upper(tipo) <> 'TRANSBORDO'").fetchone()[0]
                    + sum(con.execute(f"select count(*) from silver.{t}").fetchone()[0] for t in
                          ("silver_tu_transacciones", "silver_am_boardings", "silver_mr_viajes")))
        assert con.execute("select sum(viajes_total) from features.features_usuario").fetchone()[0] == esperado
        # ventanas recalculadas en Python para una muestra de personas
        for (pid,) in con.execute("select persona_id from features.features_usuario order by viajes_total desc limit 5").fetchall():
            evs = con.execute("select fecha, es_transbordo_operador from silver.silver_evento_unificado "
                              "where persona_id = ?", [pid]).fetchall()
            f = con.execute("select viajes_7d, viajes_30d, viajes_90d, viajes_total, dias_desde_ultimo_viaje "
                            "from features.features_usuario where persona_id = ?", [pid]).fetchone()
            viajes = [d for d, t in evs if not t]
            assert f[0] == sum((corte - d).days < 7 for d in viajes)
            assert f[1] == sum((corte - d).days < 30 for d in viajes)
            assert f[2] == sum((corte - d).days < 90 for d in viajes)
            assert f[3] == len(viajes)
            assert f[4] == min((corte - d).days for d, _ in evs)


def test_fecha_de_corte_fija_excluye_eventos_posteriores(silver):
    cfg = silver
    construir_silver(cfg, {"fecha_corte": "2026-06-15"})
    with wh(cfg) as con:
        assert con.execute("select max(fecha_corte) from features.features_usuario").fetchone()[0] == date(2026, 6, 15)
        esperado = con.execute("select count(*) from silver.silver_evento_unificado "
                               "where fecha <= date '2026-06-15' and not es_transbordo_operador").fetchone()[0]
        assert con.execute("select sum(viajes_total) from features.features_usuario").fetchone()[0] == esperado
    construir_silver(cfg)  # restaura el corte por defecto para las demás pruebas


def test_cobertura_incluye_las_zonas_sin_estaciones(silver):
    with wh(silver) as con:
        n_zonas = con.execute("select count(*) from silver.dim_zona").fetchone()[0]
        assert con.execute("select count(*) from gold.agg_cobertura_zona").fetchone()[0] == n_zonas
        assert con.execute("select sum(estaciones_total) from gold.agg_cobertura_zona").fetchone()[0] == \
            con.execute("select count(*) from silver.silver_estacion").fetchone()[0]
        sin = {z for (z,) in con.execute("select zona_id from gold.agg_cobertura_zona where sin_servicio").fetchall()}
        con_est = {z for (z,) in con.execute("select distinct zona_id from silver.silver_estacion").fetchall()}
        assert sin.isdisjoint(con_est) and sin | con_est >= {z for (z,) in con.execute("select zona_id from silver.dim_zona").fetchall()}


def test_transbordo_y_caso_metroriel(silver):
    with wh(silver) as con:
        # cada persona en exactamente una combinación
        assert con.execute("select sum(personas) from gold.agg_usuarios_por_combinacion").fetchone()[0] == \
            con.execute("select count(distinct persona_id) from silver.silver_evento_unificado").fetchone()[0]
        # usuarios con más de un sistema, recalculado con otra consulta
        multi = con.execute("select sum(personas) from gold.agg_usuarios_por_combinacion where n_modos > 1").fetchone()[0]
        ref = con.execute("select count(*) from (select persona_id from silver.silver_evento_unificado "
                          "group by 1 having count(distinct operador_cod) > 1)").fetchone()[0]
        assert multi == ref
        # caso MetroRiel: las zonas del enunciado quedan marcadas y no se pierde ninguna zona
        marcadas = {z for (z,) in con.execute("select zona_id from gold.agg_caso_metroriel where zona_en_caso").fetchall()}
        assert marcadas == {12, 8, 1, 6, 17}
        assert con.execute("select sum(viajes) from gold.agg_od_metroriel").fetchone()[0] == \
            con.execute("select count(*) from silver.silver_mr_viajes").fetchone()[0]


def test_linaje_features_y_datos_personales(silver):
    manifest = json.loads((silver.lake_dir / "dbt_target" / "manifest.json").read_text(encoding="utf-8"))
    assert violaciones_linaje(manifest) == []
    assert violaciones_features(manifest) == []
    with wh(silver) as con:
        assert violaciones_datos_personales(con, "gold") == []
        assert violaciones_datos_personales(con, "features") == []


def test_fase2_es_idempotente(silver):
    antes = {(f["capa"], f["tabla"]): (f["filas"], f["huella"]) for f in contar_capas(silver)}
    construir_silver(silver)
    despues = {(f["capa"], f["tabla"]): (f["filas"], f["huella"]) for f in contar_capas(silver)}
    assert ("features", "features_usuario") in antes and antes == despues


def test_exportador_tableau_solo_gold(silver, tmp_path):
    from flows.exportar_tableau import exportar
    salida = dict(exportar(silver, tmp_path, hechos=True))
    assert {"agg_cobertura_zona", "agg_caso_metroriel", "fact_abordaje", "dim_estacion"} <= set(salida)
    from flows.exportar_tableau import AGREGADOS, DIMENSIONES, HECHOS
    assert {p.stem for p in tmp_path.iterdir()} <= set(AGREGADOS + DIMENSIONES + HECHOS)   # nada de silver ni dim_usuario
    assert (tmp_path / "agg_cobertura_zona.csv").read_text(encoding="utf-8").startswith("zona_id,")
