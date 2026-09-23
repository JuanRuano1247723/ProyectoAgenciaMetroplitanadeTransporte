"""Pruebas de seguridad y gobernanza que no necesitan dbt."""
import os

import duckdb

from bronze.config import cargar_env
from flows.verificar_linaje import violaciones_datos_personales, violaciones_linaje


def nodo(nombre, path, schema, deps, tipo="model"):
    return {"name": nombre, "path": path, "schema": schema, "resource_type": tipo, "depends_on": {"nodes": deps}}


MANIFEST_BASE = {
    "source.metro.bronze.transmetro_validaciones": {"name": "transmetro_validaciones", "resource_type": "source", "depends_on": {"nodes": []}},
    "model.metro.stg_tm_validaciones": nodo("stg_tm_validaciones", "staging/stg_tm_validaciones.sql", "staging",
                                            ["source.metro.bronze.transmetro_validaciones"]),
    "model.metro.silver_tm_validaciones": nodo("silver_tm_validaciones", "silver/silver_tm_validaciones.sql", "silver",
                                               ["model.metro.stg_tm_validaciones"]),
}


def con_gold(**modelos):
    nodos = dict(MANIFEST_BASE)
    for n, m in modelos.items():
        nodos[f"model.metro.{n}"] = m
    return {"nodes": nodos}


def test_gold_que_lee_silver_es_valido():
    m = con_gold(fact_abordaje=nodo("fact_abordaje", "gold/fact_abordaje.sql", "gold", ["model.metro.silver_tm_validaciones"]))
    assert violaciones_linaje(m) == []


def test_gold_que_lee_bronze_directamente_se_detecta():
    m = con_gold(fact_x=nodo("fact_x", "gold/fact_x.sql", "gold", ["source.metro.bronze.transmetro_validaciones"]))
    (v,) = violaciones_linaje(m)
    assert "fact_x" in v and "Bronze" in v


def test_gold_que_lee_staging_se_detecta():
    m = con_gold(fact_y=nodo("fact_y", "gold/fact_y.sql", "gold", ["model.metro.stg_tm_validaciones"]))
    (v,) = violaciones_linaje(m)
    assert "stg_tm_validaciones" in v


def test_gold_encadenado_a_otro_gold_es_valido():
    m = con_gold(
        dim_a=nodo("dim_a", "gold/dim_a.sql", "gold", ["model.metro.silver_tm_validaciones"]),
        fact_b=nodo("fact_b", "gold/fact_b.sql", "gold", ["model.metro.dim_a"]))
    assert violaciones_linaje(m) == []


def test_datos_personales_en_gold_por_nombre_y_por_valor():
    con = duckdb.connect()
    con.execute("create schema gold")
    con.execute("create table gold.limpia as select 'a1b2c3d4e5f60718293a' as usuario_sk, 1 as abordajes")   # seudónimo de 20 hex
    con.execute("create table gold.por_nombre as select 'x' as tarjeta")
    con.execute("create table gold.por_valor as select 'TC-00012345' as usuario")
    con.execute("create table gold.hash_operador as select 'e1f5e4512ced' as usuario")
    v = violaciones_datos_personales(con)
    assert any("por_nombre.tarjeta" in x for x in v)
    assert any("por_valor.usuario" in x for x in v)
    assert any("hash_operador.usuario" in x for x in v)
    assert not any("limpia" in x for x in v)


def test_cargar_env_lee_el_archivo_sin_pisar_el_entorno(tmp_path, monkeypatch):
    archivo = tmp_path / ".env"
    archivo.write_text("# comentario\nVAR_PRUEBA_A=valor_a\nVAR_PRUEBA_B=\"entre comillas\"\nVAR_PRUEBA_C=\nVAR_PRUEBA_D=del_archivo\n")
    monkeypatch.setenv("VAR_PRUEBA_D", "del_entorno")
    try:
        cargar_env(archivo)
        assert os.environ["VAR_PRUEBA_A"] == "valor_a" and os.environ["VAR_PRUEBA_B"] == "entre comillas"
        assert "VAR_PRUEBA_C" not in os.environ                 # valor vacío: no se define
        assert os.environ["VAR_PRUEBA_D"] == "del_entorno"      # el entorno real manda
    finally:
        for k in ("VAR_PRUEBA_A", "VAR_PRUEBA_B", "VAR_PRUEBA_C"):
            os.environ.pop(k, None)


def test_el_repositorio_no_trae_secretos_ni_datos():
    from pathlib import Path
    raiz = Path(__file__).resolve().parents[1]
    assert (raiz / ".env.example").exists() and not (raiz / ".env").exists()
    ejemplo = (raiz / ".env.example").read_text()
    assert "PSEUDONIMO_SECRETO=\n" in ejemplo                   # la variable existe, sin valor
    ignorado = (raiz / ".gitignore").read_text()
    for patron in (".env", "*.duckdb", "datos_red/*", "lake/"):
        assert patron in ignorado
