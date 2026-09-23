"""Verifica dos reglas de Gold que el enunciado penaliza o exige, sobre el proyecto dbt ya construido:

  1. Linaje: un modelo de Gold solo puede depender de Silver (u otro Gold) y de semillas. Si lee un source
     (Bronze) o staging, "pierde la totalidad de 1.4, aunque el resultado numérico sea correcto".
  2. Datos personales: Gold no debe contener llaves de tarjeta crudas (ni por nombre de columna ni por el
     aspecto de sus valores), solo los seudónimos usuario_sk / persona_id.

    python -m flows.dbt_runner build && python -m flows.verificar_linaje

Sale con código 1 si hay violaciones (sirve como paso de CI o como demostración en la defensa).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb

from bronze.config import Config

COLUMNAS_CRUDAS = {"tarjeta", "num_tarjeta", "user_hash", "card", "llave_original", "llave", "id_numerico"}
PATRON_LLAVE_CRUDA = r"^(TC-[0-9]{8}|MR[0-9]{7}|[0-9]{10}|[0-9a-f]{12})$"


def es_gold(nodo: dict) -> bool:
    return nodo.get("resource_type") == "model" and (
        str(nodo.get("path", "")).replace("\\", "/").startswith("gold/") or nodo.get("schema") == "gold")


def violaciones_linaje(manifest: dict) -> list[str]:
    nodos = manifest["nodes"]
    fuera = []
    for nodo in nodos.values():
        if not es_gold(nodo):
            continue
        for dep in nodo["depends_on"]["nodes"]:
            if dep.startswith("source."):
                fuera.append(f"{nodo['name']} lee directamente de Bronze ({dep}). Gold solo puede leer Silver.")
            elif dep.startswith("model."):
                d = nodos[dep]
                if not (es_gold(d) or d.get("schema") == "silver" or str(d.get("path", "")).startswith("silver/")):
                    fuera.append(f"{nodo['name']} depende de {d['name']} (capa {d.get('schema')}); "
                                 f"Gold solo puede depender de Silver o de otro modelo Gold.")
    return fuera


def violaciones_datos_personales(con, esquema: str = "gold", muestra: int = 5000) -> list[str]:
    fuera = []
    columnas = con.execute(
        "select table_name, column_name, data_type from information_schema.columns where table_schema = ?",
        [esquema]).fetchall()
    for tabla, col, tipo in columnas:
        if col.lower() in COLUMNAS_CRUDAS:
            fuera.append(f"{esquema}.{tabla}.{col}: columna con el nombre de una llave cruda de usuario.")
        elif tipo.upper().startswith("VARCHAR"):
            hay = con.execute(
                f'select count(*) from (select "{col}" as v from {esquema}."{tabla}" limit {muestra}) '
                f"where regexp_matches(v, '{PATRON_LLAVE_CRUDA}')").fetchone()[0]
            if hay:
                fuera.append(f"{esquema}.{tabla}.{col}: {hay} valores con aspecto de llave cruda (tarjeta o hash de operador).")
    return fuera


def main() -> int:
    cfg = Config.desde_entorno()
    manifest_p = cfg.lake_dir / "dbt_target" / "manifest.json"
    wh_p = cfg.lake_dir / "warehouse.duckdb"
    if not manifest_p.exists() or not wh_p.exists():
        print("No hay proyecto construido. Corre primero: python -m flows.dbt_runner build")
        return 2
    manifest = json.loads(manifest_p.read_text(encoding="utf-8"))
    n_gold = sum(es_gold(n) for n in manifest["nodes"].values())
    con = duckdb.connect(str(wh_p), read_only=True)
    problemas = violaciones_linaje(manifest) + (violaciones_datos_personales(con) if n_gold else [])
    con.close()
    print(f"Modelos Gold encontrados: {n_gold}")
    if not n_gold:
        print("(Aún no hay modelos en models/gold: nada que verificar. Las reglas se aplicarán al crear el primero.)")
    for p in problemas:
        print("  VIOLACIÓN:", p)
    if not problemas:
        print("OK: Gold solo lee Silver y no expone llaves crudas.")
    return 1 if problemas else 0


if __name__ == "__main__":
    sys.exit(main())
