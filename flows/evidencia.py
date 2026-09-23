"""Evidencia de idempotencia (entregable 1.5): corre el flujo dos veces, cuenta cada capa después de cada corrida
y compara. Escribe un informe en evidencia/idempotencia_<fecha>.md.

    python -m flows.evidencia                    # flujo completo (requiere Kafka)
    python -m flows.evidencia --sin-streaming    # solo batch + CDC + Silver (Bronze de streaming ya cargado)
    python -m flows.evidencia --pausa 0          # publica sin pausas entre ráfagas
"""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from bronze.config import RAIZ, Config
from flows.bronze_flow import bronze_flow
from flows.dbt_runner import contar_capas


def comparar(a: list[dict], b: list[dict]) -> list[dict]:
    idx = {(r["capa"], r["tabla"]): r for r in b}
    return [{**r, "filas_2": idx[(r["capa"], r["tabla"])]["filas"], "huella_2": idx[(r["capa"], r["tabla"])]["huella"],
             "identico": r["filas"] == idx[(r["capa"], r["tabla"])]["filas"] and r["huella"] == idx[(r["capa"], r["tabla"])]["huella"]}
            for r in a]


def informe_md(comp: list[dict]) -> str:
    ok = all(r["identico"] for r in comp)
    md = ["# Evidencia de idempotencia", "",
          f"Generado: {datetime.now().isoformat(timespec='seconds')}", "",
          "Se ejecutó el flujo completo dos veces con los mismos archivos de origen y se contó cada tabla después de cada corrida.",
          "La huella es un XOR de hashes de la llave de cada fila: detecta filas cambiadas o sustituidas, no solo el conteo.", "",
          f"**Resultado: {'CONTEOS Y HUELLAS IDÉNTICOS' if ok else 'HAY DIFERENCIAS'}**", "",
          "| capa | tabla | filas corrida 1 | filas corrida 2 | idéntico |", "|---|---|---:|---:|:---:|"]
    md += [f"| {r['capa']} | {r['tabla']} | {r['filas']:,} | {r['filas_2']:,} | {'sí' if r['identico'] else 'NO'} |" for r in comp]
    return "\n".join(md) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sin-streaming", action="store_true")
    ap.add_argument("--pausa", type=float, default=2.0)
    ap.add_argument("--tamano-rafaga", type=int, default=500)
    a = ap.parse_args()
    cfg = Config.desde_entorno()
    kw = dict(incluir_streaming=not a.sin_streaming, pausa=a.pausa, tamano_rafaga=a.tamano_rafaga)

    bronze_flow(**kw)
    corrida1 = contar_capas(cfg)
    bronze_flow(**kw)
    corrida2 = contar_capas(cfg)

    comp = comparar(corrida1, corrida2)
    md = informe_md(comp)
    destino = RAIZ / "evidencia" / f"idempotencia_{datetime.now():%Y%m%d_%H%M%S}.md"
    destino.parent.mkdir(exist_ok=True)
    destino.write_text(md, encoding="utf-8")
    print(md)
    print(f"Informe guardado en {destino}")
    return 0 if all(r["identico"] for r in comp) else 1


if __name__ == "__main__":
    raise SystemExit(main())
