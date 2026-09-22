"""Flujo Prefect de la capa Bronze.

Orden de dependencias:
    topics ─► publicar(transmetro) ─► consumir(transmetro) ┐
<<<<<<< HEAD
           └► publicar(aerometro)  ─► consumir(aerometro)  ├─► conciliar Bronze ─► dbt (staging, Silver, pruebas)
=======
           └► publicar(aerometro)  ─► consumir(aerometro)  ├─► conciliar
>>>>>>> 879857a (Capa Bronze)
    catálogos, Transurbano, MetroRiel, CDC (en paralelo) ─┘

Es idempotente: reejecutarlo con los mismos archivos no agrega filas (las tareas de carga
omiten los hashes ya ingeridos y el productor no republica un archivo ya publicado).
Cada tarea reintenta 2 veces; los reintentos son seguros por las garantías de cada vía.

    python -m flows.bronze_flow                      # todo
    python -m flows.bronze_flow --sin-streaming      # solo batch + CDC (no requiere Kafka)
<<<<<<< HEAD
    python -m flows.bronze_flow --sin-silver         # solo Bronze
=======
>>>>>>> 879857a (Capa Bronze)
"""
from __future__ import annotations

import argparse

from prefect import flow, task, get_run_logger

from bronze.common import nuevo_run_id
from bronze.conciliacion import a_markdown, conciliar
from bronze.config import Config
from bronze.fuentes import BATCH, CDC, FUENTES, STREAMING
from bronze.ingesta_archivos import ingerir_archivo
<<<<<<< HEAD
from flows.dbt_runner import construir_silver
=======
>>>>>>> 879857a (Capa Bronze)


@task(name="ingesta-archivo", retries=2, retry_delay_seconds=10)
def tarea_archivo(clave: str, run_id: str) -> dict:
    reg = ingerir_archivo(Config.desde_entorno(), FUENTES[clave], run_id)
    return {k: reg.get(k) for k in ("clave", "estado", "filas_escritas_bronze", "filas_malformadas")}


@task(name="crear-topics", retries=3, retry_delay_seconds=10)
def tarea_topics() -> None:
    from bronze.streaming import asegurar_topics
    asegurar_topics(Config.desde_entorno(), [f.topic for f in STREAMING])


@task(name="publicar-kafka", retries=2, retry_delay_seconds=15)
def tarea_publicar(clave: str, run_id: str, tamano_rafaga: int, pausa: float, max_filas) -> dict:
    from bronze.streaming import publicar_archivo
    reg = publicar_archivo(Config.desde_entorno(), FUENTES[clave], tamano_rafaga=tamano_rafaga,
                           pausa=pausa, max_filas=max_filas, run_id=run_id)
    return {k: reg.get(k) for k in ("clave", "estado", "filas_leidas_origen", "ultima_linea")}


@task(name="consumir-kafka", retries=2, retry_delay_seconds=15)
def tarea_consumir(clave: str, run_id: str) -> dict:
    from bronze.streaming import ConsumidorBronze
    reg = ConsumidorBronze(Config.desde_entorno(), FUENTES[clave]).ejecutar(modo="drain", run_id=run_id)
    return {k: reg.get(k) for k in ("clave", "estado", "filas_escritas_bronze", "filas_malformadas")}


@task(name="conciliar")
def tarea_conciliar(claves: list[str], estricto: bool) -> list[dict]:
    logger = get_run_logger()
    filas = conciliar(Config.desde_entorno(), claves)
    logger.info("\n" + a_markdown(filas))
    malas = [r for r in filas if r["estado"] != "OK"]
    if malas and estricto:
        raise RuntimeError("Conciliación con descuadres: " + ", ".join(f"{r['fuente']}={r['estado']}" for r in malas))
    return filas


<<<<<<< HEAD
@task(name="silver-dbt", retries=1, retry_delay_seconds=10)
def tarea_silver() -> None:
    """Vistas de Bronze + dbt build (semillas, staging, Silver y sus pruebas)."""
    construir_silver(Config.desde_entorno())


@flow(name="bronze-silver")
def bronze_flow(incluir_streaming: bool = True, tamano_rafaga: int = 500, pausa: float = 2.0,
                max_filas: int | None = None, estricto: bool = True, incluir_silver: bool = True) -> list[dict]:
=======
@flow(name="bronze")
def bronze_flow(incluir_streaming: bool = True, tamano_rafaga: int = 500, pausa: float = 2.0,
                max_filas: int | None = None, estricto: bool = True) -> list[dict]:
>>>>>>> 879857a (Capa Bronze)
    run_id = nuevo_run_id()
    esperar = [tarea_archivo.submit(f.clave, run_id) for f in BATCH + CDC]
    claves = [f.clave for f in BATCH + CDC]

    if incluir_streaming:
        topics = tarea_topics.submit()
        for f in STREAMING:
            pub = tarea_publicar.submit(f.clave, run_id, tamano_rafaga, pausa, max_filas, wait_for=[topics])
            esperar.append(tarea_consumir.submit(f.clave, run_id, wait_for=[pub]))
            claves.append(f.clave)

<<<<<<< HEAD
    conciliacion = tarea_conciliar.submit(claves, estricto, wait_for=esperar)
    if incluir_silver:
        tarea_silver.submit(wait_for=[conciliacion]).result()
    return conciliacion.result()
=======
    return tarea_conciliar.submit(claves, estricto, wait_for=esperar).result()
>>>>>>> 879857a (Capa Bronze)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sin-streaming", action="store_true")
<<<<<<< HEAD
    ap.add_argument("--sin-silver", action="store_true")
=======
>>>>>>> 879857a (Capa Bronze)
    ap.add_argument("--tamano-rafaga", type=int, default=500)
    ap.add_argument("--pausa", type=float, default=2.0)
    ap.add_argument("--max-filas", type=int)
    a = ap.parse_args()
    bronze_flow(incluir_streaming=not a.sin_streaming, tamano_rafaga=a.tamano_rafaga,
<<<<<<< HEAD
                pausa=a.pausa, max_filas=a.max_filas, incluir_silver=not a.sin_silver)
=======
                pausa=a.pausa, max_filas=a.max_filas)
>>>>>>> 879857a (Capa Bronze)
