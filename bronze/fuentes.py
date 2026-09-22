"""Registro de las nueve fuentes: vía de ingesta, columnas esperadas y regla de partición.

Las columnas son las de los encabezados reales (ya en minúsculas), verificadas en el
notebook de exploración. Si un archivo llega con otro encabezado, la carga falla en lugar
de guardar datos desalineados.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

# Regla de partición (columna física de organización, no interpretación del dato):
#   "ingesta": fecha de ingesta            "iso": primeros 10 caracteres YYYY-MM-DD
#   "dmy":     DD/MM/YYYY -> YYYY-MM-DD (reordenamiento de texto, sin parsear fechas)


@dataclass(frozen=True)
class Fuente:
    clave: str
    operador: str
    entidad: str
    archivo: str
    via: str                       # "batch" | "streaming" | "cdc"
    formato: str                   # "csv" | "jsonl"
    columnas: tuple
    regla_particion: str = "ingesta"
    campo_particion: Optional[str] = None
    topic: Optional[str] = None
    campo_llave_mensaje: Optional[str] = None

    @property
    def indice_particion(self) -> Optional[int]:
        if self.formato == "csv" and self.campo_particion:
            return self.columnas.index(self.campo_particion)
        return None

    @property
    def indice_llave(self) -> Optional[int]:
        if self.campo_llave_mensaje:
            return self.columnas.index(self.campo_llave_mensaje)
        return None


_LISTA = [
    # ---- catálogos (batch) ---------------------------------------------------
    Fuente("tm_estaciones", "transmetro", "estaciones", "tm_estaciones.csv", "batch", "csv",
           ("estacion_id", "nombre", "linea", "zona", "lat", "lon")),
    Fuente("tu_paradas", "transurbano", "paradas", "tu_paradas.csv", "batch", "csv",
           ("cod_parada", "descripcion", "ruta", "sector")),
    Fuente("mr_estaciones", "metroriel", "estaciones", "mr_estaciones.csv", "batch", "csv",
           ("id_estacion", "nombre_estacion", "zona_nombre", "km")),
    Fuente("am_estaciones", "aerometro", "estaciones", "am_estaciones.csv", "batch", "csv",
           ("station_code", "station_name", "axis", "district")),
    # ---- operativos ----------------------------------------------------------
    Fuente("transmetro_validaciones", "transmetro", "validaciones", "transmetro_validaciones.csv",
           "streaming", "csv",
           ("validacion_id", "tarjeta", "estacion_id", "linea", "fecha_hora", "tarifa", "tipo"),
           regla_particion="iso", campo_particion="fecha_hora",
           topic="transmetro.validaciones", campo_llave_mensaje="tarjeta"),
    Fuente("aerometro_boardings", "aerometro", "boardings", "aerometro_boardings.csv",
           "streaming", "csv",
           ("boarding_id", "user_hash", "station_code", "axis", "timestamp_utc", "cabin_number", "fare"),
           regla_particion="iso", campo_particion="timestamp_utc",
           topic="aerometro.boardings", campo_llave_mensaje="user_hash"),
    Fuente("transurbano_transacciones", "transurbano", "transacciones", "transurbano_transacciones.csv",
           "batch", "csv",
           ("fecha", "hora", "num_tarjeta", "cod_parada", "ruta", "monto_centavos", "cod_estado"),
           regla_particion="dmy", campo_particion="fecha"),
    Fuente("metroriel_viajes", "metroriel", "viajes", "metroriel_viajes.jsonl", "batch", "jsonl",
           ("trip_id", "card", "entry", "exit", "fare_gtq", "duration_s"),
           regla_particion="iso", campo_particion="entry.ts"),
    # ---- CDC -----------------------------------------------------------------
    Fuente("cdc_padron_usuarios", "transmetro", "padron_cdc", "cdc_padron_usuarios.csv", "cdc", "csv",
           ("seq", "commit_ts", "op", "tarjeta", "perfil", "zona_residencia", "estado")),
]

FUENTES = {f.clave: f for f in _LISTA}
STREAMING = [f for f in _LISTA if f.via == "streaming"]
BATCH = [f for f in _LISTA if f.via == "batch"]
CDC = [f for f in _LISTA if f.via == "cdc"]
