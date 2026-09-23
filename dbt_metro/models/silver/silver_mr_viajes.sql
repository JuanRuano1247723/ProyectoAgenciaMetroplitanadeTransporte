select
    evento_sk,
    {{ pseudonimo("'metroriel:' || tarjeta") }}                         as usuario_sk,
    trip_id, tarjeta, estacion_entrada, estacion_salida,
    ts_entrada, ts_salida, cast(ts_entrada as date) as fecha, hour(ts_entrada) as hora_entrada,
    duracion_s,
    tarifa_gtq,
    not list_contains(reglas_advertencia, 'estacion_sin_catalogo')   as estaciones_en_catalogo,
    list_contains(reglas_advertencia, 'duracion_no_coincide')        as duracion_no_coincide,
    _file_hash, _source_line_number, _ingested_at
from {{ ref('stg_mr_viajes') }}
where len(reglas_rechazo) = 0
