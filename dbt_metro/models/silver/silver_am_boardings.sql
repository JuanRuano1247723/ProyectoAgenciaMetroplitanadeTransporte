select
    evento_sk,
    md5('aerometro:' || user_hash)                                   as usuario_sk,
    boarding_id, user_hash, station_code, axis, cabin_number,
    ts_utc, ts_local, cast(ts_local as date) as fecha, hour(ts_local) as hora,
    tarifa_gtq,
    not list_contains(reglas_advertencia, 'estacion_sin_catalogo')   as estacion_en_catalogo,
    _file_hash, _source_line_number, _ingested_at, _kafka_partition, _kafka_offset
from {{ ref('stg_am_boardings') }}
where len(reglas_rechazo) = 0
