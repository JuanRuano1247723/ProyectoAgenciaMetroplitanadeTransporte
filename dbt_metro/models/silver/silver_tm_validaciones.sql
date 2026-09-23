select
    evento_sk,
    {{ pseudonimo("'transmetro:' || tarjeta") }}                       as usuario_sk,
    validacion_id, tarjeta, estacion_id, linea, tipo,
    ts_local, cast(ts_local as date) as fecha, hour(ts_local)       as hora,
    tarifa_gtq,
    not list_contains(reglas_advertencia, 'estacion_sin_catalogo')  as estacion_en_catalogo,
    not list_contains(reglas_advertencia, 'tarjeta_sin_padron')     as tarjeta_en_padron,
    list_contains(reglas_advertencia, 'posible_doble_lectura')      as posible_doble_lectura,
    _file_hash, _source_line_number, _ingested_at, _kafka_partition, _kafka_offset
from {{ ref('stg_tm_validaciones') }}
where len(reglas_rechazo) = 0
