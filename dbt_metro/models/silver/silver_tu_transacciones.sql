select
    evento_sk,
    md5('transurbano:' || num_tarjeta)                               as usuario_sk,
    num_tarjeta, cod_parada, ruta, cod_estado,
    ts_local, cast(ts_local as date) as fecha, hour(ts_local)        as hora,
    monto_gtq,
    not list_contains(reglas_advertencia, 'parada_sin_catalogo')     as parada_en_catalogo,
    _file_hash, _source_line_number, _ingested_at
from {{ ref('stg_tu_transacciones') }}
where len(reglas_rechazo) = 0
