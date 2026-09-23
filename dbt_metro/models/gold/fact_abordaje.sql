{#
  GRANO: una fila por cada evento de entrada al sistema, para cualquier medio de transporte
  (cada validación de Transmetro, cada transacción de Transurbano, cada abordaje de Aerómetro,
  y la entrada de cada viaje de MetroRiel).

  Medidas (ver docs/DISENO_GOLD_GRANO.md §4):
    abordajes  -- aditiva (conteo)
    monto_gtq  -- aditiva (dinero, ya unificado a quetzales en Silver)
  Ninguna razón se guarda aquí: tarifa promedio = sum(monto_gtq) / sum(abordajes) en la consulta.
#}
with eventos as (
    select
        'TM'                        as operador_cod,
        'transmetro'                as operador_estacion,
        evento_sk, usuario_sk, fecha, hora,
        estacion_id                 as estacion_id_original,
        tarifa_gtq                  as monto_gtq,
        tipo                        as tipo_evento,
        _file_hash, _source_line_number
    from {{ ref('silver_tm_validaciones') }}

    union all

    select
        'TU', 'transurbano',
        evento_sk, usuario_sk, fecha, hora,
        cod_parada,
        monto_gtq,
        cast(cod_estado as varchar),
        _file_hash, _source_line_number
    from {{ ref('silver_tu_transacciones') }}

    union all

    select
        'AM', 'aerometro',
        evento_sk, usuario_sk, fecha, hora,
        station_code,
        tarifa_gtq,
        cast(null as varchar),
        _file_hash, _source_line_number
    from {{ ref('silver_am_boardings') }}

    union all

    select
        'MR', 'metroriel',
        evento_sk, usuario_sk, fecha, hora_entrada,
        estacion_entrada,
        tarifa_gtq,
        'ENTRADA',
        _file_hash, _source_line_number
    from {{ ref('silver_mr_viajes') }}
)
select
    e.evento_sk,
    t.transporte_sk,
    f.fecha_sk,
    cast(e.hora as integer)                     as hora_sk,
    est.estacion_sk,
    e.usuario_sk,
    e.tipo_evento,
    1                                           as abordajes,
    e.monto_gtq,
    e._file_hash,
    e._source_line_number
from eventos e
join {{ ref('dim_transporte') }} t on t.operador_cod = e.operador_cod
join {{ ref('dim_fecha') }}     f on f.fecha = e.fecha
left join {{ ref('dim_estacion') }} est
       on est.operador = e.operador_estacion
      and est.estacion_id_original = e.estacion_id_original
