{#
  silver_evento_unificado
  GRANO: un evento de entrada (abordaje) por fila, de los cuatro operadores. Solo filas ACEPTADAS
  (las rechazadas ya están en silver_cuarentena y no llegan a los silver_* de origen).
  Es el detalle atómico conformado (hora local, quetzales, zona, persona) del que se alimentan:
    - la capa de features (rúbrica 2.3: "se debe sacar de Silver, no de Gold"),
    - los agregados de Gold de demanda, cobertura y transbordo.
  Solo expone seudónimos (usuario_sk, persona_id): no se selecciona tarjeta, num_tarjeta, card ni user_hash.
  Conserva _file_hash y _source_line_number para llegar al archivo crudo (linaje a nivel de fila).
#}
with eventos as (

    select 'TM' as operador_cod, 'transmetro' as operador, evento_sk, usuario_sk, ts_local,
           estacion_id           as estacion_id_original,
           tarifa_gtq            as monto_gtq,
           (upper(tipo) = 'TRANSBORDO') as es_transbordo_operador,
           _file_hash, _source_line_number
    from {{ ref('silver_tm_validaciones') }}

    union all
    select 'TU', 'transurbano', evento_sk, usuario_sk, ts_local,
           cod_parada, monto_gtq, false, _file_hash, _source_line_number
    from {{ ref('silver_tu_transacciones') }}

    union all
    select 'AM', 'aerometro', evento_sk, usuario_sk, ts_local,
           station_code, tarifa_gtq, false, _file_hash, _source_line_number
    from {{ ref('silver_am_boardings') }}

    union all
    select 'MR', 'metroriel', evento_sk, usuario_sk, ts_entrada,
           estacion_entrada, tarifa_gtq, false, _file_hash, _source_line_number
    from {{ ref('silver_mr_viajes') }}

)

select
    e.evento_sk,
    e.operador_cod,
    e.usuario_sk,
    coalesce(u.persona_id, e.usuario_sk)             as persona_id,   -- Aerómetro (o identidad apagada): persona = usuario
    e.ts_local,
    cast(e.ts_local as date)                         as fecha,
    cast(hour(e.ts_local) as integer)                as hora,
    e.estacion_id_original,
    coalesce(s.zona_id, -1)                          as zona_id,      -- -1 = sin mapear (ver assert_zonas_sin_mapear)
    e.monto_gtq,
    e.es_transbordo_operador,
    e._file_hash,
    e._source_line_number
from eventos e
left join {{ ref('silver_usuario') }} u  on u.usuario_sk = e.usuario_sk
left join {{ ref('silver_estacion') }} s on s.operador = e.operador
                                        and s.estacion_id_original = e.estacion_id_original
