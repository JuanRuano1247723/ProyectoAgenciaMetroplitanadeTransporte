{#
  agg_transbordo_temporal   -- transbordos entre operadores INFERIDOS por cercanía en el tiempo
  Un "transbordo inferido" = dos abordajes consecutivos de la misma persona en operadores distintos con
  menos de `ventana_transbordo_min` minutos entre ellos (por defecto 60). Es el extra opcional de análisis de
  transbordo; complementa a (1) usuarios con >1 modo (agg_usuarios_por_combinacion) y
  (2) los TRANSBORDO que Transmetro registra por sí mismo (es_transbordo_operador).
  Mismo caveat de identidad que agg_usuarios_por_combinacion.
#}
with sec as (
    select
        persona_id, operador_cod, ts_local,
        lag(operador_cod) over w as op_previo,
        lag(ts_local)     over w as ts_previo
    from {{ ref('silver_evento_unificado') }}
    window w as (partition by persona_id order by ts_local, evento_sk)
)

select
    op_previo       as de_operador,
    operador_cod    as a_operador,
    cast(extract(hour from ts_local) as integer) as hora,
    count(*)                       as transbordos,
    count(distinct persona_id)     as personas      -- no aditivo entre filas; usar solo a este grano
from sec
where op_previo is not null
  and op_previo <> operador_cod
  and date_diff('minute', ts_previo, ts_local) <= {{ var('ventana_transbordo_min', 60) }}
group by all
