{#
  dim_hora: 24 filas (0-23). `es_hora_pico` se DERIVA del volumen real de abordajes por hora
  (las cuatro fuentes juntas), no de un rango supuesto como "7-9 y 17-19". Una hora es pico si su
  total supera {{ var('umbral_hora_pico') }} veces el promedio horario. `franja` es una etiqueta
  fija de conveniencia (no una clasificación de negocio).
#}
with abordajes_por_hora as (
    select hora, count(*) as total from {{ ref('silver_tm_validaciones') }} group by hora
    union all
    select hora, count(*) from {{ ref('silver_tu_transacciones') }} group by hora
    union all
    select hora, count(*) from {{ ref('silver_am_boardings') }} group by hora
    union all
    select hour(ts_entrada), count(*) from {{ ref('silver_mr_viajes') }} group by 1
),
totales as (
    select hora, sum(total) as abordajes_totales
    from abordajes_por_hora
    group by hora
),
umbral as (
    select avg(abordajes_totales) * {{ var('umbral_hora_pico') }} as corte
    from totales
),
horas as (
    select cast(range as integer) as hora from range(0, 24)
)
select
    h.hora                                                          as hora_sk,
    h.hora,
    case
        when h.hora between 0 and 4   then 'madrugada'
        when h.hora between 5 and 11  then 'manana'
        when h.hora between 12 and 13 then 'mediodia'
        when h.hora between 14 and 18 then 'tarde'
        else 'noche'
    end                                                              as franja,
    coalesce(t.abordajes_totales, 0)                                as abordajes_totales_periodo,
    coalesce(t.abordajes_totales, 0) >= (select corte from umbral)  as es_hora_pico
from horas h
left join totales t on t.hora = h.hora
order by h.hora
