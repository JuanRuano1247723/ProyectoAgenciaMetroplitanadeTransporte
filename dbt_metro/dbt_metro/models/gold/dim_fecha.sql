{#
  dim_fecha: una fila por cada fecha observada en los hechos (más los días intermedios sin eventos).
  es_dia_habil = lunes a viernes Y no feriado (seed feriados.csv). El rango sale de los propios datos,
  no de una constante, para que la dimensión nunca quede corta si el periodo de datos cambia.
#}
with rango as (
    select min(fecha) as desde, max(fecha) as hasta
    from (
        select fecha from {{ ref('silver_tm_validaciones') }}
        union all select fecha from {{ ref('silver_tu_transacciones') }}
        union all select fecha from {{ ref('silver_am_boardings') }}
        union all select fecha from {{ ref('silver_mr_viajes') }}
    )
),
calendario as (
    select cast(r.range as date) as fecha
    from rango, range(rango.desde, rango.hasta + interval 1 day, interval 1 day) as r
)
select
    md5(cast(c.fecha as varchar))                                  as fecha_sk,
    c.fecha,
    year(c.fecha)                                                  as anio,
    month(c.fecha)                                                 as mes,
    day(c.fecha)                                                   as dia_del_mes,
    dayofweek(c.fecha)                                             as dia_semana_num,  -- 0=domingo … 6=sábado
    dayname(c.fecha)                                               as dia_semana_nombre,
    (f.fecha is not null)                                          as es_feriado,
    f.nombre                                                       as nombre_feriado,
    (dayofweek(c.fecha) not in (0, 6) and f.fecha is null)         as es_dia_habil
from calendario c
left join {{ ref('feriados') }} f on f.fecha = c.fecha
order by c.fecha
