{#
  agg_caso_metroriel   -- Tableau: "¿el trazado propuesto atiende donde más se necesita?"
  Una fila por zona: demanda de los OTROS tres modos (la demanda "observada" que MetroRiel no genera),
  presencia de MetroRiel (estaciones y abordajes), y el ranking de demanda.
  `zona_en_caso` marca las zonas del enunciado (12, 8, 1, 6, 17), configurables con la variable
  zonas_caso_metroriel. Se calcula para TODAS las zonas para poder compararlas contra el resto de la ciudad.
  >>> Verifica en el enunciado si 12, 8, 1, 6 y 17 son las zonas del trazado o las de mayor demanda;
  >>> el modelo sirve para ambas lecturas porque trae la demanda y la presencia de MetroRiel de cada zona.
#}
with dem as (
    select
        zona_id,
        count(*) filter (where operador_cod <> 'MR') as abordajes_otros_modos,
        count(*) filter (where operador_cod =  'MR') as abordajes_mr
    from {{ ref('silver_evento_unificado') }}
    where zona_id <> -1
    group by zona_id
),

est as (
    select zona_id, count(*) as estaciones_mr
    from {{ ref('silver_estacion') }}
    where operador = 'metroriel'
    group by zona_id
),

z as (
    select zona_id, zona_nombre from {{ ref('dim_zona') }} where zona_id <> -1
)

select
    z.zona_id,
    z.zona_nombre,
    z.zona_id in ({{ var('zonas_caso_metroriel', '12, 8, 1, 6, 17') }})            as zona_en_caso,
    coalesce(d.abordajes_otros_modos, 0)                                           as abordajes_otros_modos,
    coalesce(d.abordajes_mr, 0)                                                    as abordajes_mr,
    coalesce(e.estaciones_mr, 0)                                                   as estaciones_mr,
    coalesce(e.estaciones_mr, 0) > 0                                               as tiene_estacion_mr,
    rank() over (order by coalesce(d.abordajes_otros_modos, 0) desc)               as ranking_demanda_otros_modos,
    coalesce(d.abordajes_otros_modos, 0) * 1.0
        / sum(coalesce(d.abordajes_otros_modos, 0)) over ()                        as pct_demanda_otros_modos
from z
left join dem d using (zona_id)
left join est e using (zona_id)
