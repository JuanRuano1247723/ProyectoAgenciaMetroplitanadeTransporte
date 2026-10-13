{#
  agg_cobertura_zona   -- Tableau: "Cobertura: qué zonas quedan sin servicio de ningún modo"
  Una fila por zona/municipio de dim_zona (incluidas las que no tienen NINGUNA estación: un LEFT JOIN
  desde la dimensión es lo que las hace visibles; partir de las estaciones las haría desaparecer).

  Dos niveles de "sin servicio":
    sin_servicio          = ninguna estación o parada de ningún operador en la zona (cobertura de infraestructura).
    sin_servicio_efectivo = hay estación pero 0 abordajes en el periodo (cobertura de uso).
  La métrica de la rúbrica "zonas con servicio / sin servicio" sale de contar sobre sin_servicio.
  #}
with zonas as (
    select zona_id, zona_nombre, tipo as tipo_zona from {{ ref('dim_zona') }}
),

est as (
    select
        zona_id,
        count(*) filter (where operador = 'transmetro') as estaciones_tm,
        count(*) filter (where operador = 'transurbano') as paradas_tu,
        count(*) filter (where operador = 'aerometro') as estaciones_am,
        count(*) filter (where operador = 'metroriel') as estaciones_mr,
        count(*)                                    as estaciones_total
    from {{ ref('silver_estacion') }}
    group by zona_id
),

dem as (
    select
        zona_id,
        count(*) filter (where operador_cod = 'TM') as abordajes_tm,
        count(*) filter (where operador_cod = 'TU') as abordajes_tu,
        count(*) filter (where operador_cod = 'AM') as abordajes_am,
        count(*) filter (where operador_cod = 'MR') as abordajes_mr,
        count(*)                                    as abordajes_total
    from {{ ref('silver_evento_unificado') }}
    group by zona_id
)

select
    z.zona_id,
    z.zona_nombre,
    z.tipo_zona,
    coalesce(e.estaciones_tm, 0)    as estaciones_tm,
    coalesce(e.paradas_tu, 0)       as paradas_tu,
    coalesce(e.estaciones_am, 0)    as estaciones_am,
    coalesce(e.estaciones_mr, 0)    as estaciones_mr,
    coalesce(e.estaciones_total, 0) as estaciones_total,
    (coalesce(e.estaciones_tm, 0) > 0)::int + (coalesce(e.paradas_tu, 0) > 0)::int
      + (coalesce(e.estaciones_am, 0) > 0)::int + (coalesce(e.estaciones_mr, 0) > 0)::int
                                    as modos_con_servicio,
    coalesce(d.abordajes_tm, 0)     as abordajes_tm,
    coalesce(d.abordajes_tu, 0)     as abordajes_tu,
    coalesce(d.abordajes_am, 0)     as abordajes_am,
    coalesce(d.abordajes_mr, 0)     as abordajes_mr,
    coalesce(d.abordajes_total, 0)  as abordajes_total,
    coalesce(e.estaciones_total, 0) = 0                              as sin_servicio,
    coalesce(d.abordajes_total, 0)  = 0                              as sin_servicio_efectivo
from zonas z
left join est e using (zona_id)
left join dem d using (zona_id)
