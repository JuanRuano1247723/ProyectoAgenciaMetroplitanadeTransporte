{#
  agg_od_metroriel   -- matriz origen-destino de MetroRiel (único operador con salida real)
  Grano: zona de entrada x zona de salida. Medidas aditivas; la duración promedio se calcula en Tableau
  como SUM(duracion_s)/SUM(viajes), nunca se guarda (G-10).
#}
select
    coalesce(se.zona_id, -1)  as zona_entrada,
    coalesce(ss.zona_id, -1)  as zona_salida,
    count(*)                  as viajes,
    sum(v.duracion_s)         as duracion_s,
    sum(v.tarifa_gtq)         as tarifa_gtq
from {{ ref('silver_mr_viajes') }} v
left join {{ ref('silver_estacion') }} se
       on se.operador = 'metroriel' and se.estacion_id_original = v.estacion_entrada
left join {{ ref('silver_estacion') }} ss
       on ss.operador = 'metroriel' and ss.estacion_id_original = v.estacion_salida
group by all
