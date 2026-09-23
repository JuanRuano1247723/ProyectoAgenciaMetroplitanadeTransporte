{# Dimensión conformada de estaciones/paradas de las cuatro redes, con la zona ya conformada. #}
select
    md5(e.operador || ':' || e.estacion_id_original)   as estacion_sk,
    e.operador,
    e.estacion_id_original,
    e.nombre,
    e.linea_o_ruta,
    e.zona_original,
    coalesce(z.zona_id, -1)                            as zona_id,
    e.lat, e.lon, e.km
from {{ ref('stg_estaciones') }} e
left join {{ ref('dim_zona') }} z on z.clave_normalizada = e.clave_zona
