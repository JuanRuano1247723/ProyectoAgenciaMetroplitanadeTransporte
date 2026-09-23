{# Catálogos de las cuatro redes, en una sola forma. Solo el último snapshot de cada catálogo. #}
with tm as (
    select 'transmetro' as operador, trim(estacion_id) as estacion_id_original, nombre,
           linea as linea_o_ruta, trim(zona) as zona_original,
           try_cast(lat as double) as lat, try_cast(lon as double) as lon, cast(null as double) as km,
           _file_hash, _ingested_at
    from {{ source('bronze', 'transmetro_estaciones') }}
    qualify dense_rank() over (order by _ingested_at desc) = 1
),
tu as (
    select 'transurbano', trim(cod_parada), descripcion, ruta, trim(sector),
           cast(null as double), cast(null as double), cast(null as double), _file_hash, _ingested_at
    from {{ source('bronze', 'transurbano_paradas') }}
    qualify dense_rank() over (order by _ingested_at desc) = 1
),
mr as (
    select 'metroriel', trim(id_estacion), nombre_estacion, cast(null as varchar), trim(zona_nombre),
           cast(null as double), cast(null as double), try_cast(km as double), _file_hash, _ingested_at
    from {{ source('bronze', 'metroriel_estaciones') }}
    qualify dense_rank() over (order by _ingested_at desc) = 1
),
am as (
    select 'aerometro', trim(station_code), station_name, axis, trim(district),
           cast(null as double), cast(null as double), cast(null as double), _file_hash, _ingested_at
    from {{ source('bronze', 'aerometro_estaciones') }}
    qualify dense_rank() over (order by _ingested_at desc) = 1
)
select *, {{ clave_zona('zona_original') }} as clave_zona
from (
    select * from tm union all select * from tu union all select * from mr union all select * from am
)
