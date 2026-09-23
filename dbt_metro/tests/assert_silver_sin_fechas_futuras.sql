select 'transmetro_validaciones' as fuente, count(*) as filas
from {{ ref('silver_tm_validaciones') }} where fecha > {{ fecha_ingesta('_ingested_at') }} having count(*) > 0
union all
select 'transurbano_transacciones', count(*) from {{ ref('silver_tu_transacciones') }}
where fecha > {{ fecha_ingesta('_ingested_at') }} having count(*) > 0
union all
select 'aerometro_boardings', count(*) from {{ ref('silver_am_boardings') }}
where fecha > {{ fecha_ingesta('_ingested_at') }} having count(*) > 0
union all
select 'metroriel_viajes', count(*) from {{ ref('silver_mr_viajes') }}
where fecha > {{ fecha_ingesta('_ingested_at') }} having count(*) > 0
