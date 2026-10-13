{{ config(severity='warn') }}
{# Zonas que no se pudieron conformar: agrégalas a seeds/dim_zona.csv. #}
select 'estacion' as origen, zona_original as valor_sin_mapear
from {{ ref('silver_estacion') }} where zona_id = -1 group by 2
union all
select 'padron', zona_residencia_original
from {{ ref('silver_padron_scd2') }}
where zona_residencia_id = -1 and zona_residencia_original is not null group by 2
