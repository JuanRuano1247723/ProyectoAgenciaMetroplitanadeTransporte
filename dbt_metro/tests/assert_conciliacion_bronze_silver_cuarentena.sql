{# Invariante por fuente: filas en Bronze = filas en Silver + filas en cuarentena. Devuelve las fuentes que no cuadran. #}
with b as (
    select 'transmetro_validaciones' as fuente, count(*) as n from {{ source('bronze', 'transmetro_validaciones') }}
    union all select 'transurbano_transacciones', count(*) from {{ source('bronze', 'transurbano_transacciones') }}
    union all select 'aerometro_boardings', count(*) from {{ source('bronze', 'aerometro_boardings') }}
    union all select 'metroriel_viajes', count(*) from {{ source('bronze', 'metroriel_viajes') }}
    union all select 'cdc_padron_usuarios', count(*) from {{ source('bronze', 'transmetro_padron_cdc') }}
),
s as (
    select 'transmetro_validaciones' as fuente, count(*) as n from {{ ref('silver_tm_validaciones') }}
    union all select 'transurbano_transacciones', count(*) from {{ ref('silver_tu_transacciones') }}
    union all select 'aerometro_boardings', count(*) from {{ ref('silver_am_boardings') }}
    union all select 'metroriel_viajes', count(*) from {{ ref('silver_mr_viajes') }}
    union all select 'cdc_padron_usuarios', count(*) from {{ ref('stg_cdc_padron') }} where len(reglas_rechazo) = 0
),
q as (select fuente, count(*) as n from {{ ref('silver_cuarentena') }} group by 1)
select b.fuente, b.n as bronze, s.n as silver, coalesce(q.n, 0) as cuarentena
from b join s using (fuente) left join q using (fuente)
where b.n <> s.n + coalesce(q.n, 0)
