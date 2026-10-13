{# El conteo por regla es coherente con la tabla de cuarentena, y toda regla usada está en el catálogo de reglas. #}
select 'suma_distinta' as problema, cast(sum(registros_en_cuarentena) as varchar) as detalle
from {{ ref('silver_dq_conteo_por_regla') }}
having sum(registros_en_cuarentena) <> (select count(*) from {{ ref('silver_cuarentena') }})
union all
select 'regla_fuera_de_catalogo', fuente || '/' || regla_principal
from {{ ref('silver_cuarentena') }} c
where not exists (select 1 from {{ ref('reglas_calidad') }} r where r.fuente = c.fuente and r.regla = c.regla_principal)
group by fuente, regla_principal
union all
select 'regla_duplicada_en_catalogo', fuente || '/' || regla
from {{ ref('reglas_calidad') }} group by fuente, regla having count(*) > 1
