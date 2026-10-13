select tarjeta, count(*) filter (where es_vigente) as vigentes
from {{ ref('silver_padron_scd2') }}
group by tarjeta
having count(*) filter (where es_vigente) <> 1
