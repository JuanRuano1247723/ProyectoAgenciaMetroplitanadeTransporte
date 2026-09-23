{# Entregable 1.2: catálogo mínimo de usuarios de MetroRiel. Solo la llave. #}
select distinct tarjeta as llave
from {{ ref('stg_mr_viajes') }}
where tarjeta is not null and tarjeta <> ''
