{# Entregable 1.2: catálogo mínimo de usuarios de Transurbano. Solo la llave: el operador nunca entregó más. #}
select distinct num_tarjeta as llave
from {{ ref('stg_tu_transacciones') }}
where num_tarjeta is not null and num_tarjeta <> ''
