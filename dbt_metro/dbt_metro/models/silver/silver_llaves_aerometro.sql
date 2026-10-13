{# Entregable 1.2: catálogo mínimo de usuarios de Aerómetro. Solo la llave. #}
select distinct user_hash as llave
from {{ ref('stg_am_boardings') }}
where user_hash is not null and user_hash <> ''
