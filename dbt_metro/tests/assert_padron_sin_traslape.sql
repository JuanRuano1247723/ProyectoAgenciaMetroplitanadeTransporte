{# Las versiones de una tarjeta no se traslapan y sus números son consecutivos. #}
select tarjeta, version, valido_desde_seq, valido_hasta_seq
from {{ ref('silver_padron_scd2') }}
where valido_hasta_seq is not null and valido_hasta_seq <= valido_desde_seq
union all
select tarjeta, max(version), null, null
from {{ ref('silver_padron_scd2') }}
group by tarjeta
having max(version) <> count(*)
