{# Entregable 1.3: conteo de registros por regla de calidad.
   registros_afectados = filas que incumplen la regla (una fila puede incumplir varias);
   registros_en_cuarentena = filas cuyo motivo principal es esta regla (suman el total de cuarentena). #}
{% set fuentes = [
    ('transmetro_validaciones', 'stg_tm_validaciones'),
    ('transurbano_transacciones', 'stg_tu_transacciones'),
    ('aerometro_boardings', 'stg_am_boardings'),
    ('metroriel_viajes', 'stg_mr_viajes'),
    ('cdc_padron_usuarios', 'stg_cdc_padron')
] %}
with eventos as (
    {% for fuente, modelo in fuentes %}
    select '{{ fuente }}' as fuente, reglas_rechazo, reglas_advertencia from {{ ref(modelo) }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),
evaluados as (select fuente, count(*) as registros_evaluados from eventos group by 1),
hits as (
    select fuente, regla, count(*) as n
    from (select fuente, unnest(reglas_rechazo) as regla from eventos) group by 1, 2
    union all
    select fuente, regla, count(*) as n
    from (select fuente, unnest(reglas_advertencia) as regla from eventos) group by 1, 2
),
cuar as (
    select fuente, regla_principal as regla, count(*) as n from {{ ref('silver_cuarentena') }} group by 1, 2
)
select
    r.fuente, r.regla, r.severidad, r.descripcion,
    e.registros_evaluados,
    coalesce(h.n, 0)                                                        as registros_afectados,
    coalesce(c.n, 0)                                                        as registros_en_cuarentena,
    round(100.0 * coalesce(h.n, 0) / e.registros_evaluados, 3)              as porcentaje_afectado
from {{ ref('reglas_calidad') }} r
join evaluados e on e.fuente = r.fuente
left join hits h on h.fuente = r.fuente and h.regla = r.regla
left join cuar c on c.fuente = r.fuente and c.regla = r.regla
order by r.fuente, r.severidad desc, registros_afectados desc, r.regla
