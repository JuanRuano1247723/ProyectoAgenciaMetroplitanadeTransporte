{# Cuarentena: filas rechazadas por alguna regla, con motivo y la fila original completa. Nada se descarta. #}
{% set fuentes = [
    ('transmetro_validaciones', 'stg_tm_validaciones'),
    ('transurbano_transacciones', 'stg_tu_transacciones'),
    ('aerometro_boardings', 'stg_am_boardings'),
    ('metroriel_viajes', 'stg_mr_viajes'),
    ('cdc_padron_usuarios', 'stg_cdc_padron')
] %}
with todo as (
    {% for fuente, modelo in fuentes %}
    select '{{ fuente }}' as fuente, reglas_rechazo, fila_original, _file_hash, _source_line_number, _ingested_at
    from {{ ref(modelo) }}
    where len(reglas_rechazo) > 0
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)
select
    md5(t.fuente || ':' || t._file_hash || ':' || cast(t._source_line_number as varchar)) as cuarentena_sk,
    t.fuente,
    t.reglas_rechazo[1]                                   as regla_principal,
    array_to_string(t.reglas_rechazo, ',')                as reglas_incumplidas,
    r.descripcion                                         as motivo,
    t.fila_original,
    t._file_hash, t._source_line_number, t._ingested_at
from todo t
left join {{ ref('reglas_calidad') }} r
       on r.fuente = t.fuente and r.regla = t.reglas_rechazo[1]
