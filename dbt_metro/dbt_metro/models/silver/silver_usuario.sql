{# Identidad del usuario. Estrategia (ver docs/DISENO_SILVER.md §5). Ambos identificadores son SEUDÓNIMOS con secreto (macro pseudonimo):
   Gold solo debe usar usuario_sk y persona_id, nunca llave_original (ver docs/SEGURIDAD_DATOS_PERSONALES.md).
   1. Certeza: cada (operador, llave) es un usuario propio (usuario_sk). Nunca se pierde.
   2. Hipótesis controlada por la variable unificar_identidad_numerica: la parte numérica de las llaves de
      Transmetro, Transurbano y MetroRiel es el mismo id de persona en formato distinto (TC-00012345,
      0000012345, MR0012345). Aerómetro usa un hash opaco: se queda como usuario propio. #}
{% set unificar = 'true' if var('unificar_identidad_numerica') else 'false' %}
with llaves as (
    select 'transmetro' as operador, tarjeta as llave from {{ ref('silver_tm_validaciones') }}
    union select 'transmetro', tarjeta from {{ ref('silver_padron_scd2') }}
    union select 'transurbano', num_tarjeta from {{ ref('silver_tu_transacciones') }}
    union select 'metroriel', tarjeta from {{ ref('silver_mr_viajes') }}
    union select 'aerometro', user_hash from {{ ref('silver_am_boardings') }}
),
base as (
    select
        {{ pseudonimo("operador || ':' || llave") }}                                           as usuario_sk,
        operador,
        llave                                                                                as llave_original,
        case when operador <> 'aerometro' then try_cast(regexp_extract(llave, '([0-9]+)$', 1) as bigint) end as id_numerico
    from llaves
)
select
    b.usuario_sk, b.operador, b.llave_original, b.id_numerico,
    case when {{ unificar }} and b.id_numerico is not null
         then {{ pseudonimo("'persona:' || cast(b.id_numerico as varchar)") }} else b.usuario_sk end as persona_id,
    case when {{ unificar }} and b.id_numerico is not null
         then 'numerica_compartida' else 'solo_operador' end                                  as regla_identidad,
    (p.tarjeta is not null)                                                                   as en_padron,
    p.perfil                                                                                  as perfil_vigente,
    p.activa                                                                                  as activa_vigente
from base b
left join {{ ref('silver_padron_vigente') }} p
       on b.operador = 'transmetro' and p.tarjeta = b.llave_original
