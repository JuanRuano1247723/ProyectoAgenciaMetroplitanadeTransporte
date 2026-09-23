{# Log de cambios del padrón de Transmetro, tipado. Una fila por operación; sin aplicar nada todavía. #}
with src as (
    select * from {{ source('bronze', 'transmetro_padron_cdc') }}
),
tipado as (
    select
        try_cast(trim(seq) as bigint)                                            as seq,
        try_cast(trim(commit_ts) as timestamp)                                   as commit_ts,
        nullif(regexp_extract(upper(trim(op)), '^(INSERT|UPDATE|DELETE)', 1), '') as op,
        trim(tarjeta)                                                            as tarjeta,
        trim(perfil)                                                             as perfil,
        trim(zona_residencia)                                                    as zona_residencia_original,
        trim(estado)                                                             as estado,
        _file_hash, _source_line_number, _ingested_at,
        to_json(src)                                                             as fila_original
    from src
)
select
    *,
    list_filter([
        case when tarjeta = 'SIN-TARJETA' then 'llave_centinela' end,
        case when tarjeta is null or (tarjeta <> 'SIN-TARJETA' and not regexp_matches(tarjeta, '^TC-[0-9]{8}$'))
             then 'llave_formato_no_transmetro' end,
        case when op is null then 'operacion_desconocida' end,
        case when seq is null then 'secuencia_invalida' end
    ], x -> x is not null) as reglas_rechazo,
    cast([] as varchar[]) as reglas_advertencia
from tipado
