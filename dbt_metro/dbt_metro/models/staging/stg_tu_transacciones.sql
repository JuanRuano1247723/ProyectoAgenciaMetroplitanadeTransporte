{# Transacciones de Transurbano (una fila = una transacción). fecha y hora vienen en columnas separadas. #}
with src as (
    select * from {{ source('bronze', 'transurbano_transacciones') }}
),
tipado as (
    select
        {{ evento_sk('transurbano') }}                                                  as evento_sk,
        trim(fecha) as fecha_txt, trim(hora) as hora_txt,
        try_strptime(trim(fecha) || ' ' || trim(hora), '%d/%m/%Y %H:%M:%S')             as ts_local,
        trim(num_tarjeta)                                                               as num_tarjeta,
        nullif(trim(cod_parada), '')                                                    as cod_parada,
        trim(ruta)                                                                      as ruta,
        trim(monto_centavos)                                                            as monto_centavos_txt,
        try_cast(trim(monto_centavos) as integer)                                       as monto_centavos,
        cast(round(try_cast(trim(monto_centavos) as integer) / 100.0, 2) as decimal(10, 2)) as monto_gtq,
        trim(cod_estado)                                                                as cod_estado,
        _file_hash, _source_line_number, _ingested_at,
        to_json(src)                                                                    as fila_original
    from src
),
marcado as (
    select
        t.*,
        row_number() over (
            partition by fecha_txt, hora_txt, num_tarjeta, coalesce(cod_parada, ''), ruta, monto_centavos_txt, cod_estado
            order by _source_line_number) as n_dup
    from tipado t
),
catalogo as (
    select distinct estacion_id_original from {{ ref('stg_estaciones') }} where operador = 'transurbano'
)
select
    m.*,
    list_filter([
        case when not regexp_matches(coalesce(m.num_tarjeta, ''), '^[0-9]{10}$') then 'llave_formato_invalido' end,
        case when m.ts_local is null then 'fecha_invalida' end,
        case when m.ts_local is not null and cast(m.ts_local as date) > {{ fecha_ingesta('m._ingested_at') }}
             then 'fecha_futura' end,
        case when m.monto_centavos is null or m.monto_centavos < 0 then 'monto_invalido' end,
        case when m.cod_parada is null then 'cod_parada_nulo' end,
        case when m.n_dup > 1 then 'duplicado_exacto' end
    ], x -> x is not null) as reglas_rechazo,
    list_filter([
        case when m.cod_parada is not null and c.estacion_id_original is null then 'parada_sin_catalogo' end
    ], x -> x is not null) as reglas_advertencia
from marcado m
left join catalogo c on c.estacion_id_original = m.cod_parada
