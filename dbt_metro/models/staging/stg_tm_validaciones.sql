{# Validaciones de Transmetro (una fila = un abordaje). Todas las filas, con las reglas que incumplen. #}
with src as (
    select * from {{ source('bronze', 'transmetro_validaciones') }}
),
tipado as (
    select
        {{ evento_sk('transmetro') }}                            as evento_sk,
        trim(validacion_id)                                      as validacion_id_txt,
        try_cast(trim(validacion_id) as bigint)                  as validacion_id,
        trim(tarjeta)                                            as tarjeta,
        trim(estacion_id)                                        as estacion_id,
        trim(linea)                                              as linea,
        try_strptime(trim(fecha_hora), '%Y-%m-%d %H:%M:%S')      as ts_local,
        try_cast(trim(tarifa) as decimal(10, 2))                 as tarifa_gtq,
        trim(tipo)                                               as tipo,
        _file_hash, _source_line_number, _ingested_at, _kafka_partition, _kafka_offset,
        to_json(src)                                             as fila_original
    from src
),
marcado as (
    select
        t.*,
        row_number() over (partition by validacion_id_txt order by _source_line_number) as n_id,
        lag(validacion_id_txt) over w as id_previo,
        lag(ts_local) over w          as ts_previo
    from tipado t
    window w as (partition by tarjeta, estacion_id order by ts_local, _source_line_number)
),
catalogo as (
    select distinct estacion_id_original from {{ ref('stg_estaciones') }} where operador = 'transmetro'
),
padron as (
    select distinct tarjeta from {{ ref('stg_cdc_padron') }} where len(reglas_rechazo) = 0
)
select
    m.*,
    list_filter([
        case when not regexp_matches(coalesce(m.tarjeta, ''), '^TC-[0-9]{8}$') then 'llave_formato_invalido' end,
        case when m.ts_local is null then 'fecha_invalida' end,
        case when m.ts_local is not null and cast(m.ts_local as date) > {{ fecha_ingesta('m._ingested_at') }}
             then 'fecha_futura' end,
        case when m.tarifa_gtq is null or m.tarifa_gtq < 0 then 'monto_invalido' end,
        case when m.n_id > 1 then 'duplicado_torniquete' end
    ], x -> x is not null) as reglas_rechazo,
    list_filter([
        case when m.id_previo is not null and m.id_previo <> m.validacion_id_txt
                  and date_diff('second', m.ts_previo, m.ts_local) between 0 and {{ var('ventana_doble_lectura_s') }}
             then 'posible_doble_lectura' end,
        case when c.estacion_id_original is null then 'estacion_sin_catalogo' end,
        case when p.tarjeta is null then 'tarjeta_sin_padron' end
    ], x -> x is not null) as reglas_advertencia
from marcado m
left join catalogo c on c.estacion_id_original = m.estacion_id
left join padron p on p.tarjeta = m.tarjeta
