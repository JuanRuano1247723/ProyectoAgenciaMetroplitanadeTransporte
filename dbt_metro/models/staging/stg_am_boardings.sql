{# Abordajes de Aerómetro. timestamp_utc viene en UTC: aquí se conserva y se convierte a hora local de Guatemala. #}
with src as (
    select * from {{ source('bronze', 'aerometro_boardings') }}
),
tipado as (
    select
        {{ evento_sk('aerometro') }}                                          as evento_sk,
        trim(boarding_id)                                                     as boarding_id_txt,
        try_cast(trim(boarding_id) as bigint)                                 as boarding_id,
        trim(user_hash)                                                       as user_hash,
        trim(station_code)                                                    as station_code,
        trim(axis)                                                            as axis,
        try_strptime(trim(timestamp_utc), '%Y-%m-%dT%H:%M:%SZ')               as ts_utc,
        (try_strptime(trim(timestamp_utc), '%Y-%m-%dT%H:%M:%SZ') at time zone 'UTC')
            at time zone '{{ var("zona_horaria_local") }}'                    as ts_local,
        try_cast(trim(cabin_number) as integer)                               as cabin_number,
        try_cast(trim(fare) as decimal(10, 2))                                as tarifa_gtq,
        _file_hash, _source_line_number, _ingested_at, _kafka_partition, _kafka_offset,
        to_json(src)                                                          as fila_original
    from src
),
marcado as (
    select t.*, row_number() over (partition by boarding_id_txt order by _source_line_number) as n_id
    from tipado t
),
catalogo as (
    select distinct estacion_id_original from {{ ref('stg_estaciones') }} where operador = 'aerometro'
)
select
    m.*,
    list_filter([
        case when not regexp_matches(coalesce(m.user_hash, ''), '^[0-9a-f]{12}$') then 'llave_formato_invalido' end,
        case when m.ts_utc is null then 'fecha_invalida' end,
        case when m.ts_local is not null and cast(m.ts_local as date) > {{ fecha_ingesta('m._ingested_at') }}
             then 'fecha_futura' end,
        case when m.tarifa_gtq is null or m.tarifa_gtq < 0 then 'monto_invalido' end,
        case when m.n_id > 1 then 'duplicado_boarding' end
    ], x -> x is not null) as reglas_rechazo,
    list_filter([
        case when c.estacion_id_original is null then 'estacion_sin_catalogo' end
    ], x -> x is not null) as reglas_advertencia
from marcado m
left join catalogo c on c.estacion_id_original = m.station_code
