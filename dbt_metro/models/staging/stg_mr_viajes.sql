{# Viajes completos de MetroRiel (una fila = un viaje entrada -> salida). #}
with src as (
    select * from {{ source('bronze', 'metroriel_viajes') }}
),
tipado as (
    select
        {{ evento_sk('metroriel') }}                                              as evento_sk,
        trim(trip_id)                                                             as trip_id_txt,
        try_cast(trim(trip_id) as bigint)                                         as trip_id,
        trim(card)                                                                as tarjeta,
        trim(struct_extract(entry, 'station'))                                    as estacion_entrada,
        try_strptime(trim(struct_extract(entry, 'ts')), '%Y-%m-%dT%H:%M:%S')      as ts_entrada,
        trim(struct_extract("exit", 'station'))                                   as estacion_salida,
        try_strptime(trim(struct_extract("exit", 'ts')), '%Y-%m-%dT%H:%M:%S')     as ts_salida,
        ("exit" is null)                                                          as exit_nulo,
        try_cast(trim(duration_s) as integer)                                     as duracion_s,
        try_cast(trim(fare_gtq) as decimal(10, 2))                                as tarifa_gtq,
        _file_hash, _source_line_number, _ingested_at,
        to_json(src)                                                              as fila_original
    from src
),
marcado as (
    select t.*, row_number() over (partition by trip_id_txt order by _source_line_number) as n_id
    from tipado t
),
catalogo as (
    select distinct estacion_id_original from {{ ref('stg_estaciones') }} where operador = 'metroriel'
)
select
    m.*,
    list_filter([
        case when not regexp_matches(coalesce(m.tarjeta, ''), '^MR[0-9]{7}$') then 'llave_formato_invalido' end,
        case when m.ts_entrada is null then 'fecha_invalida' end,
        case when m.ts_entrada is not null and cast(m.ts_entrada as date) > {{ fecha_ingesta('m._ingested_at') }}
             then 'fecha_futura' end,
        case when m.tarifa_gtq is null or m.tarifa_gtq < 0 then 'monto_invalido' end,
        case when m.exit_nulo or m.ts_salida is null then 'viaje_sin_salida' end,
        case when m.ts_salida is not null and m.ts_entrada is not null and m.ts_salida < m.ts_entrada
             then 'salida_antes_de_entrada' end,
        case when m.n_id > 1 then 'duplicado_viaje' end
    ], x -> x is not null) as reglas_rechazo,
    list_filter([
        case when m.duracion_s is not null and m.ts_entrada is not null and m.ts_salida is not null
                  and abs(m.duracion_s - date_diff('second', m.ts_entrada, m.ts_salida)) > 1
             then 'duracion_no_coincide' end,
        case when e1.estacion_id_original is null or (m.estacion_salida is not null and e2.estacion_id_original is null)
             then 'estacion_sin_catalogo' end
    ], x -> x is not null) as reglas_advertencia
from marcado m
left join catalogo e1 on e1.estacion_id_original = m.estacion_entrada
left join catalogo e2 on e2.estacion_id_original = m.estacion_salida
