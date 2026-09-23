{#
  GRANO: una fila por cada trayecto completo (de estación de entrada a estación de salida), para
  cualquier medio de transporte capaz de registrarlo. Hoy solo MetroRiel puebla esta tabla, porque
  es el único operador que trae salida real (silver_mr_viajes ya excluyó los viajes sin salida:
  regla de rechazo viaje_sin_salida). El esquema es genérico a propósito: si más adelante se
  implementa el extra de análisis de transbordo, un viaje multimodal inferido se agrega con un
  `union all` (operador_cod = 'MULTI') sin cambiar columnas ni consumidores de esta tabla.

  Medidas (ver docs/DISENO_GOLD_GRANO.md §4):
    viajes        -- aditiva (conteo)
    duracion_s    -- aditiva (segundos totales; el promedio se calcula en consulta)
    tarifa_gtq    -- aditiva
    distancia_km  -- aditiva, con cobertura parcial: NULL si alguna de las dos estaciones no trae km
  Ninguna razón se guarda aquí: duración promedio = sum(duracion_s) / sum(viajes) en la consulta.
#}
with viajes as (
    select
        'MR'                        as operador_cod,
        'metroriel'                 as operador_estacion,
        evento_sk, usuario_sk, fecha, hora_entrada,
        estacion_entrada, estacion_salida,
        duracion_s, tarifa_gtq, duracion_no_coincide,
        _file_hash, _source_line_number
    from {{ ref('silver_mr_viajes') }}
)
select
    v.evento_sk,
    t.transporte_sk,
    f.fecha_sk,
    cast(v.hora_entrada as integer)              as hora_sk,
    ee.estacion_sk                              as estacion_sk_entrada,
    es.estacion_sk                              as estacion_sk_salida,
    v.usuario_sk,
    1                                            as viajes,
    v.duracion_s,
    v.tarifa_gtq,
    case when ee.km is not null and es.km is not null
         then abs(es.km - ee.km) end            as distancia_km,
    v.duracion_no_coincide,
    v._file_hash,
    v._source_line_number
from viajes v
join {{ ref('dim_transporte') }} t on t.operador_cod = v.operador_cod
join {{ ref('dim_fecha') }}     f on f.fecha = v.fecha
left join {{ ref('dim_estacion') }} ee
       on ee.operador = v.operador_estacion and ee.estacion_id_original = v.estacion_entrada
left join {{ ref('dim_estacion') }} es
       on es.operador = v.operador_estacion and es.estacion_id_original = v.estacion_salida
