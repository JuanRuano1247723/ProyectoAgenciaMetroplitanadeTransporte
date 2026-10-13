{#
  features_usuario  (rúbrica 2.3)
  Entidad: una fila por persona (persona_id, seudónimo). Se construye desde SILVER, no desde Gold:
  lee solo silver_evento_unificado, que conserva el detalle atómico.

  Fecha de corte: variable `fecha_corte` (p. ej. dbt run --vars '{fecha_corte: 2026-07-15}').
  Si no se pasa, usa la última fecha con datos. Solo se usan eventos con fecha <= fecha_corte,
  así que ninguna feature mira "el futuro" del corte.

  Definiciones (deben coincidir con DEFINICIONES_OFICIALES.md):
    viaje          = evento de entrada que no sea un TRANSBORDO de Transmetro.
    gasto          = suma de todos los montos (incluye el de un transbordo, si lo hubo).
    hora pico      = misma regla que dim_hora: hora con volumen >= umbral_hora_pico * promedio horario.
    zona de origen = zona de la estación/parada de abordaje (se ignora -1 "sin mapear").
  Ventana de N días = dias_atras < N, donde dias_atras = fecha_corte - fecha del evento (0 = el día de corte).
#}
with ev as (
    select * from {{ ref('silver_evento_unificado') }}
),

corte as (
    select coalesce(try_cast('{{ var("fecha_corte", "") }}' as date), max(fecha)) as fecha_corte
    from ev
),

horas as (
    select hora, count(*) as n
    from ev
    group by hora
),

hora_pico as (
    select hora
    from horas
    where n >= {{ var('umbral_hora_pico', 1.5) }} * (select avg(n) from horas)
),

base as (
    select
        e.persona_id,
        e.operador_cod,
        e.zona_id,
        e.monto_gtq,
        e.fecha,
        e.es_transbordo_operador,
        not e.es_transbordo_operador                          as es_viaje,
        (p.hora is not null)                                  as es_hora_pico,
        date_diff('day', e.fecha, c.fecha_corte)              as dias_atras
    from ev e
    cross join corte c
    left join hora_pico p on p.hora = e.hora
    where e.fecha <= c.fecha_corte
),

agregados as (
    select
        persona_id,
        count(*) filter (where es_viaje)                                         as viajes_total,
        count(*) filter (where es_viaje and dias_atras <  7)                     as viajes_7d,
        count(*) filter (where es_viaje and dias_atras < 30)                     as viajes_30d,
        count(*) filter (where es_viaje and dias_atras < 90)                     as viajes_90d,
        count(distinct operador_cod)                                             as n_modos,
        count(distinct fecha)                                                    as dias_con_viaje,
        min(dias_atras)                                                          as dias_desde_ultimo_viaje,
        count(*) filter (where es_viaje and es_hora_pico)                        as viajes_hora_pico,
        sum(monto_gtq)                                                           as gasto_acumulado_gtq
    from base
    group by persona_id
),

modo as (
    select persona_id, operador_cod as modo_mas_usado
    from (
        select persona_id, operador_cod,
               row_number() over (partition by persona_id order by count(*) desc, operador_cod) as rn
        from base
        where es_viaje
        group by persona_id, operador_cod
    )
    where rn = 1
),

zona as (
    select persona_id, zona_id as zona_origen_mas_frecuente
    from (
        select persona_id, zona_id,
               row_number() over (partition by persona_id order by count(*) desc, zona_id) as rn
        from base
        where es_viaje and zona_id <> -1
        group by persona_id, zona_id
    )
    where rn = 1
)

select
    a.persona_id,
    (select fecha_corte from corte)                                              as fecha_corte,
    a.viajes_7d,
    a.viajes_30d,
    a.viajes_90d,
    a.viajes_total,
    a.dias_con_viaje,
    m.modo_mas_usado,
    a.n_modos,
    a.dias_desde_ultimo_viaje,
    a.viajes_hora_pico * 1.0 / nullif(a.viajes_total, 0)                         as prop_viajes_hora_pico,
    z.zona_origen_mas_frecuente,
    a.gasto_acumulado_gtq,
    a.gasto_acumulado_gtq * 1.0 / nullif(a.viajes_total, 0)                      as gasto_promedio_por_viaje_gtq
from agregados a
left join modo m using (persona_id)
left join zona z using (persona_id)
