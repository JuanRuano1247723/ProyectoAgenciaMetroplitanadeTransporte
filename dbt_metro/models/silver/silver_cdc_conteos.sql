{# Entregable 1.2: conteo antes/después de aplicar los borrados del CDC de Transmetro.
   "Antes": tarjetas distintas con operaciones válidas (los DELETE aún no se aplican).
   "Después": activas y dadas de baja (marcadas inactivas, no borradas). antes = activas + bajas. #}
with ev as (select * from {{ ref('stg_cdc_padron') }}),
     vig as (select * from {{ ref('silver_padron_vigente') }}),
     v   as (select * from {{ ref('silver_padron_scd2') }})
select
    (select count(*) from ev)                                                    as eventos_en_bronze,
    (select count(*) from ev where len(reglas_rechazo) > 0)                      as eventos_en_cuarentena,
    (select count(*) from ev where len(reglas_rechazo) = 0)                      as eventos_aplicados,
    (select count(*) from ev where len(reglas_rechazo) = 0 and op = 'INSERT')    as inserts,
    (select count(*) from ev where len(reglas_rechazo) = 0 and op = 'UPDATE')    as updates,
    (select count(*) from ev where len(reglas_rechazo) = 0 and op = 'DELETE')    as deletes,
    (select count(distinct tarjeta) from ev where len(reglas_rechazo) = 0)       as tarjetas_antes_de_borrados,
    (select count(*) from vig where activa)                                      as tarjetas_activas_despues,
    (select count(*) from vig where not activa)                                  as tarjetas_dadas_de_baja,
    (select count(distinct tarjeta) from v where sin_insert_previo)              as tarjetas_sin_insert_previo,
    (select count(distinct tarjeta) from v where reactivacion)                   as tarjetas_reactivadas,
    (select count(*) from (select tarjeta from ev where len(reglas_rechazo) = 0 and op = 'INSERT'
                           group by tarjeta having count(*) > 1))                as tarjetas_con_insert_repetido,
    (select count(*) from v)                                                     as versiones_scd2
