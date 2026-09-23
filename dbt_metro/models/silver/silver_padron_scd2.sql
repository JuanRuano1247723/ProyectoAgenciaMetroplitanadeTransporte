{# Padrón de Transmetro con SCD Tipo 2: una fila por versión de cada tarjeta, ordenada por seq.
   - INSERT/UPDATE fijan los atributos; DELETE marca activa = false y CONSERVA los últimos atributos conocidos
     (la tarjeta nunca se borra: se perdería el historial de sus viajes).
   - Solo se abre una versión nueva cuando algo cambia (INSERT repetido o DELETE repetido no crean versiones).
   - Una tarjeta con UPDATE y sin INSERT previo se acepta y se marca sin_insert_previo. #}
with eventos as (
    select tarjeta, seq, commit_ts, op, perfil, zona_residencia_original, estado
    from {{ ref('stg_cdc_padron') }}
    where len(reglas_rechazo) = 0
),
arrastre as (
    select
        tarjeta, seq, commit_ts, op,
        case when op = 'DELETE'
             then last_value(case when op <> 'DELETE' then perfil end ignore nulls) over w
             else perfil end                                   as perfil,
        case when op = 'DELETE'
             then last_value(case when op <> 'DELETE' then zona_residencia_original end ignore nulls) over w
             else zona_residencia_original end                 as zona_residencia_original,
        case when op = 'DELETE'
             then last_value(case when op <> 'DELETE' then estado end ignore nulls) over w
             else estado end                                   as estado,
        (op <> 'DELETE')                                       as activa
    from eventos
    window w as (partition by tarjeta order by seq rows between unbounded preceding and current row)
),
comparado as (
    select
        *,
        lag(perfil) over o                   as perfil_prev,
        lag(zona_residencia_original) over o as zona_prev,
        lag(estado) over o                   as estado_prev,
        lag(activa) over o                   as activa_prev,
        row_number() over o                  as rn
    from arrastre
    window o as (partition by tarjeta order by seq)
),
versiones as (
    select * from comparado
    where rn = 1
       or perfil is distinct from perfil_prev
       or zona_residencia_original is distinct from zona_prev
       or estado is distinct from estado_prev
       or activa is distinct from activa_prev
),
numerado as (
    select
        tarjeta,
        row_number() over p                                                as version,
        perfil, zona_residencia_original, estado, activa,
        op                                                                 as operacion_origen,
        seq                                                                as valido_desde_seq,
        commit_ts                                                          as valido_desde_ts,
        lead(seq) over p                                                   as valido_hasta_seq,
        lead(commit_ts) over p                                             as valido_hasta_ts,
        lead(seq) over p is null                                           as es_vigente,
        (row_number() over p = 1 and op <> 'INSERT')                       as sin_insert_previo,
        coalesce(activa and lag(activa) over p = false, false)             as reactivacion
    from versiones
    window p as (partition by tarjeta order by seq)
)
select
    md5(n.tarjeta || ':' || cast(n.valido_desde_seq as varchar))           as padron_sk,
    n.tarjeta, n.version,
    n.perfil, n.zona_residencia_original,
    coalesce(z.zona_id, -1)                                                as zona_residencia_id,
    n.estado, n.activa, n.operacion_origen,
    n.valido_desde_seq, n.valido_desde_ts, n.valido_hasta_seq, n.valido_hasta_ts,
    n.es_vigente, n.sin_insert_previo, n.reactivacion
from numerado n
left join {{ ref('dim_zona') }} z on z.clave_normalizada = {{ clave_zona('n.zona_residencia_original') }}
