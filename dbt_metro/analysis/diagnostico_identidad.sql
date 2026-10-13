{#
  diagnostico_identidad   (analysis: se compila con `dbt compile`, no crea tabla)
  Prueba de comportamiento descrita en DISENO_SILVER.md sec. 5: ¿hay MÁS transbordos temporales entre
  operadores con los ids reales que con los ids desplazados?
  Placebo: se desplaza el id de las personas en TODOS los operadores menos TM por un offset fijo.
  Eso rompe la identidad entre operadores y conserva la estructura interna de cada operador.

  razon = reales / placebo
     >> 1 (p. ej. 1.5+)  : apoya que el número compartido es la misma persona.
     ~ 1                 : NO la refuta (si el generador sortea el usuario de cada evento de forma
                           independiente tampoco habría correlación temporal). La fuente de verdad
                           es generar_red_metropolitana.py.
#}
with ev as (
    select persona_id, operador_cod, ts_local, evento_sk
    from {{ ref('silver_evento_unificado') }}
),
ids as (
    select persona_id, dense_rank() over (order by persona_id) - 1 as r, count(*) over () as n
    from (select distinct persona_id from ev)
),
colocado as (
    select ev.*,
           case when ev.operador_cod = 'TM'
                then ev.persona_id
                else (select i2.persona_id from ids i2 where i2.r = (i.r + 7919) % i.n) end as persona_placebo
    from ev join ids i using (persona_id)
),
pares as (
    select
        'real' as escenario, persona_id as p, operador_cod, ts_local, evento_sk from colocado
    union all
    select 'placebo', persona_placebo, operador_cod, ts_local, evento_sk from colocado
),
sec as (
    select escenario, p, operador_cod, ts_local,
           lag(operador_cod) over w as op_previo, lag(ts_local) over w as ts_previo
    from pares
    window w as (partition by escenario, p order by ts_local, evento_sk)
)
select
    escenario,
    count(*) as transbordos_inferidos
from sec
where op_previo is not null and op_previo <> operador_cod
  and date_diff('minute', ts_previo, ts_local) <= {{ var('ventana_transbordo_min', 60) }}
group by escenario
