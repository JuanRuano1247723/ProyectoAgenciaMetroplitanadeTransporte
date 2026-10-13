{#
  agg_usuarios_por_combinacion   -- Tableau: "Transbordo: cuántos usuarios usan más de un sistema"
  Grano: una fila por combinación de operadores (p. ej. 'TM', 'TM+TU', 'AM+MR+TM+TU').
  Cada persona cae en EXACTAMENTE una combinación, así que `personas` SÍ se puede sumar entre filas
  (es una partición): no rompe la regla de que usuarios distintos no es aditivo.

  OJO: depende de persona_id = hipótesis de identidad numérica compartida (S-09). Si la hipótesis es falsa,
  esta tabla mide coincidencias de números, no personas. Se apaga con la variable de dbt
  unificar_identidad_numerica: false. Reportar siempre las dos lecturas (ver analysis/diagnostico_identidad.sql).
#}
with p as (
    select
        persona_id,
        count(distinct operador_cod)                              as n_modos,
        string_agg(distinct operador_cod, '+' order by operador_cod) as combinacion,
        count(*)                                                  as abordajes
    from {{ ref('silver_evento_unificado') }}
    group by persona_id
)

select
    n_modos,
    combinacion,
    count(*)        as personas,
    sum(abordajes)  as abordajes
from p
group by n_modos, combinacion
