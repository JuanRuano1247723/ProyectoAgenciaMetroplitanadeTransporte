-- Los agregados de Gold no pierden ni duplican abordajes ni personas respecto al detalle de Silver.
with ev as (select count(*) as n, count(distinct persona_id) as personas from {{ ref('silver_evento_unificado') }}),
     d  as (select sum(abordajes) as n from {{ ref('agg_demanda_zona_hora') }}),
     c  as (select sum(personas) as n from {{ ref('agg_usuarios_por_combinacion') }}),
     z  as (select sum(abordajes_total) as n from {{ ref('agg_cobertura_zona') }})
select ev.n, d.n as demanda, z.n as cobertura, ev.personas, c.n as combinaciones
from ev, d, c, z
where d.n <> ev.n or z.n <> ev.n or c.n <> ev.personas
