-- Una fila por persona del detalle unificado (no se pierde ni se inventa nadie).
with f as (select count(*) as n from {{ ref('features_usuario') }}),
     p as (select count(distinct persona_id) as n from {{ ref('silver_evento_unificado') }}
           where fecha <= (select max(fecha_corte) from {{ ref('features_usuario') }}))
select f.n as en_features, p.n as en_silver from f, p where f.n <> p.n
