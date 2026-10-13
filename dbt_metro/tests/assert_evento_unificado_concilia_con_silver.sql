-- silver_evento_unificado debe tener exactamente un evento por cada fila aceptada de los cuatro silver_* de origen.
with u as (select count(*) as n from {{ ref('silver_evento_unificado') }}),
     o as (select (select count(*) from {{ ref('silver_tm_validaciones') }})
                + (select count(*) from {{ ref('silver_tu_transacciones') }})
                + (select count(*) from {{ ref('silver_am_boardings') }})
                + (select count(*) from {{ ref('silver_mr_viajes') }}) as n)
select u.n as unificado, o.n as origen from u, o where u.n <> o.n
