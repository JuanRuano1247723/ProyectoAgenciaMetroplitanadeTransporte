{{ config(severity='warn') }}
{# Los cuatro operadores trabajan de 04:00 a 22:59 hora local (exploración 7.8). Fuera de eso, revisar la conversión horaria. #}
select 'transmetro_validaciones' as fuente, hora from {{ ref('silver_tm_validaciones') }} where hora not between 4 and 22 group by hora
union all select 'transurbano_transacciones', hora from {{ ref('silver_tu_transacciones') }} where hora not between 4 and 22 group by hora
union all select 'aerometro_boardings', hora from {{ ref('silver_am_boardings') }} where hora not between 4 and 22 group by hora
union all select 'metroriel_viajes', hora_entrada from {{ ref('silver_mr_viajes') }} where hora_entrada not between 4 and 22 group by hora_entrada
