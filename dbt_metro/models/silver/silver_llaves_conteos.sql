{# Entregable 1.2: usuarios únicos por operador. Los catálogos salen de TODAS las filas de operación
   (incluidas las que fueron a cuarentena), porque la llave existe aunque la fila sea inválida. #}
select 'transurbano' as operador, 'transurbano_transacciones.csv' as archivo,
       (select count(*) from {{ ref('silver_llaves_transurbano') }}) as usuarios_unicos_en_catalogo,
       (select count(distinct num_tarjeta) from {{ ref('silver_tu_transacciones') }}) as usuarios_unicos_en_filas_validas
union all
select 'metroriel', 'metroriel_viajes.jsonl',
       (select count(*) from {{ ref('silver_llaves_metroriel') }}),
       (select count(distinct tarjeta) from {{ ref('silver_mr_viajes') }})
union all
select 'aerometro', 'aerometro_boardings.csv',
       (select count(*) from {{ ref('silver_llaves_aerometro') }}),
       (select count(distinct user_hash) from {{ ref('silver_am_boardings') }})
