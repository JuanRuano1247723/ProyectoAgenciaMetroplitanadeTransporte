{# Padrón vigente de Transmetro: la versión actual de cada tarjeta (activa o dada de baja). #}
select * from {{ ref('silver_padron_scd2') }} where es_vigente
