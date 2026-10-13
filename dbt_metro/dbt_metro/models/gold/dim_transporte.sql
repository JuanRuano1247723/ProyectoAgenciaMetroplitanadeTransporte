{#
  Dimensión conformada de transporte. `registra_trayecto_completo` documenta como atributo del dato,
  no como comentario del código, la advertencia central de la rúbrica: MetroRiel registra el trayecto
  completo, los otros tres registran abordajes.
#}
select
    operador_cod                       as transporte_sk,
    operador_cod,
    nombre,
    modo,
    registra_trayecto_completo,
    unidad_monetaria_origen
from {{ ref('transporte_catalogo') }}
