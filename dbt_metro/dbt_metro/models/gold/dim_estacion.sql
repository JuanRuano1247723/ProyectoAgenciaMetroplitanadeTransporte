{#
  Dimensión conformada de estaciones/paradas (silver_estacion), sin cambios de negocio: solo se
  expone en el esquema gold para que los hechos la referencien como parte del modelo dimensional.
  La zona se alcanza atravesando esta dimensión (zona_id -> dim_zona), no se repite en los hechos.
#}
select
    estacion_sk,
    operador,
    estacion_id_original,
    nombre,
    linea_o_ruta,
    zona_id,
    lat,
    lon,
    km
from {{ ref('silver_estacion') }}
