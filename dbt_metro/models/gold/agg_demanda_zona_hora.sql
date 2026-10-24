{#
  agg_demanda_zona_hora   -- consumo de Tableau: "Demanda por modo, zona y hora"
  Grano: fecha x hora x operador x zona. Medidas aditivas (abordajes, monto_gtq). Sin usuarios distintos
  (no aditivo: se calcula contra el grano atómico, ver G-10).
  Para trazar un número hasta el crudo, Tableau también puede conectarse a fact_abordaje (conserva evento_sk,
  _file_hash y _source_line_number); este agregado existe para que el tablero responda en milisegundos.
#}
select
    fecha,
    hora,
    operador_cod,
    zona_id,
    es_transbordo_operador,
    count(*)        as abordajes,
    sum(monto_gtq)  as monto_gtq
from {{ ref('silver_evento_unificado') }}
group by all
