{#
  Alarma temprana sobre Bronze: la llave técnica (_file_hash, _source_line_number) identifica la línea de un
  archivo de origen y debe ser única. Si no lo es, Bronze tiene el mismo mensaje más de una vez (por ejemplo,
  Kafka conservó mensajes de una corrida anterior y el archivo se republicó desde un lake nuevo).
  Reparación:  python -m bronze.cli reiniciar-consumidor ambos --confirmar   y luego   consume ambos
#}
{% test llave_tecnica_unica(model) %}
select _file_hash, _source_line_number, count(*) as veces
from {{ model }}
group by 1, 2
having count(*) > 1
{% endtest %}
