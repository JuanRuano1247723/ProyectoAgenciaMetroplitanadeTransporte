{#
  Seudónimo de una llave de usuario: SHA-256 con secreto (pimienta) y 20 caracteres hexadecimales (80 bits).
  - Sin el secreto no se puede regenerar ni verificar una adivinanza: las llaves reales ocupan un espacio pequeño
    (~60,000 números) y un hash sin secreto se revierte por fuerza bruta en una fracción de segundo.
  - El mismo (secreto, llave) siempre da el mismo seudónimo, así que Silver sigue siendo idempotente.
  - Cambiar el secreto cambia TODOS los seudónimos (rompe la continuidad histórica): tratarlo como una rotación.
  El secreto lo carga dbt_metro/plugins/seguridad.py desde PSEUDONIMO_SECRETO; nunca aparece en el SQL.
#}
{% macro pseudonimo(expresion) -%}
left(sha256((select secreto from seguridad.main.clave) || ':' || ({{ expresion }})), 20)
{%- endmacro %}
