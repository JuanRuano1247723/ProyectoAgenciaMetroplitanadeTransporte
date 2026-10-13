{# Clave normalizada de zona: 'Zona 10', 'Z10', 'Zone 10' -> 'zona 10'; 'Mixco' -> 'mixco'. #}
{% macro clave_zona(col) %}
(
    case
        when {{ col }} is null then null
        when regexp_matches(lower(trim({{ col }})), '^(zona|zone|z)\s*-?\s*[0-9]{1,2}$')
            then 'zona ' || cast(cast(regexp_extract(lower(trim({{ col }})), '([0-9]{1,2})$', 1) as integer) as varchar)
        else lower(strip_accents(trim({{ col }})))
    end
)
{% endmacro %}

{# Fecha (UTC) de ingesta: referencia para detectar fechas del futuro sin depender de la zona horaria de la sesión #}
{% macro fecha_ingesta(col='_ingested_at') %}
cast(timezone('UTC', {{ col }}) as date)
{% endmacro %}

{# Llave surrogate estable de un evento: mismo archivo y misma línea => mismo valor en cada corrida #}
{% macro evento_sk(prefijo) %}
md5('{{ prefijo }}:' || _file_hash || ':' || cast(_source_line_number as varchar))
{% endmacro %}
