-- Cifras de la fase 2 (tablero y recomendación). Ejecutar: python -m flows.consultas sql/entregables_fase2.sql
-- Cada consulta responde una pregunta de 2.1 y sirve para validar que el tablero de Tableau muestra el mismo número.

-- 1. Demanda: horas pico y reparto por modo (viajes = abordajes que no son TRANSBORDO)
select hora, es_hora_pico, abordajes_totales_periodo from gold.dim_hora order by hora;

select operador_cod,
       sum(abordajes) as abordajes,
       sum(abordajes) filter (where not es_transbordo_operador) as viajes,
       round(100.0 * sum(abordajes) / sum(sum(abordajes)) over (), 1) as pct_abordajes
from gold.agg_demanda_zona_hora group by operador_cod order by abordajes desc;

-- 2. Demanda: las 10 zonas con más abordajes y cuántos modos las sirven
select zona_id, zona_nombre, abordajes_total, modos_con_servicio, c.estaciones_mr
from gold.agg_cobertura_zona c
left join gold.agg_caso_metroriel m using (zona_id, zona_nombre)
order by abordajes_total desc limit 10;

-- 3. Cobertura: zonas y municipios sin servicio de ningún modo, y con estación pero sin uso
select tipo_zona, count(*) filter (where sin_servicio) as sin_servicio,
       count(*) filter (where not sin_servicio) as con_servicio,
       count(*) filter (where sin_servicio_efectivo and not sin_servicio) as con_estacion_sin_uso
from gold.agg_cobertura_zona group by tipo_zona;

select zona_id, zona_nombre, tipo_zona from gold.agg_cobertura_zona where sin_servicio order by zona_id;

-- 4. Transbordo: usuarios que usan más de un sistema (y su porcentaje)
select sum(personas) filter (where n_modos > 1) as personas_multimodo,
       sum(personas) as personas_total,
       round(100.0 * sum(personas) filter (where n_modos > 1) / sum(personas), 1) as pct_multimodo
from gold.agg_usuarios_por_combinacion;

select n_modos, combinacion, personas from gold.agg_usuarios_por_combinacion order by personas desc;

select de_operador, a_operador, sum(transbordos) as transbordos_inferidos
from gold.agg_transbordo_temporal group by 1, 2 order by 3 desc;

-- 5. Caso MetroRiel: ¿el trazado atiende donde más se necesita?
select zona_id, zona_nombre, zona_en_caso, ranking_demanda_otros_modos, abordajes_otros_modos,
       round(100 * pct_demanda_otros_modos, 1) as pct_demanda, estaciones_mr, abordajes_mr
from gold.agg_caso_metroriel order by ranking_demanda_otros_modos;

-- demanda de las zonas con estación de MetroRiel frente al total
select round(100.0 * sum(abordajes_otros_modos) filter (where tiene_estacion_mr) / sum(abordajes_otros_modos), 1)
       as pct_demanda_en_zonas_con_metroriel
from gold.agg_caso_metroriel;

-- zonas de mayor demanda (top 5) que NO tienen estación de MetroRiel
select zona_id, zona_nombre, abordajes_otros_modos from gold.agg_caso_metroriel
where not tiene_estacion_mr order by abordajes_otros_modos desc limit 5;

-- 6. Rendimiento: tamaño de cada capa (copiar a la documentación de métricas)
select table_schema, count(*) as tablas from information_schema.tables
where table_schema in ('staging','silver','gold','features') group by 1 order by 1;
