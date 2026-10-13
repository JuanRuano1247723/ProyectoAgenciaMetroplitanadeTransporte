-- Cifras para el tablero y la recomendación de 2 páginas (2.1, 2.2).
--   python -m flows.consultas sql/analisis_recomendacion.sql --salida salidas/resumen_recomendacion.txt
-- Pásame el archivo salidas/resumen_recomendacion.txt completo.
-- "viaje" = abordaje que no es TRANSBORDO de Transmetro (ver docs/DEFINICIONES_OFICIALES.md).

.print 'A1 Volumen y periodo por operador'
select operador_cod, count(*) as abordajes, count(*) filter (where not es_transbordo_operador) as viajes,
       round(sum(monto_gtq), 2) as monto_gtq, count(distinct persona_id) as personas,
       min(fecha) as primer_dia, max(fecha) as ultimo_dia
from silver.silver_evento_unificado group by operador_cod order by abordajes desc

.print 'A2 Abordajes por mes (solo los meses completos son comparables)'
select date_trunc('month', fecha) as mes, count(distinct fecha) as dias_con_datos, count(*) as abordajes,
       round(count(*) * 1.0 / count(distinct fecha)) as abordajes_por_dia
from silver.silver_evento_unificado group by 1 order by 1

.print 'A3 Calidad: reglas que afectaron filas y cuántas fueron a cuarentena'
select fuente, regla, registros_afectados, registros_en_cuarentena
from silver.silver_dq_conteo_por_regla where registros_afectados > 0 order by fuente, registros_afectados desc

.print 'A4 Abordajes en zona sin mapear (zona_id = -1) por operador'
select operador_cod, count(*) filter (where zona_id = -1) as sin_zona, count(*) as total,
       round(100.0 * count(*) filter (where zona_id = -1) / count(*), 2) as pct_sin_zona
from silver.silver_evento_unificado group by operador_cod order by operador_cod

.print 'B1 Demanda por zona (todas, ordenadas por abordajes) con porcentaje acumulado'
select c.zona_id, c.zona_nombre, c.tipo_zona, c.abordajes_tm as tm, c.abordajes_tu as tu, c.abordajes_am as am, c.abordajes_mr as mr,
       c.abordajes_total as total,
       round(100.0 * c.abordajes_total / sum(c.abordajes_total) over (), 1) as pct,
       round(100.0 * sum(c.abordajes_total) over (order by c.abordajes_total desc, c.zona_id rows between unbounded preceding and current row)
             / sum(c.abordajes_total) over (), 1) as pct_acum,
       c.modos_con_servicio as modos, c.estaciones_total as estaciones,
       round(c.abordajes_total * 1.0 / nullif(c.estaciones_total, 0)) as abordajes_por_estacion,
       m.zona_en_caso as caso, m.tiene_estacion_mr as tiene_mr
from gold.agg_cobertura_zona c left join gold.agg_caso_metroriel m using (zona_id)
where c.zona_id <> -1 order by c.abordajes_total desc, c.zona_id

.print 'B2 Concentración: zonas necesarias para reunir el 50 y el 80 por ciento de la demanda'
with z as (select zona_id, row_number() over (order by abordajes_total desc, zona_id) as n,
                  sum(abordajes_total) over (order by abordajes_total desc, zona_id rows between unbounded preceding and current row) * 1.0
                  / sum(abordajes_total) over () as acum
           from gold.agg_cobertura_zona where zona_id <> -1 and abordajes_total > 0)
select (select min(n) from z where acum >= 0.5) as zonas_para_50pct, (select min(n) from z where acum >= 0.8) as zonas_para_80pct,
       (select count(*) from z) as zonas_con_demanda

.print 'B3 Modo dominante en cada zona y su peso dentro de la zona'
with m as (select zona_id, 'TM' as modo, abordajes_tm as n from gold.agg_cobertura_zona
           union all select zona_id, 'TU', abordajes_tu from gold.agg_cobertura_zona
           union all select zona_id, 'AM', abordajes_am from gold.agg_cobertura_zona
           union all select zona_id, 'MR', abordajes_mr from gold.agg_cobertura_zona),
     r as (select zona_id, modo, n, row_number() over (partition by zona_id order by n desc, modo) as rn,
                  sum(n) over (partition by zona_id) as tot from m)
select r.zona_id, c.zona_nombre, r.modo as modo_dominante, r.n as abordajes_modo, r.tot as abordajes_zona,
       round(100.0 * r.n / r.tot, 1) as pct_en_la_zona
from r join gold.agg_cobertura_zona c using (zona_id) where r.rn = 1 and r.tot > 0 and r.zona_id <> -1 order by r.tot desc

.print 'B4 Perfil horario: porcentaje del volumen de cada modo que ocurre en cada hora'
select hora,
       round(100.0 * sum(abordajes) filter (where operador_cod = 'TM') / (select sum(abordajes) from gold.agg_demanda_zona_hora where operador_cod = 'TM'), 2) as pct_tm,
       round(100.0 * sum(abordajes) filter (where operador_cod = 'TU') / (select sum(abordajes) from gold.agg_demanda_zona_hora where operador_cod = 'TU'), 2) as pct_tu,
       round(100.0 * sum(abordajes) filter (where operador_cod = 'AM') / (select sum(abordajes) from gold.agg_demanda_zona_hora where operador_cod = 'AM'), 2) as pct_am,
       round(100.0 * sum(abordajes) filter (where operador_cod = 'MR') / (select sum(abordajes) from gold.agg_demanda_zona_hora where operador_cod = 'MR'), 2) as pct_mr
from gold.agg_demanda_zona_hora group by hora order by hora

.print 'B5 Correlación entre perfiles horarios de los modos (1 = idénticos) y mayor diferencia en puntos porcentuales'
with h as (select hora,
                  sum(abordajes) filter (where operador_cod = 'TM') * 1.0 / (select sum(abordajes) from gold.agg_demanda_zona_hora where operador_cod = 'TM') as tm,
                  sum(abordajes) filter (where operador_cod = 'TU') * 1.0 / (select sum(abordajes) from gold.agg_demanda_zona_hora where operador_cod = 'TU') as tu,
                  sum(abordajes) filter (where operador_cod = 'AM') * 1.0 / (select sum(abordajes) from gold.agg_demanda_zona_hora where operador_cod = 'AM') as am,
                  sum(abordajes) filter (where operador_cod = 'MR') * 1.0 / (select sum(abordajes) from gold.agg_demanda_zona_hora where operador_cod = 'MR') as mr
           from gold.agg_demanda_zona_hora group by hora)
select 'TM-TU' as par, round(corr(tm, tu), 4) as correlacion, round(100 * max(abs(tm - tu)), 2) as max_dif_pp from h
union all select 'TM-AM', round(corr(tm, am), 4), round(100 * max(abs(tm - am)), 2) from h
union all select 'TM-MR', round(corr(tm, mr), 4), round(100 * max(abs(tm - mr)), 2) from h
union all select 'TU-AM', round(corr(tu, am), 4), round(100 * max(abs(tu - am)), 2) from h
union all select 'TU-MR', round(corr(tu, mr), 4), round(100 * max(abs(tu - mr)), 2) from h
union all select 'AM-MR', round(corr(am, mr), 4), round(100 * max(abs(am - mr)), 2) from h

.print 'B6 Horas pico (regla de dim_hora) y qué parte del volumen concentran'
select hora, franja, abordajes_totales_periodo as abordajes,
       round(100.0 * abordajes_totales_periodo / (select sum(abordajes_totales_periodo) from gold.dim_hora), 1) as pct_del_total,
       round(100.0 * sum(abordajes_totales_periodo) over () / (select sum(abordajes_totales_periodo) from gold.dim_hora), 1) as pct_total_horas_pico
from gold.dim_hora where es_hora_pico order by hora

.print 'B7 Día hábil frente a fin de semana por modo (abordajes por día)'
select d.operador_cod, f.es_dia_habil, count(distinct d.fecha) as dias, sum(d.abordajes) as abordajes,
       round(sum(d.abordajes) * 1.0 / count(distinct d.fecha)) as abordajes_por_dia
from gold.agg_demanda_zona_hora d join gold.dim_fecha f on f.fecha = d.fecha
group by d.operador_cod, f.es_dia_habil order by d.operador_cod, f.es_dia_habil desc

.print 'B8 Abordajes por día de la semana (todos los modos; 0 = domingo)'
select f.dia_semana_num, f.dia_semana_nombre, count(distinct d.fecha) as dias, sum(d.abordajes) as abordajes,
       round(sum(d.abordajes) * 1.0 / count(distinct d.fecha)) as abordajes_por_dia
from gold.agg_demanda_zona_hora d join gold.dim_fecha f on f.fecha = d.fecha
group by 1, 2 order by 1

.print 'C1 Cobertura por tipo de zona; difieren = zonas donde sin_servicio y sin_servicio_efectivo no coinciden'
select tipo_zona, count(*) as zonas, count(*) filter (where sin_servicio) as sin_servicio,
       count(*) filter (where sin_servicio_efectivo) as sin_servicio_efectivo,
       count(*) filter (where sin_servicio <> sin_servicio_efectivo) as difieren
from gold.agg_cobertura_zona group by tipo_zona order by tipo_zona

.print 'C2 Zonas y municipios sin servicio de ningún modo'
select zona_id, zona_nombre, tipo_zona, abordajes_total from gold.agg_cobertura_zona where sin_servicio order by zona_id

.print 'C3 Cuántas zonas tienen 0, 1, 2, 3 o 4 modos con estación'
select tipo_zona, modos_con_servicio, count(*) as zonas, sum(abordajes_total) as abordajes
from gold.agg_cobertura_zona group by 1, 2 order by 1, 2

.print 'C4 Zonas por combinación de modos presentes (estación o parada)'
select coalesce(nullif(concat_ws('+', case when estaciones_tm > 0 then 'TM' end, case when paradas_tu > 0 then 'TU' end,
                 case when estaciones_am > 0 then 'AM' end, case when estaciones_mr > 0 then 'MR' end), ''), '(ninguno)') as modos_presentes,
       count(*) as zonas, sum(abordajes_total) as abordajes, list(zona_id order by zona_id) as zonas_ids
from gold.agg_cobertura_zona where zona_id <> -1 group by 1 order by zonas desc

.print 'C5 Zonas servidas por un solo modo, ordenadas por demanda'
select zona_id, zona_nombre, abordajes_total, estaciones_tm as tm, paradas_tu as tu, estaciones_am as am, estaciones_mr as mr
from gold.agg_cobertura_zona where modos_con_servicio = 1 order by abordajes_total desc limit 15

.print 'D1 Personas por número de modos usados'
select n_modos, sum(personas) as personas, round(100.0 * sum(personas) / sum(sum(personas)) over (), 1) as pct
from gold.agg_usuarios_por_combinacion group by n_modos order by n_modos

.print 'D2 Personas por combinación de operadores'
select combinacion, n_modos, personas, abordajes, round(abordajes * 1.0 / personas, 1) as abordajes_por_persona
from gold.agg_usuarios_por_combinacion order by personas desc

.print 'D3 Porcentaje multimodo (bajo la hipótesis de identidad actual)'
select sum(personas) filter (where n_modos > 1) as personas_multimodo, sum(personas) as personas_total,
       round(100.0 * sum(personas) filter (where n_modos > 1) / sum(personas), 1) as pct_multimodo
from gold.agg_usuarios_por_combinacion

.print 'D4 Transbordos que Transmetro registra por sí mismo'
select sum(abordajes) filter (where es_transbordo_operador) as transbordos_tm, sum(abordajes) as abordajes_tm,
       round(100.0 * sum(abordajes) filter (where es_transbordo_operador) / sum(abordajes), 1) as pct_de_abordajes_tm
from gold.agg_demanda_zona_hora where operador_cod = 'TM'

.print 'D5 Transbordos inferidos entre operadores (misma persona, menos de 60 minutos)'
select de_operador, a_operador, sum(transbordos) as transbordos, count(*) as horas_con_datos
from gold.agg_transbordo_temporal group by 1, 2 order by 3 desc

.print 'D6 Diagnóstico de identidad: transbordos con ids reales frente a ids desplazados (placebo)'
with ev as (select persona_id, operador_cod, ts_local, evento_sk from silver.silver_evento_unificado),
ids as (select persona_id, dense_rank() over (order by persona_id) - 1 as r, count(*) over () as n
        from (select distinct persona_id from ev)),
colocado as (select ev.*, case when ev.operador_cod = 'TM' then ev.persona_id
                               else (select i2.persona_id from ids i2 where i2.r = (i.r + 7919) % i.n) end as persona_placebo
             from ev join ids i using (persona_id)),
pares as (select 'real' as escenario, persona_id as p, operador_cod, ts_local, evento_sk from colocado
          union all select 'placebo', persona_placebo, operador_cod, ts_local, evento_sk from colocado),
sec as (select escenario, p, operador_cod, ts_local, lag(operador_cod) over w as op_previo, lag(ts_local) over w as ts_previo
        from pares window w as (partition by escenario, p order by ts_local, evento_sk)),
res as (select escenario, count(*) as n from sec
        where op_previo is not null and op_previo <> operador_cod and date_diff('minute', ts_previo, ts_local) <= 60 group by escenario)
select max(n) filter (where escenario = 'real') as reales, max(n) filter (where escenario = 'placebo') as placebo,
       round(max(n) filter (where escenario = 'real') * 1.0 / max(n) filter (where escenario = 'placebo'), 3) as razon_real_placebo
from res

.print 'E1 Estaciones de MetroRiel: zona, kilómetro, entradas y salidas'
select e.nombre, e.zona_id, e.km, coalesce(a.entradas, 0) as entradas, coalesce(s.salidas, 0) as salidas
from gold.dim_estacion e
left join (select estacion_sk, count(*) as entradas from gold.fact_abordaje group by 1) a using (estacion_sk)
left join (select estacion_sk_salida as estacion_sk, count(*) as salidas from gold.fact_viaje group by 1) s using (estacion_sk)
where e.operador = 'metroriel' order by e.km, e.nombre

.print 'E2 Zonas del caso: demanda de los otros modos y presencia de MetroRiel'
select zona_id, zona_nombre, ranking_demanda_otros_modos as ranking, abordajes_otros_modos, round(100 * pct_demanda_otros_modos, 1) as pct_demanda,
       estaciones_mr, abordajes_mr, tiene_estacion_mr
from gold.agg_caso_metroriel where zona_en_caso order by ranking_demanda_otros_modos

.print 'E3 Las 15 zonas con más demanda de los otros modos y si tienen MetroRiel'
select ranking_demanda_otros_modos as ranking, zona_id, zona_nombre, abordajes_otros_modos, round(100 * pct_demanda_otros_modos, 1) as pct_demanda,
       zona_en_caso as caso, estaciones_mr, abordajes_mr
from gold.agg_caso_metroriel order by ranking_demanda_otros_modos, zona_id limit 15

.print 'E4 Qué parte de la demanda de los otros modos cae en zonas con MetroRiel y en las zonas del caso'
select count(*) as zonas, count(*) filter (where tiene_estacion_mr) as zonas_con_mr, count(*) filter (where zona_en_caso) as zonas_del_caso,
       round(100.0 * sum(abordajes_otros_modos) filter (where tiene_estacion_mr) / sum(abordajes_otros_modos), 1) as pct_demanda_en_zonas_con_mr,
       round(100.0 * sum(abordajes_otros_modos) filter (where zona_en_caso) / sum(abordajes_otros_modos), 1) as pct_demanda_en_zonas_del_caso,
       round(100.0 * sum(abordajes_otros_modos) filter (where zona_en_caso and not tiene_estacion_mr) / sum(abordajes_otros_modos), 1) as pct_caso_sin_mr
from gold.agg_caso_metroriel

.print 'E5 Las 10 zonas de mayor demanda SIN estación de MetroRiel'
select ranking_demanda_otros_modos as ranking, zona_id, zona_nombre, abordajes_otros_modos, round(100 * pct_demanda_otros_modos, 1) as pct_demanda
from gold.agg_caso_metroriel where not tiene_estacion_mr order by abordajes_otros_modos desc limit 10

.print 'E6 Cuota de MetroRiel en las zonas donde tiene estación (MR frente a los otros modos)'
select zona_id, zona_nombre, estaciones_mr, abordajes_mr, abordajes_otros_modos,
       round(100.0 * abordajes_mr / nullif(abordajes_mr + abordajes_otros_modos, 0), 1) as pct_mr_en_la_zona
from gold.agg_caso_metroriel where tiene_estacion_mr order by abordajes_mr desc

.print 'E7 Origen-destino de MetroRiel: los 15 pares de zonas con más viajes'
select zona_entrada, zona_salida, viajes, round(100.0 * viajes / sum(viajes) over (), 1) as pct, round(duracion_s * 1.0 / viajes / 60, 1) as minutos_promedio
from gold.agg_od_metroriel order by viajes desc limit 15

.print 'E8 Viajes de MetroRiel frente a las zonas del caso'
select sum(viajes) as viajes,
       sum(viajes) filter (where zona_entrada in (select zona_id from gold.agg_caso_metroriel where zona_en_caso)) as origen_en_caso,
       sum(viajes) filter (where zona_salida in (select zona_id from gold.agg_caso_metroriel where zona_en_caso)) as destino_en_caso,
       sum(viajes) filter (where zona_entrada in (select zona_id from gold.agg_caso_metroriel where zona_en_caso)
                             and zona_salida in (select zona_id from gold.agg_caso_metroriel where zona_en_caso)) as ambos_en_caso,
       sum(viajes) filter (where zona_entrada = zona_salida) as misma_zona
from gold.agg_od_metroriel

.print 'E9 MetroRiel: duración, distancia, velocidad y tarifa'
select count(*) as viajes, round(avg(duracion_s) / 60, 1) as minutos_promedio, round(avg(distancia_km), 2) as km_promedio,
       round(sum(distancia_km) / (sum(duracion_s) filter (where distancia_km is not null) / 3600.0), 1) as velocidad_kmh,
       round(avg(tarifa_gtq), 2) as tarifa_promedio_gtq, count(*) filter (where distancia_km is null) as viajes_sin_km
from gold.fact_viaje

.print 'E10 Tarjetas activas del padrón por zona de residencia (solo Transmetro, cobertura parcial), top 12'
select p.zona_residencia_id as zona_id, count(*) as tarjetas_activas, c.tiene_estacion_mr, c.zona_en_caso as caso
from silver.silver_padron_vigente p left join gold.agg_caso_metroriel c on c.zona_id = p.zona_residencia_id
where p.activa group by 1, 3, 4 order by 2 desc, 1 limit 12

.print 'E11 Personas por zona de abordaje más frecuente (features), top 12'
select f.zona_origen_mas_frecuente as zona_id, count(*) as personas, c.tiene_estacion_mr, c.zona_en_caso as caso
from features.features_usuario f left join gold.agg_caso_metroriel c on c.zona_id = f.zona_origen_mas_frecuente
group by 1, 3, 4 order by 2 desc, 1 limit 12

.print 'F1 Features: usuarios activos (al menos un viaje en 30 días) y resumen'
select count(*) as personas, max(fecha_corte) as fecha_corte, count(*) filter (where viajes_30d >= 1) as activos_30d,
       round(100.0 * count(*) filter (where viajes_30d >= 1) / count(*), 1) as pct_activos,
       round(avg(viajes_total), 1) as viajes_promedio, quantile_cont(viajes_total, 0.5) as viajes_mediana,
       round(avg(n_modos), 2) as modos_promedio, round(avg(gasto_acumulado_gtq), 2) as gasto_promedio_gtq,
       round(100 * avg(prop_viajes_hora_pico), 1) as pct_viajes_en_hora_pico
from features.features_usuario

.print 'G1 Filas en Silver y Gold'
select 'silver_evento_unificado' as tabla, count(*) as filas from silver.silver_evento_unificado
union all select 'silver_cuarentena', count(*) from silver.silver_cuarentena
union all select 'gold.fact_abordaje', count(*) from gold.fact_abordaje
union all select 'gold.fact_viaje', count(*) from gold.fact_viaje
union all select 'features_usuario', count(*) from features.features_usuario