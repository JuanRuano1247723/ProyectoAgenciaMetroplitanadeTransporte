-- Entregables de Silver (1.2 y 1.3). Después de correr:  python -m flows.dbt_runner build
--   python -m flows.consultas sql/entregables_silver.sql      (o, con la CLI de duckdb:  duckdb lake/warehouse.duckdb < sql/entregables_silver.sql)
-- Todas son consultas de solo lectura.

.print '== 1.2  Modelo CDC de Transmetro: conteo antes/después de aplicar los borrados'
SELECT eventos_en_bronze, eventos_en_cuarentena, eventos_aplicados, inserts, updates, deletes,
       tarjetas_antes_de_borrados, tarjetas_activas_despues, tarjetas_dadas_de_baja,
       tarjetas_sin_insert_previo, tarjetas_reactivadas, tarjetas_con_insert_repetido, versiones_scd2
FROM silver.silver_cdc_conteos;

.print '== 1.2  Catálogos de llaves distintas (solo la llave) y usuarios únicos por operador'
SELECT operador, archivo, usuarios_unicos_en_catalogo, usuarios_unicos_en_filas_validas
FROM silver.silver_llaves_conteos ORDER BY operador;

.print '== 1.3  Conteo de registros por regla de calidad (solo reglas con incumplimientos)'
SELECT fuente, regla, severidad, registros_evaluados, registros_afectados, registros_en_cuarentena, porcentaje_afectado
FROM silver.silver_dq_conteo_por_regla WHERE registros_afectados > 0 ORDER BY fuente, severidad DESC, registros_afectados DESC;

.print '== 1.3  Reglas que no encontraron nada (prueba de que se evaluaron)'
SELECT fuente, regla, severidad FROM silver.silver_dq_conteo_por_regla WHERE registros_afectados = 0 ORDER BY 1, 2;

.print '== 1.3  Cuarentena: filas por fuente y motivo'
SELECT fuente, regla_principal, motivo, count(*) AS filas
FROM silver.silver_cuarentena GROUP BY 1, 2, 3 ORDER BY 1, 4 DESC;

.print '== 1.3  Invariante: Bronze = Silver + cuarentena (por fuente)'
WITH s AS (
    SELECT 'transmetro_validaciones' AS fuente, count(*) AS silver FROM silver.silver_tm_validaciones UNION ALL
    SELECT 'transurbano_transacciones', count(*) FROM silver.silver_tu_transacciones UNION ALL
    SELECT 'aerometro_boardings', count(*) FROM silver.silver_am_boardings UNION ALL
    SELECT 'metroriel_viajes', count(*) FROM silver.silver_mr_viajes UNION ALL
    SELECT 'cdc_padron_usuarios', count(*) FROM staging.stg_cdc_padron WHERE len(reglas_rechazo) = 0),
q AS (SELECT fuente, count(*) AS cuarentena FROM silver.silver_cuarentena GROUP BY 1)
SELECT s.fuente, s.silver, coalesce(q.cuarentena, 0) AS cuarentena, s.silver + coalesce(q.cuarentena, 0) AS total
FROM s LEFT JOIN q USING (fuente) ORDER BY 1;

.print '== Zonas que no se pudieron conformar (agregar a seeds/dim_zona.csv)'
SELECT 'estacion' AS origen, zona_original FROM silver.silver_estacion WHERE zona_id = -1 GROUP BY 2
UNION ALL SELECT 'padron', zona_residencia_original FROM silver.silver_padron_scd2
WHERE zona_residencia_id = -1 AND zona_residencia_original IS NOT NULL GROUP BY 2;

.print '== Identidad: personas con presencia en más de un operador (según la estrategia numérica)'
SELECT operadores, count(*) AS personas FROM (
    SELECT persona_id, count(DISTINCT operador) AS operadores FROM silver.silver_usuario GROUP BY 1)
GROUP BY 1 ORDER BY 1;

.print '== Diagnóstico: llaves MR del padrón; ¿existe la tarjeta TC con el mismo número entre las validaciones? (se espera 0)'
SELECT count(*) AS llaves_mr_en_padron,
       count(*) FILTER (WHERE EXISTS (
           SELECT 1 FROM staging.stg_tm_validaciones v
           WHERE v.tarjeta = 'TC-' || lpad(substr(c.tarjeta, 3), 8, '0'))) AS con_tc_equivalente_en_validaciones
FROM (SELECT DISTINCT tarjeta FROM staging.stg_cdc_padron WHERE regexp_matches(tarjeta, '^MR[0-9]{7}$')) c;
