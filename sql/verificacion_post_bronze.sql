-- Verificación de Bronze con los datos reales.
--   python -m bronze.cli vistas
--   python -m flows.consultas sql/verificacion_post_bronze.sql      (o, con la CLI de duckdb:  duckdb lake/bronze_vistas.duckdb < sql/verificacion_post_bronze.sql)
-- Todas son consultas de solo lectura sobre las vistas bronze.*; no modifican nada.

.print '== 1. Filas por tabla Bronze y por tabla de malformadas'
SELECT 'transmetro_validaciones' AS tabla, count(*) AS filas FROM bronze.transmetro_validaciones UNION ALL
SELECT 'aerometro_boardings',      count(*) FROM bronze.aerometro_boardings UNION ALL
SELECT 'transurbano_transacciones', count(*) FROM bronze.transurbano_transacciones UNION ALL
SELECT 'metroriel_viajes',         count(*) FROM bronze.metroriel_viajes UNION ALL
SELECT 'transmetro_padron_cdc',    count(*) FROM bronze.transmetro_padron_cdc
ORDER BY 1;

.print '== 2. CDC: forma de la llave tarjeta (SIN-TARJETA y llaves con formato MR no son tarjetas de Transmetro)'
SELECT CASE WHEN tarjeta = 'SIN-TARJETA'                    THEN 'SIN-TARJETA'
            WHEN regexp_matches(tarjeta, '^TC-[0-9]{8}$')   THEN 'TC-########'
            WHEN regexp_matches(tarjeta, '^MR[0-9]{7}$')    THEN 'MR#######'
            ELSE 'OTRO' END AS patron,
       count(*) AS filas, count(DISTINCT tarjeta) AS tarjetas_distintas
FROM bronze.transmetro_padron_cdc GROUP BY 1 ORDER BY filas DESC;

.print '== 3. CDC: operaciones por tipo y cuerpo de los DELETE'
SELECT op, count(*) AS filas,
       count(*) FILTER (WHERE perfil IS NULL AND zona_residencia IS NULL AND estado IS NULL) AS sin_cuerpo
FROM bronze.transmetro_padron_cdc GROUP BY op ORDER BY op;

.print '== 4. CDC: seq es único y creciente en el archivo; commit_ts no'
SELECT count(*) AS filas, count(DISTINCT seq) AS seq_distintos,
       count(*) FILTER (WHERE CAST(seq AS BIGINT) < CAST(prev_seq AS BIGINT)) AS seq_fuera_de_orden,
       count(*) FILTER (WHERE commit_ts < prev_ts) AS commit_ts_fuera_de_orden
FROM (SELECT seq, commit_ts,
             lag(seq) OVER (ORDER BY _source_line_number) AS prev_seq,
             lag(commit_ts) OVER (ORDER BY _source_line_number) AS prev_ts
      FROM bronze.transmetro_padron_cdc);

.print '== 5. Transurbano: particiones fuera del periodo de Transmetro (fechas anómalas conservadas)'
SELECT _partition_date, count(*) AS filas
FROM bronze.transurbano_transacciones
WHERE _partition_date NOT BETWEEN (SELECT min(_partition_date) FROM bronze.transmetro_validaciones)
                              AND (SELECT max(_partition_date) FROM bronze.transmetro_validaciones)
GROUP BY 1 ORDER BY 1;

.print '== 6. Transmetro: filas duplicadas exactas conservadas en Bronze'
SELECT count(*) - count(DISTINCT (validacion_id, tarjeta, estacion_id, linea, fecha_hora, tarifa, tipo)) AS duplicadas_exactas
FROM bronze.transmetro_validaciones;

.print '== 7. Transmetro: tarjetas de validaciones sin ninguna operación en el CDC'
SELECT count(DISTINCT v.tarjeta) AS tarjetas_sin_padron,
       (SELECT count(DISTINCT tarjeta) FROM bronze.transmetro_validaciones) AS tarjetas_en_validaciones
FROM bronze.transmetro_validaciones v
LEFT JOIN (SELECT DISTINCT tarjeta FROM bronze.transmetro_padron_cdc) c USING (tarjeta)
WHERE c.tarjeta IS NULL;

.print '== 8. MetroRiel: viajes sin salida (exit nulo)'
SELECT count(*) AS viajes, count(*) FILTER (WHERE "exit" IS NULL) AS sin_salida,
       count(*) FILTER (WHERE "exit" IS NULL AND duration_s IS NULL) AS sin_salida_y_sin_duracion
FROM bronze.metroriel_viajes;

.print '== 9. Aerómetro: la partición es la fecha UTC (puede ir un día adelante de la fecha local)'
SELECT _partition_date, count(*) AS filas, min(timestamp_utc) AS primero, max(timestamp_utc) AS ultimo
FROM bronze.aerometro_boardings GROUP BY 1 ORDER BY 1;

.print '== 10. Metadatos completos en todas las tablas operativas'
SELECT 'transmetro_validaciones' AS tabla,
       count(*) FILTER (WHERE _ingested_at IS NULL OR _file_hash IS NULL OR _source_line_number IS NULL
                          OR _kafka_topic IS NULL OR _kafka_offset IS NULL) AS filas_con_metadatos_incompletos
FROM bronze.transmetro_validaciones UNION ALL
SELECT 'aerometro_boardings',
       count(*) FILTER (WHERE _ingested_at IS NULL OR _file_hash IS NULL OR _source_line_number IS NULL
                          OR _kafka_topic IS NULL OR _kafka_offset IS NULL) FROM bronze.aerometro_boardings UNION ALL
SELECT 'transurbano_transacciones',
       count(*) FILTER (WHERE _ingested_at IS NULL OR _file_hash IS NULL OR _source_line_number IS NULL)
FROM bronze.transurbano_transacciones;
