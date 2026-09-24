-- ============================================================================
-- DDL de Gold — Agencia Metropolitana de Transporte
-- ============================================================================
-- Este archivo documenta el esquema físico que dbt crea en el esquema `gold` del warehouse
-- (lake/warehouse.duckdb) al correr `python -m flows.dbt_runner build`. Cada tabla real la
-- construye su modelo dbt correspondiente (models/gold/*.sql) con CREATE TABLE AS SELECT;
-- este DDL es la documentación explícita de esa estructura, con los tipos exactos que DuckDB
-- infiere (verificados con `describe gold.<tabla>` sobre una corrida real).
--
-- Trazabilidad de origen: models/gold/*.sql | Diseño y justificación: docs/DISENO_GOLD_GRANO.md
-- Reglas de linaje (Gold solo lee Silver; solo seudónimos): flows/verificar_linaje.py
-- ============================================================================

-- ----------------------------------------------------------------------------
-- DIMENSIÓN: dim_transporte
-- 4 filas fijas. registra_trayecto_completo documenta, como atributo del dato y no como
-- comentario del código, que MetroRiel registra trayecto completo y los otros tres abordajes.
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_transporte (
    transporte_sk               VARCHAR      PRIMARY KEY,   -- = operador_cod ('TM','TU','AM','MR')
    operador_cod                VARCHAR      NOT NULL,
    nombre                      VARCHAR      NOT NULL,
    modo                        VARCHAR      NOT NULL,
    registra_trayecto_completo  BOOLEAN      NOT NULL,
    unidad_monetaria_origen     VARCHAR      NOT NULL
);

-- ----------------------------------------------------------------------------
-- DIMENSIÓN: dim_fecha
-- Una fila por cada fecha observada en los hechos. El rango sale de los propios datos
-- (min/max de fecha en las 4 fuentes Silver), no de una constante.
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_fecha (
    fecha_sk             VARCHAR   PRIMARY KEY,
    fecha                DATE      NOT NULL UNIQUE,
    anio                 BIGINT    NOT NULL,
    mes                  BIGINT    NOT NULL,
    dia_del_mes          BIGINT    NOT NULL,
    dia_semana_num       BIGINT    NOT NULL,   -- 0 = domingo … 6 = sábado (convención DuckDB)
    dia_semana_nombre    VARCHAR   NOT NULL,
    es_feriado           BOOLEAN   NOT NULL,
    nombre_feriado       VARCHAR,              -- NULL si es_feriado = false
    es_dia_habil         BOOLEAN   NOT NULL    -- lunes a viernes Y no feriado (seed feriados.csv)
);

-- ----------------------------------------------------------------------------
-- DIMENSIÓN: dim_hora
-- 24 filas fijas (0-23). es_hora_pico se DERIVA del volumen real de abordajes por hora
-- (las 4 fuentes juntas): pico si supera var('umbral_hora_pico') veces el promedio horario.
-- No es un rango de horas supuesto a priori.
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_hora (
    hora_sk                       INTEGER   PRIMARY KEY,     -- = hora (0-23)
    hora                          INTEGER   NOT NULL,
    franja                       VARCHAR   NOT NULL,        -- madrugada|manana|mediodia|tarde|noche (etiqueta fija)
    abordajes_totales_periodo    HUGEINT   NOT NULL,        -- volumen real observado, base del cálculo de pico
    es_hora_pico                 BOOLEAN   NOT NULL         -- DERIVADO del dato, ver models/gold/dim_hora.sql
);

-- ----------------------------------------------------------------------------
-- DIMENSIÓN: dim_estacion
-- Conformada entre las 4 redes (silver_estacion). Se referencia dos veces en fact_viaje
-- (entrada y salida) mediante dos alias en la consulta, no dos columnas FK distintas en la tabla.
-- La zona se alcanza atravesando esta dimensión (zona_id -> seed dim_zona, en el esquema silver).
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_estacion (
    estacion_sk             VARCHAR   PRIMARY KEY,
    operador                VARCHAR   NOT NULL,             -- 'transmetro'|'transurbano'|'metroriel'|'aerometro'
    estacion_id_original    VARCHAR   NOT NULL,
    nombre                  VARCHAR,
    linea_o_ruta            VARCHAR,
    zona_id                 INTEGER   NOT NULL,              -- FK a silver.dim_zona(zona_id); -1 = sin mapear
    lat                     DOUBLE,                           -- solo Transmetro la trae
    lon                     DOUBLE,                           -- solo Transmetro la trae
    km                      DOUBLE                            -- solo MetroRiel la trae; base de distancia_km
);

-- ----------------------------------------------------------------------------
-- DIMENSIÓN: dim_usuario
-- A propósito NO incluye llave_original ni id_numerico (sí existen en silver.silver_usuario,
-- para la trazabilidad interna que exige un auditor de fraude). Gold solo expone seudónimos.
-- Verificado automáticamente por flows/verificar_linaje.py.
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_usuario (
    usuario_sk        VARCHAR   PRIMARY KEY,    -- seudónimo por (operador, llave); SHA-256 con secreto, 20 hex
    persona_id        VARCHAR   NOT NULL,       -- seudónimo de persona unificada (hipótesis de identidad numérica)
    operador          VARCHAR   NOT NULL,
    regla_identidad   VARCHAR   NOT NULL,       -- 'solo_operador' | 'numerica_compartida'
    en_padron         BOOLEAN   NOT NULL,       -- false para el 59.6% de las tarjetas de Transmetro sin CDC
    perfil_vigente    VARCHAR,                  -- NULL si en_padron = false ("Desconocido" a nivel de consulta)
    activa_vigente    BOOLEAN                   -- NULL si en_padron = false
);

-- ----------------------------------------------------------------------------
-- HECHO: fact_abordaje
-- GRANO: una fila por cada evento de entrada al sistema, para cualquier medio de transporte
-- (cada validación de Transmetro, cada transacción de Transurbano, cada abordaje de Aerómetro,
-- y la entrada de cada viaje de MetroRiel).
--
-- Medidas:
--   abordajes   ADITIVA  (conteo; se suma por cualquier dimensión, incluido el tiempo)
--   monto_gtq   ADITIVA  (dinero, ya unificado a quetzales en Silver)
-- No hay medida semi-aditiva ni no-aditiva almacenada en este hecho: es un hecho de flujo puro.
-- La tarifa promedio se calcula en consulta: sum(monto_gtq) / sum(abordajes). Nunca se guarda.
-- ----------------------------------------------------------------------------
CREATE TABLE gold.fact_abordaje (
    evento_sk               VARCHAR         PRIMARY KEY,      -- degenerado: identifica el evento de origen
    transporte_sk            VARCHAR         NOT NULL REFERENCES gold.dim_transporte(transporte_sk),
    fecha_sk                 VARCHAR         NOT NULL REFERENCES gold.dim_fecha(fecha_sk),
    hora_sk                  INTEGER         NOT NULL REFERENCES gold.dim_hora(hora_sk),
    estacion_sk              VARCHAR         REFERENCES gold.dim_estacion(estacion_sk),  -- NULL: 0 casos observados
    usuario_sk               VARCHAR         NOT NULL REFERENCES gold.dim_usuario(usuario_sk),
    tipo_evento               VARCHAR,                          -- degenerado: 'tipo' (TM) / cod_estado (TU) / 'ENTRADA' (MR)
    abordajes                 INTEGER         NOT NULL,          -- medida aditiva, = 1 por fila
    monto_gtq                 DECIMAL(10,2)   NOT NULL,          -- medida aditiva
    _file_hash                VARCHAR         NOT NULL,          -- trazabilidad a Bronze (penalización 2.1 si falta)
    _source_line_number       BIGINT          NOT NULL
);

-- ----------------------------------------------------------------------------
-- HECHO: fact_viaje
-- GRANO: una fila por cada trayecto completo (de estación de entrada a estación de salida),
-- para cualquier medio de transporte capaz de registrarlo. Hoy solo MetroRiel puebla esta
-- tabla (es el único operador que trae salida real). El esquema es genérico a propósito: un
-- viaje multimodal inferido (extra de análisis de transbordo) se agregaría con otro valor de
-- transporte_sk, sin cambiar columnas ni consumidores.
--
-- Medidas:
--   viajes         ADITIVA
--   duracion_s     ADITIVA  (segundos totales; el promedio se calcula en consulta)
--   tarifa_gtq     ADITIVA
--   distancia_km   ADITIVA CON LÍMITE = tratamiento semi-aditivo por cobertura, no por naturaleza
--                  temporal: solo mr_estaciones trae km, así que es NULL para estaciones sin ese
--                  dato. Es aditiva por definición (ver criterios de referencia), pero no cubre
--                  hoy todo el tráfico; se declara en el diccionario de datos.
-- No hay medida no-aditiva almacenada: usuarios_distintos (no aditiva) se calcula siempre
-- contra el grano atómico, nunca se guarda un conteo distinto precalculado.
-- ----------------------------------------------------------------------------
CREATE TABLE gold.fact_viaje (
    evento_sk               VARCHAR         PRIMARY KEY,
    transporte_sk            VARCHAR         NOT NULL REFERENCES gold.dim_transporte(transporte_sk),
    fecha_sk                 VARCHAR         NOT NULL REFERENCES gold.dim_fecha(fecha_sk),
    hora_sk                  INTEGER         NOT NULL REFERENCES gold.dim_hora(hora_sk),        -- hora de entrada
    estacion_sk_entrada      VARCHAR         REFERENCES gold.dim_estacion(estacion_sk),          -- NULL: 0 casos
    estacion_sk_salida       VARCHAR         REFERENCES gold.dim_estacion(estacion_sk),          -- NULL: 0 casos
    usuario_sk               VARCHAR         NOT NULL REFERENCES gold.dim_usuario(usuario_sk),
    viajes                    INTEGER         NOT NULL,          -- medida aditiva, = 1 por fila
    duracion_s                INTEGER         NOT NULL,          -- medida aditiva
    tarifa_gtq                DECIMAL(10,2)   NOT NULL,          -- medida aditiva
    distancia_km              DOUBLE,                            -- medida aditiva con cobertura parcial (ver arriba)
    duracion_no_coincide       BOOLEAN         NOT NULL,          -- degenerado: heredado de la advertencia en Silver → Trabajando en ello
    _file_hash                VARCHAR         NOT NULL,
    _source_line_number       BIGINT          NOT NULL
);

-- ============================================================================
-- Diagrama de estrella (texto)
-- ============================================================================
--                                    dim_fecha
--                                        |
--                dim_transporte -- fact_abordaje -- dim_estacion
--                                        |
--                                  dim_usuario
--                                        |
--                                    dim_hora
--
--                                    dim_fecha
--                                        |
--   dim_estacion (rol: entrada) -- fact_viaje -- dim_estacion (rol: salida)
--                                        |     \
--                              dim_transporte   dim_usuario
--                                        |
--                                    dim_hora
--
-- dim_estacion es una dimensión de rol (role-playing) en fact_viaje: la misma tabla física
-- se referencia dos veces con distinto alias (entrada/salida), no se duplica físicamente.
-- ============================================================================
