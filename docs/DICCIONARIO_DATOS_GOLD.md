# Diccionario de datos de Gold (3.1)

Para cada campo: qué significa, de qué fuente sale y qué transformación se le aplicó. Cada medida se guarda como numerador y denominador aditivos; ninguna razón se guarda precalculada (G-10). Gold solo lee Silver y solo expone seudónimos. El linaje hasta el archivo crudo lo genera dbt (`python -m flows.dbt_runner docs`).

Los campos con "Heredado" no se transforman en Gold: la transformación ocurrió en Silver (ver `DISENO_SILVER.md`). Los agregados `agg_*` son tablas pequeñas para Tableau; su verdad está en los hechos y en `silver_evento_unificado`.


## `gold.dim_transporte`

Dimensión conformada de operadores (4 filas). Fuente: seed transporte_catalogo.

| Campo | Significado | Fuente | Transformación |
|---|---|---|---|
| `transporte_sk` | Llave del operador (igual al código). | seed | Es el propio operador_cod. |
| `operador_cod` | Código del operador: TM, TU, AM, MR. | seed | Ninguna. |
| `nombre` | Nombre comercial del operador. | seed | Ninguna. |
| `modo` | Modo de transporte (BRT, bus convencional, teleférico, tren ligero). | seed | Ninguna. |
| `registra_trayecto_completo` | true si el operador registra entrada y salida (solo MetroRiel). | seed | Ninguna. |
| `unidad_monetaria_origen` | Unidad en la que el operador entrega el dinero (Transurbano: centavos). | seed | Informativa: Silver ya convirtió todo a quetzales. |

## `gold.dim_fecha`

Calendario, una fila por día entre la primera y la última fecha con datos. Fuente: silver_* (rango) + seed feriados.

| Campo | Significado | Fuente | Transformación |
|---|---|---|---|
| `fecha_sk` | Llave sustituta de la fecha. | calculada | md5 de la fecha. |
| `fecha` | Fecha en hora local de Guatemala. | fecha de los hechos de Silver | Generada como serie diaria desde min(fecha) hasta max(fecha). |
| `anio, mes, dia_del_mes` | Componentes de la fecha. | fecha | year/month/day. |
| `dia_semana_num` | Día de la semana, 0 = domingo … 6 = sábado. | fecha | dayofweek. |
| `dia_semana_nombre` | Nombre del día (en inglés, tal como lo devuelve DuckDB). | fecha | dayname. |
| `es_feriado, nombre_feriado` | Si la fecha está en el calendario de feriados y cuál es. | seed feriados | Cruce por fecha. |
| `es_dia_habil` | Lunes a viernes y no feriado. | fecha + seed feriados | dia_semana no en (0,6) y sin feriado. |

## `gold.dim_hora`

24 filas, una por hora local. Fuente: silver_* (volumen por hora).

| Campo | Significado | Fuente | Transformación |
|---|---|---|---|
| `hora_sk, hora` | Hora local 0–23. | generada | range(0,24). |
| `franja` | Etiqueta de conveniencia: madrugada, mañana, mediodía, tarde, noche. | derivada de la hora | Rangos fijos; NO es una clasificación de negocio. |
| `abordajes_totales_periodo` | Abordajes de los cuatro operadores en esa hora, todo el periodo. | silver_tm/tu/am/mr | Conteo por hora y suma. |
| `es_hora_pico` | true si el volumen de la hora ≥ umbral_hora_pico × promedio horario. | calculada del dato | Umbral configurable (var umbral_hora_pico = 1.5); no es un rango supuesto. |

## `gold.dim_estacion`

Estaciones y paradas de las cuatro redes. Fuente: silver_estacion.

| Campo | Significado | Fuente | Transformación |
|---|---|---|---|
| `estacion_sk` | Llave sustituta de la estación. | silver_estacion | md5(operador:id). |
| `operador` | Operador (transmetro, transurbano, aerometro, metroriel). | catálogos de Bronze | Nombre normalizado. |
| `estacion_id_original` | Identificador de la estación o parada en el catálogo del operador. | catálogos | Ninguna. |
| `nombre` | Nombre de la estación o parada. | catálogos | Ninguna. |
| `linea_o_ruta` | Línea (Transmetro), ruta (Transurbano) o eje (Aerómetro). | catálogos | Unificado en una columna. |
| `zona_id` | Zona conformada (1–25, municipios 101–113, −1 = sin mapear). | zona de cada catálogo | Macro clave_zona + cruce con dim_zona. |
| `lat, lon` | Coordenadas (solo Transmetro las trae). | tm_estaciones | Ninguna. |
| `km` | Kilometraje sobre el trazado (solo MetroRiel). | mr_estaciones | Ninguna. |

## `gold.dim_usuario`

Usuarios, solo con seudónimos. Fuente: silver_usuario.

| Campo | Significado | Fuente | Transformación |
|---|---|---|---|
| `usuario_sk` | Seudónimo de (operador, llave): SHA-256 con secreto, 20 hex. | llaves de los 4 operadores | macro pseudonimo. |
| `persona_id` | Seudónimo de la persona según la hipótesis de identidad numérica (S-09). | parte numérica de la llave | Si la hipótesis se apaga, es igual a usuario_sk. |
| `operador` | Operador de la llave. | silver_usuario | Ninguna. |
| `regla_identidad` | numerica_compartida o solo_operador. | silver_usuario | Según la variable unificar_identidad_numerica. |
| `en_padron` | true si la tarjeta de Transmetro aparece en el padrón. | silver_padron_vigente | Cruce por tarjeta. |
| `perfil_vigente, activa_vigente` | Perfil y estado de la versión vigente en el padrón (solo Transmetro). | CDC del padrón | SCD2 → versión vigente. |

## `gold.fact_abordaje`

Un evento de entrada por fila, de los cuatro operadores. Fuente: silver_tm_validaciones, silver_tu_transacciones, silver_am_boardings, silver_mr_viajes.

| Campo | Significado | Fuente | Transformación |
|---|---|---|---|
| `evento_sk` | Identificador estable del evento (hash de archivo y línea). | Silver | Heredado. |
| `transporte_sk, fecha_sk, hora_sk, estacion_sk, usuario_sk` | Llaves a las dimensiones. | Silver + dimensiones | Cruces por código/fecha/estación/seudónimo. |
| `tipo_evento` | Tipo de evento del operador (TM: ENTRADA/TRANSBORDO; TU: cod_estado sin interpretar; MR: ENTRADA). | tm.tipo, tu.cod_estado | Sin interpretar (no hay catálogo). |
| `abordajes` | Medida aditiva: siempre 1. | — | Constante por fila; se suma. |
| `monto_gtq` | Medida aditiva: tarifa en quetzales. | tm/am/mr tarifa, tu monto_centavos | Transurbano dividido entre 100 en Silver. |
| `_file_hash, _source_line_number` | Rastro hasta la línea del archivo crudo. | Bronze | Heredado. |

## `gold.fact_viaje`

Un trayecto completo por fila (hoy solo MetroRiel). Fuente: silver_mr_viajes.

| Campo | Significado | Fuente | Transformación |
|---|---|---|---|
| `evento_sk` | Identificador estable del viaje. | Silver | Heredado. |
| `transporte_sk, fecha_sk, hora_sk, usuario_sk` | Llaves a las dimensiones (hora = hora de entrada). | Silver | Cruces. |
| `estacion_sk_entrada, estacion_sk_salida` | Estación de entrada y de salida (dim_estacion como dimensión de rol). | estacion_entrada/salida | Cruce por operador e id. |
| `viajes` | Medida aditiva: siempre 1. | — | Constante. |
| `duracion_s` | Medida aditiva: segundos del viaje. | duration_s | Heredado; el promedio se calcula en la consulta. |
| `tarifa_gtq` | Medida aditiva: tarifa en quetzales. | fare_gtq | Heredado. |
| `distancia_km` | Medida aditiva con cobertura parcial: |km salida − km entrada|; NULL si falta algún km. | mr_estaciones.km | abs(diferencia). |
| `duracion_no_coincide` | Advertencia: duración distinta a salida − entrada. | regla de Silver | Heredado. |
| `_file_hash, _source_line_number` | Rastro hasta el archivo crudo. | Bronze | Heredado. |

## `gold.agg_demanda_zona_hora`

Demanda por día, hora, operador y zona (consumo de Tableau). Fuente: silver_evento_unificado.

| Campo | Significado | Fuente | Transformación |
|---|---|---|---|
| `fecha, hora` | Fecha y hora locales del abordaje. | ts_local | Truncado a fecha y hora. |
| `operador_cod` | TM, TU, AM o MR. | silver_evento_unificado | Ninguna. |
| `zona_id` | Zona de la estación de abordaje (−1 = sin mapear). | silver_estacion | Cruce por operador y estación. |
| `es_transbordo_operador` | true si el evento es un TRANSBORDO de Transmetro (no cuenta como viaje). | tm.tipo | upper(tipo) = 'TRANSBORDO'. |
| `abordajes` | Medida aditiva: eventos de entrada. | — | count(*). |
| `monto_gtq` | Medida aditiva: dinero cobrado. | tarifa/monto | sum(). |

## `gold.agg_cobertura_zona`

Una fila por zona o municipio: infraestructura y uso por operador. Fuente: dim_zona, silver_estacion, silver_evento_unificado.

| Campo | Significado | Fuente | Transformación |
|---|---|---|---|
| `zona_id, zona_nombre, tipo_zona` | Zona o municipio (zona_capital, municipio, desconocido). | seed dim_zona | Ninguna; incluye zonas sin estaciones. |
| `estaciones_tm, paradas_tu, estaciones_am, estaciones_mr, estaciones_total` | Estaciones o paradas por operador en la zona. | silver_estacion | count por operador. |
| `modos_con_servicio` | Cuántos de los 4 modos tienen al menos una estación en la zona (0–4). | silver_estacion | Suma de indicadores. |
| `abordajes_tm, abordajes_tu, abordajes_am, abordajes_mr, abordajes_total` | Abordajes en la zona por operador. | silver_evento_unificado | count por operador. |
| `sin_servicio` | true si no hay NINGUNA estación de ningún modo (cobertura de infraestructura). | derivada | estaciones_total = 0. |
| `sin_servicio_efectivo` | true si no hubo abordajes en el periodo (cobertura de uso). | derivada | abordajes_total = 0. |

## `gold.agg_usuarios_por_combinacion`

Personas por combinación de operadores que usan. Fuente: silver_evento_unificado.

| Campo | Significado | Fuente | Transformación |
|---|---|---|---|
| `n_modos` | Cuántos operadores distintos usa la persona. | persona_id | count(distinct operador). |
| `combinacion` | Operadores usados, p. ej. TM+TU. | persona_id | string_agg ordenado. |
| `personas` | Personas en esa combinación; se puede sumar entre filas (partición). | persona_id | count(*) por combinación. |
| `abordajes` | Abordajes de esas personas. | silver_evento_unificado | sum(). |

## `gold.agg_transbordo_temporal`

Transbordos inferidos: dos abordajes consecutivos de la misma persona en operadores distintos dentro de ventana_transbordo_min. Fuente: silver_evento_unificado.

| Campo | Significado | Fuente | Transformación |
|---|---|---|---|
| `de_operador, a_operador` | Operador previo y operador del abordaje. | lag() por persona ordenado por tiempo | Pares con operador distinto. |
| `hora` | Hora local del segundo abordaje. | ts_local | hour(). |
| `transbordos` | Número de transbordos inferidos. | — | count(*). |
| `personas` | Personas distintas; NO aditivo entre filas. | persona_id | count(distinct). |

## `gold.agg_caso_metroriel`

Una fila por zona: demanda de los otros modos frente a la presencia de MetroRiel. Fuente: dim_zona, silver_estacion, silver_evento_unificado.

| Campo | Significado | Fuente | Transformación |
|---|---|---|---|
| `zona_id, zona_nombre` | Zona (se excluye −1). | seed dim_zona | Ninguna. |
| `zona_en_caso` | true si la zona está en zonas_caso_metroriel (12, 8, 1, 6, 17). | variable de dbt | in (...). |
| `abordajes_otros_modos` | Abordajes de Transmetro, Transurbano y Aerómetro en la zona. | silver_evento_unificado | count excluyendo MR. |
| `abordajes_mr, estaciones_mr, tiene_estacion_mr` | Abordajes y estaciones de MetroRiel en la zona. | silver_evento_unificado, silver_estacion | count. |
| `ranking_demanda_otros_modos` | Posición de la zona por demanda de los otros modos (1 = mayor). | derivada | rank(). |
| `pct_demanda_otros_modos` | Proporción de la demanda total de los otros modos que cae en la zona. | derivada | zona / suma de zonas. |

## `gold.agg_od_metroriel`

Matriz origen-destino de MetroRiel por zona. Fuente: silver_mr_viajes, silver_estacion.

| Campo | Significado | Fuente | Transformación |
|---|---|---|---|
| `zona_entrada, zona_salida` | Zona de la estación de entrada y de la de salida. | silver_estacion | Cruce por estación. |
| `viajes, duracion_s, tarifa_gtq` | Medidas aditivas. | silver_mr_viajes | count / sum. |
