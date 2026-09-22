# Capa Silver — Diseño y decisiones

Agencia Metropolitana de Transporte · Proyecto 1 · Fases 1.2 (staging y CDC) y 1.3 (Silver)

## 1. Qué es Silver aquí

Silver lee **solo Bronze** (nunca Gold lee Bronze) y entrega datos limpios, conformados y con historia. Se construye con dbt sobre DuckDB:

```
Bronze (Parquet, inmutable)
   │  vistas de solo lectura (python -m bronze.cli vistas)
   ▼
staging.*   tipado y reglas de calidad por fila. Tablas que se reconstruyen completas en cada corrida ("el staging se vacía")
   │
   ├──► silver.silver_*          filas aceptadas, conformadas
   ├──► silver.silver_cuarentena filas rechazadas, con motivo y la fila original completa
   └──► silver.silver_dq_*       conteo de registros por regla de calidad
```

Cada modelo de Silver es una función pura de Bronze (no usa `current_timestamp`), así que reconstruirlo produce exactamente el mismo resultado. Eso es lo que hace idempotente a Silver (sección 9).

## 2. Cómo cubre la rúbrica (1.2 y 1.3)

| Requisito | Dónde |
|---|---|
| 1.2 Modelo CDC con conteo antes/después de aplicar los borrados | `silver_padron_scd2`, `silver_cdc_conteos` |
| 1.2 Catálogos de llaves distintas (Transurbano, MetroRiel, Aerómetro), solo la llave, con usuarios únicos | `silver_llaves_transurbano`, `silver_llaves_metroriel`, `silver_llaves_aerometro`, `silver_llaves_conteos` |
| 1.3 Unificar formatos: fechas, montos a quetzales, Aerómetro UTC → hora local | `stg_*` y `silver_*` (sección 4) |
| 1.3 Conformar la dimensión zona | seed `dim_zona`, `silver_estacion` (sección 6) |
| 1.3 Resolver la identidad del usuario | `silver_usuario` (sección 5) |
| 1.3 Reglas de calidad y cuarentena con motivo | `silver_cuarentena`, `reglas_calidad` (sección 7) |
| 1.3 Conteo de registros por regla | `silver_dq_conteo_por_regla` |
| 1.3 Historizar el padrón con SCD Tipo 2 | `silver_padron_scd2` (sección 8) |
| 1.5 Idempotencia con dos corridas | `python -m flows.evidencia` (sección 9) |

Las consultas que muestran cada entregable están en `sql/entregables_silver.sql`.

## 3. Modelos

| Capa | Modelo | Contenido |
|---|---|---|
| staging | `stg_estaciones` | Los cuatro catálogos en una sola forma (último snapshot de cada uno) |
| staging | `stg_cdc_padron` | Log CDC tipado, con reglas de rechazo |
| staging | `stg_tm_validaciones`, `stg_tu_transacciones`, `stg_am_boardings`, `stg_mr_viajes` | Todas las filas, tipadas, con la lista de reglas de rechazo y de advertencia que incumple cada una |
| silver | `silver_tm_validaciones`, `silver_tu_transacciones`, `silver_am_boardings`, `silver_mr_viajes` | Filas aceptadas |
| silver | `silver_estacion` | Dimensión conformada de estaciones y paradas de las cuatro redes, con `zona_id` |
| silver | `silver_padron_scd2`, `silver_padron_vigente` | Padrón de Transmetro con historia (SCD2) y su versión actual |
| silver | `silver_usuario` | Un usuario por (operador, llave), con `persona_id` |
| silver | `silver_llaves_*`, `silver_llaves_conteos` | Catálogos mínimos de llaves y sus conteos |
| silver | `silver_cuarentena`, `silver_dq_conteo_por_regla`, `silver_cdc_conteos` | Cuarentena, conteos por regla, conteos del CDC |
| seed | `dim_zona`, `reglas_calidad` | 25 zonas de la capital, 13 municipios y "sin mapear"; catálogo de las 34 reglas |

Cada fila de los hechos lleva `evento_sk` (hash de `_file_hash` + `_source_line_number`), que es estable entre corridas, y `usuario_sk`.

## 4. Unificación de formatos

| Aspecto | Regla |
|---|---|
| Fechas | Un solo estándar: `TIMESTAMP` local de Guatemala. Transmetro: `%Y-%m-%d %H:%M:%S`. Transurbano: `fecha` (`DD/MM/YYYY`) + `hora` → `%d/%m/%Y %H:%M:%S`. MetroRiel: ISO 8601 sin zona (`ts_entrada`, `ts_salida`), asumida local |
| Aerómetro | Se conserva `ts_utc` y se calcula `ts_local` con la zona `America/Guatemala` (UTC−6, sin horario de verano). La exploración 7.8 lo confirmó: en UTC crudo cubre las 24 horas y en hora local la ventana 04:00–22:59 igual que los otros tres |
| Montos | `DECIMAL(10,2)` en quetzales. Transurbano viene en centavos (`65`, `130`) y se divide entre 100 |
| Zona | `zona_id` conformado (sección 6) |
| Llaves | Se recortan espacios. La parte numérica se extrae solo para la identidad (sección 5) |
| `cod_estado` (Transurbano) | Se conserva sin interpretar: no hay catálogo. Es una pregunta abierta para el profesor |

## 5. Identidad del usuario

**Problema.** Cuatro llaves: `TC-00012345`, `0000012345`, `MR0012345` y un hash de 12 caracteres. Sin resolverla, no se puede saber si una persona usa más de un operador, que es la pregunta central de la Agencia (¿resuelve la red integrada el problema?).

**Evidencia disponible** (exploración 7.5):

1. El enunciado muestra la misma parte numérica (`12345`) en los tres primeros formatos.
2. Las partes numéricas de Transmetro, Transurbano, MetroRiel y el padrón ocupan el mismo rango (1 a unos 60,000).
3. El solapamiento entre operadores es el que darían conjuntos independientes: la razón observado/esperado es 0.997–1.001 entre Transmetro, Transurbano y MetroRiel, y ≈1.07 con el CDC. **Esto no distingue** entre "la misma persona usa varios operadores de forma independiente" y "los números se repiten por casualidad". No prueba identidad; tampoco la descarta.
4. El hash de Aerómetro es opaco (pendiente el resultado de la prueba con md5/sha1/sha256).

**Estrategia, en dos capas:**

1. **Certeza.** Cada `(operador, llave_original)` es un usuario propio (`usuario_sk`). Nunca se pierde información.
2. **Hipótesis controlada.** Para Transmetro, Transurbano y MetroRiel, la parte numérica es el id de la persona (`persona_id = 'P' + id numérico`, `regla_identidad = 'numerica_compartida'`). Aerómetro queda como usuario propio (`solo_operador`).

**Por qué esta estrategia.** Con la evidencia de 1 y 2, la hipótesis es razonable, y el formato del enunciado apunta a un diseño intencional. Al mantener las dos capas, si es falsa basta cambiar una variable de dbt (`unificar_identidad_numerica: false`) y `persona_id` vuelve a ser `usuario_sk`, sin tocar nada más.

**Riesgo.** Si es falsa, los transbordos entre operadores serían falsos positivos.

**Cómo confirmarla:**
- Lo definitivo: leer `generar_red_metropolitana.py` (cómo genera `tarjeta`, `num_tarjeta`, `card` y `user_hash`).
- Si el generador no lo aclara, una prueba de comportamiento: para ids presentes en dos operadores, contar cuántas veces un viaje de uno ocurre a menos de 30 minutos de un viaje del otro y compararlo con el mismo conteo tras desplazar los ids. **Solo puede confirmar, no descartar:** si el generador sortea el usuario de cada evento de forma independiente, aunque la población sea compartida no habrá correlación temporal y la razón saldrá cerca de 1 en cualquier caso. Un resultado alto confirma la identidad; uno cercano a 1 no la refuta, así que la fuente de verdad sigue siendo el generador.

## 6. Dimensión zona

Cada catálogo escribe la zona distinto: `Zona 10` (Transmetro, MetroRiel), `Z10` (sector de Transurbano), y en Aerómetro `Zona 12` o un municipio (`Mixco`, `Villa Nueva`, `San Miguel Petapa`). El padrón trae `zona_residencia`.

La macro `clave_zona` normaliza (`Z10`, `Zona 10` y `Zone 10` → `zona 10`; quita acentos y mayúsculas en municipios) y se une con `dim_zona` (25 zonas de la capital + 13 municipios). Lo que no se conforma queda con `zona_id = -1` y la prueba `assert_zonas_sin_mapear` lo lista como advertencia: no falla el proceso, pero muestra qué agregar al seed.

## 7. Reglas de calidad y cuarentena

Dos niveles: **rechazo** (la fila va a cuarentena) y **advertencia** (la fila se queda en Silver con una marca). Una fila puede incumplir varias reglas; la cuarentena registra el motivo principal (el primero por prioridad) y todos los incumplidos. Cada fila de cuarentena guarda la fila original completa (JSON) y su `_file_hash` y `_source_line_number` para volver a Bronze.

| Fuente | Rechazo | Advertencia |
|---|---|---|
| Transmetro | `duplicado_torniquete`, `fecha_futura`, `fecha_invalida`, `llave_formato_invalido`, `monto_invalido` | `posible_doble_lectura`, `estacion_sin_catalogo`, `tarjeta_sin_padron` |
| Transurbano | `cod_parada_nulo`, `fecha_futura`, `fecha_invalida`, `llave_formato_invalido`, `monto_invalido`, `duplicado_exacto` | `parada_sin_catalogo` |
| Aerómetro | `duplicado_boarding`, `fecha_futura`, `fecha_invalida`, `llave_formato_invalido`, `monto_invalido` | `estacion_sin_catalogo` |
| MetroRiel | `viaje_sin_salida`, `salida_antes_de_entrada`, `fecha_futura`, `fecha_invalida`, `llave_formato_invalido`, `monto_invalido`, `duplicado_viaje` | `duracion_no_coincide`, `estacion_sin_catalogo` |
| CDC del padrón | `llave_centinela` (SIN-TARJETA), `llave_formato_no_transmetro`, `operacion_desconocida`, `secuencia_invalida` | — |

- **Fecha del futuro:** fecha del evento posterior a la fecha de ingesta (UTC). No usa `current_date`, así que el resultado no cambia con el día que se corra.
- **Duplicado de torniquete:** mismo `validacion_id`; se conserva la primera lectura (menor línea de origen).
- **Doble lectura:** misma tarjeta y estación, distinto id, dentro de 60 s. Es una heurística que solo marca; no rechaza.

**Lo que ya se sabe del origen** (exploración): 817 filas de Transurbano con fecha 2027, 4,189 sin `cod_parada`, 3,589 viajes de MetroRiel sin salida y 1,115 filas duplicadas en Transmetro. El conteo definitivo lo da `silver_dq_conteo_por_regla`.

**Invariante:** por cada fuente, filas en Bronze = filas en Silver + filas en cuarentena. Lo verifica la prueba `assert_conciliacion_bronze_silver_cuarentena`.

### Cambio respecto a la recomendación anterior

Recomendé conservar con una marca las filas con `cod_parada` nulo y los viajes sin salida. Con el enunciado (1.3) a la vista, esas dos situaciones se listan entre los "registros malos" que van a cuarentena, así que ahora son **rechazo**. No se pierde nada: la fila original queda completa y recuperable, y no entra a los hechos ni a las duraciones.

## 8. Padrón de Transmetro con SCD Tipo 2

- Los eventos válidos se ordenan por `seq` (único y creciente; `commit_ts` tiene empates y filas fuera de orden).
- **INSERT y UPDATE** fijan los atributos (`perfil`, `zona_residencia`, `estado`).
- **DELETE** marca `activa = false` y conserva los últimos atributos conocidos. La tarjeta no se borra: se perdería el historial de sus viajes.
- Solo se abre una versión cuando algo cambia. Un INSERT repetido o un DELETE repetido no crean versiones.
- Una tarjeta con UPDATE y sin INSERT previo se acepta (`sin_insert_previo`). Una operación posterior a un DELETE la reactiva (`reactivacion`).
- Cada versión tiene `valido_desde_seq`, `valido_hasta_seq` (nulo en la vigente) y `es_vigente`. Exactamente una versión vigente por tarjeta.

**Conteo antes/después** (`silver_cdc_conteos`): "antes" son las tarjetas distintas con operaciones válidas, sin aplicar los DELETE; "después" son las activas y las dadas de baja. Se cumple `antes = activas + bajas`.

## 9. Idempotencia (1.5)

Staging y Silver se reconstruyen completos desde Bronze, y Bronze ya es idempotente (llave técnica). El script de evidencia corre el flujo completo dos veces, cuenta cada tabla de Bronze, staging y Silver, y calcula una **huella** (XOR de hashes de la llave de cada fila) para detectar filas cambiadas y no solo el conteo:

```bash
python -m flows.evidencia                    # con Kafka
python -m flows.evidencia --sin-streaming    # si el Bronze de streaming ya está cargado
```

Escribe `evidencia/idempotencia_<fecha>.md`. Ese es el entregable de las dos corridas; para la presentación se repite en vivo.

## 10. Pruebas

- **dbt (117):** una alarma sobre Bronze (`llave_tecnica_unica`, en las 9 fuentes: la misma línea de origen no puede estar dos veces; si falla, dbt omite los modelos que dependen de esa fuente y no se construye una Silver con datos repetidos), no nulos y unicidad de llaves, integridad de zona, y las de negocio: conciliación Bronze = Silver + cuarentena, una versión vigente por tarjeta, sin traslape de versiones, `antes = activas + bajas`, sin fechas futuras en Silver, suma de cuarentena = suma de conteos por regla, zonas sin mapear (advertencia), horas fuera de 04–22 (advertencia).
- **pytest (17 de Silver + 38 de Bronze):** sobre datos sintéticos con anomalías conocidas; comparan los conteos por regla contra los CSV de origen, verifican el SCD2 contra una implementación independiente en Python, la conversión horaria, la identidad y la idempotencia.

## 11. Limitaciones y pendientes

- **Datos reales sin correr.** Todo se probó con datos sintéticos con los mismos encabezados, incluido un volumen igual al real (Silver: unos 25 s). Los datos reales pueden mostrar patrones nuevos (formato del hash de Aerómetro, valores de `zona_residencia` o de `estado` en el CDC).
- **Llaves con formato MR en el padrón.** Hoy van a cuarentena (`llave_formato_no_transmetro`). Hay indicios de que no son tarjetas de Transmetro con formato sucio: en la exploración, el solapamiento numérico entre el padrón y las validaciones (17,430) es idéntico al solapamiento por llave exacta `TC-` (17,430), y el padrón tiene 22,462 partes numéricas para 22,462 llaves que no son `SIN-TARJETA`. Es decir, los números de las llaves `MR` no coinciden con ninguna tarjeta que viaje en Transmetro, así que normalizarlas a `TC-` no añadiría coincidencias. Es una inferencia sobre conteos agregados; la consulta de diagnóstico de `sql/entregables_silver.sql` la comprueba directamente.
- **Validaciones sin padrón.** Se conservan con la advertencia `tarjeta_sin_padron`: no se inventan datos del padrón.
- **Malformadas de Bronze** (0 en estos datos) viven en `bronze/_malformed` y no se replican en la cuarentena de Silver.
- **`cod_estado`** sin interpretar. **Gold** (1.4: grano, matriz del bus, hechos, dimensiones y DDL) es el siguiente paso.
