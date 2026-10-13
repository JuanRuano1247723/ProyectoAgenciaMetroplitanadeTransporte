# Gold: grano, matriz del bus, dimensiones y DDL

Estado: **implementado**. Modelos en `dbt_metro/models/gold/`, DDL documentado en `dbt_metro/DDL_GOLD.sql`, verificado con `python -m flows.dbt_runner build` y `python -m flows.verificar_linaje`. Cualquier cambio a este diseño se anota en `docs/BITACORA_DECISIONES.md`. El enunciado pide empezar por la matriz del bus antes de escribir una tabla; por eso este documento existe desde antes del primer modelo.

## 1. Decisión de grano

| Opción | Grano | A favor | En contra |
|---|---|---|---|
| A | Un abordaje, para los cuatro operadores | Simple y comparable | MetroRiel pierde la salida y la duración |
| B | Un viaje puerta a puerta | Responde "¿cuánto viaja una persona?" | Hay que **inferir** el viaje en tres de cuatro sistemas (no traen salida) y encadenar usuarios entre operadores, que hoy es solo una hipótesis |
| **C (implementada)** | **Dos hechos: `fact_abordaje` y `fact_viaje`, ambos con `dim_transporte`** | Usa toda la información de MetroRiel sin inferir nada en los otros tres; el esquema queda listo para un viaje multimodal inferido | Dos tablas que no se deben sumar entre sí |

**Granos, en una frase cada uno:**

- `fact_abordaje`: *una fila por abordaje: cada validación de Transmetro, cada transacción de Transurbano, cada abordaje de Aerómetro y la entrada de cada viaje completo de MetroRiel.*
- `fact_viaje_metroriel`: *una fila por viaje completo de MetroRiel, de su estación de entrada a su estación de salida.*

**Por qué C.** El abordaje es la única unidad que los cuatro sistemas registran, y es la que permite comparar operadores. El viaje puerta a puerta exigiría suponer la identidad entre operadores
y las estaciones de destino, que los datos no traen. Y MetroRiel, que sí entrega el trayecto, tiene su propio hecho donde la duración y la salida existen de verdad.

**Lo que hay que defender.**
- La entrada de un viaje de MetroRiel aparece en los dos hechos. **Cada medida vive en un solo hecho**: el monto en `fact_abordaje` (en la fila de entrada de MetroRiel), la duración y la estación de salida en `fact_viaje`. Nunca se suman entre hechos.
- Los 3,589 viajes de MetroRiel sin salida están en cuarentena (regla `viaje_sin_salida`), por lo que su entrada tampoco está en `fact_abordaje`. Subestima levemente las entradas de MetroRiel (1.2 %); se puede recuperar desde la cuarentena si el equipo lo decide.
- Se agregó `dim_transporte` (conformada, referenciada por los dos hechos) en lugar de una columna literal repetida por operador: evita el string mágico en cada `UNION ALL` y guarda como atributo del dato, no como comentario del código, cuál transporte registra trayecto completo.

## 2. Matriz del bus

`dim_transporte` reemplaza a la fila "operador" que manejábamos como columna literal: es una dimensión conformada de bajo cardinal (Transmetro, Transurbano, Aerómetro, MetroRiel) que evita repetir strings mágicos en cada `UNION ALL` y deja documentado, como atributo del dato y no como comentario del código, cuál operador registra trayecto completo y cuáles registran abordaje.

| Proceso (hecho) | dim_fecha | dim_hora | dim_transporte | dim_estacion | dim_zona | dim_usuario |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| `fact_abordaje` | ✓ | ✓ | ✓ | ✓ | ✓ (vía estación) | ✓ |
| `fact_viaje` | ✓ | ✓ (entrada) | ✓ | ✓ **dos veces**: entrada y salida | ✓ dos veces | ✓ |

`fact_viaje` tiene grano genérico ("un trayecto completo, para cualquier medio capaz de registrarlo"), no "un viaje de MetroRiel": hoy solo MetroRiel puebla la tabla porque es el único operador que trae salida real, pero el esquema queda listo para un futuro viaje multimodal inferido (el extra opcional de análisis de transbordo) sin rediseñar nada. Esto es intencional y se documenta en el diccionario de datos, para que no se lea como una tabla mal cargada.

## 3. Dimensiones conformadas (de dónde salen en Silver)

| Dimensión | Fuente en Silver | Notas |
|---|---|---|
| `dim_fecha` | `fecha` de los hechos (hora local) | Incluye día de la semana y `es_dia_habil`. Los feriados van en una semilla con dueño (verificar el calendario, p. ej. el 30 de junio) |
| `dim_hora` | `hora` (0–23) | `franja` y `es_hora_pico` se **derivan del dato**, no se suponen. Con el perfil observado (picos en las horas 7 y 17) y un umbral de 1.5 veces el promedio horario, salen esas dos; confirmarlo con el perfil completo antes de fijar el umbral en el modelo |
| `dim_transporte` | Semilla (4 filas fijas) | `operador_cod`, `nombre`, `modo`, `registra_trayecto_completo` (solo MetroRiel en `true` hoy) |
| `dim_estacion` | `silver_estacion` | Une estaciones y paradas de las cuatro redes, con `zona_id`, `lat`, `lon` y `km`. Se referencia dos veces en `fact_viaje` (entrada y salida) |
| `dim_zona` | Semilla `dim_zona` | Zonas 1–25 y municipios; la zona de una estación sale de `silver_estacion` |
| `dim_usuario` | `silver_usuario` **solo con seudónimos** | `usuario_sk`, `persona_id`, `perfil_vigente`, `en_padron`. Miembro **"Desconocido"** para las tarjetas sin padrón (el 59.6 % de las validaciones de Transmetro) |

`cod_estado` (Transurbano) y `tipo` (Transmetro) quedan como atributos degenerados directamente en `fact_abordaje` (no ameritan una dimensión propia todavía): `cod_estado` no tiene catálogo confirmado y `tipo` tiene solo 2-3 valores conocidos.

## 4. Medidas

Regla general: nunca se guarda una razón, un porcentaje ni un promedio ya calculado. Se guardan siempre el numerador y el denominador, ambos aditivos, y la razón se calcula en la consulta.

### `fact_abordaje`

| Medida | Origen | Clase |
|---|---|---|
| `abordajes` | `count(*)` | **Aditiva** |
| `monto_gtq` | `tarifa_gtq` / `monto_gtq` según operador | **Aditiva** |

### `fact_viaje`

| Medida | Origen | Clase |
|---|---|---|
| `viajes` | `count(*)` | **Aditiva** |
| `duracion_s` | `duracion_s` (MetroRiel) | **Aditiva** — se suma para dar minutos totales; el promedio se calcula en consulta (`sum(duracion_s)/sum(viajes)`), nunca se guarda |
| `tarifa_gtq` | `tarifa_gtq` | **Aditiva** |
| `distancia_km` | `abs(km_salida − km_entrada)` desde `silver_estacion` | **Aditiva, con límite de cobertura** — solo `mr_estaciones` trae `km`; para cualquier operador sin `km` en su catálogo, la medida sale `NULL`. Es aditiva por definición, pero no cubre hoy todo el tráfico; se declara en el diccionario de datos |

### No aditivas (nunca se guarda la razón; se guardan numerador y denominador)

| Razón que se necesita | Numerador (aditivo, en la tabla) | Denominador (aditivo, en la tabla) |
|---|---|---|
| Tarifa promedio por abordaje | `monto_gtq` | `abordajes` |
| Duración promedio de viaje | `duracion_s` | `viajes` |
| Tarifa promedio por viaje | `tarifa_gtq` | `viajes` |
| Velocidad promedio (km/h) | `distancia_km` | `duracion_s / 3600` |
| Usuarios distintos | — | — (ver nota abajo) |

`usuarios_distintos` (`count(distinct usuario_sk)`) es no aditivo por una razón distinta a una razón numerador/denominador: sumarlo entre zonas o fechas cuenta dos veces a la misma persona si aparece en ambas. No se guarda precalculado en ningún nivel de agregación; se calcula siempre contra el grano atómico de `fact_abordaje` o `fact_viaje`, a la granularidad que pida cada consulta.

### Semi-aditiva (no aplica a estos dos hechos; candidato si se agrega un tercero)

`fact_abordaje` y `fact_viaje` son ambos hechos de **flujo** (eventos que ya ocurrieron): en un hecho de flujo puro, sumar por tiempo siempre es válido, así que no aparece naturalmente una medida "aditiva por todo menos por tiempo". Forzar una aquí sería incorrecto. El candidato real, si el equipo decide agregarlo como tercer hecho opcional, es un snapshot del padrón:

**`fact_padron_diario`** (opcional) — una fila por tarjeta activa al cierre de cada día. Medida `tarjetas_activas` = `count(*)`: se puede sumar por zona de residencia o perfil dentro de un mismo día, pero sumarla a través de varios días cuenta la misma tarjeta activa repetidas veces. El agregado correcto entre fechas es un promedio o el último valor, nunca una suma.

## 5. Reglas para quien construya Gold (implementadas y verificadas)

1. **Gold solo lee Silver** (`ref('silver_…')`) o a otro modelo Gold, nunca `source()` ni staging. Un Gold que lee Bronze pierde la totalidad de 1.4. Lo verifica `python -m flows.verificar_linaje` (0 violaciones sobre los 7 modelos reales).
2. **Solo seudónimos**: `usuario_sk` y `persona_id`, expuestos por `gold/dim_usuario.sql`. Los hechos de Silver traen también `tarjeta`, `num_tarjeta` y `user_hash` (zona restringida): **no se seleccionan en Gold**. `flows/verificar_linaje.py` también revisa nombres y valores de columna para detectar una llave cruda filtrada por error.
3. **Trazabilidad al dato crudo**: cada fila de hecho conserva `evento_sk`, `_file_hash` y `_source_line_number`. Sin eso, el tablero pierde la mitad de 2.1.
4. **Fecha y hora siempre desde `fecha`/`hora` de Silver** (ya en hora local). Aerómetro está particionado por fecha UTC en Bronze: usar la partición desplazaría las noches al día siguiente.
5. El bloque `gold:` de `dbt_project.yml` ya está activo (`+schema: gold`).
6. Gold se reconstruye completo en cada corrida, igual que Silver: es una función pura de Silver, así que la idempotencia (1.5) se demuestra igual, construyendo dos veces y comparando conteos y huellas (`flows/dbt_runner.py::contar_capas`, que ahora incluye la capa `gold`).

## 6. La prueba de fuego de gobernanza: ¿por qué dos personas obtendrían números distintos de "viajes del mes"?

Cada punto es una definición que el equipo debe fijar y asignar a un dueño:

1. **Con o sin cuarentena.** Con las filas crudas, Transmetro tiene 1,115 lecturas duplicadas más. Regla propuesta: solo filas de Silver.
2. **¿Un transbordo es otro viaje?** Transmetro trae `tipo` (`ENTRADA` y `TRANSBORDO` aparecen en los datos; confirmar el dominio completo). Contar los transbordos infla los viajes.
3. **¿Abordaje o viaje?** Un viaje de MetroRiel es un viaje; un abordaje de otro operador no necesariamente lo es. Hay que nombrar dos medidas distintas (`abordajes` y `viajes`) y definir cuál es "viajes del mes" oficial.
4. **¿Qué mes?** Debe calcularse con hora local. Por fecha UTC, las noches de Aerómetro caerían en el mes siguiente en las fronteras.
5. **Meses completos.** Los datos van del 1 de junio al 15 de julio de 2026: **solo junio está completo**.

Un cálculo distinto en cualquiera de los cinco puntos da otro número. Ese es el contenido del "documento de definiciones oficiales con dueño" que pide 3.1.
