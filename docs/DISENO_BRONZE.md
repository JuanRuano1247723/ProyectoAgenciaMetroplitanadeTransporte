# Capa Bronze — Diseño y decisiones

Agencia Metropolitana de Transporte · Proyecto 1 · Fase 1.1 (Ingesta y capa Bronze)

## 1. Resumen

Bronze conserva cada archivo de origen **tal como llegó**, con marca de tiempo de ingesta y particionado por fecha, en un
lake de carpetas + Parquet. Recibe las nueve fuentes por la vía que les corresponde:

| Vía | Fuentes | Mecanismo |
|---|---|---|
| Streaming (Kafka) | `transmetro_validaciones`, `aerometro_boardings` | productor Python que publica el CSV línea por línea; consumidor que escribe micro-lotes a Parquet |
| Batch | `tm_estaciones`, `tu_paradas`, `mr_estaciones`, `am_estaciones`, `metroriel_viajes`, `transurbano_transacciones` | lectura del archivo completo, un hash por archivo |
| CDC | `cdc_padron_usuarios` | el log de cambios se guarda completo, sin aplicar operaciones |

Cada línea física de cada archivo termina en **una** de dos tablas: la tabla Bronze o la tabla de malformadas. Nada se descarta.

## 2. Hallazgos de la exploración que gobiernan el diseño

| Hallazgo (notebook 01) | Decisión de Bronze |
|---|---|
| `num_tarjeta` de Transurbano tiene ceros a la izquierda (`0000025494`); pandas lo infiere como entero | Todo dato es `STRING`; nunca se infieren tipos |
| Transurbano trae `fecha` como `DD/MM/YYYY` y `hora` en otra columna; 817 filas caen en 2027-07/08, fuera del periodo (2026-06-01 → 2026-07-15) | Se guardan tal cual, separadas. La partición reordena el texto (`DD/MM/YYYY` → `YYYY-MM-DD`) sin parsear. Las fechas anómalas quedan visibles como particiones 2027 |
| Aerómetro trae `Z` (UTC) en el 100 % de las filas; 04:00–22:59 hora local = 10:00–04:59 UTC | El string UTC se guarda intacto; la conversión es de Silver |
| MetroRiel: 299,100 viajes, esquema estable, profundidad 1, sin listas; `exit` es `null` en 3,589 (1.20 %) junto con `duration_s` | `entry` y `exit` como `STRUCT`; `exit` puede ser `NULL`; se guarda además `_raw_json` |
| CDC: `seq` único y creciente en el archivo; `commit_ts` tiene 6,383 duplicados, 15,479 filas fuera de orden y 33 empates dentro de una misma tarjeta | Se conservan ambas columnas intactas. El orden de aplicación (`seq`) se decide en Silver |
| CDC: `tarjeta` incluye `SIN-TARJETA` y llaves con formato `MR#######`; los DELETE traen perfil, zona y estado vacíos | Se conservan tal cual; las cadenas vacías se guardan como `NULL` |
| Transmetro trae 1,115 filas duplicadas exactas (mismo `validacion_id` y mismo contenido) | Bronze las conserva; Silver deduplica |
| Transurbano no tiene id de evento | La única llave de unicidad en Bronze es la técnica (`_file_hash` + `_source_line_number`) |
| Ninguna fuente operativa trae zona; vive en los catálogos | Los catálogos entran como snapshots versionados |
| Archivos: 0 líneas con distinto número de campos, 0 JSON inválidos, UTF-8 sin BOM | La ruta de malformadas existe y está probada, pero con estos datos se espera vacía |

## 3. Arquitectura y estructura de carpetas

```
lake/
├── landing/                                   copia cruda de cada archivo (verificada por hash)
│   └── <operador>/<entidad>/ingest_date=YYYY-MM-DD/<hash16>__<archivo>
└── bronze/
    ├── <operador>/<entidad>/_partition_date=YYYY-MM-DD/part-*.parquet
    ├── _malformed/<operador>/<entidad>/part-*.parquet
    └── _control/
        ├── ingest_log/            bitácora: un Parquet por evento (solo append)
        ├── kafka_commits/         marcadores de micro-lotes confirmados, por topic y partición
        └── producer_checkpoints/  última ráfaga confirmada del productor
```

Tablas: `transmetro/{estaciones, validaciones, padron_cdc}`, `transurbano/{paradas, transacciones}`,
`metroriel/{estaciones, viajes}`, `aerometro/{estaciones, boardings}`.

## 4. Decisiones

### D1 · Bronze vive en un lake (carpetas + Parquet); el warehouse queda para Silver/Gold

- **Decisión.** Carpetas particionadas con Parquet (zstd). DuckDB lee los archivos en el lugar mediante vistas (`bronze_vistas.duckdb`).
- **Por qué.** (1) MetroRiel es JSON anidado: Parquet lo conserva como `STRUCT` sin aplanar, y `_raw_json` guarda el original.
  (2) Almacenamiento y cómputo quedan desacoplados: se puede reprocesar Bronze completo sin tocar el origen. (3) Los archivos son inmutables y
  reproducibles desde `landing/`. (4) Un warehouse también podría guardar STRUCT; la razón decisiva no es que no pueda, sino la inmutabilidad y el reprocesamiento barato.
- **Consecuencia.** No hay `MERGE`/`UPDATE` sobre Parquet plano. Es deliberado: Bronze es solo *append* y la idempotencia se resuelve como en D6.
  Un formato de tabla (Delta/Iceberg) aportaría transacciones y compactación, pero a este volumen no compensa la complejidad.

### D2 · Zona landing: copia cruda de cada archivo

Cada archivo se copia con su hash (`<hash16>__<nombre>`) y se verifica la copia. Es la garantía de reproducibilidad: Bronze puede reconstruirse desde landing.

### D3 · Política de tipos

- Todas las columnas de datos son `STRING` tal como llegaron (sin convertir centavos, zonas, fechas, llaves ni UTC).
- Excepción por estructura: `entry` y `exit` de MetroRiel son `STRUCT(station STRING, ts STRING)`, con hojas de texto.
  Los números JSON conservan su texto exacto (`12.50` no se convierte en `12.5`).
- Los metadatos sí tienen tipo (`_ingested_at` timestamp, `_source_line_number` entero, `_kafka_offset` entero).
- Un campo vacío de CSV se guarda como `NULL`, porque el CSV no distingue vacío de nulo.

### D4 · Encabezados

Se normalizan a minúsculas y sin espacios en los extremos. El encabezado original queda en la bitácora (`encabezado_original`). Si tras normalizar difiere del esperado
(en nombres u orden), la carga **falla** en lugar de guardar datos desalineados. El encabezado nunca entra como dato: el productor lo omite y el lector lo consume aparte.

### D5 · Partición por fecha

- **Operativos** (`_partition_date` = fecha del evento). Es la primera parte del texto original (`YYYY-MM-DD`; para Transurbano se reordena `DD/MM/YYYY`).
  Para Aerómetro es la fecha **UTC**, que desde las 18:00 hora local va un día adelante. No importa: la partición es organización física y Silver calcula la fecha local real.
- **Catálogos y CDC** (fecha de ingesta). Son snapshots o logs cuyo contenido no es un evento con fecha propia.
- **Respaldo.** Si una fila no permite extraer la fecha, cae a la fecha de ingesta y se cuenta en la bitácora (`filas_particion_por_defecto`).
- **Por qué no solo fecha de ingesta.** Los datos se generan de una sola vez: darían una única partición inútil para podar consultas.
- La hora nunca se trunca: el string original completo (con hora) está en su columna.

### D6 · Inmutabilidad e idempotencia: llave técnica, no upsert por llave de negocio

Un `merge`/`upsert` por llave de negocio en Bronze **destruiría datos que deben conservarse**: colapsaría el log CDC (cada INSERT/UPDATE/DELETE de una tarjeta es un hecho)
y fusionaría en silencio los 1,115 duplicados de Transmetro. El objetivo, que no haya duplicación *por la carga*, se cumple con una llave técnica:

| Vía | Unicidad garantizada por | Cómo |
|---|---|---|
| Batch / CDC | `_file_hash` + `_source_line_number` | el hash del archivo ya registrado como OK se omite; los archivos Parquet tienen nombre determinista (`part-<hash16>-<n>`), así que un reintento sobrescribe el mismo archivo |
| Streaming | `_kafka_topic` + `_kafka_partition` + `_kafka_offset` | el consumidor retoma en (último offset confirmado + 1); cada micro-lote se escribe **antes** de su marcador de confirmación |

Los duplicados que ya vienen en el origen se conservan y se resuelven en Silver.
El `upsert` por llave de negocio (padrón vigente, deduplicación de eventos) va en Silver.

### D7 · Registros malformados

Una línea que no se puede interpretar (número de campos distinto, JSON inválido, línea vacía, bytes no UTF-8) se guarda en `bronze/_malformed/` con `_raw_line`, `_motivo`
y su trazabilidad. La cuarentena con motivo de negocio ocurre en Silver.

### D8 · Metadatos

Todas las tablas: `_ingested_at`, `_batch_id`, `_source_file`, `_file_hash`, `_source_line_number`.
MetroRiel: `_raw_json`. Streaming: `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp`.
En streaming, `_source_file`, `_file_hash` y `_source_line_number` viajan como *headers* del mensaje, de modo que el rastro hasta la línea del archivo original se conserva.

### D9 · Transurbano entra por batch (justificación)

Transurbano se ingiere por **batch**. El análisis de la Agencia es retrospectivo (estudio de demanda, horas pico) y ningún consumidor necesita las transacciones en segundos,
así que un topic y un consumidor permanente añadirían operación sin beneficio. El hecho de que el flujo no sea continuo no basta como argumento (Transmetro también se interrumpe de noche y entra por streaming);
lo decisivo es la **ausencia de requisito de latencia**.

**Consecuencias:**
1. La latencia de los datos es igual a la frecuencia de carga.
2. Las transacciones tardías entran en la siguiente corrida.
3. La idempotencia se controla con el hash del archivo, no con offsets. Como la fuente no trae id de evento, la unicidad en Bronze es `_file_hash` + `_source_line_number`.
4. Se opera un topic y un consumidor menos.
5. Si un día se necesitara baja latencia, habría que cambiar de vía y aceptar más operación.

### D10 · Streaming: simulación, micro-lotes y recuperación

- **Simulación.** El productor publica la línea cruda de cada fila, con la llave `tarjeta` (Transmetro) o `user_hash` (Aerómetro) como llave del mensaje.
  Parámetros: `--tamano-rafaga` (500), `--pausa` (2 s), `--max-filas`. Con los valores por defecto, Transmetro tarda unos 24 minutos y Aerómetro unos 14; para trabajar rápido, `--pausa 0`.
- **Micro-lotes.** N filas **o** T segundos, lo que ocurra primero (`MICROLOTE_FILAS`, `MICROLOTE_SEGUNDOS`). La cola final siempre se escribe.
- **Recuperación.** Si el proceso muere entre escribir el Parquet y su marcador, al reiniciar se revierten los archivos sin marcador (nunca confirmados) y se repite el micro-lote.
  El commit de offsets en Kafka es informativo.
- **Garantía.** Consumidor: *exactly-once* hacia Bronze. Productor: *at-least-once*; si se cae a media ráfaga puede repetir como máximo una ráfaga (limitación conocida; los repetidos son detectables por `_file_hash` + `_source_line_number`).
- **Archivos pequeños.** Cada micro-lote genera un archivo por (partición Kafka × fecha). A este volumen es aceptable; una tarea de compactación futura no alteraría los datos.

### D11 · CDC

El archivo `cdc_padron_usuarios.csv` se guarda completo: una fila por operación, con `seq`, `commit_ts` y `op` intactos, y los DELETE sin cuerpo (`NULL`). Aplicar las operaciones para construir el padrón vigente
(y contar tarjetas activas y dadas de baja) es el trabajo de la fase 1.2 en Silver/staging.

### D12 · Bitácora y conciliación

Cada evento (archivo cargado, omitido, fallido, ráfaga publicada, micro-lote consumido) deja una fila en `_control/ingest_log`. La conciliación **no repite** el conteo de la carga:
cuenta las líneas físicas del archivo de origen y las filas de Bronze y malformadas leyendo los Parquet, filtradas por el hash del archivo actual. `origen = Bronze + malformadas` o el estado es
`DESCUADRE` (o `NO_INGERIDO`).

### D13 · Orquestación

Un flujo de Prefect (`flows/bronze_flow.py`): catálogos, Transurbano, MetroRiel y CDC en paralelo; crear topics → publicar → consumir por cada fuente streaming; conciliación al final.
Dos reintentos por tarea (seguros por D6). El flujo completo es idempotente.

## 5. Esquemas de las tablas Bronze

Columnas de datos: todas `STRING`, tal como llegaron. La columna `_partition_date` viene de la ruta (DuckDB la expone como `DATE`).

| Tabla | Columnas de datos | Metadatos |
|---|---|---|
| `transmetro/validaciones` | `validacion_id, tarjeta, estacion_id, linea, fecha_hora, tarifa, tipo` | base + Kafka |
| `aerometro/boardings` | `boarding_id, user_hash, station_code, axis, timestamp_utc, cabin_number, fare` | base + Kafka |
| `transurbano/transacciones` | `fecha, hora, num_tarjeta, cod_parada, ruta, monto_centavos, cod_estado` | base |
| `metroriel/viajes` | `trip_id, card, entry STRUCT(station, ts), exit STRUCT(station, ts), fare_gtq, duration_s` | base + `_raw_json` |
| `transmetro/padron_cdc` | `seq, commit_ts, op, tarjeta, perfil, zona_residencia, estado` | base |
| `transmetro/estaciones` | `estacion_id, nombre, linea, zona, lat, lon` | base |
| `transurbano/paradas` | `cod_parada, descripcion, ruta, sector` | base |
| `metroriel/estaciones` | `id_estacion, nombre_estacion, zona_nombre, km` | base |
| `aerometro/estaciones` | `station_code, station_name, axis, district` | base |

Metadatos base: `_ingested_at` (timestamp UTC), `_batch_id`, `_source_file`, `_file_hash` (`STRING`), `_source_line_number` (`BIGINT`).
Kafka: `_kafka_topic`, `_kafka_partition` (`INT`), `_kafka_offset` (`BIGINT`), `_kafka_timestamp`.
Malformadas: `_raw_line`, `_motivo` + trazabilidad.

## 6. Conteos por archivo (entregable)

Se genera con `python -m bronze.cli conciliar --csv salidas/conteos_bronze.csv`. Conteos de origen tomados del inventario del notebook (sección 1);
como esa revisión no encontró líneas con distinto número de campos ni JSON inválidos, lo esperado es `filas_bronze = filas_origen` y `filas_malformadas = 0`.

| Archivo | Vía | Filas de origen | Bronze (esperado) | Malformadas (esperado) |
|---|---|---:|---:|---:|
| `tm_estaciones.csv` | batch | 104 | 104 | 0 |
| `tu_paradas.csv` | batch | 328 | 328 | 0 |
| `mr_estaciones.csv` | batch | 22 | 22 | 0 |
| `am_estaciones.csv` | batch | 14 | 14 | 0 |
| `transmetro_validaciones.csv` | streaming | 363,221 | 363,221 | 0 |
| `aerometro_boardings.csv` | streaming | 203,554 | 203,554 | 0 |
| `transurbano_transacciones.csv` | batch | 832,791 | 832,791 | 0 |
| `metroriel_viajes.jsonl` | batch | 299,100 | 299,100 | 0 |
| `cdc_padron_usuarios.csv` | CDC | 31,050 | 31,050 | 0 |

Las columnas "Bronze" y "Malformadas" reales salen de la conciliación al correr la carga con los datos del profesor.

## 7. Lo que Bronze NO hace

Conversión de centavos a quetzales, normalización de zonas, conversión UTC → hora local, unión de `fecha` y `hora` de Transurbano, unificación de llaves de usuario, deduplicación,
aplicación de operaciones CDC, cuarentena con motivo de negocio y catálogos de llaves distintas. Todo eso es de Silver/staging. Gold nunca lee Bronze.

## 8. Traspaso a Silver: lo que hay que resolver allí

1. **Orden del CDC:** ordenar por `seq` (único, creciente, sin empates dentro de una tarjeta). `commit_ts` no sirve como orden único.
2. **Llave `tarjeta` del CDC:** `SIN-TARJETA` no es una llave (colapsaría a todos los usuarios sin tarjeta) y hay llaves con formato `MR#######` en el padrón de Transmetro. Decidir su tratamiento y cuarentena.
3. **Anomalías de secuencia CDC** (según la exploración): 14,617 tarjetas sin INSERT inicial, 655 con operación posterior a un DELETE, 828 con INSERT repetido, 141 con más de un DELETE.
4. **Padrón vs. validaciones:** solo el 40.3 % de las tarjetas de Transmetro aparece en el CDC (25,825 tarjetas sin padrón, 216,565 validaciones). Esas filas no se descartan.
5. **Bajas:** 19,526 validaciones son de tarjetas dadas de baja; marcar inactiva, no borrar.
6. **Transurbano:** parsear `fecha` con `%d/%m/%Y`; cuarentena para las 817 filas fuera del periodo; `cod_parada` nulo en 4,189 filas; `cod_estado` sin catálogo; dinero en centavos.
7. **MetroRiel:** 3,589 viajes sin salida; `entry.ts`/`exit.ts` sin zona horaria (se asumen locales, coherente con Transmetro).
8. **Aerómetro:** UTC → hora local (UTC−6) para el análisis de horas pico.
9. **Duplicados exactos de Transmetro:** 1,115 filas; deduplicar por `validacion_id`.
10. **Zonas:** cada catálogo escribe la zona distinto (`Zona 10`, `Z10`, `Mixco`); normalizar en Silver.

## 9. Cómo verificar y correr

```bash
pip install -r requirements.txt
pytest                                   # 34 pruebas, sin Kafka
python -m bronze.cli batch && python -m bronze.cli cdc
docker compose up -d && python -m bronze.cli topics
python -m bronze.cli produce ambos --pausa 0     # o con la pausa de 2 s por ráfaga
python -m bronze.cli consume ambos
python -m bronze.cli conciliar --csv salidas/conteos_bronze.csv
python -m bronze.cli vistas
duckdb lake/bronze_vistas.duckdb < sql/verificacion_post_bronze.sql
python -m flows.bronze_flow              # todo con Prefect
```

## 10. Limitaciones conocidas

- **Kafka real sin probar.** La lógica de productor y consumidor se probó con un doble en memoria (incluye caídas simuladas); `docker-compose.yml` y las llamadas a `confluent-kafka` están escritas pero no se corrieron contra un broker.
- **Datos reales sin correr.** Las pruebas usan datos sintéticos con los mismos encabezados, formatos y anomalías, incluido un volumen igual al real (unos 25 s en total, con el broker simulado).
- **Productor at-least-once** (D10).
- **Archivos pequeños en streaming** (D10).
- **dbt** (`sources` de Bronze con pruebas) se declara al armar el proyecto dbt de Silver.
