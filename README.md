# Agencia Metropolitana de Transporte · De Bronze a Gold

Pipeline de datos de la red metropolitana (Transmetro, Transurbano, Aerómetro y MetroRiel), con arquitectura medallón:

```
 Archivos fuente ──► BRONZE ──────────► SILVER ───────────► GOLD ───────────► Tableau
 (9 archivos)        lake Parquet       dbt-duckdb          dimensional +      tablero y
                     batch/Kafka/CDC    limpieza, SCD2,     agregados +        recomendación
                                        cuarentena,         features
                                        identidad
```

| Capa | Qué contiene | Quién la construye | Dónde queda |
|------|--------------|--------------------|-------------|
| Bronze | El dato tal como llegó, más columnas técnicas (`_file_hash`, `_source_line_number`, `_batch_id`, `_ingested_at`, `_source_file`) | `python -m bronze.cli ...` | `lake/` (Parquet) y `lake/bronze_vistas.duckdb` |
| Staging y Silver | Tipos, zona horaria local, deduplicación, cuarentena con motivo, padrón SCD2, usuario seudonimizado, `silver_evento_unificado` | dbt (`flows.dbt_runner`) | `lake/warehouse.duckdb`, esquemas `staging` y `silver` |
| Gold | Hechos (`fact_abordaje`, `fact_viaje`), dimensiones, agregados `agg_*` para Tableau y `features.features_usuario` | dbt | `lake/warehouse.duckdb`, esquemas `gold` y `features` |

Documentación de diseño: [Bronze](docs/DISENO_BRONZE.md) · [Silver](docs/DISENO_SILVER.md) · [Gold (grano)](docs/DISENO_GOLD_GRANO.md) · [Fase 2](docs/DISENO_FASE2.md) · [Seguridad](docs/SEGURIDAD_DATOS_PERSONALES.md) · [Bitácora de decisiones](docs/BITACORA_DECISIONES.md)
Gobernanza: [Diccionario Gold](docs/DICCIONARIO_DATOS_GOLD.md) · [Definiciones oficiales](docs/DEFINICIONES_OFICIALES.md) · [Diccionario de features](docs/DICCIONARIO_FEATURES.md)

---

## Entorno virtual y notebook de exploración

```powershell
python -m venv prof1cieciadedatos            # crear el entorno virtual
.\prof1cieciadedatos\Scripts\Activate.ps1    # activarlo en PowerShell (Linux/macOS: source prof1cieciadedatos/bin/activate)
pip install -r requirements.txt
```

El notebook de exploración de los archivos fuente se abre con el entorno activado: [COMPLETAR: nombre del notebook y cómo se abre].

---

## 0. Requisitos y preparación

- Python 3.10 o superior.
- Docker (solo para Kafka, es decir, el streaming de Transmetro y Aerómetro). Sin Docker se puede correr todo lo demás.
- Los 9 archivos que genera `generar_red_metropolitana.py`.

```bash
pip install -r requirements.txt
```

**Los datos no se suben a Git.** Colócalos en `datos_red/` (o apunta a su carpeta con `DATA_DIR`):

| Archivo | Vía de ingesta |
|---------|----------------|
| `tm_estaciones.csv`, `tu_paradas.csv`, `mr_estaciones.csv`, `am_estaciones.csv` | batch (catálogos) |
| `transurbano_transacciones.csv`, `metroriel_viajes.jsonl` | batch |
| `transmetro_validaciones.csv`, `aerometro_boardings.csv` | streaming (Kafka) |
| `cdc_padron_usuarios.csv` | CDC (log de cambios del padrón) |

**El secreto de seudonimización es obligatorio.** Sin él, Silver no se construye.

```bash
cp .env.example .env
python -c "import secrets; print(secrets.token_urlsafe(32))"    # pega el resultado en PSEUDONIMO_SECRETO dentro de .env
```

En PowerShell, en lugar del `.env`: `$env:PSEUDONIMO_SECRETO = "<valor largo y aleatorio>"`.
`.env`, `lake/`, `*.duckdb` y `datos_red/*` están en `.gitignore`. Si cambias el secreto, todos los seudónimos cambian: reconstruye Silver y Gold.

Variables opcionales: `DATA_DIR`, `LAKE_DIR`, `KAFKA_BOOTSTRAP`, `KAFKA_PARTICIONES`, `MICROLOTE_FILAS`, `MICROLOTE_SEGUNDOS`, `CHUNK_FILAS`.

Comprobar que todo está bien antes de empezar (no necesita Kafka):

```bash
pytest
```

---

## 1. Bronze: ingesta al lake

Cada comando es idempotente: repetirlo con los mismos archivos no agrega filas.

```bash
# 1.1 Batch: catálogos, Transurbano y MetroRiel
python -m bronze.cli batch

# 1.2 CDC: log de cambios del padrón de usuarios
python -m bronze.cli cdc

# 1.3 Streaming: Transmetro y Aerómetro por Kafka
docker compose up -d                          # Kafka en modo KRaft, un nodo
python -m bronze.cli topics                   # crea los topics
python -m bronze.cli produce ambos --pausa 0  # publica los dos CSV
python -m bronze.cli consume ambos            # consume hacia Bronze (modo drain)

# 1.4 Conciliación: conteo por archivo, para comprobar que no se perdió ni duplicó nada
python -m bronze.cli conciliar --csv salidas/conteos_bronze.csv
```

Notas:
- Con la pausa por defecto (500 mensajes cada 2 s), publicar Transmetro tarda unos 24 minutos y Aerómetro unos 14. `--pausa 0` lo acelera.
- `--max-filas N` publica solo N filas; una nueva corrida continúa donde quedó.
- Si `produce` se interrumpe (por ejemplo, por un error de checkpoint), los mensajes de la última ráfaga pueden estar ya en Kafka aunque el checkpoint no lo diga. Al reintentar se republican; no daña Bronze, porque `consume` descarta los repetidos (mismo archivo y línea). Compruébalo con `conciliar`.
- Sin Kafka, omite 1.3: el resto del pipeline funciona, pero Transmetro y Aerómetro no estarán en Bronze.
- Las filas con formato inválido no se pierden: quedan en tablas `malformadas_*`.

**Resultado esperado:** Parquet en `lake/`, y `lake/bronze_vistas.duckdb` con vistas de solo lectura (`python -m bronze.cli vistas` la regenera). `conciliar` no debe mostrar `DUPLICADOS_TECNICOS`.

Si Bronze quedó duplicado (por ejemplo, al cambiar de carpeta conservando los mensajes de Kafka):

```bash
python -m bronze.cli reiniciar-consumidor ambos              # simulación: muestra qué borraría
python -m bronze.cli reiniciar-consumidor ambos --confirmar
python -m bronze.cli consume ambos                           # reconstruye desde Kafka, sin repetidos
python -m bronze.cli conciliar
```

---

## 2. Silver y Gold: dbt

**Antes de `build`, Bronze debe estar completo** (batch, CDC y `consume`). Si falta alguna tabla, el comando se detiene con la lista de lo que falta y qué correr.

Un solo comando crea las vistas sobre Bronze, carga las semillas y construye staging, Silver, Gold y features, ejecutando las pruebas de cada modelo.

```bash
python -m flows.dbt_runner build
```

Orden lógico de lo que se construye:

1. **Semillas** (`dbt_metro/seeds/`): `dim_zona`, `feriados`, `transporte_catalogo`, `reglas_calidad`.
2. **Staging** (`staging.stg_*`): se reconstruye completo en cada corrida.
3. **Silver** (`silver.*`): eventos limpios por operador, `silver_cuarentena` (lo rechazado, con motivo y regla), `silver_padron_scd2` (historial del padrón), `silver_estacion`, `silver_usuario` (llave seudonimizada) y `silver_evento_unificado` (los cuatro operadores en una sola tabla).
4. **Gold** (`gold.*`):
   - Dimensiones: `dim_transporte`, `dim_fecha`, `dim_hora`, `dim_estacion`, `dim_usuario`, además de `dim_zona`.
   - Hechos: `fact_abordaje` (una fila por abordaje, los 4 operadores) y `fact_viaje` (hoy solo MetroRiel, que tiene entrada y salida).
   - Agregados para Tableau: `agg_demanda_zona_hora`, `agg_cobertura_zona`, `agg_usuarios_por_combinacion`, `agg_transbordo_temporal`, `agg_caso_metroriel`, `agg_od_metroriel`.
5. **Features** (`features.features_usuario`): una fila por persona, construida desde Silver (no desde Gold).
6. **Pruebas**: unicidad, no nulos, relaciones entre hechos y dimensiones, y pruebas de conciliación (Bronze = Silver + cuarentena; los agregados suman lo mismo que Silver).

Evidencia de que Silver y Gold quedaron bien:

```bash
python -m flows.verificar_linaje        # Gold solo lee Silver/Gold; features solo lee Silver; ninguna llave cruda de usuario
python -m flows.consultas sql/verificacion_post_bronze.sql
python -m flows.consultas sql/entregables_silver.sql
```

`flows.consultas` ejecuta un `.sql` desde Python (en PowerShell no funciona `duckdb base < archivo.sql`). Usa `--db` para elegir la base y `--salida archivo.txt` para guardar el resultado.

Opciones de `build`:

```bash
python -m flows.dbt_runner build --sin-unificar-identidad   # ver "Identidad del usuario"
python -m flows.dbt_runner docs                              # documentación y linaje de dbt
```

Para fijar la fecha de corte de las features, define `fecha_corte` en `dbt_metro/dbt_project.yml`; por defecto es la última fecha con datos.

---

## 3. Todo en un solo flujo (Prefect)

```bash
python -m flows.bronze_flow                  # Bronze + conciliación + Silver + Gold
python -m flows.bronze_flow --sin-streaming  # no requiere Kafka
python -m flows.bronze_flow --sin-silver     # solo Bronze
```

Opciones de streaming: `--tamano-rafaga`, `--pausa`, `--max-filas`.

**Evidencia de idempotencia.** Corre el flujo dos veces, cuenta cada tabla de Bronze, staging, Silver, Gold y features (con una huella del contenido) y escribe `evidencia/idempotencia_<fecha>.md`:

```bash
python -m flows.evidencia --sin-streaming    # si el streaming de Bronze ya está cargado
python -m flows.evidencia                    # flujo completo, con Kafka
```

Genérala con los datos reales y repítela en vivo el día de la presentación.

---

## 4. Gold hacia Tableau y la recomendación

**Opción A: conectar Tableau directamente a `lake/warehouse.duckdb`** (conector DuckDB o MotherDuck desde Tableau Exchange). Usa las tablas del esquema `gold`.

**Opción B: exportar CSV**, si no hay conector:

```bash
python -m flows.exportar_tableau --incluir-hechos      # escribe en salidas/tableau/ (solo tablas Gold)
```

**Cifras para el tablero y el documento de recomendación:**

```bash
python -m flows.consultas sql/analisis_recomendacion.sql --salida salidas/resumen_recomendacion.txt
```

Qué modelo alimenta cada gráfica y las definiciones (viaje, usuario activo, hora pico, cobertura): [`docs/DISENO_FASE2.md`](docs/DISENO_FASE2.md) y [`docs/DEFINICIONES_OFICIALES.md`](docs/DEFINICIONES_OFICIALES.md).

---

## 5. Orden completo, de cero a Gold

```bash
pip install -r requirements.txt
cp .env.example .env                                   # y rellena PSEUDONIMO_SECRETO
# copia los 9 archivos a datos_red/

python -m bronze.cli batch
python -m bronze.cli cdc
docker compose up -d
python -m bronze.cli topics
python -m bronze.cli produce ambos --pausa 0
python -m bronze.cli consume ambos
python -m bronze.cli conciliar --csv salidas/conteos_bronze.csv

python -m flows.dbt_runner build
python -m flows.verificar_linaje
python -m flows.consultas sql/analisis_recomendacion.sql --salida salidas/resumen_recomendacion.txt
python -m flows.exportar_tableau --incluir-hechos
```

---

## Identidad del usuario

Transmetro, Transurbano y MetroRiel usan números de tarjeta. Por defecto, la misma parte numérica en dos operadores se trata como la misma persona (hipótesis explícita; ver `docs/DISENO_SILVER.md`, sección 5). Aerómetro usa un hash opaco y no se puede unir. La hipótesis no está demostrada: `analysis/diagnostico_identidad.sql` compara los transbordos reales contra un placebo con ids desplazados. Con `--sin-unificar-identidad` cada (operador, llave) es un usuario distinto.

## Seguridad y datos personales

- Las llaves de usuario se seudonimizan con SHA-256 y un secreto antes de llegar a Gold; Gold no contiene números de tarjeta.
- `flows.verificar_linaje` y `tests/test_seguridad.py` lo comprueban. El CI falla si `.env`, datos o bases `.duckdb` entran al repositorio.
- Detalle: [`docs/SEGURIDAD_DATOS_PERSONALES.md`](docs/SEGURIDAD_DATOS_PERSONALES.md).

## Estructura del repositorio

```
bronze/            paquete: config, fuentes, ingesta (batch + CDC), streaming (Kafka), conciliación, cli
dbt_metro/         proyecto dbt-duckdb: staging, Silver, Gold, features, semillas, macros y pruebas
flows/             flujo de Prefect, ejecutor de dbt, conciliación, linaje, exportador a Tableau, evidencia
tests/             85 pruebas pytest (datos sintéticos + doble en memoria de Kafka)
sql/               consultas de verificación y de análisis
docs/              diseño, gobernanza, seguridad y bitácora de decisiones
datos_red/         aquí van los 9 archivos de datos (no se versionan)
docker-compose.yml Kafka (KRaft, un nodo)
```

## Problemas frecuentes

| Síntoma | Causa y solución |
|---------|------------------|
| `Falta PSEUDONIMO_SECRETO` | No hay `.env` ni variable de entorno. Ver sección 0. |
| dbt falla en `source_llave_tecnica_unica_...` | Bronze quedó duplicado. Ver `reiniciar-consumidor` en la sección 1. |
| `Could not set lock on file ... warehouse.duckdb` | Otro programa tiene abierta la base (Tableau, DBeaver, un shell de DuckDB). Ciérralo y repite. |
| `PermissionError: [WinError 5]` en `produce` o `batch` | El lake está en OneDrive. Mueve `LAKE_DIR` fuera (sección 0). |
| `Bronze está incompleto; faltan estas tablas: ...` | No se ha corrido alguna vía de ingesta. Corre lo que indica el mensaje (normalmente `consume ambos`). |
| `Table with name ... does not exist` en muchas pruebas | Lo mismo: faltan tablas en Bronze, o `LAKE_DIR` apunta a otro lake. |
| Tableau no ve datos nuevos | Reconstruye con `flows.dbt_runner build` y refresca la conexión, o vuelve a exportar los CSV. |
