# Proyecto 1 · Bronze y Silver

Ingesta de los nueve archivos de la red metropolitana a un lake (Bronze) y su integración en Silver con dbt.

- Diseño de Bronze: [`docs/DISENO_BRONZE.md`](docs/DISENO_BRONZE.md)
- Diseño de Silver: [`docs/DISENO_SILVER.md`](docs/DISENO_SILVER.md)

## Estructura

```
bronze/            paquete: config, fuentes, ingesta (batch + CDC), streaming (Kafka), conciliación, cli
dbt_metro/         proyecto dbt-duckdb: staging, Silver, cuarentena, semillas y pruebas
flows/             flujo de Prefect, ejecutor de dbt y evidencia de idempotencia
tests/             55 pruebas pytest (datos sintéticos + doble en memoria de Kafka)
sql/               consultas de verificación y de entregables
docs/              diseño de Bronze y de Silver
datos_red/         aquí van los 9 archivos que genera generar_red_metropolitana.py
docker-compose.yml Kafka (KRaft, un nodo)
```

## Inicio rápido

```bash
pip install -r requirements.txt
cp /ruta/a/datos_red/* datos_red/        # o: export DATA_DIR=/ruta/a/datos_red

pytest                                   # no necesita Kafka

# --- Bronze
python -m bronze.cli batch               # catálogos, Transurbano, MetroRiel
python -m bronze.cli cdc                 # log de cambios del padrón
docker compose up -d                     # Kafka
python -m bronze.cli topics
python -m bronze.cli produce ambos --pausa 0
python -m bronze.cli consume ambos
python -m bronze.cli conciliar --csv salidas/conteos_bronze.csv

# --- Silver (vistas de Bronze + dbt build: semillas, staging, Silver y pruebas)
python -m flows.dbt_runner build
python -m flows.consultas sql/entregables_silver.sql      # ejecuta el .sql desde Python (sirve en PowerShell)
python -m flows.consultas sql/verificacion_post_bronze.sql
```

> `duckdb base < archivo.sql` **no funciona en PowerShell** (no admite `<`) y `pip install duckdb` no instala ese ejecutable; por eso existe `flows.consultas`.

Con la pausa por defecto (500 mensajes, 2 s) publicar Transmetro tarda unos 24 minutos y Aerómetro unos 14.
`--max-filas N` publica solo N filas; una nueva corrida continúa donde quedó.

## Si cambias de carpeta y conservas Kafka

Los mensajes de la corrida anterior siguen en el topic. El consumidor descarta los repetidos (mismo archivo y línea), así que Bronze no se duplica.
Si Bronze ya quedó duplicado (`conciliar` muestra `DUPLICADOS_TECNICOS`, o dbt falla en `source_llave_tecnica_unica_...`):

```bash
python -m bronze.cli reiniciar-consumidor ambos            # simulación: muestra qué borraría
python -m bronze.cli reiniciar-consumidor ambos --confirmar
python -m bronze.cli consume ambos                         # reconstruye desde Kafka, sin repetidos
python -m bronze.cli conciliar
python -m flows.dbt_runner build
```

## Todo con Prefect

```bash
python -m flows.bronze_flow                 # Bronze + conciliación + Silver
python -m flows.bronze_flow --sin-silver    # solo Bronze
python -m flows.bronze_flow --sin-streaming # no requiere Kafka
```

## Evidencia de idempotencia (1.5)

```bash
python -m flows.evidencia --sin-streaming   # si Bronze de streaming ya está cargado
python -m flows.evidencia                   # flujo completo, con Kafka
```

Corre el flujo dos veces, cuenta cada tabla de Bronze, staging y Silver (con una huella del contenido) y escribe
`evidencia/idempotencia_<fecha>.md`. Hay que generarlo con los datos reales; repetirlo en vivo el día de la presentación.

## Identidad del usuario

Por defecto, las llaves de Transmetro, Transurbano y MetroRiel con la misma parte numérica se tratan como la misma persona
(hipótesis explícita, ver `docs/DISENO_SILVER.md` sección 5). Para desactivarla:

```bash
python -m flows.dbt_runner build --sin-unificar-identidad
```

## Variables de entorno

`DATA_DIR`, `LAKE_DIR`, `KAFKA_BOOTSTRAP`, `KAFKA_PARTICIONES`, `MICROLOTE_FILAS`, `MICROLOTE_SEGUNDOS`, `CHUNK_FILAS`.

Reejecutar cualquier comando con los mismos archivos no agrega filas.
