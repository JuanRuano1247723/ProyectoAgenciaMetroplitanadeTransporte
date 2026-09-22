# Proyecto 1 · Capa Bronze

### Para ejecución de Notebook de exploración de archivos
```
python -m prof1cieciadedatos venv #crear entorno virtual

.prof1cieciadedatos\Scripts\Activate #ejecutar entorno virtual windows

source .prof1cieciadedatos/bin/activate #ejecutar entorno virtual macOS 

```
Instalar dependencias

```
pip install pandas matplotlib missingno jupyter ipykernel
python -m ipykernel install --user --name=.prof1cieciadedatos --display-name "Python (.venv)"
```
para ejecutar el notebook
```
jupyter notebook
```

Ingesta de los nueve archivos de la red metropolitana hacia un lake (carpetas + Parquet).
El diseño, las decisiones y sus justificaciones están en [`docs/DISENO_BRONZE.md`](docs/DISENO_BRONZE.md).

## Estructura

```
bronze/            paquete: config, fuentes, ingesta_archivos (batch + CDC), streaming (Kafka), conciliacion, cli
flows/             flujo de Prefect
tests/             34 pruebas (datos sintéticos + doble en memoria de Kafka)
sql/               consultas de verificación sobre Bronze (DuckDB)
docs/              diseño de Bronze
datos_red/         aquí van los 9 archivos que genera generar_red_metropolitana.py
docker-compose.yml Kafka (KRaft, un nodo)
```

## Inicio rápido

```bash
pip install -r requirements.txt
cp /ruta/a/datos_red/* datos_red/        # o: export DATA_DIR=/ruta/a/datos_red

pytest                                   # no necesita Kafka

python -m bronze.cli batch               # catálogos, Transurbano, MetroRiel
python -m bronze.cli cdc                 # log de cambios del padrón

docker compose up -d                     # Kafka
python -m bronze.cli topics
python -m bronze.cli produce ambos --pausa 0      # publica ambos CSV (rápido)
python -m bronze.cli consume ambos                # escribe micro-lotes a Bronze

python -m bronze.cli conciliar --csv salidas/conteos_bronze.csv
python -m bronze.cli vistas              # crea lake/bronze_vistas.duckdb
```

Con la pausa por defecto (500 mensajes, 2 s) publicar Transmetro tarda unos 24 minutos y Aerómetro unos 14.
`--max-filas N` publica solo N filas (útil para demostraciones); una nueva corrida continúa donde quedó.

## Variables de entorno

`DATA_DIR`, `LAKE_DIR`, `KAFKA_BOOTSTRAP`, `KAFKA_PARTICIONES`, `MICROLOTE_FILAS`, `MICROLOTE_SEGUNDOS`, `CHUNK_FILAS`.

## Todo con Prefect

```bash
python -m flows.bronze_flow                 # batch + CDC + streaming + conciliación
python -m flows.bronze_flow --sin-streaming # no requiere Kafka
```

Reejecutar cualquier comando con los mismos archivos no agrega filas.
