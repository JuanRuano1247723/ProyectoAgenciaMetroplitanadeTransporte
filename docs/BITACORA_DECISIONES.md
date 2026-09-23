# Bitácora de decisiones

Registro de las decisiones de diseño, con lo que se descartó y por qué. Estado: **Aceptada** (implementada y probada), **Propuesta** (falta confirmar) o **Abierta**.
El detalle de cada una está en `DISENO_BRONZE.md`, `DISENO_SILVER.md`, `DISENO_GOLD_GRANO.md` y `SEGURIDAD_DATOS_PERSONALES.md`.

## Bronze

| ID | Decisión | Alternativa descartada | Por qué | Estado |
|---|---|---|---|---|
| B-01 | Bronze en un lake (carpetas + Parquet); el warehouse para Silver y Gold | Bronze en el warehouse | Conserva el JSON anidado de MetroRiel como `STRUCT`, desacopla almacenamiento y cómputo, y permite reprocesar sin tocar el origen | Aceptada |
| B-02 | Copia cruda de cada archivo (*landing*), verificada por hash | Solo Parquet | Reproducibilidad: Bronze puede reconstruirse | Aceptada |
| B-03 | Todo dato como `STRING`, tal como llegó | Inferir tipos | La inferencia pierde los ceros a la izquierda de `num_tarjeta` | Aceptada |
| B-04 | Partición por la **fecha del evento** (catálogos y CDC por fecha de ingesta) | Solo fecha de ingesta | Los datos llegan de una vez: daría una sola partición | Aceptada |
| B-05 | Idempotencia por llave técnica (`_file_hash` + línea; topic + partición + offset) | `upsert` por llave de negocio | Un upsert colapsaría el log CDC y fundiría los 1,115 duplicados reales | Aceptada |
| B-06 | Transurbano por **batch** | Streaming | No hay requisito de latencia; un topic menos que operar; la fuente no trae id de evento | Aceptada |
| B-07 | Streaming en micro-lotes (N filas o T segundos), marcador tras escribir, recuperación por reversión | Commit de offsets de Kafka como única garantía | La escritura a Parquet y el commit no son atómicos | Aceptada |
| B-08 | El consumidor descarta mensajes repetidos (mismo archivo y línea) | Confiar en que el productor no repite | **Incidente:** al cambiar de carpeta con Kafka intacto, el productor republicó y Bronze quedó con cada fila dos veces; dbt lo detectó en una prueba de unicidad. Corregido, con alarma y comando de reparación | Aceptada |
| B-09 | El CDC se guarda completo, sin aplicar operaciones | Aplicar en la carga | Aplicarlo destruiría el historial que Silver necesita para el SCD2 | Aceptada |

## Silver

| ID | Decisión | Alternativa descartada | Por qué | Estado |
|---|---|---|---|---|
| S-01 | dbt-duckdb; staging y Silver se reconstruyen completos en cada corrida | Modelos incrementales | Es una función pura de Bronze: idempotente por construcción (los incrementales quedan como extra) | Aceptada |
| S-02 | El CDC se ordena por `seq` | Ordenar por `commit_ts` | `seq` es único y creciente; `commit_ts` tiene 15,479 filas fuera de orden y empates | Aceptada |
| S-03 | SCD2 del padrón: DELETE marca inactiva y conserva atributos; UPDATE sin INSERT se acepta | Borrar; rechazar los UPDATE sin INSERT | Borrar pierde el historial; el 65 % de las tarjetas no tiene INSERT (el log es una ventana) | Aceptada |
| S-04 | Dos niveles: **rechazo** (cuarentena con motivo y fila original) y **advertencia** (se queda con marca) | Descartar | Descartar en silencio pierde la mitad de 1.3 | Aceptada |
| S-05 | "Fecha futura" = posterior a la fecha de ingesta | Comparar con `current_date` | El resultado no cambia según el día en que se corra | Aceptada |
| S-06 | `cod_parada` nulo y viaje sin salida → **rechazo** | Conservarlos con marca | El enunciado los lista como registros malos; siguen recuperables desde la cuarentena | Aceptada (cambio de criterio) |
| S-07 | `SIN-TARJETA` y llaves que no son `TC-########` → rechazo | Normalizarlas | `SIN-TARJETA` no identifica a nadie; las llaves `MR` no coinciden con ninguna tarjeta que viaje en Transmetro (0 de 992) | Aceptada |
| S-08 | Zona conformada con semilla `dim_zona` (25 zonas y municipios) y macro `clave_zona` | Un mapeo manual por catálogo | Lo no mapeado no falla en silencio: avisa | Aceptada |
| S-09 | Identidad en dos capas: cada (operador, llave) es un usuario, más una **hipótesis** de persona por parte numérica | Unificar sin declararlo; no unificar | El solapamiento observado es el del azar: no prueba ni descarta identidad. Se apaga con una variable | Aceptada, con límite declarado |
| S-10 | Aerómetro: UTC−6 fijo | Zona con horario de verano | Guatemala no lo usa; la ventana 04:00–22:59 coincide con los otros tres operadores | Aceptada |

## Manejo de nulos y de cuarentena

| ID | Decisión | Alternativa descartada | Por qué | Estado |
|---|---|---|---|---|
| N-01 | Bronze: un campo CSV vacío se guarda como `NULL`, nunca como cadena vacía `""` | Guardar `""` tal cual | El CSV no distingue "vacío" de "ausente"; guardar `""` obligaría a repetir `col = '' OR col IS NULL` en cada consulta de Silver | Aceptada |
| N-02 | Bronze nunca imputa ni descarta un nulo | Rellenar con un valor por defecto | Bronze conserva el dato tal como llegó; imputar es una decisión de negocio, no de ingesta | Aceptada |
| N-03 | En Silver, un nulo relevante para una regla de negocio se evalúa explícitamente (`cod_parada IS NULL`, `exit IS NULL`) y decide si la fila es rechazo o advertencia | Dejar que el nulo se propague en cálculos posteriores | Un nulo sin resolver se filtra solo en agregaciones (`SUM`/`COUNT` lo ignoran), lo que subestima cifras sin que nadie lo note | Aceptada |
| N-04 | Un nulo que no dispara ninguna regla de calidad pasa a Silver tal cual (por ejemplo, `cod_estado` sin catálogo) | Rechazar cualquier fila con un nulo en cualquier columna | Rechazar de más pierde información sin justificación de negocio; solo se cuarentena lo que el enunciado o las reglas declaradas señalan como "malo" | Aceptada |

- **Cuarentena de solo-append.** `silver_cuarentena` une, de las cinco fuentes con reglas de rechazo, toda fila donde `reglas_rechazo` no está vacío. Guarda `fuente`, `regla_principal`, la lista completa de reglas incumplidas, el `motivo` (desde la semilla `reglas_calidad`), y la **fila original completa** (`fila_original`, un `STRUCT` con todas las columnas de Bronze) más `_file_hash` y `_source_line_number` para volver a Bronze. Nada se transforma ni se limpia antes de cuarentenar: es la fila tal como salió de staging.
- **Una fila puede incumplir más de una regla.** `reglas_rechazo` es una lista; `cuarentena_sk` se calcula una vez por fila (no por regla), así que una fila con dos motivos de rechazo aparece una sola vez en `silver_cuarentena`, con `regla_principal` = la primera de la lista y `reglas_incumplidas` con todas, separadas por coma. Esto es lo que evita que `unique(cuarentena_sk)` falle cuando dos reglas coinciden en una misma fila (ver incidente B-08, que era un problema distinto: la misma fila *física* repetida en Bronze).
- **Invariante verificada:** por cada fuente, filas de Bronze = filas de Silver + filas de cuarentena (sección "Invariante" de `sql/entregables_silver.sql` y prueba `test_silver.py`). Si no cuadra, algo se perdió sin quedar registrado.
- **Conteo por regla, incluidas las reglas que no dispararon.** `sql/entregables_silver.sql` reporta también las reglas que evaluaron 0 filas afectadas (`estacion_sin_catalogo`, `fecha_futura` en Aerómetro, etc.), como prueba de que sí se evaluaron y no se omitieron.
- **Advertencia vs. rechazo:** una advertencia dejan la fila en Silver con una columna booleana (`tarjeta_en_padron`, `posible_doble_lectura`, `duracion_no_coincide`); un rechazo la manda a cuarentena y la excluye de Silver. La tabla `reglas_calidad` (seed) fija qué regla es cuál; cambiar la severidad de una regla es editar esa semilla, no el código de los modelos.

## Seguridad

| ID | Decisión | Por qué | Estado |
|---|---|---|---|
| X-01 | Seudónimos con secreto (`SHA-256(secreto:llave)`, 20 hex) en Silver, antes de Gold | El `md5` sin secreto anterior se revertía en 0.03 s | Aceptada |
| X-02 | Secretos solo en el entorno o `.env` (fuera de Git); el secreto vive en memoria y no aparece en logs ni en la base | Un secreto en Git queda en el historial | Aceptada |
| X-03 | Acceso por rol (analista, ciencia de datos, auditor, ingeniería) | El analista no necesita el detalle por individuo | Aceptada (convención y verificación; sin RBAC real en DuckDB) |
| X-04 | Retención del detalle con llave: 13 meses | 12 meses para comparar contra el año anterior más un margen | **Propuesta** (confirmar contra la normativa aplicable) |

## Gold

| ID | Decisión | Alternativa descartada | Por qué | Estado |
|---|---|---|---|---|
| G-01 | Dos hechos: `fact_abordaje` y `fact_viaje` | Un solo grano ("viaje puerta a puerta") para los cuatro operadores | Obligaría a inferir el viaje completo en tres de cuatro sistemas que no traen salida; MetroRiel sí la trae y perdería información si se lo fuerza al grano de abordaje | Aceptada |
| G-02 | `fact_abordaje`: una fila por evento de entrada, cualquier operador | Un hecho por operador (cuatro tablas) | El abordaje es la única unidad que los cuatro sistemas registran; permite comparar operadores sin normalizar después | Aceptada |
| G-03 | `fact_viaje`: grano genérico ("un trayecto completo, para cualquier transporte capaz de registrarlo"), no "viaje de MetroRiel" | Nombrar la tabla y las columnas específicas de MetroRiel (`mr_estacion_entrada`, etc.) | Deja el esquema listo para un viaje multimodal inferido (extra de transbordo) sin rediseñar nada; hoy solo MetroRiel puebla la tabla, y eso es intencional, no un error de carga | Aceptada |
| G-04 | `dim_transporte` (dimensión conformada) en vez de una columna literal por operador | Un `CASE WHEN` o string repetido en cada `UNION ALL` | Evita el string mágico repetido y documenta como atributo del dato, no como comentario del código, cuál operador registra trayecto completo (`registra_trayecto_completo`) | Aceptada |
| G-05 | La entrada de un viaje de MetroRiel aparece en **ambos** hechos, pero cada medida vive en un solo hecho (monto en `fact_abordaje`, duración y salida en `fact_viaje`) | Excluir a MetroRiel de `fact_abordaje` | Sin esto, `fact_abordaje` subestimaría la demanda total del sistema; sumar la misma medida en los dos hechos sí sería un error, por eso se reparte | Aceptada |
| G-06 | `dim_estacion` como dimensión de rol en `fact_viaje` (entrada y salida vía dos alias) | Dos tablas de dimensión física (`dim_estacion_entrada`, `dim_estacion_salida`) | Es la misma estación física jugando dos roles; duplicarla físicamente rompería la conformidad con `fact_abordaje` | Aceptada |
| G-07 | `dim_hora.es_hora_pico` se deriva del volumen real de abordajes (umbral configurable, var `umbral_hora_pico`) | Un rango fijo supuesto (p. ej. "7-9 y 17-19") | La rúbrica exige hora pico en la dimensión tiempo; un rango supuesto no se sostiene si el patrón real difiere | Aceptada |
| G-08 | `dim_usuario` (wrapper de Gold) excluye `llave_original` e `id_numerico`, aunque `silver_usuario` sí los tiene | Exponer todas las columnas de `silver_usuario` en Gold | Gold es la capa que puede llegar a un tablero; una llave cruda ahí anula la seudonimización de Silver. Verificado por `flows/verificar_linaje.py` | Aceptada |
| G-09 | `distancia_km` es aditiva pero con cobertura parcial (NULL si alguna estación no trae `km`) | Excluir la medida hasta tener `km` para las cuatro redes | Solo `mr_estaciones` trae `km` hoy; excluirla pierde valor para MetroRiel, que sí la puede usar completa | Aceptada, límite declarado |
| G-10 | Ninguna razón (promedio, porcentaje, tasa) se guarda precalculada; siempre numerador y denominador aditivos | Guardar `tarifa_promedio`, `duracion_promedio`, etc. como columnas | Promediar promedios ya agregados da un número incorrecto; el numerador/denominador se puede reagregar a cualquier nivel | Aceptada, verificada (`test_gold_medidas_no_aditivas_no_se_almacenan_precalculadas`) |
| G-11 | "Viajes del mes": definición oficial con dueño | Dejar que cada quien calcule con su propio criterio | Cinco puntos (cuarentena, transbordos, abordaje vs. viaje, mes UTC vs. local, mes incompleto) hacen que dos personas obtengan números distintos | **Abierta** |

## Abiertas

- **Llaves del padrón que no son `TC`, `MR` ni `SIN-TARJETA`:** 5,223 filas (4,038 llaves) están en cuarentena sin haberse inspeccionado. Pueden cambiar los conteos de activas y de bajas y la decisión de identidad.
- **Cómo genera las llaves el generador de datos** y qué significa `cod_estado`.
- **Resultado de la prueba de hash de Aerómetro:** si es opaco o derivable de un id.
