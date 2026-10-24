# Definiciones oficiales (3.1)

Los cuatro operadores miden a su manera; en el warehouse compartido cada término tiene **una** definición. Estas son las de la Agencia. Cambiar cualquiera requiere la autorización del dueño (columna "Dueño") y una entrada en `BITACORA_DECISIONES.md`.

> Los nombres de los dueños son `[ASIGNAR]`: complétalos con los integrantes del equipo antes de entregar. Cada nombre debe poder defender la definición en la defensa oral.

## 1. Términos centrales

| Término | Definición oficial | Dónde se aplica | Dueño |
|---|---|---|---|
| **Abordaje** | Un evento de entrada al sistema: cada validación de Transmetro, cada transacción de Transurbano, cada abordaje de Aerómetro y la entrada de cada viaje completo de MetroRiel. Solo filas aceptadas por Silver (las de cuarentena no cuentan). | `fact_abordaje.abordajes` | `[ASIGNAR]` Gold / hechos |
| **Viaje** | Un abordaje que **no** es un TRANSBORDO de Transmetro. En MetroRiel, un viaje es un trayecto completo con entrada y salida; los viajes sin salida están en cuarentena y no cuentan. Un transbordo es la continuación de un viaje, no uno nuevo. | `features_usuario.viajes_*`; en Tableau: `SUM(abordajes)` con `es_transbordo_operador = false` | `[ASIGNAR]` Gold / hechos |
| **Viajes del mes** | Cantidad de viajes (definición anterior) cuya **fecha local** (UTC−6) cae en el mes calendario. Solo los meses completos son comparables: de este conjunto, únicamente **junio de 2026** está completo (el periodo va del 1 de junio al 15 de julio). | Cualquier cifra mensual | `[ASIGNAR]` Gold / hechos |
| **Usuario activo** | Persona (`persona_id`) con **al menos un viaje en los 30 días anteriores a la fecha de corte** (`features_usuario.viajes_30d ≥ 1`), contando los cuatro operadores. Es la misma ventana de la capa de features, para no tener dos criterios. | Tablero y features | `[ASIGNAR]` Identidad y usuarios |
| **Tarjeta activa en el padrón** | Tarjeta de Transmetro cuya versión vigente en `silver_padron_scd2` tiene `activa = true`. **No es** "usuario activo": el padrón solo cubre Transmetro y solo al 40.3 % de sus tarjetas. | `silver_cdc_conteos`, `dim_usuario.activa_vigente` | `[ASIGNAR]` Silver / CDC |
| **Persona** | `persona_id`: seudónimo de la parte numérica de la llave de Transmetro, Transurbano y MetroRiel (hipótesis S-09; Aerómetro es una persona por llave). Si la hipótesis se apaga (`unificar_identidad_numerica: false`), persona = (operador, llave). | `dim_usuario`, features, transbordo | `[ASIGNAR]` Identidad y usuarios |
| **Usuario con más de un sistema** | Persona con eventos en dos o más operadores en el periodo (`n_modos ≥ 2`). Depende de la hipótesis de identidad; reportarlo siempre con esa salvedad. | `agg_usuarios_por_combinacion` | `[ASIGNAR]` Identidad y usuarios |
| **Transbordo inferido** | Dos abordajes consecutivos de la misma persona en operadores distintos con ≤ 60 minutos entre ellos (`ventana_transbordo_min`). Distinto del TRANSBORDO que Transmetro registra por sí mismo. | `agg_transbordo_temporal` | `[ASIGNAR]` Gold / hechos |
| **Hora pico** | Hora local cuyo volumen total de abordajes es ≥ 1.5 veces el promedio horario (`umbral_hora_pico`). Se deriva del dato, no de un rango supuesto. | `dim_hora.es_hora_pico` | `[ASIGNAR]` Gold / dimensiones |
| **Zona sin servicio** | Zona o municipio de `dim_zona` sin ninguna estación ni parada de ningún operador (`sin_servicio`). Se distingue de `sin_servicio_efectivo` (hay estación pero 0 abordajes). | `agg_cobertura_zona` | `[ASIGNAR]` Zonas |
| **Fecha y hora** | Siempre hora local de Guatemala (UTC−6, sin horario de verano). Aerómetro se convierte desde UTC en Silver; nunca se usa la partición de Bronze. | Todas las capas | `[ASIGNAR]` Silver |

## 2. Por qué dos personas obtendrían números distintos de "viajes del mes" (y cómo lo cierra este documento)

| Punto de divergencia | Resolución oficial |
|---|---|
| Con o sin cuarentena | Solo filas de Silver (las rechazadas no cuentan; siguen recuperables). |
| ¿Un transbordo es otro viaje? | No. |
| ¿Abordaje o viaje? | Son dos medidas con nombre propio. "Viajes del mes" usa la de **viaje**. |
| ¿Qué mes? | Mes por fecha **local**, no UTC. |
| Meses incompletos | Solo se comparan meses completos; julio (hasta el 15) se rotula como parcial. |

**Prueba de fuego.** En Tableau, `viajes` = `SUM(IF NOT [es_transbordo_operador] THEN [abordajes] END)` sobre `agg_demanda_zona_hora`, filtrado por mes de `fecha`. En SQL: `select count(*) from silver.silver_evento_unificado where not es_transbordo_operador and date_trunc('month', fecha) = date '2026-06-01'`. Las dos deben dar el mismo número; si no, hay una definición sin cerrar.

## 3. Dueños por dimensión conformada

Si mañana cambia la definición de zona, **autoriza el dueño de Zonas** (`[ASIGNAR]`) y se anota en la bitácora.

| Dimensión | Dueño | Qué autoriza |
|---|---|---|
| `dim_zona` / zona conformada | `[ASIGNAR]` | Zonas, municipios y el mapeo desde cada catálogo |
| `dim_estacion` | `[ASIGNAR]` | Alta y baja de estaciones y paradas |
| `dim_transporte` | `[ASIGNAR]` | Alta de operadores y sus atributos |
| `dim_fecha` / `dim_hora` | `[ASIGNAR]` | Feriados y umbral de hora pico |
| `dim_usuario` / identidad | `[ASIGNAR]` | Hipótesis de identidad, seudonimización y rotación del secreto |
