# Capa de features (2.3)

Tabla `features.features_usuario`, construida con dbt desde **Silver** (`silver_evento_unificado`), no desde Gold: el feature engineering necesita el detalle atómico. `python -m flows.verificar_linaje` falla si esta capa lee Gold, staging o Bronze, o si expone una llave cruda.

- **Entidad:** una fila por persona (`persona_id`, seudónimo).
- **Fecha de corte:** columna `fecha_corte`; por defecto la última fecha con datos (el periodo va del 2026-06-01 al 2026-07-15). Se fija con `--vars '{fecha_corte: "AAAA-MM-DD"}'`. Solo se usan eventos con fecha ≤ corte: ninguna feature ve el futuro.
- **Ventana de N días:** eventos con `fecha_corte − fecha < N` (0 = el día de corte).

| Columna | Significado | Cálculo |
|---|---|---|
| `persona_id` | Persona (seudónimo). | De `silver_usuario`; Aerómetro = una persona por llave. |
| `fecha_corte` | Fecha de corte declarada. | Parámetro o máxima fecha con datos. |
| `viajes_7d`, `viajes_30d`, `viajes_90d` | Viajes en los últimos 7, 30 y 90 días. | Eventos que no son TRANSBORDO de Transmetro. |
| `viajes_total` | Viajes en todo el historial hasta el corte. | Igual. |
| `dias_con_viaje` | Días distintos con actividad. | `count(distinct fecha)`. |
| `modo_mas_usado` | Operador con más viajes (empate: orden alfabético del código). | `row_number()` por conteo. |
| `n_modos` | Cantidad de operadores distintos usados (1–4). | `count(distinct operador_cod)`. |
| `dias_desde_ultimo_viaje` | Días entre el último evento y el corte. | `min(fecha_corte − fecha)`. |
| `prop_viajes_hora_pico` | Fracción de viajes en hora pico. | Misma regla que `dim_hora` (≥ 1.5 × promedio horario). |
| `zona_origen_mas_frecuente` | Zona de abordaje más frecuente (se ignora −1 "sin mapear"). | `row_number()` por conteo. |
| `gasto_acumulado_gtq` | Dinero total en quetzales. | Suma de todos los montos, incluidos los de un transbordo. |
| `gasto_promedio_por_viaje_gtq` | Gasto medio por viaje. | `gasto_acumulado / viajes_total`. |

**Limitaciones declaradas.**
1. Con ~45 días de datos, `viajes_90d` es igual a `viajes_total`: la ventana de 90 días cubre todo el periodo.
2. `n_modos` y `modo_mas_usado` dependen de la hipótesis de identidad (S-09); con la hipótesis apagada, cada llave es una persona y `n_modos` es casi siempre 1.
3. Las features son seudonimizadas, no anónimas: la secuencia de estaciones identifica a una persona (ver `SEGURIDAD_DATOS_PERSONALES.md`).

**Qué se podría predecir y por qué sirve a la Agencia.** `[COMPLETAR con las cifras reales]` Con esta tabla se podría predecir qué usuarios dejarán de viajar en las próximas semanas (abandono) a partir de su frecuencia reciente, su recencia y sus modos; la Agencia podría dirigir incentivos de integración tarifaria hacia quienes están por abandonar o hacia quienes usan un solo modo y viven cerca de otro.
