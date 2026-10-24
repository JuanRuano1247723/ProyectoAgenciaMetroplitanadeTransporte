# Fase 2 — Tablero, recomendación y features: diseño

## 1. Modelos nuevos

| Modelo | Capa | Lee | Para qué |
|---|---|---|---|
| `silver_evento_unificado` | Silver | los 4 `silver_*` de hechos, `silver_usuario`, `silver_estacion` | Detalle atómico unificado, con `persona_id`, zona y hora local |
| `features_usuario` | features | solo `silver_evento_unificado` | 2.3: una fila por persona con fecha de corte |
| `agg_demanda_zona_hora` | Gold | `silver_evento_unificado` | Demanda por modo, zona y hora |
| `agg_cobertura_zona` | Gold | `dim_zona`, `silver_estacion`, `silver_evento_unificado` | Zonas sin servicio |
| `agg_usuarios_por_combinacion` | Gold | `silver_evento_unificado` | Usuarios que usan más de un sistema |
| `agg_transbordo_temporal` | Gold | `silver_evento_unificado` | Transbordos inferidos (ventana de 60 min) |
| `agg_caso_metroriel`, `agg_od_metroriel` | Gold | `dim_zona`, `silver_*` | Caso MetroRiel y origen-destino |

Las pruebas de dbt (`assert_evento_unificado_concilia_con_silver`, `assert_features_*`, `assert_gold_agg_concilia_con_silver`) y `tests/test_fase2.py` verifican que ningún agregado pierde ni duplica abordajes o personas, que las ventanas de features coinciden con un cálculo independiente en Python y que la fase es idempotente.

## 2. Conexión a Tableau

1. **Preferida:** conector *MotherDuck / DuckDB* del Tableau Exchange (necesita el driver JDBC de DuckDB), abriendo `lake/warehouse.duckdb` en solo lectura.
2. **Plan B:** `python -m flows.exportar_tableau` genera CSV de Gold en `salidas/tableau/`; con `--incluir-hechos` también `fact_abordaje` y `fact_viaje`. Solo exporta el esquema `gold`.
3. Para las hojas usar los `agg_*` (responden rápido; el tiempo de consulta del tablero es una métrica de la rúbrica). Para rastrear un número hasta el archivo crudo, una hoja de detalle sobre `fact_abordaje` con `evento_sk`, `_file_hash` y `_source_line_number`.

Campos calculados en Tableau:

```
Viajes            = SUM(IF NOT [es_transbordo_operador] THEN [abordajes] END)
Tarifa promedio   = SUM([monto_gtq]) / SUM([abordajes])
Duración prom (min) (OD) = SUM([duracion_s]) / SUM([viajes]) / 60
% multimodo       = SUM(IF [n_modos] > 1 THEN [personas] END) / SUM([personas])
```

## 3. Hojas del tablero

| Pregunta (2.1) | Fuente | Gráficos | Qué analizar |
|---|---|---|---|
| Demanda por modo, zona y hora | `agg_demanda_zona_hora` | Mapa de calor hora × día de la semana (un panel por modo); barras de viajes por zona coloreadas por modo; líneas de abordajes por hora y modo | Horas pico (la regla de `dim_hora` las deriva); qué modo domina cada zona; si los perfiles horarios coinciden o se complementan |
| Cobertura | `agg_cobertura_zona` | Mapa o tabla ordenada por `modos_con_servicio`; KPI de zonas con y sin servicio | Zonas y municipios con 0 modos; zonas con estación pero sin uso (`sin_servicio_efectivo`); zonas servidas por un solo modo |
| Transbordo | `agg_usuarios_por_combinacion`, `agg_transbordo_temporal` | Barras de personas por combinación; matriz operador anterior × siguiente; KPI `% multimodo` | Cuántos usan más de un sistema y qué pares se combinan. **Siempre con la salvedad de la hipótesis de identidad (S-09)** |
| Caso MetroRiel | `agg_caso_metroriel`, `agg_od_metroriel` | Barras de demanda de los otros modos por zona con las zonas del caso resaltadas y marca de estación de MetroRiel; dispersión demanda × estaciones; mapa de calor origen-destino de MetroRiel | Si las zonas de mayor demanda tienen estación; qué porcentaje de la demanda cae en zonas con MetroRiel; cuáles de las 5 zonas del caso quedan sin servir |

Filtros comunes: rango de fechas, operador, `es_dia_habil` (de `dim_fecha`), y un selector de "solo meses completos" para no comparar julio parcial con junio.

## 4. Cómo se escribe la recomendación (2.2)

Cada afirmación lleva su cifra de `sql/entregables_fase2.sql` y la hoja del tablero que la muestra. Estructura sugerida de las dos páginas:

1. **Respuesta corta** a "¿los cuatro modos integrados resuelven el problema de movilidad? ¿dónde sí y dónde no?".
2. **Dónde sí:** zonas con demanda alta y más de un modo; cifra de transbordo y su salvedad.
3. **Dónde no:** zonas sin servicio, zonas con estación sin uso, demanda alta sin estación de MetroRiel.
4. **Recomendación concreta:** una estación de transbordo, un corredor o un modo subutilizado, con su número.
5. **Límites:** periodo de 45 días, hipótesis de identidad, `cod_estado` sin interpretar, filas en cuarentena que no se contaron.

## 5. Límites declarados

- La cifra de usuarios con más de un sistema depende de que el número compartido entre llaves sea la misma persona. `analysis/diagnostico_identidad.sql` la contrasta con un placebo; un resultado cercano a 1 no la refuta (el generador puede sortear cada evento de forma independiente). Lo definitivo es el generador de datos.
- `viajes_90d` coincide con `viajes_total` por el corto periodo.
- Las zonas sin mapear (`zona_id = −1`) quedan fuera de la cobertura y del caso MetroRiel, y se listan con la advertencia `assert_zonas_sin_mapear`.
- Las pruebas usan datos sintéticos; las cifras reales salen al correr con los datos de la Agencia.
