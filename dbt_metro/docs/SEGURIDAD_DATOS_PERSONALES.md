# Seguridad y manejo de datos personales (3.3)

Los datos son generados, pero el escenario no: un sistema real de transporte sabe dónde estuvo cada persona, a qué hora, todos los días.
Cuatro decisiones, cada una con su justificación, lo que está **implementado y verificado** y sus **límites**.

## 1. Nada de credenciales en el repositorio

**Decisión.** Toda configuración sensible entra por variables de entorno o por un archivo `.env` local que **no se sube a Git**.
El repositorio trae `.env.example` con los nombres de las variables y ningún valor real.

- `.gitignore` excluye `.env`, `*.duckdb`, `lake/` y los datos de origen (`datos_red/*`).
- `profiles.yml` de dbt no contiene rutas ni secretos: lee `LAKE_DIR` y el plugin lee `PSEUDONIMO_SECRETO`.
- Verificación: una prueba automática comprueba que existe `.env.example` sin valor y que `.gitignore` cubre esos patrones; una búsqueda de `password`, `secret`, `token` o `api_key` en el código no encuentra nada.

**Por qué.** Un secreto que entra a Git es un secreto publicado, aunque se borre después (queda en el historial).
**Límite.** El Kafka de `docker-compose.yml` es de desarrollo (sin autenticación, solo `localhost`). En producción llevaría TLS y SASL.

## 2. Seudonimizar la llave de usuario antes de Gold

**Decisión.** Las llaves de usuario (`TC-…`, `0000…`, `MR…`, hash de Aerómetro) se convierten en seudónimos en Silver:
`SHA-256(secreto : operador : llave)`, 20 caracteres hexadecimales. Lo mismo para `persona_id` (la persona unificada por la hipótesis de identidad).
Gold solo puede usar `usuario_sk` y `persona_id`. El secreto sale de `PSEUDONIMO_SECRETO` y vive **solo en memoria** durante la corrida.

**Por qué con secreto.** La versión anterior era `md5(operador:llave)` sin secreto. Como las tarjetas son un espacio pequeño (unos 60,000 números con formato conocido),
recuperar una tarjeta a partir de su hash tomó **0.03 segundos** en una prueba. Un hash sin secreto no protege nada; con secreto, quien no lo tenga no puede regenerar ni verificar una adivinanza.
El mismo (secreto, llave) siempre da el mismo seudónimo, así que Silver sigue siendo idempotente.

**Verificado con pruebas.** El seudónimo coincide con la fórmula, ya no es el `md5`, es el mismo en los hechos y en `silver_usuario`, y **el secreto no aparece en la base, en el SQL compilado ni en los logs de dbt**.
`python -m flows.verificar_linaje` falla si un modelo Gold contiene una columna llamada `tarjeta`, `user_hash`, etc., o valores con aspecto de llave cruda.

**Límites (declarados).**
- Seudonimizar **no es anonimizar**: la secuencia de estaciones y horas de una persona la identifica por sí sola. Por eso el detalle individual sigue siendo dato personal (decisiones 3 y 4).
- `perfil` y `zona_residencia` del padrón son cuasi-identificadores; combinados con estación y hora reducen mucho el anonimato.
- Cambiar el secreto cambia **todos** los seudónimos y rompe la continuidad histórica: es una rotación planificada, no un ajuste. Si se pierde, los seudónimos no se pueden regenerar.
- Las llaves crudas siguen existiendo en Silver (`llave_original`, los hechos y los catálogos de llaves de 1.2). Silver es zona restringida; solo Gold y el tablero son "seguros".

## 3. Quién debería ver qué

| Rol | Bronze / landing | Silver (llaves crudas) | Gold y tablero (seudónimos) | Cuarentena | Secreto de seudonimización |
|---|:---:|:---:|:---:|:---:|:---:|
| Ingeniería de datos | sí | sí | sí | sí | custodia |
| Analista / tablero de la Agencia | no | no | sí | no | no |
| Ciencia de datos (features, recomendación) | no | no | sí, solo seudónimos | no | no |
| Auditor de fraude | no | **sí, por solicitud registrada** | sí | solo lectura | no |

**Por qué.** Un analista responde preguntas sobre la red (dónde, cuándo, cuánta gente): no necesita saber quién es cada tarjeta. Un auditor de fraude sí necesita volver del seudónimo a la tarjeta,
pero como excepción justificada y con registro de quién consultó qué. La cuarentena contiene filas originales completas, así que se trata como Silver.

**Límite.** En este proyecto el control es por **convención y verificación automática** (Gold no puede leer Bronze ni exponer llaves crudas), porque DuckDB local no tiene roles.
En un warehouse real se implementaría con permisos por esquema (`GRANT SELECT ON SCHEMA gold TO analistas`) y registro de acceso al esquema `silver`.

## 4. Retención del detalle individual

**Propuesta: 13 meses** para el detalle con llave (Bronze, landing, Silver y cuarentena). Después se elimina, y solo se conservan los agregados de Gold sin llave individual.

**Por qué ese plazo.** El análisis de la Agencia necesita comparar un mes contra el mismo mes del año anterior (12 meses) más un margen de un mes para cierres y revisiones de fraude.
Guardar más tiempo del que exige el propósito aumenta el riesgo sin dar valor. Como Bronze se particiona por fecha del evento, borrar el mes más antiguo es eliminar una partición.
Esto es la única excepción a que Bronze sea solo *append*, y debe hacerse con un proceso explícito y registrado.

**Límite.** El plazo es una propuesta técnica del equipo: debe confirmarse contra la normativa de protección de datos y los requisitos de auditoría que apliquen a la Agencia.
