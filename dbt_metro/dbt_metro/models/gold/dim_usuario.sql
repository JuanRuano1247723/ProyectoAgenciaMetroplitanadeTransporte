{#
  Dimensión conformada de usuario. A propósito NO incluye llave_original ni id_numerico (silver_usuario
  sí los tiene, para la trazabilidad interna que exige un auditor de fraude): Gold solo expone
  seudónimos (ver docs/SEGURIDAD_DATOS_PERSONALES.md). python -m flows.verificar_linaje falla si
  alguna columna cruda se filtra aquí.
#}
select
    usuario_sk,
    persona_id,
    operador,
    regla_identidad,
    en_padron,
    perfil_vigente,
    activa_vigente
from {{ ref('silver_usuario') }}
