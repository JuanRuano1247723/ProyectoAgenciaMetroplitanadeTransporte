select * from {{ ref('silver_cdc_conteos') }}
where tarjetas_antes_de_borrados <> tarjetas_activas_despues + tarjetas_dadas_de_baja
   or eventos_en_bronze <> eventos_en_cuarentena + eventos_aplicados
