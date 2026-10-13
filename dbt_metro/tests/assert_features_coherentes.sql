-- Las ventanas deben ser anidadas, la proporción estar en [0,1] y ninguna feature mirar después de la fecha de corte.
select persona_id from {{ ref('features_usuario') }}
where viajes_7d > viajes_30d or viajes_30d > viajes_90d or viajes_90d > viajes_total
   or prop_viajes_hora_pico < 0 or prop_viajes_hora_pico > 1
   or dias_desde_ultimo_viaje < 0
   or n_modos < 1 or n_modos > 4
