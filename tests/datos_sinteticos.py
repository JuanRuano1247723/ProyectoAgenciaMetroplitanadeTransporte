"""Genera archivos de prueba con los mismos encabezados y anomalías observadas en los datos reales.

No sustituye a generar_red_metropolitana.py: solo sirve para probar la capa Bronze sin depender
del volumen real.
"""
from __future__ import annotations

import random
from pathlib import Path


def generar(d: Path, seed: int = 7, n_tm=2000, n_tu=3000, n_am=1500, n_mr=1200, n_cdc=600) -> dict:
    rnd = random.Random(seed)
    d.mkdir(parents=True, exist_ok=True)

    # ---- catálogos: mismas formas que los reales (Zona N / ZN / municipios)
    tm = [f"TM-L{l}-{i:02d},Estación L{l} {i},L{l},Zona {i + 3},14.5{i},-90.5{i}" for l in range(1, 4) for i in range(1, 10)]
    (d / "tm_estaciones.csv").write_text("estacion_id,nombre,linea,zona,lat,lon\n" + "\n".join(tm) + "\n")
    tu = [f"R-{r}-{i},Parada {i} ruta R-{r},R-{r},Z{i + 3}" for r in range(101, 121) for i in range(1, 9)]
    (d / "tu_paradas.csv").write_text("cod_parada,descripcion,ruta,sector\n" + "\n".join(tu) + "\n")
    mr = [f"{i},MR {i:02d},{'Zona 12' if i < 12 else 'Zona 8'},{i * 0.95:.2f}" for i in range(1, 23)]
    (d / "mr_estaciones.csv").write_text("id_estacion,nombre_estacion,zona_nombre,km\n" + "\n".join(mr) + "\n")
    distritos = ["Mixco", "Zona 12", "Villa Nueva", "Zona 13", "San Miguel Petapa", "Zona 7", "Zona 30"]   # 'Zona 30' no existe en dim_zona
    am = [f"AM1{i},Aerometro Eje 1 - Torre {i},Eje 1,{distritos[i - 1]}" for i in range(1, 8)]
    (d / "am_estaciones.csv").write_text("station_code,station_name,axis,district\n" + "\n".join(am) + "\n")

    # ---- transmetro (streaming): 30 filas duplicadas exactas al final
    filas = [f"{i},TC-{rnd.randint(1, 900):08d},TM-L{rnd.randint(1, 3)}-0{rnd.randint(1, 9)},L1,"
             f"2026-06-0{rnd.randint(1, 5)} {rnd.randint(4, 22):02d}:{rnd.randint(0, 59):02d}:{rnd.randint(0, 59):02d},"
             f"{rnd.choice(['1.00', '0.50', '0.00'])},{rnd.choice(['ENTRADA', 'TRANSBORDO'])}"
             for i in range(1, n_tm + 1)]
    filas += filas[:30]
    (d / "transmetro_validaciones.csv").write_text(
        "validacion_id,tarjeta,estacion_id,linea,fecha_hora,tarifa,tipo\n" + "\n".join(filas) + "\n")

    # ---- transurbano (batch): fechas DD/MM/YYYY, 25 filas en 2027, cod_parada vacío, 2 líneas rotas
    filas = []
    for i in range(n_tu):
        cod = "" if i % 75 == 0 else f"R-{rnd.randint(101, 120)}-{rnd.randint(1, 8)}"
        filas.append(f"0{rnd.randint(1, 5)}/06/2026,{rnd.randint(4, 22):02d}:{rnd.randint(0, 59):02d}:{rnd.randint(0, 59):02d},"
                     f"{rnd.randint(1, 900):010d},{cod},R-{rnd.randint(101, 120)},{rnd.choice([65, 130])},{rnd.randint(1, 3)}")
    filas += [f"{rnd.randint(14, 30)}/07/2027,10:00:00,{rnd.randint(1, 900):010d},R-101-1,R-101,130,1" for _ in range(25)]
    filas.insert(100, "x,y")
    filas.insert(200, "")
    (d / "transurbano_transacciones.csv").write_text(
        "fecha,hora,num_tarjeta,cod_parada,ruta,monto_centavos,cod_estado\n" + "\n".join(filas) + "\n")

    # ---- aerometro (streaming): UTC, cruza medianoche
    filas = [f"{i},{rnd.randint(0, 16 ** 12 - 1):012x},AM1{rnd.randint(1, 7)},Eje 1,"
             f"2026-06-0{rnd.randint(1, 4)}T{rnd.choice([10, 15, 22, 23, 0, 2, 4]):02d}:{rnd.randint(0, 59):02d}:{rnd.randint(0, 59):02d}Z,"
             f"{rnd.randint(1, 10)},3.50" for i in range(1, n_am + 1)]
    (d / "aerometro_boardings.csv").write_text(
        "boarding_id,user_hash,station_code,axis,timestamp_utc,cabin_number,fare\n" + "\n".join(filas) + "\n")

    # ---- metroriel (batch): fare con formato exacto (12.50), exit nulo, línea rota, línea vacía, campo nuevo
    lineas = []
    for i in range(1, n_mr + 1):
        fare = "12.50" if i % 10 == 0 else "3.0"
        extra = ', "extra_field": 1' if i == 5 else ""
        dia = rnd.randint(1, 5)
        card = f"MR{rnd.randint(1, 900):07d}"
        entrada = f'"entry": {{"station": 12, "ts": "2026-06-0{dia}T19:11:11"}}'
        if i % 25 == 0:
            lineas.append(f'{{"trip_id": {i}, "card": "{card}", {entrada}, "exit": null, '
                          f'"fare_gtq": {fare}, "duration_s": null{extra}}}')
        else:
            lineas.append(f'{{"trip_id": {i}, "card": "{card}", {entrada}, '
                          f'"exit": {{"station": 15, "ts": "2026-06-0{dia}T19:23:31"}}, "fare_gtq": {fare}, "duration_s": 740{extra}}}')
    lineas.insert(50, '{"trip_id": 9999, "card":')
    lineas.insert(60, "")
    (d / "metroriel_viajes.jsonl").write_text("\n".join(lineas) + "\n")

    # ---- cdc: SIN-TARJETA, llaves con formato MR, DELETE sin cuerpo, commit_ts desordenado
    filas, seq = [], 0
    for _ in range(n_cdc):
        seq += 1
        op = rnd.choices(["INSERT", "UPDATE", "DELETE"], [35, 50, 15])[0]
        llave = rnd.choices([f"TC-{rnd.randint(1, 300):08d}", "SIN-TARJETA", f"MR{rnd.randint(1, 300):07d}"], [90, 6, 4])[0]
        ts = f"2026-06-0{rnd.randint(1, 5)}T{rnd.randint(0, 23):02d}:{rnd.randint(0, 59):02d}:00"
        cuerpo = ",,," if op == "DELETE" else f",{rnd.choice(['general', 'estudiante'])},Zona {rnd.randint(1, 18)},ACTIVA"
        filas.append(f"{seq},{ts},{op},{llave}{cuerpo}".replace(",,,,", ",,,") if op == "DELETE" else f"{seq},{ts},{op},{llave}{cuerpo[0:0]}{cuerpo}")
    # normaliza: 7 columnas siempre
    norm = []
    for f in filas:
        campos = f.split(",")
        if campos[2] == "DELETE":
            campos = campos[:4] + ["", "", ""]
        norm.append(",".join(campos[:7]))
    (d / "cdc_padron_usuarios.csv").write_text("seq,commit_ts,op,tarjeta,perfil,zona_residencia,estado\n" + "\n".join(norm) + "\n")

    return {
        "transmetro_validaciones": {"filas": n_tm + 30, "malformadas": 0},
        "aerometro_boardings": {"filas": n_am, "malformadas": 0},
        "transurbano_transacciones": {"filas": n_tu + 25, "malformadas": 2},
        "metroriel_viajes": {"filas": n_mr, "malformadas": 2},
        "cdc_padron_usuarios": {"filas": n_cdc, "malformadas": 0},
        "tm_estaciones": {"filas": 27, "malformadas": 0}, "tu_paradas": {"filas": 160, "malformadas": 0},
        "mr_estaciones": {"filas": 22, "malformadas": 0}, "am_estaciones": {"filas": 7, "malformadas": 0},
    }
