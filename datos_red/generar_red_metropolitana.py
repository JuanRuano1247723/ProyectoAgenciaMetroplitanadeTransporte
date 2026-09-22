"""
=====================================================================
  GENERADOR DE DATOS  ·  Red Metropolitana de Transporte
  Proyecto 1  ·  Ciencia de Datos  ·  IIS Sección 2
=====================================================================

  Simula los sistemas de datos de CUATRO operadores que hoy NO están
  integrados. Cada uno registra los viajes a su manera, con su propio
  formato, sus propias llaves y sus propias definiciones.

  Ese es el punto del proyecto: los cuatro archivos NO se pueden unir
  tal como están.

  Cifras calibradas con fuentes públicas (2026):
    Transmetro    8 líneas · ~300 vehículos · Q1.00 · frec. 5 min pico
    Transurbano  41 líneas · ~455 vehículos · 270,000 pasajeros/día
    MetroRiel    21 km · 22 estaciones · 41 min · frec. 3 min pico
    Aerometro     2 ejes · 450 cabinas · meta 374,000 personas/día

  Requisitos:  solo Python 3
  Ejecutar:    python generar_red_metropolitana.py
=====================================================================
"""
import csv, os, json, random, datetime, hashlib

# ---------------------------------------------------------------
DIAS          = 45          # días de operación a simular
N_USUARIOS    = 60_000      # usuarios distintos en toda la red
ESCALA        = 0.08        # fracción del volumen real (0.08 ≈ 8%)
                            # con 1.0 se generan ~2.5 GB: no lo hagan
                            # sin necesidad. Con 0.08 son ~200 MB.
SEMILLA       = 2026
CARPETA       = "datos_red"
# ---------------------------------------------------------------

random.seed(SEMILLA)
os.makedirs(CARPETA, exist_ok=True)
INICIO = datetime.date(2026, 6, 1)

ZONAS = ["Zona 1", "Zona 4", "Zona 6", "Zona 7", "Zona 8", "Zona 9",
         "Zona 10", "Zona 11", "Zona 12", "Zona 13", "Zona 17", "Zona 18",
         "Mixco", "Villa Nueva", "San Miguel Petapa", "Santa Catarina Pinula"]

# Catalogo de zonas con estaciones o paradas de algun operador.
ZONAS_SERVIDAS = ["Zona 1", "Zona 4", "Zona 6", "Zona 7", "Zona 8", "Zona 9",
                  "Zona 10", "Zona 11", "Zona 12", "Zona 13", "Zona 17", "Zona 18",
                  "Mixco", "Villa Nueva", "San Miguel Petapa"]

# =====================================================================
#  CATÁLOGOS  ·  cada operador nombra las cosas a su manera
# =====================================================================

# --- Transmetro: 8 líneas, estaciones con código propio -------------
tm_lineas = [("L1","Centra Sur - Centro"), ("L2","Eje Central"),
             ("L6","Atanasio Tzul"), ("L7","Calzada San Juan"),
             ("L12","Calzada Aguilar Batres"), ("L13","Villa Nueva"),
             ("L18","Bulevar El Naranjo"), ("R5","Ruta 5")]
tm_estaciones = []
for i, (lin, nom) in enumerate(tm_lineas):
    for k in range(1, 14):
        tm_estaciones.append({
            "estacion_id": f"TM-{lin}-{k:02d}",
            "nombre": f"Estación {nom} {k}",
            "linea": lin,
            "zona": ZONAS_SERVIDAS[(i*3+k) % len(ZONAS_SERVIDAS)],
            "lat": round(14.55 + random.uniform(-0.09, 0.09), 6),
            "lon": round(-90.55 + random.uniform(-0.09, 0.09), 6),
        })

# --- Transurbano: 41 rutas, nombra "paradas", no "estaciones" -------
tu_rutas = [f"R-{n:03d}" for n in range(101, 142)]
tu_paradas = []
for i, r in enumerate(tu_rutas):
    for k in range(1, 9):
        tu_paradas.append({
            "cod_parada": f"{r}-{k}",
            "descripcion": f"Parada {k} ruta {r}",
            "ruta": r,
            # OJO: Transurbano usa nombres de zona en MAYÚSCULAS y sin "Zona"
            "sector": ZONAS_SERVIDAS[(i*2+k) % len(ZONAS_SERVIDAS)].replace("Zona ", "Z").upper(),
        })

# --- MetroRiel: 22 estaciones, zonas 12-8-1-6-17 --------------------
mr_zonas = ["Zona 12", "Zona 8", "Zona 1", "Zona 6", "Zona 17"]
mr_estaciones = [{
    "id_estacion": k,
    "nombre_estacion": f"MR {k:02d}",
    "zona_nombre": mr_zonas[min(k * len(mr_zonas) // 23, len(mr_zonas)-1)],
    "km": round(k * 21 / 22, 2),
} for k in range(1, 23)]

# --- Aerometro: 2 ejes, cabinas -------------------------------------
am_estaciones = [{
    "station_code": f"AM{e}{k}",
    "station_name": f"Aerometro Eje {e} - Torre {k}",
    "axis": f"Eje {e}",
    "district": ("Mixco" if e == 1 else "Zona 7") if k <= 3 else ZONAS_SERVIDAS[(e*4+k) % len(ZONAS_SERVIDAS)],
} for e in (1, 2) for k in range(1, 8)]

# =====================================================================
#  USUARIOS  ·  el problema central: cada sistema tiene su propia llave
# =====================================================================
# Un mismo usuario puede tener tarjeta de varios sistemas. No hay
# ninguna tabla que las relacione: los grupos deben resolverlo.

usuarios = []
for i in range(1, N_USUARIOS + 1):
    perfil = random.choices(
        ["estudiante", "trabajador", "adulto_mayor", "general"],
        weights=[18, 55, 9, 18])[0]
    usuarios.append({
        "n": i,
        "perfil": perfil,
        "zona_origen": random.choice(ZONAS),
        # tarjetas: no todos tienen todas
        "tm": f"TC-{i:08d}"          if random.random() < 0.72 else None,
        "tu": f"{i:010d}"            if random.random() < 0.61 else None,
        "mr": f"MR{i:07d}"           if random.random() < 0.38 else None,
        "am": hashlib.md5(f"am{i}".encode()).hexdigest()[:12]
                                      if random.random() < 0.24 else None,
    })

def hora_del_dia():
    """Distribución realista: dos picos, mañana y tarde."""
    r = random.random()
    if r < 0.28:   h = random.choices([5,6,7,8], weights=[1,4,6,3])[0]
    elif r < 0.55: h = random.choices([16,17,18,19], weights=[3,6,5,2])[0]
    elif r < 0.80: h = random.randint(9, 15)
    else:          h = random.choice([4, 20, 21, 22])
    return h, random.randint(0, 59), random.randint(0, 59)

def escribir(nombre, encabezado, filas):
    ruta = os.path.join(CARPETA, nombre)
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(encabezado); w.writerows(filas)
    mb = os.path.getsize(ruta) / 1024 / 1024
    print(f"  {nombre:<34} {mb:8.2f} MB")

print("Generando la red metropolitana...\n")
print("CATÁLOGOS")

escribir("tm_estaciones.csv",
         ["estacion_id","nombre","linea","zona","lat","lon"],
         [[e["estacion_id"],e["nombre"],e["linea"],e["zona"],e["lat"],e["lon"]]
          for e in tm_estaciones])
escribir("tu_paradas.csv",
         ["cod_parada","descripcion","ruta","sector"],
         [[p["cod_parada"],p["descripcion"],p["ruta"],p["sector"]] for p in tu_paradas])
escribir("mr_estaciones.csv",
         ["id_estacion","nombre_estacion","zona_nombre","km"],
         [[e["id_estacion"],e["nombre_estacion"],e["zona_nombre"],e["km"]]
          for e in mr_estaciones])
escribir("am_estaciones.csv",
         ["station_code","station_name","axis","district"],
         [[e["station_code"],e["station_name"],e["axis"],e["district"]]
          for e in am_estaciones])

# =====================================================================
#  1 · TRANSMETRO  ·  CSV de validaciones, una fila por abordaje
# =====================================================================
print("\nOPERACIÓN")

u_tm = [u for u in usuarios if u["tm"]]
def gen_tm():
    vid = 0
    for d in range(DIAS):
        fecha = INICIO + datetime.timedelta(days=d)
        hab = fecha.weekday() < 5
        n = int(random.gauss(115_000*ESCALA if hab else 62_000*ESCALA, 6_000*ESCALA))
        for _ in range(n):
            vid += 1
            u = random.choice(u_tm)
            e = random.choice(tm_estaciones)
            h, m, s = hora_del_dia()
            ts = datetime.datetime.combine(fecha, datetime.time(h, m, s))
            tarifa = {"estudiante":0.50,"adulto_mayor":0.00}.get(u["perfil"], 1.00)
            fila = [vid, u["tm"], e["estacion_id"], e["linea"],
                    ts.strftime("%Y-%m-%d %H:%M:%S"), f"{tarifa:.2f}",
                    random.choice(["ENTRADA","ENTRADA","ENTRADA","TRANSBORDO"])]
            yield fila
            # el torniquete duplica ~0.3% de las lecturas
            if random.random() < 0.003:
                yield fila
escribir("transmetro_validaciones.csv",
         ["validacion_id","tarjeta","estacion_id","linea","fecha_hora","tarifa","tipo"],
         gen_tm())

# =====================================================================
#  2 · TRANSURBANO  ·  CSV con formato distinto: fecha y hora separadas,
#      monto en centavos, y estado del viaje como código numérico
# =====================================================================
u_tu = [u for u in usuarios if u["tu"]]
ESTADOS_TU = {1:"OK", 2:"OK", 3:"OK", 7:"SALDO_INSUF", 9:"TARJETA_INVALIDA"}
def gen_tu():
    for d in range(DIAS):
        fecha = INICIO + datetime.timedelta(days=d)
        hab = fecha.weekday() < 5
        n = int(random.gauss(268_000*ESCALA if hab else 141_000*ESCALA, 9_000*ESCALA))
        for i in range(n):
            u = random.choice(u_tu)
            p = random.choice(tu_paradas)
            h, m, s = hora_del_dia()
            est = random.choices(list(ESTADOS_TU), weights=[40,40,15,4,1])[0]
            # ~0.5% de las filas llega sin código de parada
            cod = "" if random.random() < 0.005 else p["cod_parada"]
            # ~0.1% trae fecha del futuro (reloj mal configurado en la unidad)
            fe = fecha + datetime.timedelta(days=400) if random.random() < 0.001 else fecha
            yield [f"{fe:%d/%m/%Y}", f"{h:02d}:{m:02d}:{s:02d}",
                   u["tu"], cod, p["ruta"],
                   130 if u["perfil"] != "estudiante" else 65,   # centavos
                   est]
escribir("transurbano_transacciones.csv",
         ["fecha","hora","num_tarjeta","cod_parada","ruta","monto_centavos","cod_estado"],
         gen_tu())

# =====================================================================
#  3 · METRORIEL  ·  JSON Lines, con ORIGEN Y DESTINO en la misma fila
#      (el único sistema que registra el trayecto completo)
# =====================================================================
u_mr = [u for u in usuarios if u["mr"]]
ruta_mr = os.path.join(CARPETA, "metroriel_viajes.jsonl")
with open(ruta_mr, "w", encoding="utf-8") as f:
    tid = 0
    for d in range(DIAS):
        fecha = INICIO + datetime.timedelta(days=d)
        hab = fecha.weekday() < 5
        n = int(random.gauss(96_000*ESCALA if hab else 48_000*ESCALA, 5_000*ESCALA))
        for _ in range(n):
            tid += 1
            u = random.choice(u_mr)
            o = random.choice(mr_estaciones)
            dd = random.choice([e for e in mr_estaciones if e["id_estacion"] != o["id_estacion"]])
            h, m, s = hora_del_dia()
            ent = datetime.datetime.combine(fecha, datetime.time(h, m, s))
            dur = int(abs(o["km"] - dd["km"]) * 2.4 * 60) + random.randint(60, 400)
            reg = {
                "trip_id": tid,
                "card": u["mr"],
                "entry": {"station": o["id_estacion"], "ts": ent.isoformat()},
                "exit":  {"station": dd["id_estacion"],
                          "ts": (ent + datetime.timedelta(seconds=dur)).isoformat()},
                "fare_gtq": 3.0 if u["perfil"] != "estudiante" else 1.5,
                "duration_s": dur,
            }
            # ~1.2% de los viajes no registra salida: el pasajero no validó
            if random.random() < 0.012:
                reg["exit"] = None
                reg["duration_s"] = None
            f.write(json.dumps(reg, ensure_ascii=False) + "\n")
print(f"  {'metroriel_viajes.jsonl':<34} {os.path.getsize(ruta_mr)/1024/1024:8.2f} MB")

# =====================================================================
#  4 · AEROMETRO  ·  CSV en INGLÉS, timestamp en UTC (no hora local),
#      y una fila por CABINA, no por pasajero
# =====================================================================
u_am = [u for u in usuarios if u["am"]]
def gen_am():
    bid = 0
    for d in range(DIAS):
        fecha = INICIO + datetime.timedelta(days=d)
        hab = fecha.weekday() < 5
        n = int(random.gauss(63_000*ESCALA if hab else 34_000*ESCALA, 4_000*ESCALA))
        for _ in range(n):
            bid += 1
            u = random.choice(u_am)
            e = random.choice(am_estaciones)
            h, m, s = hora_del_dia()
            local = datetime.datetime.combine(fecha, datetime.time(h, m, s))
            utc = local + datetime.timedelta(hours=6)     # Guatemala = UTC-6
            yield [bid, u["am"], e["station_code"], e["axis"],
                   utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                   random.randint(1, 10),                  # cabin_number
                   "3.50"]
escribir("aerometro_boardings.csv",
         ["boarding_id","user_hash","station_code","axis",
          "timestamp_utc","cabin_number","fare"],
         gen_am())

# =====================================================================
#  5 · CDC  ·  cambios en el padrón de usuarios, capturados del log
# =====================================================================
# Este archivo NO es una foto del padrón: es el registro de CAMBIOS.
# Incluye inserciones, actualizaciones y BORRADOS (tombstones).
# Un borrado llega con la llave y sin cuerpo.
#
# AMBIGÜEDAD INTENCIONAL -- no "corregir" sin decidir a propósito:
# el campo "tarjeta" toma la primera tarjeta que el usuario tenga entre
# tm/tu/mr (prioridad en ese orden), no una sola marca fija. Esto mezcla
# formatos de tres operadores dentro de la misma columna del padrón.
# Es deliberado: los grupos deben decidir y JUSTIFICAR de quién es este
# padrón -- de un solo operador, o un registro central ambiguo -- como
# parte de "resolver la identidad del usuario" (seccion 1.3). No hay
# una respuesta correcta unica; se evalua el criterio, no el acierto.

print()
cdc = []
seq = 0
for d in range(DIAS):
    fecha = INICIO + datetime.timedelta(days=d)
    # altas del día
    for _ in range(int(N_USUARIOS * 0.004)):
        seq += 1
        u = random.choice(usuarios)
        cdc.append([seq, f"{fecha}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00",
                    "INSERT", u["tm"] or u["tu"] or u["mr"] or "SIN-TARJETA",
                    u["perfil"], u["zona_origen"], "ACTIVA"])
    # cambios de perfil o de zona
    for _ in range(int(N_USUARIOS * 0.006)):
        seq += 1
        u = random.choice(usuarios)
        cdc.append([seq, f"{fecha}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00",
                    "UPDATE", u["tm"] or u["tu"] or u["mr"] or "SIN-TARJETA",
                    random.choice(["estudiante","trabajador","adulto_mayor","general"]),
                    random.choice(ZONAS), "ACTIVA"])
    # bajas: tarjeta cancelada. Llega SIN cuerpo.
    for _ in range(int(N_USUARIOS * 0.0015)):
        seq += 1
        u = random.choice(usuarios)
        cdc.append([seq, f"{fecha}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00",
                    "DELETE", u["tm"] or u["tu"] or u["mr"] or "SIN-TARJETA",
                    "", "", ""])
print("CDC · padrón de usuarios")
escribir("cdc_padron_usuarios.csv",
         ["seq","commit_ts","op","tarjeta","perfil","zona_residencia","estado"],
         cdc)

print(f"""
Listo. Los archivos están en ./{CARPETA}/

  Los cuatro operadores NO comparten llaves de usuario, ni nombres de
  zona, ni formato de fecha, ni unidad monetaria, ni siquiera el
  significado de una fila.

  Y el archivo de CDC trae INSERT, UPDATE y DELETE. Los borrados
  llegan con la llave y sin cuerpo: si los ignoran, esas tarjetas
  seguirán activas en su warehouse para siempre.

  Ese es el problema que deben resolver.
""")
